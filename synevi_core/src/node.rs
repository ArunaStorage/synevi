use crate::coordinator::Coordinator;
use crate::replica::ReplicaConfig;
use crate::wait_handler::{CheckResult, WaitHandler};
use std::fmt::Debug;
use std::sync::atomic::Ordering;
use std::sync::RwLock;
use std::sync::{atomic::AtomicU64, Arc};
use synevi_network::consensus_transport::{ApplyRequest, CommitRequest};
use synevi_network::network::{Network, NetworkInterface};
use synevi_network::replica::Replica;
use synevi_persistence::mem_store::MemStore;
use synevi_types::traits::Store;
use synevi_types::types::{
    ExecutorResult, InternalSyneviResult, SyneviResult, TransactionPayload, UpsertEvent,
};
use synevi_types::{Executor, State, SyneviError, T};
use tracing::instrument;
use ulid::Ulid;

#[derive(Debug, Default)]
pub struct Stats {
    pub total_requests: AtomicU64,
    pub total_accepts: AtomicU64,
    pub total_recovers: AtomicU64,
}

pub struct Node<N, E, S = MemStore>
where
    N: Network + Send + Sync,
    E: Executor + Send + Sync,
    S: Store + Send + Sync,
{
    pub network: N,
    pub executor: E,
    pub event_store: Arc<S>,
    pub stats: Stats,
    pub wait_handler: WaitHandler<S>,
    semaphore: Arc<tokio::sync::Semaphore>,
    self_clone: RwLock<Option<Arc<Self>>>,
}

impl<N, E> Node<N, E, MemStore>
where
    N: Network,
    E: Executor,
{
    #[instrument(level = "trace", skip(network, executor))]
    pub async fn new_with_network_and_executor(
        id: Ulid,
        serial: u16,
        network: N,
        executor: E,
    ) -> Result<Arc<Self>, SyneviError> {
        let store = MemStore::new(serial)?;
        Self::new(id, serial, network, executor, store).await
    }
}

impl<N, E, S> Node<N, E, S>
where
    N: Network,
    E: Executor,
    S: Store,
{
    #[instrument(level = "trace", skip(network, executor, store))]
    pub async fn new(
        id: Ulid,
        serial: u16,
        network: N,
        executor: E,
        store: S,
    ) -> Result<Arc<Self>, SyneviError> {
        let stats = Stats {
            total_requests: AtomicU64::new(0),
            total_accepts: AtomicU64::new(0),
            total_recovers: AtomicU64::new(0),
        };

        //let reorder_buffer = ReorderBuffer::new(event_store.clone());
        // let reorder_clone = reorder_buffer.clone();
        // tokio::spawn(async move {
        //     reorder_clone.run().await.unwrap();
        // });

        let arc_store = Arc::new(store);

        let wait_handler = WaitHandler::new(arc_store.clone());

        let node = Arc::new(Node {
            event_store: arc_store,
            wait_handler,
            network,
            stats,
            semaphore: Arc::new(tokio::sync::Semaphore::new(10)),
            executor,
            self_clone: RwLock::new(None),
        });
        node.self_clone
            .write()
            .expect("Locking self_clone failed")
            .replace(node.clone());

        node.set_ready();

        let replica = ReplicaConfig::new(node.clone());
        node.network.spawn_server(replica).await?;

        let node_clone = node.clone();
        tokio::spawn(async move { node_clone.run_check_recovery().await });

        // If no config / persistence -> default
        Ok(node)
    }

    pub fn set_ready(&self) -> () {
        self.network
            .get_node_status()
            .info
            .ready
            .store(true, Ordering::Relaxed);
    }

    pub fn is_ready(&self) -> bool {
        self.network
            .get_node_status()
            .info
            .ready
            .load(Ordering::Relaxed)
    }

    pub fn has_members(&self) -> bool {
        self.network
            .get_node_status()
            .has_members
            .load(Ordering::Relaxed)
    }

    pub fn get_serial(&self) -> u16 {
        self.network.get_node_status().info.serial
    }

    pub fn get_ulid(&self) -> Ulid {
        self.network.get_node_status().info.id
    }

    #[instrument(level = "trace", skip(network, executor, store))]
    pub async fn new_with_member(
        id: Ulid,
        serial: u16,
        network: N,
        executor: E,
        store: S,
        member_host: String,
    ) -> Result<Arc<Self>, SyneviError> {
        let stats = Stats {
            total_requests: AtomicU64::new(0),
            total_accepts: AtomicU64::new(0),
            total_recovers: AtomicU64::new(0),
        };

        let arc_store = Arc::new(store);
        let wait_handler = WaitHandler::new(arc_store.clone());

        let node = Arc::new(Node {
            event_store: arc_store,
            network,
            wait_handler,
            stats,
            semaphore: Arc::new(tokio::sync::Semaphore::new(10)),
            executor,
            self_clone: RwLock::new(None),
        });

        node.self_clone
            .write()
            .expect("Locking self_clone failed")
            .replace(node.clone());

        let replica = ReplicaConfig::new(node.clone());
        node.network.spawn_server(replica.clone()).await?;
        let node_clone = node.clone();
        tokio::spawn(async move { node_clone.run_check_recovery().await });
        node.reconfigure(replica, member_host).await?;

        Ok(node)
    }

    #[instrument(level = "trace", skip(self))]
    pub async fn add_member(
        &self,
        id: Ulid,
        serial: u16,
        host: String,
        ready: bool,
    ) -> Result<(), SyneviError> {
        self.network.add_member(id, serial, host, ready).await?;
        Ok(())
    }

    #[instrument(level = "trace", skip(self))]
    pub async fn ready_member(&self, id: Ulid, serial: u16) -> Result<(), SyneviError> {
        self.network.ready_member(id, serial).await?;
        Ok(())
    }

    #[instrument(level = "trace", skip(self, transaction))]
    pub async fn transaction(self: Arc<Self>, id: u128, transaction: E::Tx) -> SyneviResult<E> {
        if !self.has_members() {
            tracing::warn!("Consensus omitted: No members in the network");
        } else if !self.is_ready() {
            return Err(SyneviError::NotReady);
        };
        let _permit = self.semaphore.acquire().await?;
        let mut coordinator =
            Coordinator::new(self.clone(), TransactionPayload::External(transaction), id).await;
        let tx_result = coordinator.run().await?;

        match tx_result {
            ExecutorResult::External(e) => Ok(e),
            ExecutorResult::Internal(e) => {
                Err(SyneviError::InternalTransaction(format!("{:?}", e)))
            }
        }
    }

    pub(super) async fn internal_transaction(
        self: Arc<Self>,
        id: u128,
        transaction: TransactionPayload<E::Tx>,
    ) -> InternalSyneviResult<E> {
        if !self.is_ready() {
            return Err(SyneviError::NotReady);
        };
        let _permit = self.semaphore.acquire().await?;
        let mut coordinator = Coordinator::new(self.clone(), transaction, id).await;
        coordinator.run().await
    }

    pub fn get_stats(&self) -> (u64, u64, u64) {
        (
            self.stats
                .total_requests
                .load(std::sync::atomic::Ordering::Relaxed),
            self.stats
                .total_accepts
                .load(std::sync::atomic::Ordering::Relaxed),
            self.stats
                .total_recovers
                .load(std::sync::atomic::Ordering::Relaxed),
        )
    }

    #[instrument(level = "trace", skip(self))]
    pub async fn commit(&self, event: UpsertEvent) -> Result<(), SyneviError> {
        let t0_commit = event.t_zero.clone();
        let t_commit = event.t.clone();

        let prev_event = self.event_store.get_event(t0_commit)?;
        self.event_store.upsert_tx(event)?;
        self.wait_handler.notify_commit(&t0_commit, &t_commit);

        if !prev_event.is_some_and(|e| e.state > State::Commited && e.dependencies.is_empty()) {
            if let Some(waiter) = self.wait_handler.get_waiter(&t0_commit) {
                waiter.await.map_err(|e| {
                    tracing::error!("Error waiting for commit: {:?}", e);
                    SyneviError::ReceiveError(format!("Error waiting for commit"))
                })?
            };
        }
        Ok(())
    }

    #[instrument(level = "trace", skip(self))]
    pub async fn apply(&self, event: UpsertEvent) -> Result<(), SyneviError> {
        let t0_apply = event.t_zero.clone();

        let prev_event = self.event_store.get_event(t0_apply)?;

        let needs_wait = if let Some(prev_event) = prev_event {
            prev_event.state < State::Applied
        } else {
            let mut commit_event = event.clone();
            commit_event.state = State::Commited;
            self.commit(commit_event).await?;
            true
        };
        if event
            .dependencies
            .as_ref()
            .is_some_and(|deps| !deps.is_empty())
            && needs_wait
        {
            if let Some(waiter) = self.wait_handler.get_waiter(&t0_apply) {
                waiter.await.map_err(|e| {
                    tracing::error!("Error waiting for commit: {:?}", e);
                    SyneviError::ReceiveError(format!("Error waiting for commit"))
                })?;
            }
        }
        println!(
            "[{:?}]Applied event: t0: {:?}, t: {:?}",
            self.get_serial(),
            event.t_zero,
            event.t
        );

        self.event_store.upsert_tx(event)?;
        self.wait_handler.notify_apply(&t0_apply);
        Ok(())
    }

    async fn run_check_recovery(&self) {
        let self_clonable = self
            .self_clone
            .read()
            .expect("Locking self_clone failed")
            .clone()
            .expect("Self clone is None");

        loop {
            match self.wait_handler.check_recovery() {
                CheckResult::NoRecovery => (),
                CheckResult::RecoverEvent(recover_event) => {
                    let self_clone = self_clonable.clone();
                    match tokio::spawn(Coordinator::recover(self_clone, recover_event)).await {
                        Ok(Ok(_)) => (),
                        Ok(Err(e)) => {
                            tracing::error!("Error recovering event: {:?}", e);
                        }
                        Err(e) => {
                            tracing::error!("JoinError recovering event: {:?}", e);
                        }
                    }
                }
                CheckResult::RecoverUnknown(t0_recover) => {
                    let interface = self.network.get_interface().await;
                    match interface.broadcast_recovery(t0_recover).await {
                        Ok(true) => (),
                        Ok(false) => {
                            println!("Unknown recovery failed");
                            self.wait_handler.notify_apply(&t0_recover);
                        }
                        Err(err) => {
                            tracing::error!("Error broadcasting recovery: {:?}", err);
                            panic!("Error broadcasting recovery: {:?}", err);
                        }
                    }
                }
            }

            tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
        }
    }

    #[instrument(level = "trace", skip(self, replica))]
    async fn reconfigure(
        &self,
        replica: ReplicaConfig<N, E, S>,
        member_host: String,
    ) -> Result<(), SyneviError> {
        // 1. Broadcast self_config to other member
        let expected = self.network.join_electorate(member_host).await?;

        // 2. wait for JoinElectorate responses with expected majority and config from others

        while self
            .network
            .get_node_status()
            .members_responded
            .load(Ordering::Relaxed)
            < (expected / 2) + 1
        {
            tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
        }

        let (last_applied, _) = self.event_store.last_applied();
        self.sync_events(last_applied, &replica).await?;

        // 3. Send ReadyJoinElectorate && set myself to ready
        self.set_ready();
        self.network.ready_electorate().await?;
        Ok(())
    }

    async fn sync_events(
        &self,
        last_applied: T,
        replica: &ReplicaConfig<N, E, S>,
    ) -> Result<(), SyneviError> {
        // 2.2 else Request stream with events until last_applied (highest t of JoinElectorate)
        let mut rcv = self
            .network
            .get_stream_events(last_applied.into())
            .await?;
        while let Some(event) = rcv.recv().await {
            let state: State = event.state.into();
            match state {
                State::Applied => {
                    replica
                        .apply(ApplyRequest {
                            id: event.id,
                            event: event.transaction,
                            timestamp_zero: event.t_zero,
                            timestamp: event.t,
                            dependencies: event.dependencies,
                            execution_hash: event.execution_hash,
                            transaction_hash: event.transaction_hash,
                        })
                        .await?;
                }
                State::Commited => {
                    replica
                        .commit(CommitRequest {
                            id: event.id,
                            event: event.transaction,
                            timestamp_zero: event.t_zero,
                            timestamp: event.t,
                            dependencies: event.dependencies,
                        })
                        .await?;
                }
                _ => (),
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::coordinator::Coordinator;
    use crate::{node::Node, tests::DummyExecutor};
    use std::collections::BTreeMap;
    use std::net::SocketAddr;
    use std::str::FromStr;
    use std::sync::Arc;
    use synevi_network::network::GrpcNetwork;
    use synevi_network::network::Network;
    use synevi_types::traits::Store;
    use synevi_types::{Executor, State, SyneviError, T, T0};
    use ulid::Ulid;

    impl<N, E> Node<N, E>
    where
        N: Network,
        E: Executor<Tx = Vec<u8>>,
    {
        pub async fn failing_transaction(
            self: Arc<Self>,
            id: u128,
            transaction: Vec<u8>,
        ) -> Result<(), SyneviError> {
            let _permit = self.semaphore.acquire().await?;
            let mut coordinator = Coordinator::new(
                self.clone(),
                synevi_types::types::TransactionPayload::External(transaction),
                id,
            )
            .await;
            coordinator.failing_pre_accept().await?;
            Ok(())
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn recovery_single() {
        let node_names: Vec<_> = (0..5).map(|_| Ulid::new()).collect();
        let mut nodes: Vec<Arc<Node<GrpcNetwork, DummyExecutor>>> = vec![];

        for (i, m) in node_names.iter().enumerate() {
            let socket_addr = SocketAddr::from_str(&format!("0.0.0.0:{}", 13200 + i)).unwrap();
            let network = synevi_network::network::GrpcNetwork::new(
                socket_addr,
                format!("http://localhost:{}", 13200 + i),
                *m,
                i as u16,
            );
            let node = Node::new_with_network_and_executor(*m, i as u16, network, DummyExecutor)
                .await
                .unwrap();
            nodes.push(node);
        }
        for (i, name) in node_names.iter().enumerate() {
            for (i2, node) in nodes.iter_mut().enumerate() {
                if i != i2 {
                    node.add_member(
                        *name,
                        i as u16,
                        format!("http://0.0.0.0:{}", 13200 + i),
                        true,
                    )
                    .await
                    .unwrap();
                }
            }
        }
        let coordinator = nodes.pop().unwrap();
        coordinator
            .clone()
            .failing_transaction(1, b"failing".to_vec())
            .await
            .unwrap();

        let _result = coordinator
            .clone()
            .transaction(2, Vec::from("F"))
            .await
            .unwrap();

        let coord = coordinator.event_store.get_event_store();
        for node in nodes {
            assert_eq!(
                node.event_store.get_event_store(),
                coord,
                "Node: {:?}",
                node.get_serial()
            );
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn recovery_random_test() {
        let node_names: Vec<_> = (0..5).map(|_| Ulid::new()).collect();
        let mut nodes: Vec<Arc<Node<GrpcNetwork, DummyExecutor>>> = vec![];

        for (i, m) in node_names.iter().enumerate() {
            let socket_addr = SocketAddr::from_str(&format!("0.0.0.0:{}", 13100 + i)).unwrap();
            let network = synevi_network::network::GrpcNetwork::new(
                socket_addr,
                format!("http://localhost:{}", 13100 + i),
                *m,
                i as u16,
            );
            let node = Node::new_with_network_and_executor(*m, i as u16, network, DummyExecutor)
                .await
                .unwrap();
            nodes.push(node);
        }
        for (i, name) in node_names.iter().enumerate() {
            for (i2, node) in nodes.iter_mut().enumerate() {
                if i != i2 {
                    node.add_member(
                        *name,
                        i as u16,
                        format!("http://0.0.0.0:{}", 13100 + i),
                        true,
                    )
                    .await
                    .unwrap();
                }
            }
        }
        let coordinator = nodes.pop().unwrap();

        // Working coordinator
        for _ in 0..10 {
            // Working coordinator
            coordinator
                .clone()
                .failing_transaction(1, b"failing".to_vec())
                .await
                .unwrap();
        }
        coordinator
            .clone()
            .transaction(0, Vec::from("last transaction"))
            .await
            .unwrap()
            .unwrap();

        let coordinator_store: BTreeMap<T0, T> = coordinator
            .event_store
            .get_event_store()
            .into_values()
            .map(|e| (e.t_zero, e.t))
            .collect();
        assert!(coordinator
            .event_store
            .get_event_store()
            .iter()
            .all(|(_, e)| e.state == State::Applied));

        let mut got_mismatch = false;
        for node in nodes {
            let node_store: BTreeMap<T0, T> = node
                .event_store
                .get_event_store()
                .into_values()
                .map(|e| (e.t_zero, e.t))
                .collect();
            assert!(node
                .event_store
                .get_event_store()
                .clone()
                .iter()
                .all(|(_, e)| e.state == State::Applied));
            assert_eq!(coordinator_store.len(), node_store.len());
            if coordinator_store != node_store {
                println!("Node: {:?}", node.get_serial());
                let mut node_store_iter = node_store.iter();
                for (k, v) in coordinator_store.iter() {
                    if let Some(next) = node_store_iter.next() {
                        if next != (k, v) {
                            println!("Diff: Got {:?}, Expected: {:?}", next, (k, v));
                            println!("Nanos: {:?} | {:?}", next.1 .0.get_nanos(), v.0.get_nanos());
                        }
                    }
                }
                got_mismatch = true;
            }

            assert!(!got_mismatch);
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn single_node_consensus() {
        let id = Ulid::new();
        let node = Node::new_with_network_and_executor(
            id,
            0,
            synevi_network::network::GrpcNetwork::new(
                SocketAddr::from_str("0.0.0.0:1337").unwrap(),
                format!("http://localhost:1337"),
                id,
                0,
            ),
            DummyExecutor,
        )
        .await
        .unwrap();

        let result = node.transaction(0, vec![127u8]).await.unwrap().unwrap();

        assert_eq!(result, vec![127u8]);
    }

    // #[tokio::test(flavor = "multi_thread")]
    // async fn reconfiguration() {
    //     todo!()
    // }
}
