use crate::coordinator::Coordinator;
use crate::replica::ReplicaConfig;
use crate::utils::from_dependency;
use crate::wait_handler::WaitHandler;
use std::fmt::Debug;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{atomic::AtomicU64, Arc};
use synevi_network::consensus_transport::{ApplyRequest, CommitRequest};
use synevi_network::network::{Network, NodeInfo};
use synevi_network::reconfiguration::{BufferedMessage, Report};
use synevi_network::replica::Replica;
use synevi_persistence::mem_store::MemStore;
use synevi_types::traits::Store;
use synevi_types::types::{SyneviResult, TransactionPayload, UpsertEvent};
use synevi_types::{Executor, State, SyneviError, T, T0};
use tokio::sync::mpsc::Receiver;
use tokio::sync::RwLock;
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
    pub info: NodeInfo,
    pub network: N,
    pub executor: E,
    pub event_store: Arc<S>,
    pub stats: Stats,
    pub wait_handler: RwLock<Option<Arc<WaitHandler<N, E, S>>>>,
    semaphore: Arc<tokio::sync::Semaphore>,
    has_members: AtomicBool,
    is_ready: Arc<AtomicBool>,
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
        let node_name = NodeInfo { id, serial };

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

        let node = Arc::new(Node {
            info: node_name,
            event_store: Arc::new(store),
            network,
            stats,
            semaphore: Arc::new(tokio::sync::Semaphore::new(10)),
            executor,
            wait_handler: RwLock::new(None),
            has_members: AtomicBool::new(false),
            is_ready: Arc::new(AtomicBool::new(true)),
        });

        let wait_handler = WaitHandler::new(node.clone());
        let wait_handler_clone = wait_handler.clone();
        tokio::spawn(async move {
            wait_handler_clone.run().await.unwrap();
        });
        *node.wait_handler.write().await = Some(wait_handler);

        let ready = Arc::new(AtomicBool::new(true));
        let (replica, _) = ReplicaConfig::new(node.clone(), ready);
        node.network.spawn_server(replica).await?;

        // If no config / persistence -> default
        Ok(node)
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
        let node_name = NodeInfo { id, serial };

        let stats = Stats {
            total_requests: AtomicU64::new(0),
            total_accepts: AtomicU64::new(0),
            total_recovers: AtomicU64::new(0),
        };

        let ready = Arc::new(AtomicBool::new(false));
        let node = Arc::new(Node {
            info: node_name,
            event_store: Arc::new(store),
            network,
            stats,
            semaphore: Arc::new(tokio::sync::Semaphore::new(10)),
            executor,
            wait_handler: RwLock::new(None),
            has_members: AtomicBool::new(false),
            is_ready: ready.clone(),
        });

        let wait_handler = WaitHandler::new(node.clone());
        let wait_handler_clone = wait_handler.clone();
        tokio::spawn(async move {
            wait_handler_clone.run().await.unwrap();
        });
        *node.wait_handler.write().await = Some(wait_handler);

        let (replica, config_receiver) = ReplicaConfig::new(node.clone(), ready.clone());
        node.network.spawn_server(replica.clone()).await?;
        node.reconfigure(replica, member_host, config_receiver, ready)
            .await?;

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
        self.has_members
            .store(true, std::sync::atomic::Ordering::Relaxed);
        Ok(())
    }

    #[instrument(level = "trace", skip(self))]
    pub async fn ready_member(&self, id: Ulid, serial: u16) -> Result<(), SyneviError> {
        self.network.ready_member(id, serial).await?;
        Ok(())
    }

    #[instrument(level = "trace", skip(self, transaction))]
    pub async fn transaction(
        self: Arc<Self>,
        id: u128,
        transaction: TransactionPayload<E::Tx>,
    ) -> SyneviResult<E> {
        if !self.has_members.load(std::sync::atomic::Ordering::Relaxed) {
            tracing::warn!("Consensus omitted: No members in the network");
        } else if !self.is_ready.load(Ordering::Relaxed) {
            return Err(SyneviError::NotReady);
        };
        let _permit = self.semaphore.acquire().await?;
        let mut coordinator = Coordinator::new(self.clone(), transaction, id).await;
        coordinator.run().await
    }

    pub async fn get_wait_handler(&self) -> Result<Arc<WaitHandler<N, E, S>>, SyneviError> {
        let lock = self.wait_handler.read().await;
        let handler = lock
            .as_ref()
            .ok_or_else(|| SyneviError::MissingWaitHandler)?
            .clone();
        Ok(handler)
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

    pub fn get_info(&self) -> NodeInfo {
        self.info.clone()
    }

    #[instrument(level = "trace", skip(self, replica))]
    async fn reconfigure(
        &self,
        replica: ReplicaConfig<N, E, S>,
        member_host: String,
        config_receiver: Receiver<Report>,
        ready: Arc<AtomicBool>,
    ) -> Result<(), SyneviError> {
        // 1. Broadcast self_config to other member
        dbg!("Broadcast");
        let all_members = self.network.broadcast_config(member_host).await?;

        // 2. wait for JoinElectorate responses with expected majority and config from others
        dbg!("Join");
        self.join_electorate(config_receiver, all_members, &replica)
            .await?;

        // 3. Send ReadyJoinElectorate && set myself to ready
        dbg!("Ready");
        ready.store(true, Ordering::Relaxed);
        self.network.ready_electorate().await?;
        Ok(())
    }

    async fn join_electorate(
        &self,
        mut receiver: Receiver<Report>,
        all_members: u32,
        replica: &ReplicaConfig<N, E, S>,
    ) -> Result<(), SyneviError> {
        let mut member_count = 0;
        let mut highest_applied = T::default();
        let mut last_applied: [u8; 32] = [0; 32];
        while let Some(report) = receiver.recv().await {
            self.add_member(report.node_id, report.node_serial, report.node_host, true)
                .await?;
            if report.last_applied > highest_applied {
                highest_applied = report.last_applied;
                last_applied = report.last_applied_hash;
            }
            member_count += 1;
            if member_count >= all_members {
                break;
            }
        }

        dbg!("Sync");
        // 2.1 if majority replies with 0 events -> skip to 2.4.
        self.sync_events(highest_applied, last_applied, &replica)
            .await?;

        // 2.4 Apply buffered commits & applies
        dbg!("Apply buffered");
        let mut rcv = replica.send_buffered().await?;
        while let Some((t, request)) = rcv
            .recv()
            .await
            .ok_or_else(|| SyneviError::ReceiveError("Channel closed".to_string()))?
        {
            dbg!(t);
            match request {
                BufferedMessage::Commit(req) => {
                    replica.commit(req, true).await?;
                }
                BufferedMessage::Apply(req) => {
                    replica.apply(req, true).await?;
                }
            }
        }
        Ok(())
    }

    async fn sync_events(
        &self,
        highest_applied: T,
        last_applied: [u8; 32],
        replica: &ReplicaConfig<N, E, S>,
    ) -> Result<(), SyneviError> {
        let mut last_applied_t_zero = T0::default();
        if highest_applied != T::default() {
            // 2.2 else Request stream with events until last_applied (highest t of JoinElectorate)
            let mut rcv = self.network.get_stream_events(highest_applied).await?;
            //let mut join_set = JoinSet::new();
            while let Some(event) = rcv.recv().await {
                last_applied_t_zero = event.t_zero.as_slice().try_into()?;
                let state: State = event.state.into();
                dbg!("Got event", &last_applied_t_zero, &state);
                self.event_store
                    .upsert_tx(UpsertEvent {
                        id: u128::from_be_bytes(event.id.as_slice().try_into()?),
                        t_zero: last_applied_t_zero,
                        t: event.t.as_slice().try_into()?,
                        state,
                        transaction: Some(event.transaction.clone()),
                        dependencies: Some(from_dependency(event.dependencies.clone())?),
                        ballot: Some(event.ballot.as_slice().try_into()?),
                        execution_hash: Some(event.execution_hash.as_slice().try_into()?),
                    })
                    .await?;
                match state {
                    State::Applied => {
                        //let clone = replica.clone();
                        //join_set.spawn(async move {
                        // let res = replica
                        //     .commit(
                        //         CommitRequest {
                        //             id: event.id.clone(),
                        //             event: event.transaction.clone(),
                        //             timestamp_zero: event.t_zero.clone(),
                        //             timestamp: event.t.clone(),
                        //             dependencies: event.dependencies.clone(),
                        //         },
                        //         false,
                        //     )
                        //     .await;
                        // dbg!(&res);
                        let res = replica
                            .apply(
                                ApplyRequest {
                                    id: event.id,
                                    event: event.transaction,
                                    timestamp_zero: event.t_zero,
                                    timestamp: event.t,
                                    dependencies: event.dependencies,
                                    execution_hash: event.execution_hash,
                                    transaction_hash: event.transaction_hash,
                                },
                                false,
                            )
                            .await;
                        dbg!(&res);
                        //});
                    }

                    _ => {
                        panic!("Unexpected sync event");
                    }
                }
            }
            //let _ = join_set.join_all().await.iter();
            // 2.3 Check if last_applied == last applied execution hash
            let (t, last_applied_self) = self.event_store.last_applied_hash().await?;
            if last_applied_self != last_applied {
                dbg!(&t, &last_applied_self, last_applied);
                //panic!("Mismatched execution hashes for last_applied event")
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
    use synevi_types::types::{ExecutorResult, TransactionPayload};
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
            .transaction(
                2,
                synevi_types::types::TransactionPayload::External(Vec::from("F")),
            )
            .await
            .unwrap();

        let coord = coordinator.event_store.get_event_store().await;
        for node in nodes {
            assert_eq!(
                node.event_store.get_event_store().await,
                coord,
                "Node: {:?}",
                node.get_info()
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
        match coordinator
            .clone()
            .transaction(
                0,
                synevi_types::types::TransactionPayload::External(Vec::from("last transaction")),
            )
            .await
            .unwrap()
        {
            ExecutorResult::External(e) => e.unwrap(),
            _ => panic!(),
        };

        let coordinator_store: BTreeMap<T0, T> = coordinator
            .event_store
            .get_event_store()
            .await
            .into_values()
            .map(|e| (e.t_zero, e.t))
            .collect();
        assert!(coordinator
            .event_store
            .get_event_store()
            .await
            .iter()
            .all(|(_, e)| e.state == State::Applied));

        let mut got_mismatch = false;
        for node in nodes {
            let node_store: BTreeMap<T0, T> = node
                .event_store
                .get_event_store()
                .await
                .into_values()
                .map(|e| (e.t_zero, e.t))
                .collect();
            assert!(node
                .event_store
                .get_event_store()
                .await
                .clone()
                .iter()
                .all(|(_, e)| e.state == State::Applied));
            assert_eq!(coordinator_store.len(), node_store.len());
            if coordinator_store != node_store {
                println!("Node: {:?}", node.get_info());
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

        let result = match node
            .transaction(0, TransactionPayload::External(vec![127u8]))
            .await
            .unwrap()
        {
            ExecutorResult::External(e) => e.unwrap(),
            _ => panic!(),
        };

        assert_eq!(result, vec![127u8]);
    }
}
