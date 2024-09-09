use crate::coordinator::Coordinator;
use crate::replica::ReplicaConfig;
use crate::wait_handler::WaitHandler;
use std::fmt::Debug;
use std::sync::atomic::AtomicBool;
use std::sync::{atomic::AtomicU64, Arc};
use synevi_network::network::{Network, NodeInfo};
use synevi_persistence::mem_store::MemStore;
use synevi_types::traits::Store;
use synevi_types::types::{Config, SyneviResult, TxPayload};
use synevi_types::{Executor, SyneviError};
use tokio::sync::{Mutex, RwLock};
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
    pub event_store: Mutex<S>,
    pub stats: Stats,
    pub wait_handler: RwLock<Option<Arc<WaitHandler<N, E, S>>>>,
    semaphore: Arc<tokio::sync::Semaphore>,
    has_members: AtomicBool,
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
            event_store: Mutex::new(store),
            network,
            stats,
            semaphore: Arc::new(tokio::sync::Semaphore::new(10)),
            executor,
            wait_handler: RwLock::new(None),
            has_members: AtomicBool::new(false),
        });

        let wait_handler = WaitHandler::new(node.clone());
        let wait_handler_clone = wait_handler.clone();
        tokio::spawn(async move {
            wait_handler_clone.run().await.unwrap();
        });

        *node.wait_handler.write().await = Some(wait_handler);

        let replica = ReplicaConfig::new(node.clone());

        // Spawn tonic server
        node.network.spawn_server(replica).await?;

        // If no config / persistence -> default
        Ok(node)
    }

    #[instrument(level = "trace", skip(self))]
    pub async fn add_member(&self, id: Ulid, serial: u16, host: String) -> Result<(), SyneviError> {
        self.network.add_member(id, serial, host).await?;
        self.has_members
            .store(true, std::sync::atomic::Ordering::Relaxed);
        Ok(())
    }

    #[instrument(level = "trace", skip(self, transaction))]
    pub async fn transaction(self: Arc<Self>, id: u128, transaction: E::Tx) -> SyneviResult<E> {
        if !self.has_members.load(std::sync::atomic::Ordering::Relaxed) {
            tracing::warn!("Consensus omitted: No members in the network");
        };
        let _permit = self.semaphore.acquire().await?;
        let mut coordinator = Coordinator::new(
            self.clone(),
            synevi_types::types::TxPayload::Custom(transaction),
            id,
        )
        .await;
        coordinator.run().await
    }

    #[instrument(level = "trace", skip(self))]
    pub async fn config_change(self: Arc<Self>, id: u128, config: Config) -> SyneviResult<E> {
        if !self.has_members.load(std::sync::atomic::Ordering::Relaxed) {
            tracing::warn!("Consensus omitted: No members in the network");
        };
        let _permit = self.semaphore.acquire().await?;
        let mut coordinator =
            Coordinator::new(self.clone(), TxPayload::ConfigChange(config), id).await;
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
            let mut coordinator = Coordinator::new(self.clone(), synevi_types::types::TxPayload::Custom(transaction), id).await;
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
            let network = synevi_network::network::GrpcNetwork::new(socket_addr);
            let node = Node::new_with_network_and_executor(*m, i as u16, network, DummyExecutor)
                .await
                .unwrap();
            nodes.push(node);
        }
        for (i, name) in node_names.iter().enumerate() {
            for (i2, node) in nodes.iter_mut().enumerate() {
                if i != i2 {
                    node.add_member(*name, i as u16, format!("http://0.0.0.0:{}", 13200 + i))
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

        let coord = coordinator.event_store.lock().await.get_event_store();
        for node in nodes {
            assert_eq!(
                node.event_store.lock().await.get_event_store(),
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
            let network = synevi_network::network::GrpcNetwork::new(socket_addr);
            let node = Node::new_with_network_and_executor(*m, i as u16, network, DummyExecutor)
                .await
                .unwrap();
            nodes.push(node);
        }
        for (i, name) in node_names.iter().enumerate() {
            for (i2, node) in nodes.iter_mut().enumerate() {
                if i != i2 {
                    node.add_member(*name, i as u16, format!("http://0.0.0.0:{}", 13100 + i))
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
            .lock()
            .await
            .get_event_store()
            .into_values()
            .map(|e| (e.t_zero, e.t))
            .collect();
        assert!(coordinator
            .event_store
            .lock()
            .await
            .get_event_store()
            .iter()
            .all(|(_, e)| e.state == State::Applied));

        let mut got_mismatch = false;
        for node in nodes {
            let node_store: BTreeMap<T0, T> = node
                .event_store
                .lock()
                .await
                .get_event_store()
                .into_values()
                .map(|e| (e.t_zero, e.t))
                .collect();
            assert!(node
                .event_store
                .lock()
                .await
                .get_event_store()
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
        let node = Node::new_with_network_and_executor(
            Ulid::new(),
            0,
            synevi_network::network::GrpcNetwork::new(
                SocketAddr::from_str("0.0.0.0:1337").unwrap(),
            ),
            DummyExecutor,
        )
        .await
        .unwrap();

        let result = node.transaction(0, vec![127u8]).await.unwrap().unwrap();

        assert_eq!(result, vec![127u8]);
    }
}
