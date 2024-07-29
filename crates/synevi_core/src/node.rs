use crate::coordinator::Coordinator;
use crate::replica::ReplicaConfig;
use crate::wait_handler::WaitHandler;
use anyhow::{anyhow, Result};
use diesel_ulid::DieselUlid;
use std::fmt::Debug;
use std::sync::{atomic::AtomicU64, Arc};
use synevi_network::network::{Network, NodeInfo};
use synevi_persistence::event_store::{EventStore, Store};
use synevi_types::{Executor, Transaction};
use tokio::sync::{Mutex, RwLock};
use tracing::instrument;

#[derive(Debug, Default)]
pub struct Stats {
    pub total_requests: AtomicU64,
    pub total_accepts: AtomicU64,
    pub total_recovers: AtomicU64,
}

pub struct Node<N, E, S = EventStore>
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
}

impl<N, E, S> Node<N, E, S>
where
    N: Network,
    E: Executor,
    S: Store,
{
    #[instrument(level = "trace", skip(network, executor))]
    pub async fn new_with_network_and_executor(
        id: DieselUlid,
        serial: u16,
        network: N,
        executor: E,
    ) -> Result<Arc<Self>> {
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

        let store = S::new(serial)?;

        let node = Arc::new(Node {
            info: node_name,
            event_store: Mutex::new(store),
            network,
            stats,
            semaphore: Arc::new(tokio::sync::Semaphore::new(10)),
            executor,
            wait_handler: RwLock::new(None),
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
    pub async fn add_member(&mut self, id: DieselUlid, serial: u16, host: String) -> Result<()> {
        self.network.add_member(id, serial, host).await
    }

    #[instrument(level = "trace", skip(self, transaction))]
    pub async fn transaction(
        self: Arc<Self>,
        id: u128,
        transaction: E::Tx,
    ) -> Result<<E::Tx as Transaction>::ExecutionResult> {
        let _permit = self.semaphore.acquire().await?;
        let mut coordinator = Coordinator::new(self.clone(), transaction, id).await;
        coordinator.run().await
    }

    pub async fn get_wait_handler(&self) -> Result<Arc<WaitHandler<N, E, S>>> {
        let lock = self.wait_handler.read().await;
        let handler = lock
            .as_ref()
            .ok_or_else(|| anyhow!("Missing wait_handler"))?
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
}

#[cfg(test)]
mod tests {
    use crate::coordinator::Coordinator;
    use crate::tests::NetworkMock;
    use crate::{node::Node, tests::DummyExecutor};
    use anyhow::Result;
    use diesel_ulid::DieselUlid;
    use rand::distributions::{Distribution, Uniform};
    use std::collections::BTreeMap;
    use std::net::SocketAddr;
    use std::str::FromStr;
    use std::sync::Arc;
    use synevi_network::network::Network;
    use synevi_network::{consensus_transport::State, network::GrpcNetwork};
    use synevi_types::{Executor, T, T0};

    impl<N, E> Node<N, E>
    where
        N: Network,
        E: Executor<Tx = Vec<u8>>,
    {
        pub async fn failing_transaction(
            self: Arc<Self>,
            id: u128,
            transaction: Vec<u8>,
        ) -> Result<()> {
            let _permit = self.semaphore.acquire().await?;
            let mut coordinator = Coordinator::new(self.clone(), transaction, id).await;
            coordinator.failing_pre_accept().await?;
            Ok(())
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn recovery_single() {
        let node_names: Vec<_> = (0..5).map(|_| DieselUlid::generate()).collect();
        let mut nodes: Vec<Arc<Node<GrpcNetwork, DummyExecutor>>> = vec![];

        for (i, m) in node_names.iter().enumerate() {
            let socket_addr = SocketAddr::from_str(&format!("0.0.0.0:{}", 13100 + i)).unwrap();
            let network = synevi_network::network::GrpcNetwork::new(socket_addr);
            let (sdx, _) = tokio::sync::mpsc::channel(100);
            let node = Node::new_with_network_and_executor(*m, i as u16, network, DummyExecutor)
                .await
                .unwrap();
            nodes.push(node);
        }
        for (i, name) in node_names.iter().enumerate() {
            for (i2, node) in nodes.iter_mut().enumerate() {
                if i != i2 {
                    node.add_member(*name, i as u16, format!("http://localhost:{}", 13100 + i))
                        .await
                        .unwrap();
                }
            }
        }
        let coordinator = nodes.pop().unwrap();
        let interface = coordinator.network.get_interface().await;
        coordinator
            .failing_transaction(1, b"failing".to_vec())
            .await
            .unwrap();

        // let accepted = coordinator_iter.next().await.unwrap();
        // let committed = coordinator_iter.next().await.unwrap();
        // let applied = coordinator_iter.next().await.unwrap();

        let result = coordinator
            .transaction(1, Vec::from("First"))
            .await
            .unwrap();

        let coord = coordinator.get_event_store().lock().await.events.clone();
        for node in nodes {
            assert_eq!(node.get_event_store().lock().await.events, coord);
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn recovery_random_test() {
        let node_names: Vec<_> = (0..5).map(|_| DieselUlid::generate()).collect();
        let mut nodes: Vec<Node> = vec![];

        for (i, m) in node_names.iter().enumerate() {
            let socket_addr = SocketAddr::from_str(&format!("0.0.0.0:{}", 13000 + i)).unwrap();
            let network = Arc::new(synevi_network::network::GrpcNetwork::new(socket_addr));
            let (sdx, _) = tokio::sync::mpsc::channel(100);
            let node = Node::new_with_parameters(*m, i as u16, network, None, sdx)
                .await
                .unwrap();
            nodes.push(node);
        }
        for (i, name) in node_names.iter().enumerate() {
            for (i2, node) in nodes.iter_mut().enumerate() {
                if i != i2 {
                    node.add_member(*name, i as u16, format!("http://localhost:{}", 13000 + i))
                        .await
                        .unwrap();
                }
            }
        }
        let coordinator = nodes.pop().unwrap();
        let arc_coordinator = Arc::new(coordinator);

        let coordinator = arc_coordinator.clone();

        let mut rng = rand::thread_rng();
        let die = Uniform::from(0..10);

        // Working coordinator
        for _ in 0..10 {
            // Working coordinator
            let mut coordinator_iter = CoordinatorIterator::new(
                coordinator.info.clone(),
                coordinator.event_store.clone(),
                coordinator.network.get_interface().await,
                Vec::from("Recovery transaction"),
                coordinator.stats.clone(),
                coordinator.wait_handler.clone(),
                0,
            )
            .await;
            while coordinator_iter.next().await.unwrap().is_some() {
                let throw = die.sample(&mut rng);
                if throw == 0 {
                    break;
                }
            }
        }
        coordinator
            .transaction(0, Vec::from("last transaction"))
            .await
            .unwrap();

        let coordinator_store: BTreeMap<T0, T> = arc_coordinator
            .get_event_store()
            .lock()
            .await
            .events
            .clone()
            .into_values()
            .map(|e| (e.t_zero, e.t))
            .collect();
        assert!(arc_coordinator
            .get_event_store()
            .lock()
            .await
            .events
            .clone()
            .iter()
            .all(|(_, e)| e.state == State::Applied));

        let mut got_mismatch = false;
        for node in nodes {
            let node_store: BTreeMap<T0, T> = node
                .get_event_store()
                .lock()
                .await
                .events
                .clone()
                .into_values()
                .map(|e| (e.t_zero, e.t))
                .collect();
            assert!(node
                .get_event_store()
                .lock()
                .await
                .events
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
    async fn database_test() {
        let network = Arc::new(NetworkMock::default());
        let path = "../../tests/database/init_test_db".to_string();
        let (sdx, _) = tokio::sync::mpsc::channel(100);
        let node = Node::new_with_parameters(DieselUlid::generate(), 0, network, Some(path), sdx)
            .await
            .unwrap();
        node.transaction(
            u128::from_be_bytes(DieselUlid::generate().as_byte_array()),
            Vec::from("this is a transaction test"),
        )
        .await
        .unwrap();
        let mut event_store = node.event_store.lock().await;
        let db = event_store.database.clone();
        let all = db.unwrap().read_all().unwrap();
        let db_entry =
            synevi_persistence::event::Event::from_bytes(all.first().cloned().unwrap()).unwrap();
        let ev_entry = event_store.events.first_entry().unwrap();
        let ev_entry = ev_entry.get();

        assert_eq!(&db_entry, ev_entry);
    }
}
