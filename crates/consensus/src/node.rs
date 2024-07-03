use crate::coordinator::CoordinatorIterator;
use crate::event_store::EventStore;
use crate::replica::ReplicaConfig;
use anyhow::Result;
use bytes::Bytes;
use consensus_transport::network::{Network, NodeInfo};
use diesel_ulid::DieselUlid;
use std::sync::{atomic::AtomicU64, Arc};
use tokio::sync::Mutex;
use tracing::instrument;

#[derive(Debug, Default)]
pub struct Stats {
    pub total_requests: AtomicU64,
    pub total_accepts: AtomicU64,
}

pub struct Node {
    info: Arc<NodeInfo>,
    network: Arc<Mutex<dyn Network + Send + Sync>>,
    event_store: Arc<Mutex<EventStore>>,
    stats: Arc<Stats>,
    semaphore: Arc<tokio::sync::Semaphore>,
}

impl Node {
    pub async fn new_with_config() -> Self {
        todo!()
    }

    #[instrument(level = "trace")]
    pub async fn new_with_parameters(
        id: DieselUlid,
        serial: u16,
        network: Arc<Mutex<dyn Network + Send + Sync>>,
        db_path: Option<String>,
    ) -> Result<Self> {
        let node_name = Arc::new(NodeInfo { id, serial });
        let event_store = Arc::new(Mutex::new(EventStore::init(db_path)));

        let stats = Arc::new(Stats {
            total_requests: AtomicU64::new(0),
            total_accepts: AtomicU64::new(0),
        });

        let replica = Arc::new(ReplicaConfig {
            node_info: node_name.clone(),
            event_store: event_store.clone(),
            network: network.clone(),
            stats: stats.clone(),
        });
        // Spawn tonic server
        network.lock().await.spawn_server(replica).await?;

        // If no config / persistence -> default
        Ok(Node {
            info: node_name,
            event_store,
            network,
            stats,
            semaphore: Arc::new(tokio::sync::Semaphore::new(20)),
        })
    }

    #[instrument(level = "trace", skip(self))]
    pub async fn add_member(&mut self, id: DieselUlid, serial: u16, host: String) -> Result<()> {
        self.network.lock().await.add_member(id, serial, host).await
    }

    #[instrument(level = "trace", skip(self))]
    pub async fn transaction(&self, transaction: Bytes) -> Result<()> {
        let _permit = self.semaphore.clone().acquire_owned().await?;
        let interface = self.network.lock().await.get_interface();
        let mut coordinator_iter = CoordinatorIterator::new(
            self.info.clone(),
            self.event_store.clone(),
            interface,
            transaction,
            self.stats.clone(),
        )
        .await;

        while coordinator_iter.next().await?.is_some() {}
        Ok(())
    }

    #[instrument(level = "trace", skip(self))]
    pub fn get_event_store(&self) -> Arc<Mutex<EventStore>> {
        self.event_store.clone()
    }

    #[instrument(level = "trace", skip(self))]
    pub fn get_info(&self) -> Arc<NodeInfo> {
        self.info.clone()
    }

    pub fn get_stats(&self) -> (u64, u64) {
        (
            self.stats
                .total_requests
                .load(std::sync::atomic::Ordering::Relaxed),
            self.stats
                .total_accepts
                .load(std::sync::atomic::Ordering::Relaxed),
        )
    }
}

#[cfg(test)]
mod tests {
    use crate::coordinator::CoordinatorIterator;
    use crate::node::Node;
    use crate::utils::{T, T0};
    use bytes::Bytes;
    use consensus_transport::consensus_transport::State;
    use diesel_ulid::DieselUlid;
    use rand::distributions::{Distribution, Uniform};
    use std::collections::BTreeMap;
    use std::net::SocketAddr;
    use std::str::FromStr;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::sync::Mutex;

    #[tokio::test(flavor = "multi_thread")]
    async fn recovery_single() {
        let node_names: Vec<_> = (0..5).map(|_| DieselUlid::generate()).collect();
        let mut nodes: Vec<Node> = vec![];

        for (i, m) in node_names.iter().enumerate() {
            let socket_addr = SocketAddr::from_str(&format!("0.0.0.0:{}", 13100 + i)).unwrap();
            let network = Arc::new(Mutex::new(
                consensus_transport::network::NetworkConfig::new(socket_addr),
            ));
            let node = Node::new_with_parameters(*m, i as u16, network, None)
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
        let arc_coordinator = Arc::new(coordinator);

        let coordinator = arc_coordinator.clone();
        let interface = coordinator.network.lock().await.get_interface();
        let mut coordinator_iter = CoordinatorIterator::new(
            coordinator.info.clone(),
            coordinator.event_store.clone(),
            interface,
            Bytes::from("Recovery transaction"),
            coordinator.stats.clone(),
        )
        .await;
        coordinator_iter.next().await.unwrap();

        // let accepted = coordinator_iter.next().await.unwrap();
        // let committed = coordinator_iter.next().await.unwrap();
        // let applied = coordinator_iter.next().await.unwrap();

        arc_coordinator
            .transaction(Bytes::from("First"))
            .await
            .unwrap();

        let coord = arc_coordinator
            .get_event_store()
            .lock()
            .await
            .events
            .clone();
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
            let network = Arc::new(Mutex::new(
                consensus_transport::network::NetworkConfig::new(socket_addr),
            ));
            let node = Node::new_with_parameters(*m, i as u16, network, None)
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

        for _ in 0..10 {
            let interface = coordinator.network.lock().await;
            // Working coordinator
            let mut coordinator_iter = CoordinatorIterator::new(
                coordinator.info.clone(),
                coordinator.event_store.clone(),
                interface.get_interface(),
                Bytes::from("Recovery transaction"),
                coordinator.stats.clone(),
            )
            .await;

            while coordinator_iter.next().await.unwrap().is_some() {
                let throw = die.sample(&mut rng);
                if throw == 0 {
                    match coordinator_iter {
                        CoordinatorIterator::Initialized(_) => {
                            println!("Break at Init")
                        }
                        CoordinatorIterator::PreAccepted(_) => {
                            println!("Break at PreAccepted")
                        }
                        CoordinatorIterator::Accepted(_) => {
                            println!("Break at Accepted")
                        }
                        CoordinatorIterator::Committed(_) => {
                            println!("Break at Committed")
                        }
                        CoordinatorIterator::Applied => {
                            println!("Break at Applied")
                        }
                        CoordinatorIterator::Recovering => {
                            println!("Break at Recovering")
                        }
                        CoordinatorIterator::RestartRecovery => {
                            println!("Break at RestartRecovery")
                        }
                    }
                    break;
                }
            }
        }
        coordinator
            .transaction(Bytes::from("last transaction"))
            .await
            .unwrap();

        tokio::time::sleep(Duration::from_secs(1)).await;

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
            .all(|(_, e)| (e.state.borrow()).0 == State::Applied));

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
                .all(|(_, e)| (e.state.borrow()).0 == State::Applied));
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
}
