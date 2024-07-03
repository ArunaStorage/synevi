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
    semaphore: tokio::sync::Semaphore
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
        db_path: Option<String>
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
        let semaphore = tokio::sync::Semaphore::new(10);

        // If no config / persistence -> default
        Ok(Node {
            info: node_name,
            event_store,
            network,
            stats,
            semaphore
        })
    }

    #[instrument(level = "trace", skip(self))]
    pub async fn add_member(&mut self, id: DieselUlid, serial: u16, host: String) -> Result<()> {
        self.network.lock().await.add_member(id, serial, host).await
    }

    #[instrument(level = "trace", skip(self))]
    pub async fn transaction(&self, transaction: Bytes) -> Result<()> {
        let _permit = self.semaphore.acquire().await?;
        let mut coordinator_iter = CoordinatorIterator::new(
            self.info.clone(),
            self.event_store.clone(),
            self.network.lock().await.get_interface(),
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
