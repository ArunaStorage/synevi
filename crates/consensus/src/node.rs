use crate::coordinator::Coordinator;
use crate::event_store::EventStore;
use crate::replica::ReplicaConfig;
use anyhow::Result;
use bytes::Bytes;
use consensus_transport::network::{Network, NodeInfo};
use diesel_ulid::DieselUlid;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::instrument;

pub struct Node {
    info: Arc<NodeInfo>,
    network: Box<dyn Network + Send + Sync>,
    event_store: Arc<Mutex<EventStore>>,
}

impl Node {
    pub async fn new_with_config() -> Self {
        todo!()
    }

    #[instrument(level = "trace")]
    pub async fn new_with_parameters(
        id: DieselUlid,
        serial: u16,
        mut network: Box<dyn Network + Send + Sync>,
    ) -> Result<Self> {
        let node_name = Arc::new(NodeInfo { id, serial });
        let event_store = Arc::new(Mutex::new(EventStore::init()));

        let replica = Arc::new(ReplicaConfig {
            node: Arc::new(node_name.id.to_string()),
            event_store: event_store.clone(),
        });
        // Spawn tonic server
        network.spawn_server(replica).await?;

        // If no config / persistence -> default
        Ok(Node {
            info: node_name,
            event_store,
            network,
        })
    }

    #[instrument(level = "trace", skip(self))]
    pub async fn add_member(&mut self, id: DieselUlid, serial: u16, host: String) -> Result<()> {
        self.network.add_member(id, serial, host).await
    }

    #[instrument(level = "trace", skip(self))]
    pub async fn transaction(&self, transaction: Bytes) -> Result<()> {
        let mut coordinator = Coordinator::new(
            self.info.clone(),
            self.network.get_interface(),
            self.event_store.clone(),
        );
        coordinator.transaction(transaction).await?;
        Ok(())
    }

    #[instrument(level = "trace", skip(self))]
    pub fn get_event_store(&self) -> Arc<Mutex<EventStore>> {
        self.event_store.clone()
    }
}
