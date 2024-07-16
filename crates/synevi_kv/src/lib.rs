use ahash::RandomState;
use anyhow::{anyhow, Result};
use diesel_ulid::DieselUlid;
use std::collections::HashMap;
use std::sync::Arc;
use synevi_consensus::node::Node;
use synevi_network::network::Network;

pub struct KVStore {
    pub kv_store: HashMap<String, String, RandomState>,
    pub node: Node,
}

impl KVStore {
    pub async fn init(
        id: DieselUlid,
        serial: u16,
        network: Arc<dyn Network + Send + Sync>,
        members: Vec<(DieselUlid, u16, String)>,
        path: Option<String>,
    ) -> Result<Self> {
        let mut node = Node::new_with_parameters(id, serial, network, path).await?;
        for (ulid, id, host) in members {
            node.add_member(ulid, id, host).await?;
        }
        Ok(KVStore {
            kv_store: HashMap::default(),
            node,
        })
    }

    pub async fn read(&self, key: String) -> Result<String> {
        let log = format!("READ: {key}");
        self.node.transaction(log.into_bytes()).await?;
        self.kv_store
            .get(&key)
            .ok_or_else(|| anyhow!("Key not found"))
            .cloned()
    }

    pub async fn write(&mut self, key: String, value: String) -> Result<()> {
        let log = format!("WRITE: {key}: {value}");
        self.node.transaction(log.into_bytes()).await?;
        self.kv_store.insert(key, value);
        Ok(())
    }
}
