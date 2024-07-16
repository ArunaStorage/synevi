use std::sync::Arc;
use anyhow::Result;
use synevi_network::network::Network;
use crate::maelstrom_config::MaelstromConfig;

mod maelstrom_config;
mod messages;
mod network;
mod protocol;

#[tokio::main]
pub async fn main() -> Result<()> { 
    let config = MaelstromConfig::new("dummy1".to_string(), vec![]).await;
    let replica = Arc::new(MaelstromConfig::new("dummy2".to_string(), vec![]).await);
    config.spawn_server(replica).await.unwrap();
    Ok(())
}
