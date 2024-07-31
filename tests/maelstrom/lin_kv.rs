use anyhow::Result;
use maelstrom_server::MaelstromServer;
mod maelstrom_server;
mod messages;
mod network;
mod protocol;

#[tokio::main]
pub async fn main() -> Result<()> {
    MaelstromServer::spawn().await?;
    Ok(())
}
