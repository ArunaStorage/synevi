use anyhow::Result;
use maelstrom_server::MaelstromServer;
mod maelstrom_server;
mod messages;
mod network;
mod protocol;

#[tokio::main]
pub async fn main() -> Result<()> {
    eprintln!("Starting Maelstrom server");
    MaelstromServer::spawn().await?;
    Ok(())
}
