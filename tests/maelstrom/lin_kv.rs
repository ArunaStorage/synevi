use maelstrom_server::MaelstromServer;
mod maelstrom_server;
mod messages;
mod network;
mod protocol;

#[tokio::main]
pub async fn main() {
    eprintln!("Starting Maelstrom server");
    MaelstromServer::spawn().await.unwrap().0.await.unwrap();
}
