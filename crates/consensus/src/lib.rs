pub mod coordinator;
mod event_store;
pub mod replica;
mod utils;

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;
    use std::str::FromStr;
    use std::sync::{Arc};
    use bytes::Bytes;
    use tonic::transport::{Channel, Server};
    use consensus_transport::consensus_transport::consensus_transport_server::ConsensusTransportServer;
    use crate::coordinator::{Coordinator, Member, MemberInfo};
    use crate::event_store::EventStore;
    use crate::replica::Replica;

    #[tokio::test]
    async fn happy_path() {
        let nodes = ["first", "second", "third" , "fourth", "fifth"];
        let mut members = Vec::new();
        let mut event_stores = Vec::new();
        for (n,member) in nodes.iter().enumerate() {
            let host = ["http://localhost:1000".to_string(), n.to_string()].join("");
            //let channel= Channel::from_shared(host.clone()).unwrap().connect().await.unwrap() ;
            members.push(MemberInfo {
                host,
                node: member.to_string(),
            });
            event_stores.push(Arc::new(EventStore::init()));
        }
        let mut members_with_channel = Vec::new();
        for (i, m)  in members.clone().iter().enumerate() {
            dbg!(&m);
            let members_clone = members.clone();
            let info = m.clone();
            let event_store = event_stores[i].clone();
            tokio::spawn(async move {
                let builder = Server::builder().add_service(ConsensusTransportServer::new(Replica{
                    node: info.node.to_string(),
                    members: members_clone,
                    event_store,
                }));
                let port = info.host.strip_prefix("http://localhost").unwrap();
                let addr = SocketAddr::from_str(&["0.0.0.0", port].join("")).unwrap() ;
                builder.serve(addr).await.unwrap();
            });
            dbg!("Spawned");
            members_with_channel.push(Member {
                info: m.clone(),
                channel: Channel::from_shared(m.clone().host.clone()).unwrap().connect().await.unwrap()
            })
        }
        
        dbg!("Start coordinator");
        let mut coordinator = Coordinator::builder("first".to_string(), members_with_channel, event_stores[0].clone()).await;
        dbg!("Start transaction");
        coordinator.transaction(Bytes::from("This is a transaction")).await.unwrap();
        dbg!("Sent transaction");
    }
}