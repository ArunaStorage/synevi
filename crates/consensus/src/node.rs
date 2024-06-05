use std::net::SocketAddr;
use crate::event_store::EventStore;
use std::sync::Arc;
use tonic::transport::{Channel, Server};
use crate::coordinator::{Coordinator, Member, MemberInfo};
use anyhow::Result;
use bytes::Bytes;
use tokio::task::JoinSet;
use consensus_transport::consensus_transport::consensus_transport_server::ConsensusTransportServer;
use crate::replica::Replica;

pub struct Node {
    name: Arc<String>,
    members: Vec<Arc<Member>>,
    event_store: Arc<EventStore>,
    join_set: JoinSet<Result<()>>,
}

impl Node {

    pub async fn new_with_config() -> Self {
        todo!()
    }

    pub async fn new_with_parameters(name: String, socket_addr: SocketAddr) -> Self {

        let node_name = Arc::new(name);
        let node_name_clone = node_name.clone();
        let event_store = Arc::new(EventStore::init());
        let event_store_clone = event_store.clone();
        let mut join_set = JoinSet::new();
        join_set.spawn(async move {
            let builder = Server::builder().add_service(ConsensusTransportServer::new(Replica{
                node: node_name_clone,
                event_store: event_store_clone,
            }));
            builder.serve(socket_addr).await?;
            Ok(())
        });


        // If no config / persistence -> default
        Node {
            name: node_name,
            members: vec![],
            event_store,
            join_set
        }
    }

    pub async fn run(&mut self) -> Result<()> {
        self.join_set.join_next().await.ok_or(anyhow::anyhow!("Empty joinset"))???;
        Ok(())
    }

    pub async fn do_stuff(&self) -> Result<()> {
        Ok(())
    }

    pub async fn add_member(&mut self, member: MemberInfo) -> Result<()> {

        let channel = Channel::from_shared(member.host.clone())?.connect().await?;
        self.members.push(Arc::new(Member {
            info: member,
            channel,
        }));

        Ok(())
    }

    pub async fn transaction(&self, transaction: Bytes) -> Result<()> {
        let mut coordinator = Coordinator::new(self.name.clone(), self.members.clone(), self.event_store.clone());
        coordinator.transaction(transaction).await
    }
}
