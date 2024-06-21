use crate::coordinator::Coordinator;
use crate::event_store::EventStore;
use crate::replica::Replica;
use anyhow::Result;
use bytes::Bytes;
use consensus_transport::consensus_transport::consensus_transport_server::ConsensusTransportServer;
use diesel_ulid::DieselUlid;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::{sync::Mutex, task::JoinSet};
use tonic::transport::{Channel, Server};
use tracing::instrument;

#[derive(Debug)]
pub struct Node {
    info: Arc<NodeInfo>,
    members: Vec<Arc<Member>>,
    event_store: Arc<Mutex<EventStore>>,
    join_set: JoinSet<Result<()>>,
}

#[derive(Clone, Debug)]
pub struct NodeInfo {
    pub id: DieselUlid,
    pub serial: u16,
    pub host: String,
}

#[derive(Clone, Debug)]
pub struct Member {
    pub info: NodeInfo,
    pub channel: Channel,
}

impl Node {
    pub async fn new_with_config() -> Self {
        todo!()
    }

    #[instrument(level = "trace")]
    pub async fn new_with_parameters(id: DieselUlid, serial: u16, socket_addr: SocketAddr) -> Self {
        let node_name = Arc::new(NodeInfo {
            id,
            serial,
            host: format!("{}", socket_addr),
        });
        let node_name_clone = node_name.clone();
        let event_store = Arc::new(Mutex::new(EventStore::init()));
        let event_store_clone = event_store.clone();
        let mut join_set = JoinSet::new();
        join_set.spawn(async move {
            let builder = Server::builder().add_service(ConsensusTransportServer::new(Replica {
                node: Arc::new(node_name_clone.host.clone()),
                event_store: event_store_clone,
            }));
            builder.serve(socket_addr).await?;
            Ok(())
        });

        // If no config / persistence -> default
        Node {
            info: node_name,
            members: vec![],
            event_store,
            join_set,
        }
    }

    #[instrument(level = "trace", skip(self))]
    pub async fn run(&mut self) -> Result<()> {
        self.join_set
            .join_next()
            .await
            .ok_or(anyhow::anyhow!("Empty joinset"))???;
        Ok(())
    }

    #[instrument(level = "trace", skip(self))]
    pub async fn do_stuff(&self) -> Result<()> {
        Ok(())
    }

    #[instrument(level = "trace", skip(self))]
    pub async fn add_member(&mut self, id: DieselUlid, serial: u16, host: String) -> Result<()> {
        let channel = Channel::from_shared(host.clone())?.connect().await?;
        self.members.push(Arc::new(Member {
            channel,
            info: NodeInfo { id, serial, host },
        }));
        Ok(())
    }

    #[instrument(level = "trace", skip(self))]
    pub async fn transaction(&self, transaction: Bytes) -> Result<()> {
        let mut coordinator = Coordinator::new(
            self.info.clone(),
            self.members.clone(),
            self.event_store.clone(),
        );
        coordinator.transaction(transaction).await?;
        Ok(())
    }
}
