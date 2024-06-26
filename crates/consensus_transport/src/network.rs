use crate::{
    consensus_transport::{
        consensus_transport_client::ConsensusTransportClient,
        consensus_transport_server::ConsensusTransportServer, AcceptRequest, AcceptResponse,
        ApplyRequest, ApplyResponse, CommitRequest, CommitResponse, PreAcceptRequest,
        PreAcceptResponse,
    },
    replica::{Replica, ReplicaBox},
};
use anyhow::Result;
use diesel_ulid::DieselUlid;
use std::{net::SocketAddr, sync::Arc};
use tokio::task::JoinSet;
use tonic::transport::{Channel, Server};

#[async_trait::async_trait]
pub trait NetworkInterface: std::fmt::Debug + Send + Sync {
    async fn broadcast(&self, request: BroadcastRequest) -> Result<Vec<BroadcastResponse>>;
}

#[async_trait::async_trait]
pub trait Network: std::fmt::Debug {
    async fn add_members(&mut self, members: Vec<Arc<Member>>);
    async fn add_member(&mut self, id: DieselUlid, serial: u16, host: String) -> Result<()>;
    async fn spawn_server(&mut self, server: Arc<dyn Replica>) -> Result<()>;
    fn get_interface(&self) -> Arc<dyn NetworkInterface>;
}

#[derive(Clone, Debug)]
pub struct NodeInfo {
    pub id: DieselUlid,
    pub serial: u16,
}

#[derive(Clone, Debug)]
pub struct Member {
    pub info: NodeInfo,
    pub host: String,
    pub channel: Channel,
}

#[derive(Debug)]
pub enum BroadcastRequest {
    PreAccept(PreAcceptRequest),
    Accept(AcceptRequest),
    Commit(CommitRequest),
    Apply(ApplyRequest),
    // TODO: Recover
}

#[derive(Debug)]
pub enum BroadcastResponse {
    PreAccept(PreAcceptResponse),
    Accept(AcceptResponse),
    Commit(CommitResponse),
    Apply(ApplyResponse),
    // TODO: Recover
}

#[derive(Debug)]
pub struct NetworkConfig {
    pub socket_addr: SocketAddr,
    pub members: Vec<Arc<Member>>,
    join_set: JoinSet<Result<()>>,
}

#[derive(Debug)]
pub struct NetworkSet {
    members: Vec<Arc<Member>>,
}

impl NetworkConfig {
    pub fn new(socket_addr: SocketAddr) -> Self {
        Self {
            socket_addr,
            members: Vec::new(),
            join_set: JoinSet::new(),
        }
    }

    pub fn create_network_set(&self) -> Arc<NetworkSet> {
        Arc::new(NetworkSet {
            members: self.members.clone(),
        })
    }
}

#[async_trait::async_trait]
impl Network for NetworkConfig {
    async fn add_members(&mut self, members: Vec<Arc<Member>>) {
        self.members.extend(members);
    }

    async fn add_member(&mut self, id: DieselUlid, serial: u16, host: String) -> Result<()> {
        let channel = Channel::from_shared(host.clone())?.connect().await?;
        self.members.push(Arc::new(Member {
            info: NodeInfo { id, serial },
            host,
            channel,
        }));

        Ok(())
    }

    async fn spawn_server(&mut self, server: Arc<dyn Replica>) -> Result<()> {
        let new_replica_box = ReplicaBox::new(server);
        let addr = self.socket_addr;
        self.join_set.spawn(async move {
            let builder =
                Server::builder().add_service(ConsensusTransportServer::new(new_replica_box));
            builder.serve(addr).await?;
            Ok(())
        });
        Ok(())
    }

    fn get_interface(&self) -> Arc<dyn NetworkInterface> {
        self.create_network_set()
    }
}

#[async_trait::async_trait]
impl NetworkInterface for NetworkSet {
    async fn broadcast(&self, request: BroadcastRequest) -> Result<Vec<BroadcastResponse>> {
        //dbg!("[broadcast]: Start");
        let mut responses: JoinSet<Result<BroadcastResponse>> = JoinSet::new();
        let mut result = Vec::new();

        // Send PreAccept request to every known member
        // Call match only once ...

        //dbg!("[broadcast]: Create clients & requests");
        let mut await_majority = true;
        match &request {
            BroadcastRequest::PreAccept(req) => {
                // ... and then iterate over every member ...
                for replica in &self.members {
                    let channel = replica.channel.clone();
                    let request = req.clone();
                    // ... and send a request to member
                    responses.spawn(async move {
                        let mut client = ConsensusTransportClient::new(channel);
                        Ok(BroadcastResponse::PreAccept(
                            client.pre_accept(request).await?.into_inner(),
                        ))
                    });
                }
            }
            BroadcastRequest::Accept(req) => {
                for replica in &self.members {
                    let channel = replica.channel.clone();
                    let request = req.clone();
                    responses.spawn(async move {
                        let mut client = ConsensusTransportClient::new(channel);
                        Ok(BroadcastResponse::Accept(
                            client.accept(request).await?.into_inner(),
                        ))
                    });
                }
            }
            BroadcastRequest::Commit(req) => {
                for replica in &self.members {
                    let channel = replica.channel.clone();
                    let request = req.clone();
                    responses.spawn(async move {
                        let mut client = ConsensusTransportClient::new(channel);
                        Ok(BroadcastResponse::Commit(
                            client.commit(request).await?.into_inner(),
                        ))
                    });
                }
            }
            BroadcastRequest::Apply(req) => {
                await_majority = false;
                for replica in &self.members {
                    let channel = replica.channel.clone();
                    let request = req.clone();
                    responses.spawn(async move {
                        let mut client = ConsensusTransportClient::new(channel);
                        Ok(BroadcastResponse::Apply(
                            client.apply(request).await?.into_inner(),
                        ))
                    });
                }
            }
        }

        let majority = (self.members.len() / 2) + 1;
        let mut counter = 0_usize;

        // Poll majority
        if await_majority {
            while let Some(response) = responses.join_next().await {
                result.push(response??);
                counter += 1;
                if counter >= majority {
                    break;
                }
            }
            //tokio::spawn(async move { while responses.join_next().await.is_some() {} });
        } else {
            //tokio::spawn(async move {
            while let Some(r) = responses.join_next().await {
                r.unwrap().unwrap();
            }
            //});
        }
        Ok(result)
    }
}
