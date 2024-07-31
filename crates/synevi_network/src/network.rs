use crate::configure_transport::time_service_server::TimeServiceServer;
use crate::consensus_transport::{RecoverRequest, RecoverResponse};
use crate::error::BroadCastError;
use crate::latency_service::get_latency;
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
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::{net::SocketAddr, sync::Arc};
use tokio::sync::{Mutex, RwLock};
use tokio::task::JoinSet;
use tonic::metadata::{AsciiMetadataKey, AsciiMetadataValue};
use tonic::transport::{Channel, Server};

#[async_trait::async_trait]
pub trait NetworkInterface: Send + Sync {
    async fn broadcast(
        &self,
        request: BroadcastRequest,
    ) -> Result<Vec<BroadcastResponse>, BroadCastError>;
}

#[async_trait::async_trait]
pub trait Network: Send + Sync + 'static {
    type Ni: NetworkInterface;
    async fn add_members(&self, members: Vec<(DieselUlid, u16, String)>);
    async fn add_member(&self, id: DieselUlid, serial: u16, host: String) -> Result<()>;
    async fn spawn_server<R: Replica + 'static>(&self, server: R) -> Result<()>;
    async fn get_interface(&self) -> Arc<Self::Ni>;
    async fn get_waiting_time(&self, node_serial: u16) -> u64;
}

// Blanket implementation for Arc<N> where N: Network
#[async_trait::async_trait]
impl<N> Network for Arc<N>
where
    N: Network,
{
    type Ni = N::Ni;

    async fn add_members(&self, members: Vec<(DieselUlid, u16, String)>) {
        self.add_members(members).await;
    }

    async fn add_member(&self, id: DieselUlid, serial: u16, host: String) -> Result<()> {
        self.add_member(id, serial, host).await
    }

    async fn spawn_server<R: Replica + 'static>(&self, server: R) -> Result<()> {
        self.spawn_server(server).await
    }

    async fn get_interface(&self) -> Arc<Self::Ni> {
        self.get_interface().await
    }

    async fn get_waiting_time(&self, node_serial: u16) -> u64 {
        self.get_waiting_time(node_serial).await
    }
}

#[async_trait::async_trait]
impl<N> NetworkInterface for Arc<N>
where
    N: NetworkInterface,
{
    async fn broadcast(
        &self,
        request: BroadcastRequest,
    ) -> Result<Vec<BroadcastResponse>, BroadCastError> {
        self.broadcast(request).await
    }
}

#[derive(Clone, Debug, Default)]
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
pub struct MemberWithLatency {
    pub member: Arc<Member>,
    pub latency: AtomicU64,
    pub skew: AtomicI64,
}

#[derive(Debug, Clone)]
pub enum BroadcastRequest {
    PreAccept(PreAcceptRequest, u16),
    Accept(AcceptRequest),
    Commit(CommitRequest),
    Apply(ApplyRequest),
    Recover(RecoverRequest),
    // TODO: Recover
}

#[derive(Debug, Clone)]
pub enum BroadcastResponse {
    PreAccept(PreAcceptResponse),
    Accept(AcceptResponse),
    Commit(CommitResponse),
    Apply(ApplyResponse),
    Recover(RecoverResponse),
    // TODO: Recover
}

#[derive(Debug)]
pub struct GrpcNetwork {
    pub socket_addr: SocketAddr,
    pub members: Arc<RwLock<Vec<MemberWithLatency>>>,
    join_set: Mutex<JoinSet<Result<()>>>,
}

#[derive(Debug)]
pub struct GrpcNetworkSet {
    members: Vec<Arc<Member>>,
}

impl GrpcNetwork {
    pub fn new(socket_addr: SocketAddr) -> Self {
        Self {
            socket_addr,
            members: Arc::new(RwLock::new(Vec::new())),
            join_set: Mutex::new(JoinSet::new()),
        }
    }

    pub async fn create_network_set(&self) -> Arc<GrpcNetworkSet> {
        Arc::new(GrpcNetworkSet {
            members: self
                .members
                .read()
                .await
                .iter()
                .map(|e| e.member.clone())
                .collect(),
        })
    }
}

#[async_trait::async_trait]
impl Network for GrpcNetwork {
    type Ni = GrpcNetworkSet;

    async fn add_members(&self, members: Vec<(DieselUlid, u16, String)>) {
        for (id, serial, host) in members {
            self.add_member(id, serial, host).await.unwrap();
        }
    }

    async fn add_member(&self, id: DieselUlid, serial: u16, host: String) -> Result<()> {
        let channel = Channel::from_shared(host.clone())?.connect().await?;
        self.members.write().await.push(MemberWithLatency {
            member: Arc::new(Member {
                info: NodeInfo { id, serial },
                host,
                channel,
            }),
            latency: AtomicU64::new(500),
            skew: AtomicI64::new(0),
        });
        Ok(())
    }

    async fn spawn_server<R: Replica + 'static>(&self, server: R) -> Result<()> {
        let new_replica_box = ReplicaBox::new(server);
        let addr = self.socket_addr;
        self.join_set.lock().await.spawn(async move {
            let builder = Server::builder()
                .add_service(ConsensusTransportServer::new(new_replica_box.clone()))
                .add_service(TimeServiceServer::new(new_replica_box));
            builder.serve(addr).await?;
            Ok(())
        });

        let members = self.members.clone();
        self.join_set.lock().await.spawn(get_latency(members));
        Ok(())
    }

    async fn get_interface(&self) -> Arc<GrpcNetworkSet> {
        self.create_network_set().await
    }

    async fn get_waiting_time(&self, node_serial: u16) -> u64 {
        // Wait (max latency of majority + skew) - (latency from node)/2
        let mut max_latency = 0;
        let mut node_latency = 0;
        for member in self.members.read().await.iter() {
            let member_latency = member.latency.load(Ordering::Relaxed);
            if member_latency > max_latency {
                max_latency = member_latency;
            }
            if node_serial == member.member.info.serial {
                node_latency = member_latency;
            }
        }

        (max_latency) - (node_latency / 2)
    }
}

#[async_trait::async_trait]
impl NetworkInterface for GrpcNetworkSet {
    async fn broadcast(
        &self,
        request: BroadcastRequest,
    ) -> Result<Vec<BroadcastResponse>, BroadCastError> {
        //dbg!("[broadcast]: Start");
        let mut responses: JoinSet<Result<BroadcastResponse, BroadCastError>> = JoinSet::new();
        let mut result = Vec::new();

        // Send PreAccept request to every known member
        // Call match only once ...
        let mut await_majority = true;
        let mut broadcast_all = false;
        match &request {
            BroadcastRequest::PreAccept(req, serial) => {
                // ... and then iterate over every member ...
                for replica in &self.members {
                    let channel = replica.channel.clone();
                    let inner = req.clone();
                    let mut request = tonic::Request::new(inner);
                    request.metadata_mut().append(
                        AsciiMetadataKey::from_bytes("NODE_SERIAL".as_bytes())?,
                        AsciiMetadataValue::from(*serial),
                    );
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
            BroadcastRequest::Recover(req) => {
                await_majority = false;
                broadcast_all = true;
                for replica in &self.members {
                    let channel = replica.channel.clone();
                    let request = req.clone();
                    responses.spawn(async move {
                        let mut client = ConsensusTransportClient::new(channel);
                        Ok(BroadcastResponse::Recover(
                            client.recover(request).await?.into_inner(),
                        ))
                    });
                }
            }
        }

        let majority = (self.members.len() / 2) + 1;
        let mut counter = 0_usize;

        // Poll majority
        // TODO: Electorates for PA ?
        if await_majority {
            while let Some(response) = responses.join_next().await {
                // TODO: Resiliency to network errors
                match response {
                    Ok(Ok(r)) => result.push(r),
                    Ok(Err(e)) => {
                        tracing::error!("Error in response: {:?}", e);
                        println!("Error in response: {:?}", e);
                        continue;
                    }
                    Err(_) => {
                        tracing::error!("Join error");
                        println!("Join error");
                        continue;
                    }
                };
                counter += 1;
                if counter >= majority {
                    break;
                }
            }
            // Try to send the request only to a majority
            tokio::spawn(async move { while responses.join_next().await.is_some() {} });
        } else {
            // TODO: Differentiate between push and forget and wait for all response
            // -> Apply vs Recover
            if broadcast_all {
                while let Some(response) = responses.join_next().await {
                    result.push(response??);
                }
            } else {
                //tokio::spawn(async move {
                while let Some(r) = responses.join_next().await {
                    match r {
                        Ok(Err(e)) => {
                            println!("Apply: Error in response: {:?}", e);
                            tracing::error!("Apply: Error in response: {:?}", e);
                        }
                        Err(_) => {
                            println!("Apply: Join error");
                            tracing::error!("Apply: Join error");
                        }
                        _ => {}
                    };
                }
                return Ok(result); // No majority needed -> return early
            }
        }

        if result.len() < majority {
            println!("Majority not reached: {:?}", result);
            return Err(BroadCastError::MajorityNotReached);
        }
        Ok(result)
    }
}
