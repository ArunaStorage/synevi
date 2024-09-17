use crate::configure_transport::init_service_server::{InitService, InitServiceServer};
use crate::configure_transport::reconfiguration_service_client::ReconfigurationServiceClient;
use crate::configure_transport::reconfiguration_service_server::{
    ReconfigurationService, ReconfigurationServiceServer,
};
use crate::configure_transport::time_service_server::TimeServiceServer;
use crate::configure_transport::{Config, JoinElectorateRequest};
use crate::consensus_transport::{RecoverRequest, RecoverResponse};
use crate::latency_service::get_latency;
use crate::reconfiguration::{Reconfiguration, ReplicaBuffer};
use crate::{
    consensus_transport::{
        consensus_transport_client::ConsensusTransportClient,
        consensus_transport_server::ConsensusTransportServer, AcceptRequest, AcceptResponse,
        ApplyRequest, ApplyResponse, CommitRequest, CommitResponse, PreAcceptRequest,
        PreAcceptResponse,
    },
    replica::{Replica, ReplicaBox},
};
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::{net::SocketAddr, sync::Arc};
use synevi_types::error::SyneviError;
use tokio::sync::{Mutex, RwLock};
use tokio::task::JoinSet;
use tonic::metadata::{AsciiMetadataKey, AsciiMetadataValue};
use tonic::transport::{Channel, Server};
use ulid::Ulid;

#[async_trait::async_trait]
pub trait NetworkInterface: Send + Sync {
    async fn broadcast(
        &self,
        request: BroadcastRequest,
    ) -> Result<Vec<BroadcastResponse>, SyneviError>;
}

#[async_trait::async_trait]
pub trait Network: Send + Sync + 'static {
    type Ni: NetworkInterface;
    async fn add_members(&self, members: Vec<(Ulid, u16, String)>);
    async fn add_member(&self, id: Ulid, serial: u16, host: String) -> Result<(), SyneviError>;
    async fn spawn_server<R: Replica + 'static + Reconfiguration>(
        &self,
        replica_server: R,
    ) -> Result<(), SyneviError>;
    async fn spawn_init_server(
        &self,
        replica_buffer: Arc<ReplicaBuffer>,
    ) -> Result<(), SyneviError>;
    async fn get_interface(&self) -> Arc<Self::Ni>;
    async fn get_waiting_time(&self, node_serial: u16) -> u64;
    async fn get_member_len(&self) -> u32;
    async fn broadcast_config(&self) -> Result<u32, SyneviError>; // Majority
}

// Blanket implementation for Arc<N> where N: Network
#[async_trait::async_trait]
impl<N> Network for Arc<N>
where
    N: Network,
{
    type Ni = N::Ni;

    async fn add_members(&self, members: Vec<(Ulid, u16, String)>) {
        self.as_ref().add_members(members).await;
    }

    async fn add_member(&self, id: Ulid, serial: u16, host: String) -> Result<(), SyneviError> {
        self.as_ref().add_member(id, serial, host).await
    }

    // TODO: Spawn onboarding server process

    async fn spawn_server<R: Replica + 'static + Reconfiguration>(
        &self,
        server: R,
    ) -> Result<(), SyneviError> {
        self.as_ref().spawn_server(server).await
    }

    async fn get_interface(&self) -> Arc<Self::Ni> {
        self.as_ref().get_interface().await
    }

    async fn get_waiting_time(&self, node_serial: u16) -> u64 {
        self.as_ref().get_waiting_time(node_serial).await
    }

    async fn get_member_len(&self) -> u32 {
        self.as_ref().get_member_len().await
    }
    async fn spawn_init_server(
        &self,
        replica_buffer: Arc<ReplicaBuffer>,
    ) -> Result<(), SyneviError> {
        self.as_ref().spawn_init_server(replica_buffer).await
    }
    async fn broadcast_config(&self) -> Result<u32, SyneviError> {
        self.as_ref().broadcast_config().await
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
    ) -> Result<Vec<BroadcastResponse>, SyneviError> {
        self.as_ref().broadcast(request).await
    }
}

#[derive(Clone, Debug, Default)]
pub struct NodeInfo {
    pub id: Ulid,
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
    pub self_info: (NodeInfo, String),
    pub members: Arc<RwLock<Vec<MemberWithLatency>>>,
    join_set: Mutex<JoinSet<Result<(), SyneviError>>>,
}

#[derive(Debug)]
pub struct GrpcNetworkSet {
    members: Vec<Arc<Member>>,
}

impl GrpcNetwork {
    pub fn new(socket_addr: SocketAddr, address: String, node_id: Ulid, node_serial: u16) -> Self {
        Self {
            socket_addr,
            self_info: (
                NodeInfo {
                    id: node_id,
                    serial: node_serial,
                },
                address,
            ),
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

    async fn add_members(&self, members: Vec<(Ulid, u16, String)>) {
        for (id, serial, host) in members {
            self.add_member(id, serial, host).await.unwrap();
        }
    }

    async fn add_member(&self, id: Ulid, serial: u16, host: String) -> Result<(), SyneviError> {
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

    async fn spawn_server<R: Replica + 'static + Reconfiguration>(
        &self,
        server: R,
    ) -> Result<(), SyneviError> {
        let new_replica_box = ReplicaBox::new(server);
        let addr = self.socket_addr;
        self.join_set.lock().await.spawn(async move {
            let builder = Server::builder()
                .add_service(ConsensusTransportServer::new(new_replica_box.clone()))
                .add_service(TimeServiceServer::new(new_replica_box.clone()))
                .add_service(ReconfigurationServiceServer::new(new_replica_box));
            builder.serve(addr).await?;
            Ok(())
        });

        let members = self.members.clone();
        self.join_set.lock().await.spawn(get_latency(members));
        Ok(())
    }

    async fn spawn_init_server(&self, server: Arc<ReplicaBuffer>) -> Result<(), SyneviError> {
        let addr = self.socket_addr;
        self.join_set.lock().await.spawn(async move {
            let builder = Server::builder()
                .add_service(ConsensusTransportServer::new(server.clone()))
                .add_service(InitServiceServer::new(server));
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

    async fn get_member_len(&self) -> u32 {
        // TODO: Impl electorates and calc majority for them
        self.members.read().await.len() as u32
    }

    async fn broadcast_config(&self) -> Result<u32, SyneviError> {
        let config = Config {
            node_serial: self.self_info.0.serial as u32,
            node_id: self.self_info.0.id.to_bytes().to_vec(),
            host: self.self_info.1.clone(),
        };
        // actual host
        let members = self.members.read().await;
        let Some(member) = members.first() else {
            return Err(SyneviError::NoMembersFound);
        };
        let channel = member.member.channel.clone();
        let request = tonic::Request::new(JoinElectorateRequest {
            config: Some(config),
        });
        let mut client = ReconfigurationServiceClient::new(channel);
        let response = client.join_electorate(request).await?;
        Ok(response.into_inner().majority)
    }
}

#[async_trait::async_trait]
impl NetworkInterface for GrpcNetworkSet {
    async fn broadcast(
        &self,
        request: BroadcastRequest,
    ) -> Result<Vec<BroadcastResponse>, SyneviError> {
        //dbg!("[broadcast]: Start");
        let mut responses: JoinSet<Result<BroadcastResponse, SyneviError>> = JoinSet::new();
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

        if result.len() < majority && !self.members.is_empty() {
            println!("Majority not reached: {:?}", result);
            return Err(SyneviError::MajorityNotReached);
        }
        Ok(result)
    }
}
