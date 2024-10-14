use crate::configure_transport::init_service_client::InitServiceClient;
use crate::configure_transport::init_service_server::InitServiceServer;
use crate::configure_transport::reconfiguration_service_client::ReconfigurationServiceClient;
use crate::configure_transport::reconfiguration_service_server::ReconfigurationServiceServer;
use crate::configure_transport::time_service_server::TimeServiceServer;
use crate::configure_transport::{
    Config, GetEventRequest, GetEventResponse, JoinElectorateRequest, ReadyElectorateRequest,
    ReportLastAppliedRequest,
};
use crate::consensus_transport::{RecoverRequest, RecoverResponse, TryRecoveryRequest};
use crate::latency_service::get_latency;
use crate::reconfiguration::Reconfiguration;
use crate::{
    consensus_transport::{
        consensus_transport_client::ConsensusTransportClient,
        consensus_transport_server::ConsensusTransportServer, AcceptRequest, AcceptResponse,
        ApplyRequest, ApplyResponse, CommitRequest, CommitResponse, PreAcceptRequest,
        PreAcceptResponse,
    },
    replica::{Replica, ReplicaBox},
};
use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::{net::SocketAddr, sync::Arc};
use synevi_types::error::SyneviError;
use synevi_types::{T, T0};
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
    async fn broadcast_recovery(&self, t0: T0) -> Result<bool, SyneviError>; // All members
}

#[async_trait::async_trait]
pub trait Network: Send + Sync + 'static {
    type Ni: NetworkInterface;
    async fn add_members(&self, members: Vec<(Ulid, u16, String)>);
    async fn add_member(
        &self,
        id: Ulid,
        serial: u16,
        host: String,
        ready: bool,
    ) -> Result<(), SyneviError>;
    async fn spawn_server<R: Replica + 'static + Reconfiguration>(
        &self,
        replica_server: R,
    ) -> Result<(), SyneviError>;
    async fn get_interface(&self) -> Arc<Self::Ni>;
    async fn get_waiting_time(&self, node_serial: u16) -> u64;
    async fn get_member_len(&self) -> u32;
    async fn broadcast_config(&self, host: String) -> Result<(u32, Vec<u8>), SyneviError>; // All members
    async fn report_config(
        &self,
        last_applied: T,
        last_applied_hash: [u8; 32],
        host: String,
    ) -> Result<(), SyneviError>;
    async fn get_stream_events(
        &self,
        last_applied: Vec<u8>,
        self_event: Vec<u8>,
    ) -> Result<tokio::sync::mpsc::Receiver<GetEventResponse>, SyneviError>;
    async fn ready_electorate(&self) -> Result<(), SyneviError>;
    async fn ready_member(&self, id: Ulid, serial: u16) -> Result<(), SyneviError>;
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

    async fn add_member(
        &self,
        id: Ulid,
        serial: u16,
        host: String,
        ready: bool,
    ) -> Result<(), SyneviError> {
        self.as_ref().add_member(id, serial, host, ready).await
    }

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
    async fn broadcast_config(&self, host: String) -> Result<(u32, Vec<u8>), SyneviError> {
        self.as_ref().broadcast_config(host).await
    }
    async fn get_stream_events(
        &self,
        last_applied: Vec<u8>,
        self_event: Vec<u8>,
    ) -> Result<tokio::sync::mpsc::Receiver<GetEventResponse>, SyneviError> {
        self.as_ref()
            .get_stream_events(last_applied, self_event)
            .await
    }

    async fn ready_electorate(&self) -> Result<(), SyneviError> {
        self.as_ref().ready_electorate().await
    }

    async fn ready_member(&self, id: Ulid, serial: u16) -> Result<(), SyneviError> {
        self.as_ref().ready_member(id, serial).await
    }

    async fn report_config(
        &self,
        last_applied: T,
        last_applied_hash: [u8; 32],
        host: String,
    ) -> Result<(), SyneviError> {
        self.as_ref()
            .report_config(last_applied, last_applied_hash, host)
            .await
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
    async fn broadcast_recovery(&self, t0: T0) -> Result<bool, SyneviError> {
        self.as_ref().broadcast_recovery(t0).await
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
    pub ready_electorate: bool,
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
}

#[derive(Debug, Clone)]
pub enum BroadcastResponse {
    PreAccept(PreAcceptResponse),
    Accept(AcceptResponse),
    Commit(CommitResponse),
    Apply(ApplyResponse),
    Recover(RecoverResponse),
}

#[derive(Debug)]
pub struct GrpcNetwork {
    pub socket_addr: SocketAddr,
    pub self_info: (NodeInfo, String),
    pub members: Arc<RwLock<HashMap<Ulid, MemberWithLatency, ahash::RandomState>>>,
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
            members: Arc::new(RwLock::new(HashMap::default())),
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
                .map(|(_, e)| e.member.clone())
                .collect(),
        })
    }
}

#[async_trait::async_trait]
impl Network for GrpcNetwork {
    type Ni = GrpcNetworkSet;

    async fn add_members(&self, members: Vec<(Ulid, u16, String)>) {
        for (id, serial, host) in members {
            self.add_member(id, serial, host, true).await.unwrap();
        }
    }

    async fn add_member(
        &self,
        id: Ulid,
        serial: u16,
        host: String,
        ready: bool,
    ) -> Result<(), SyneviError> {
        let channel = Channel::from_shared(host.clone())?.connect().await?;
        let mut writer = self.members.write().await;
        if writer.get(&id).is_none() {
            writer.insert(
                id,
                MemberWithLatency {
                    member: Arc::new(Member {
                        info: NodeInfo { id, serial },
                        host,
                        channel,
                        ready_electorate: ready,
                    }),
                    latency: AtomicU64::new(500),
                    skew: AtomicI64::new(0),
                },
            );
        }
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
                .add_service(ReconfigurationServiceServer::new(new_replica_box.clone()))
                .add_service(InitServiceServer::new(new_replica_box));
            if let Err(err) = builder.serve(addr).await {
                return Err(SyneviError::TonicTransportError(err));
            };
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
        for (_, member) in self.members.read().await.iter() {
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
        (self.members.read().await.len() + 1) as u32
    }

    async fn broadcast_config(&self, host: String) -> Result<(u32, Vec<u8>), SyneviError> {
        let config = Config {
            node_serial: self.self_info.0.serial as u32,
            node_id: self.self_info.0.id.to_bytes().to_vec(),
            host: self.self_info.1.clone(),
        };
        let channel = Channel::from_shared(host)?.connect().await?;
        let request = tonic::Request::new(JoinElectorateRequest {
            config: Some(config),
        });
        let mut client = ReconfigurationServiceClient::new(channel);
        let response = client.join_electorate(request).await?;
        let response = response.into_inner();
        Ok((response.majority, response.self_event))
    }


    async fn report_config(
        &self,
        last_applied: T,
        last_applied_hash: [u8; 32],
        host: String,
    ) -> Result<(), SyneviError> {
        let config = Config {
            node_serial: self.self_info.0.serial as u32,
            node_id: self.self_info.0.id.to_bytes().to_vec(),
            host: self.self_info.1.clone(),
        };
        let channel = Channel::from_shared(host)?.connect().await?;
        let request = tonic::Request::new(ReportLastAppliedRequest {
            config: Some(config),
            last_applied: last_applied.into(),
            last_applied_hash: last_applied_hash.into(),
        });
        let mut client = InitServiceClient::new(channel);
        let _res = client.report_last_applied(request).await?;
        Ok(())
    }

    async fn get_stream_events(
        &self,
        last_applied: Vec<u8>,
        self_event: Vec<u8>,
    ) -> Result<tokio::sync::mpsc::Receiver<GetEventResponse>, SyneviError> {
        let lock = self.members.read().await;
        let mut members = lock.iter();
        let Some((_, member)) = members.find(|(_, m)| m.member.ready_electorate) else {
            return Err(SyneviError::NoMembersFound);
        };
        let channel = member.member.channel.clone();
        let request = GetEventRequest {
            last_applied,
            self_event,
        };

        let (sdx, rcv) = tokio::sync::mpsc::channel(200);
        tokio::spawn(async move {
            let mut client = ReconfigurationServiceClient::new(channel);
            let mut response = client
                .get_events(tonic::Request::new(request))
                .await?
                .into_inner();
            loop {
                let msg = response.message().await;
                match msg {
                    Ok(Some(msg)) => {
                        sdx.send(msg).await.map_err(|_| {
                            SyneviError::SendError("Error sending event".to_string())
                        })?;
                        continue;
                    }
                    Ok(None) => return Ok(()),
                    Err(e) => return Err(SyneviError::TonicStatusError(e)),
                }
            }
        });
        Ok(rcv)
    }

    async fn ready_electorate(&self) -> Result<(), SyneviError> {
        let lock = self.members.read().await;
        let mut members = lock.iter();
        let Some((_, member)) = members.next() else {
            return Err(SyneviError::NoMembersFound);
        };
        let channel = member.member.channel.clone();
        let request = tonic::Request::new(ReadyElectorateRequest {
            node_id: self.self_info.0.id.to_bytes().to_vec(),
            node_serial: self.self_info.0.serial as u32,
        });
        let mut client = ReconfigurationServiceClient::new(channel);
        let _res = client.ready_electorate(request).await?.into_inner();
        Ok(())
    }

    async fn ready_member(&self, id: Ulid, _serial: u16) -> Result<(), SyneviError> {
        let mut lock = self.members.write().await;
        if let Some(member) = lock.get_mut(&id) {
            let new_member = Member {
                info: member.member.info.clone(),
                host: member.member.host.clone(),
                channel: member.member.channel.clone(),
                ready_electorate: true,
            };
            member.member = Arc::new(new_member);
        } else {
            return Err(SyneviError::NoMembersFound);
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl NetworkInterface for GrpcNetworkSet {
    async fn broadcast(
        &self,
        request: BroadcastRequest,
    ) -> Result<Vec<BroadcastResponse>, SyneviError> {
        let mut responses: JoinSet<Result<(bool, BroadcastResponse), SyneviError>> = JoinSet::new();
        let mut result = Vec::new();

        // Send PreAccept request to every known member
        // Call match only once ...
        let mut await_majority = true;
        let mut broadcast_all = false;
        match &request {
            BroadcastRequest::PreAccept(req, serial) => {
                // ... and then iterate over every member ...
                for replica in &self.members {
                    let ready = replica.ready_electorate;
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
                        Ok((
                            ready,
                            BroadcastResponse::PreAccept(
                                client.pre_accept(request).await?.into_inner(),
                            ),
                        ))
                    });
                }
            }
            BroadcastRequest::Accept(req) => {
                for replica in &self.members {
                    let ready = replica.ready_electorate;
                    let channel = replica.channel.clone();
                    let request = req.clone();
                    responses.spawn(async move {
                        let mut client = ConsensusTransportClient::new(channel);
                        Ok((
                            ready,
                            BroadcastResponse::Accept(client.accept(request).await?.into_inner()),
                        ))
                    });
                }
            }
            BroadcastRequest::Commit(req) => {
                for replica in &self.members {
                    let ready = replica.ready_electorate;
                    let channel = replica.channel.clone();
                    let request = req.clone();
                    responses.spawn(async move {
                        let mut client = ConsensusTransportClient::new(channel);
                        Ok((
                            ready,
                            BroadcastResponse::Commit(client.commit(request).await?.into_inner()),
                        ))
                    });
                }
            }
            BroadcastRequest::Apply(req) => {
                await_majority = false;
                for replica in &self.members {
                    let ready = replica.ready_electorate;
                    let channel = replica.channel.clone();
                    let request = req.clone();
                    responses.spawn(async move {
                        let mut client = ConsensusTransportClient::new(channel);
                        Ok((
                            ready,
                            BroadcastResponse::Apply(client.apply(request).await?.into_inner()),
                        ))
                    });
                }
            }
            BroadcastRequest::Recover(req) => {
                await_majority = false;
                broadcast_all = true;
                for replica in &self.members {
                    // TODO: Not sure if neccessary
                    let ready = replica.ready_electorate;
                    let channel = replica.channel.clone();
                    let request = req.clone();
                    responses.spawn(async move {
                        let mut client = ConsensusTransportClient::new(channel);
                        Ok((
                            ready,
                            BroadcastResponse::Recover(client.recover(request).await?.into_inner()),
                        ))
                    });
                }
            }
        }

        let all = self
            .members
            .iter()
            .filter(|member| member.ready_electorate)
            .count();
        let majority = if all == 0 { 0 } else { (all / 2) + 1 };
        let mut counter = 0_usize;

        // Poll majority
        // TODO: Electorates for PA ?
        if await_majority {
            while let Some(response) = responses.join_next().await {
                // TODO: Resiliency to network errors
                match response {
                    Ok(Ok((ready, response))) => {
                        if ready {
                            result.push(response);
                            counter += 1;
                            if counter >= majority {
                                break;
                            }
                        }
                    }
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
            }
            // Try to send the request only to a majority
            tokio::spawn(async move { while responses.join_next().await.is_some() {} });
        } else {
            // TODO: Differentiate between push and forget and wait for all response
            // -> Apply vs Recover
            if broadcast_all {
                while let Some(response) = responses.join_next().await {
                    match response {
                        Ok(Ok((_, res))) => {
                            result.push(res);
                        }
                        _ => {
                            println!("Recover: Join error");
                            tracing::error!("Recover: Join error");
                            continue;
                        }
                    }
                }
            } else {
                //tokio::spawn(async move {
                while let Some(r) = &responses.join_next().await {
                    match r {
                        Ok(Err(e)) => {
                            println!("Apply: Error in response: {:?}", e);
                            tracing::error!("Apply: Error in response: {:?}", e);
                            continue;
                        }
                        Err(_) => {
                            println!("Apply: Join error");
                            tracing::error!("Apply: Join error");
                            continue;
                        }
                        _ => {}
                    };
                }
                //});
                return Ok(result); // No majority needed -> return early
            }
        }

        if result.len() < majority && !self.members.is_empty() {
            println!("Majority not reached: {:?}/{}", result, majority);
            println!("Members: {:?}", &self.members);
            return Err(SyneviError::MajorityNotReached);
        }
        Ok(result)
    }

    async fn broadcast_recovery(&self, t0: T0) -> Result<bool, SyneviError> {
        let mut responses: JoinSet<Result<bool, SyneviError>> = JoinSet::new();
        let inner_request = TryRecoveryRequest { timestamp_zero: t0.into() };
        for replica in &self.members {
            let channel = replica.channel.clone();
            let request = tonic::Request::new(inner_request.clone());
            responses.spawn(async move {
                let mut client = ConsensusTransportClient::new(channel);
                let result = client.try_recovery(request).await?.into_inner().accepted;
                Ok(result)
            });
        }

        let mut counter = 0;
        while let Some(result) = responses.join_next().await {
            match result {
                Ok(Ok(true)) => return Ok(true),
                Ok(Ok(false)) => {
                    counter += 1;
                    continue
                },
                errors => {
                    tracing::error!("Error in broadcast try_recovery: {:?}", errors);
                    continue
                },
            }
        }

        if counter > (self.members.len() / 2) {
            Ok(false)
        }else{
            Err(SyneviError::UnrecoverableTransaction)
        }
    }
}
