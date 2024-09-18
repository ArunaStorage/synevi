use crate::{
    configure_transport::{
        init_service_server::InitService, Config, GetEventRequest, GetEventResponse,
        JoinElectorateRequest, JoinElectorateResponse, ReadyElectorateRequest,
        ReadyElectorateResponse, ReportLastAppliedRequest, ReportLastAppliedResponse,
    },
    consensus_transport::{
        consensus_transport_server::ConsensusTransport, AcceptRequest, AcceptResponse,
        ApplyRequest, ApplyResponse, CommitRequest, CommitResponse, PreAcceptRequest,
        PreAcceptResponse, RecoverRequest, RecoverResponse,
    },
};
use std::{collections::BTreeMap, sync::Arc};
use synevi_types::{SyneviError, T, T0};
use tokio::sync::{
    mpsc::{channel, Receiver, Sender},
    Mutex,
};
use tonic::{Request, Response, Status};
use ulid::Ulid;

pub struct ReplicaBuffer {
    inner: Arc<Mutex<BTreeMap<T0, BufferedMessage>>>,
    notifier: Sender<Report>,
}

impl ReplicaBuffer {
    pub fn new(sdx: Sender<Report>) -> Self {
        ReplicaBuffer {
            inner: Arc::new(Mutex::new(BTreeMap::new())),
            notifier: sdx,
        }
    }

    pub async fn send_buffered(
        &self,
    ) -> Result<Receiver<Option<(T0, BufferedMessage)>>, SyneviError> {
        let (sdx, rcv) = channel(100);
        let inner = self.inner.clone();
        tokio::spawn(async move {
            loop {
                let mut lock = inner.lock().await;
                if let Some(event) = lock.pop_first() {
                    sdx.send(Some(event)).await.map_err(|_| {
                        SyneviError::SendError(
                            "Channel for receiving buffered messages closed".to_string(),
                        )
                    })?;
                } else {
                    sdx.send(None).await.map_err(|_| {
                        SyneviError::SendError(
                            "Channel for receiving buffered messages closed".to_string(),
                        )
                    })?;
                    break;
                }
            }
            Ok::<(), SyneviError>(())
        });
        Ok(rcv)
    }
}

#[async_trait::async_trait]
pub trait Reconfiguration {
    async fn join_electorate(
        &self,
        request: JoinElectorateRequest,
    ) -> Result<JoinElectorateResponse, SyneviError>;
    async fn get_events(
        &self,
        request: GetEventRequest,
    ) -> tokio::sync::mpsc::Receiver<Result<GetEventResponse, SyneviError>>;
    async fn ready_electorate(
        &self,
        request: ReadyElectorateRequest,
    ) -> Result<ReadyElectorateResponse, SyneviError>;
}

pub enum BufferedMessage {
    Commit(CommitRequest),
    Apply(ApplyRequest),
}

pub struct Report {
    pub node_id: Ulid,
    pub node_serial: u16,
    pub node_host: String,
    pub last_applied: T,
    pub last_applied_hash: [u8; 32],
}

#[async_trait::async_trait]
impl ConsensusTransport for Arc<ReplicaBuffer> {
    async fn pre_accept(
        &self,
        request: tonic::Request<PreAcceptRequest>,
    ) -> Result<tonic::Response<PreAcceptResponse>, tonic::Status> {
        self.as_ref().pre_accept(request).await
    }

    async fn commit(
        &self,
        request: tonic::Request<CommitRequest>,
    ) -> Result<tonic::Response<CommitResponse>, tonic::Status> {
        self.as_ref().commit(request).await
    }

    async fn accept(
        &self,
        request: tonic::Request<AcceptRequest>,
    ) -> Result<tonic::Response<AcceptResponse>, tonic::Status> {
        self.as_ref().accept(request).await
    }

    async fn apply(
        &self,
        request: tonic::Request<ApplyRequest>,
    ) -> Result<tonic::Response<ApplyResponse>, tonic::Status> {
        self.as_ref().apply(request).await
    }

    async fn recover(
        &self,
        request: tonic::Request<RecoverRequest>,
    ) -> Result<tonic::Response<RecoverResponse>, tonic::Status> {
        self.recover(request).await
    }
}

#[tonic::async_trait]
impl InitService for Arc<ReplicaBuffer> {
    async fn report_last_applied(
        &self,
        request: tonic::Request<ReportLastAppliedRequest>,
    ) -> Result<tonic::Response<ReportLastAppliedResponse>, tonic::Status> {
        self.as_ref().report_last_applied(request).await
    }
}

#[tonic::async_trait]
impl ConsensusTransport for ReplicaBuffer {
    async fn pre_accept(
        &self,
        _request: Request<PreAcceptRequest>,
    ) -> Result<Response<PreAcceptResponse>, Status> {
        Ok(Response::new(PreAcceptResponse {
            nack: true,
            ..Default::default()
        }))
    }

    async fn accept(
        &self,
        _request: Request<AcceptRequest>,
    ) -> Result<Response<AcceptResponse>, Status> {
        Ok(Response::new(AcceptResponse {
            nack: true,
            ..Default::default()
        }))
    }

    async fn commit(
        &self,
        request: Request<CommitRequest>,
    ) -> Result<Response<CommitResponse>, Status> {
        let request = request.into_inner();
        let t0 = request
            .timestamp_zero
            .as_slice()
            .try_into()
            .map_err(|_| tonic::Status::invalid_argument("Timestamp zero invalid"))?;
        self.inner
            .lock()
            .await
            .insert(t0, BufferedMessage::Commit(request));
        Ok(Response::new(CommitResponse {}))
    }

    async fn apply(
        &self,
        request: Request<ApplyRequest>,
    ) -> Result<Response<ApplyResponse>, Status> {
        let request = request.into_inner();
        let t0 = request
            .timestamp_zero
            .as_slice()
            .try_into()
            .map_err(|_| tonic::Status::invalid_argument("Timestamp zero invalid"))?;
        self.inner
            .lock()
            .await
            .insert(t0, BufferedMessage::Apply(request));
        Ok(Response::new(ApplyResponse {}))
    }

    async fn recover(
        &self,
        _request: Request<RecoverRequest>,
    ) -> Result<Response<RecoverResponse>, Status> {
        Err(tonic::Status::unimplemented(
            "Recover is not implemented for ReplicaBuffers",
        ))
    }
}

#[async_trait::async_trait]
impl InitService for ReplicaBuffer {
    async fn report_last_applied(
        &self,
        request: tonic::Request<ReportLastAppliedRequest>,
    ) -> Result<tonic::Response<ReportLastAppliedResponse>, tonic::Status> {
        let request = request.into_inner();
        let Some(Config {
            node_serial,
            node_id,
            host,
        }) = request.config
        else {
            return Err(tonic::Status::invalid_argument("Missing config"));
        };
        let report = Report {
            node_id: Ulid::from_bytes(
                node_id
                    .try_into()
                    .map_err(|_| tonic::Status::invalid_argument("Invalid node id"))?,
            ),
            node_serial: node_serial
                .try_into()
                .map_err(|_| tonic::Status::invalid_argument("Invalid node serial"))?,
            node_host: host,
            last_applied: request
                .last_applied
                .as_slice()
                .try_into()
                .map_err(|_| tonic::Status::invalid_argument("Invalid T"))?,
            last_applied_hash: request
                .last_applied_hash
                .try_into()
                .map_err(|_| tonic::Status::invalid_argument("Invalid hash"))?,
        };
        self.notifier
            .send(report)
            .await
            .map_err(|_| tonic::Status::internal("Sender closed for receiving configs"))?;
        Ok(Response::new(ReportLastAppliedResponse {}))
    }
}
