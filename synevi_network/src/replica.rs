use crate::{
    configure_transport::{
        init_service_server::InitService, reconfiguration_service_server::ReconfigurationService,
        time_service_server::TimeService, GetEventRequest, GetEventResponse, GetTimeRequest,
        GetTimeResponse, JoinElectorateRequest, JoinElectorateResponse, ReadyElectorateRequest,
        ReadyElectorateResponse, ReportElectorateRequest, ReportElectorateResponse,
    },
    consensus_transport::*,
    reconfiguration::Reconfiguration,
};
use bytes::{BufMut, BytesMut};
use consensus_transport_server::ConsensusTransport;
use std::time;
use std::{str::FromStr, sync::Arc};
use synevi_types::error::SyneviError;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

#[async_trait::async_trait]
pub trait Replica: Send + Sync {
    async fn pre_accept(
        &self,
        request: PreAcceptRequest,
        node_serial: u16,
    ) -> Result<PreAcceptResponse, SyneviError>;

    async fn accept(&self, request: AcceptRequest) -> Result<AcceptResponse, SyneviError>;

    async fn commit(&self, request: CommitRequest) -> Result<CommitResponse, SyneviError>;

    async fn apply(&self, request: ApplyRequest) -> Result<ApplyResponse, SyneviError>;

    async fn recover(&self, request: RecoverRequest) -> Result<RecoverResponse, SyneviError>;

    async fn try_recover(
        &self,
        request: TryRecoveryRequest,
    ) -> Result<TryRecoveryResponse, SyneviError>;
}

pub struct ReplicaBox<R>
where
    R: Replica + Reconfiguration,
{
    inner: Arc<R>,
}

impl<R> Clone for ReplicaBox<R>
where
    R: Replica + Reconfiguration,
{
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<R> ReplicaBox<R>
where
    R: Replica + Reconfiguration,
{
    pub fn new(replica: R) -> Self {
        Self {
            inner: Arc::new(replica),
        }
    }
}

#[tonic::async_trait]
impl<R> TimeService for ReplicaBox<R>
where
    R: Replica + 'static + Reconfiguration,
{
    async fn get_time(
        &self,
        request: Request<GetTimeRequest>,
    ) -> Result<Response<GetTimeResponse>, Status> {
        let mut full_buf = BytesMut::with_capacity(32);

        let time_stamp = time::SystemTime::now()
            .duration_since(time::UNIX_EPOCH)
            .unwrap() // This must fail if the system clock is before the UNIX_EPOCH
            .as_nanos();

        full_buf.put_u128(time_stamp);

        let value = request.into_inner().timestamp;
        if value.len() != 16 {
            return Err(tonic::Status::invalid_argument("Invalid time"));
        }
        let got_time = u128::from_be_bytes(
            value[0..16]
                .try_into()
                .map_err(|_| tonic::Status::invalid_argument("Invalid time"))?,
        );

        full_buf.put_i128(time_stamp as i128 - got_time as i128);

        Ok(Response::new(GetTimeResponse {
            local_timestamp: full_buf.split_to(16).freeze().to_vec(),
            diff: full_buf.freeze().to_vec(),
        }))
    }
}

#[tonic::async_trait]
impl<R> ConsensusTransport for ReplicaBox<R>
where
    R: Replica + 'static + Reconfiguration,
{
    async fn pre_accept(
        &self,
        request: Request<PreAcceptRequest>,
    ) -> Result<Response<PreAcceptResponse>, Status> {
        let (metadata, _, request) = request.into_parts();
        let serial = u16::from_str(
            metadata
                .get("NODE_SERIAL")
                .ok_or_else(|| Status::invalid_argument("No node serial specified"))?
                .to_str()
                .map_err(|_| Status::invalid_argument("Wrong node serial specified"))?,
        )
        .map_err(|_| Status::invalid_argument("Wrong node serial specified"))?;

        Ok(Response::new(
            self.inner
                .pre_accept(request, serial)
                .await
                .map_err(|e| Status::internal(e.to_string()))?,
        ))
    }

    async fn accept(
        &self,
        request: Request<AcceptRequest>,
    ) -> Result<Response<AcceptResponse>, Status> {
        Ok(Response::new(
            self.inner
                .accept(request.into_inner())
                .await
                .map_err(|e| tonic::Status::internal(e.to_string()))?,
        ))
    }

    async fn commit(
        &self,
        request: Request<CommitRequest>,
    ) -> Result<Response<CommitResponse>, Status> {
        Ok(Response::new(
            self.inner
                .commit(request.into_inner())
                .await
                .map_err(|e| tonic::Status::internal(e.to_string()))?,
        ))
    }

    async fn apply(
        &self,
        request: Request<ApplyRequest>,
    ) -> Result<Response<ApplyResponse>, Status> {
        Ok(Response::new(
            self.inner
                .apply(request.into_inner())
                .await
                .map_err(|e| tonic::Status::internal(e.to_string()))?,
        ))
    }

    async fn recover(
        &self,
        request: Request<RecoverRequest>,
    ) -> Result<Response<RecoverResponse>, Status> {
        Ok(Response::new(
            self.inner
                .recover(request.into_inner())
                .await
                .map_err(|e| tonic::Status::internal(e.to_string()))?,
        ))
    }

    async fn try_recovery(
        &self,
        request: Request<TryRecoveryRequest>,
    ) -> Result<Response<TryRecoveryResponse>, Status> {
        Ok(Response::new(
            self.inner
                .try_recover(request.into_inner())
                .await
                .map_err(|e| tonic::Status::internal(e.to_string()))?,
        ))
    }
}

#[async_trait::async_trait]
impl<R: Replica + 'static + Reconfiguration> ReconfigurationService for ReplicaBox<R> {
    async fn join_electorate(
        &self,
        request: tonic::Request<JoinElectorateRequest>,
    ) -> Result<tonic::Response<JoinElectorateResponse>, tonic::Status> {
        Ok(Response::new(
            self.inner
                .join_electorate(request.into_inner())
                .await
                .map_err(|e| tonic::Status::internal(e.to_string()))?,
        )) // TODO: Replace unwrap
    }

    type GetEventsStream = ReceiverStream<Result<GetEventResponse, tonic::Status>>;

    async fn get_events(
        &self,
        request: tonic::Request<GetEventRequest>,
    ) -> Result<tonic::Response<Self::GetEventsStream>, tonic::Status> {
        let mut receiver = self
            .inner
            .get_events(request.into_inner())
            .await
            .map_err(|err| tonic::Status::internal(err.to_string()))?;

        let (sdx, rcv) = tokio::sync::mpsc::channel(200);
        tokio::spawn(async move {
            while let Some(event) = receiver.recv().await {
                sdx.send(event.map_err(|e| tonic::Status::internal(e.to_string())))
                    .await
                    .map_err(|_e| tonic::Status::internal("Sender closed"))
                    .unwrap();
            }
        });

        let stream = ReceiverStream::new(rcv);

        Ok(Response::new(stream))
    }

    async fn ready_electorate(
        &self,
        request: tonic::Request<ReadyElectorateRequest>,
    ) -> Result<tonic::Response<ReadyElectorateResponse>, tonic::Status> {
        Ok(Response::new(
            self.inner
                .ready_electorate(request.into_inner())
                .await
                .map_err(|e| tonic::Status::internal(e.to_string()))?,
        ))
    }
}
#[async_trait::async_trait]
impl<R: Replica + 'static + Reconfiguration> InitService for ReplicaBox<R> {
    async fn report_electorate(
        &self,
        request: tonic::Request<ReportElectorateRequest>,
    ) -> Result<tonic::Response<ReportElectorateResponse>, tonic::Status> {
        Ok(Response::new(
            self.inner
                .report_electorate(request.into_inner())
                .await
                .map_err(|e| tonic::Status::internal(e.to_string()))?,
        ))
    }
}
