use crate::{
    configure_transport::{time_service_server::TimeService, GetTimeRequest, GetTimeResponse},
    consensus_transport::*,
};
use bytes::{BufMut, BytesMut};
use consensus_transport_server::ConsensusTransport;
use synevi_types::error::BroadCastError;
use std::time;
use std::{str::FromStr, sync::Arc};
use tonic::{Request, Response, Status};

#[async_trait::async_trait]
pub trait Replica: Send + Sync {
    async fn pre_accept(
        &self,
        request: PreAcceptRequest,
        node_serial: u16,
    ) -> Result<PreAcceptResponse, BroadCastError>;

    async fn accept(&self, request: AcceptRequest) -> Result<AcceptResponse, BroadCastError>;

    async fn commit(&self, request: CommitRequest) -> Result<CommitResponse, BroadCastError>;

    async fn apply(&self, request: ApplyRequest) -> Result<ApplyResponse, BroadCastError>;

    async fn recover(&self, request: RecoverRequest) -> Result<RecoverResponse, BroadCastError>;
}

pub struct ReplicaBox<R>
where
    R: Replica,
{
    inner: Arc<R>,
}

impl<R> Clone for ReplicaBox<R>
where
    R: Replica,
{
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<R> ReplicaBox<R>
where
    R: Replica,
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
    R: Replica + 'static,
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
    R: Replica + 'static,
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
}
