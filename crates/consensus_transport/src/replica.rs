use std::sync::Arc;

use crate::consensus_transport::*;
use anyhow::Result;
use consensus_transport_server::ConsensusTransport;
use tonic::{Request, Response, Status};

#[async_trait::async_trait]
pub trait Replica: std::fmt::Debug + Send + Sync {
    async fn pre_accept(&self, request: PreAcceptRequest) -> Result<PreAcceptResponse>;

    async fn accept(&self, request: AcceptRequest) -> Result<AcceptResponse>;

    async fn commit(&self, request: CommitRequest) -> Result<CommitResponse>;

    async fn apply(&self, request: ApplyRequest) -> Result<ApplyResponse>;

    async fn recover(&self, request: RecoverRequest) -> Result<RecoverResponse>;
}

#[derive(Debug)]
pub struct ReplicaBox {
    inner: Arc<dyn Replica>,
}

impl ReplicaBox {
    pub fn new(replica: Arc<dyn Replica>) -> Self {
        Self { inner: replica }
    }
}

#[tonic::async_trait]
impl ConsensusTransport for ReplicaBox {
    async fn pre_accept(
        &self,
        request: Request<PreAcceptRequest>,
    ) -> Result<Response<PreAcceptResponse>, Status> {
        Ok(Response::new(
            self.inner
                .pre_accept(request.into_inner())
                .await
                .map_err(|e| tonic::Status::internal(e.to_string()))?,
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
