use crate::consensus_transport::{
        consensus_transport_server::ConsensusTransport, AcceptRequest, AcceptResponse,
        ApplyRequest, ApplyResponse, CommitRequest, CommitResponse, PreAcceptRequest,
        PreAcceptResponse, RecoverRequest, RecoverResponse,
    };
use std::sync::Arc;
use tokio::sync::Mutex;
use tonic::{Request, Response, Status};

pub struct ReplicaBuffer {
    inner: Arc<Mutex<Vec<BufferedMessage>>>,
}

pub enum BufferedMessage {
    Commit(CommitRequest),
    Apply(ApplyRequest),
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
        self.inner
            .lock()
            .await
            .push(BufferedMessage::Commit(request.into_inner()));
        Ok(Response::new(CommitResponse {}))
    }

    async fn apply(
        &self,
        request: Request<ApplyRequest>,
    ) -> Result<Response<ApplyResponse>, Status> {
        self.inner
            .lock()
            .await
            .push(BufferedMessage::Apply(request.into_inner()));
        Ok(Response::new(ApplyResponse {}))
    }

    async fn recover(
        &self,
        _request: Request<RecoverRequest>,
    ) -> Result<Response<RecoverResponse>, Status> {
        Err(tonic::Status::unimplemented("Recover is not implemented for ReplicaBuffers"))
    }
}
