use std::sync::Arc;
use tonic::{Request, Response, Status};
use consensus_transport::consensus_transport::*;
use consensus_transport::consensus_transport::consensus_transport_server::ConsensusTransport;


pub struct Replica {
    backend: Arc<()>
}
#[tonic::async_trait]
impl ConsensusTransport for Replica {
    async fn pre_accept(&self, request: Request<PreAcceptRequest>) -> Result<Response<PreAcceptResponse>, Status> {
        todo!()
    }

    async fn commit(&self, request: Request<CommitRequest>) -> Result<Response<CommitResponse>, Status> {
        todo!()
    }

    async fn apply(&self, request: Request<ApplyRequest>) -> Result<Response<ApplyResponse>, Status> {
        todo!()
    }

    async fn recover(&self, request: Request<RecoverRequest>) -> Result<Response<RecoverResponse>, Status> {
        todo!()
    }
}