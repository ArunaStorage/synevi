use crate::consensus_transport::{
    AcceptResponse, ApplyResponse, CommitResponse, PreAcceptResponse,
};
use anyhow::anyhow;

pub trait IntoInner<T> {
    fn into_inner(self) -> anyhow::Result<T>;
}
impl IntoInner<PreAcceptResponse> for crate::network::BroadcastResponse {
    fn into_inner(self) -> anyhow::Result<PreAcceptResponse> {
        if let crate::network::BroadcastResponse::PreAccept(response) = self {
            Ok(response)
        } else {
            Err(anyhow!("Invalid conversion"))
        }
    }
}
impl IntoInner<AcceptResponse> for crate::network::BroadcastResponse {
    fn into_inner(self) -> anyhow::Result<AcceptResponse> {
        if let crate::network::BroadcastResponse::Accept(response) = self {
            Ok(response)
        } else {
            Err(anyhow!("Invalid conversion"))
        }
    }
}
impl IntoInner<CommitResponse> for crate::network::BroadcastResponse {
    fn into_inner(self) -> anyhow::Result<CommitResponse> {
        if let crate::network::BroadcastResponse::Commit(response) = self {
            Ok(response)
        } else {
            Err(anyhow!("Invalid conversion"))
        }
    }
}
impl IntoInner<ApplyResponse> for crate::network::BroadcastResponse {
    fn into_inner(self) -> anyhow::Result<ApplyResponse> {
        if let crate::network::BroadcastResponse::Apply(response) = self {
            Ok(response)
        } else {
            Err(anyhow!("Invalid conversion"))
        }
    }
}
