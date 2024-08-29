use synevi_types::error::BroadCastError;

use crate::consensus_transport::{
    AcceptResponse, ApplyResponse, CommitResponse, PreAcceptResponse, RecoverResponse,
};

pub trait IntoInner<T> {
    fn into_inner(self) -> Result<T, BroadCastError>;
}
impl IntoInner<PreAcceptResponse> for crate::network::BroadcastResponse {
    fn into_inner(self) -> Result<PreAcceptResponse, BroadCastError> {
        if let crate::network::BroadcastResponse::PreAccept(response) = self {
            Ok(response)
        } else {
            Err(BroadCastError::IntoInnerError)
        }
    }
}

impl IntoInner<AcceptResponse> for crate::network::BroadcastResponse {
    fn into_inner(self) -> Result<AcceptResponse, BroadCastError> {
        if let crate::network::BroadcastResponse::Accept(response) = self {
            Ok(response)
        } else {
            Err(BroadCastError::IntoInnerError)
        }
    }
}
impl IntoInner<CommitResponse> for crate::network::BroadcastResponse {
    fn into_inner(self) -> Result<CommitResponse, BroadCastError> {
        if let crate::network::BroadcastResponse::Commit(response) = self {
            Ok(response)
        } else {
            Err(BroadCastError::IntoInnerError)
        }
    }
}
impl IntoInner<ApplyResponse> for crate::network::BroadcastResponse {
    fn into_inner(self) -> Result<ApplyResponse, BroadCastError> {
        if let crate::network::BroadcastResponse::Apply(response) = self {
            Ok(response)
        } else {
            Err(BroadCastError::IntoInnerError)

        }
    }
}
impl IntoInner<RecoverResponse> for crate::network::BroadcastResponse {
    fn into_inner(self) -> Result<RecoverResponse, BroadCastError> {
        if let crate::network::BroadcastResponse::Recover(response) = self {
            Ok(response)
        } else {
            Err(BroadCastError::IntoInnerError)
        }
    }
}
