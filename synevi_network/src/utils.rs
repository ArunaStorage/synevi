use synevi_types::error::SyneviError;

use crate::consensus_transport::{
    AcceptResponse, ApplyResponse, CommitResponse, PreAcceptResponse, RecoverResponse,
};

pub trait IntoInner<T> {
    fn into_inner(self) -> Result<T, SyneviError>;
}
impl IntoInner<PreAcceptResponse> for crate::network::BroadcastResponse {
    fn into_inner(self) -> Result<PreAcceptResponse, SyneviError> {
        if let crate::network::BroadcastResponse::PreAccept(response) = self {
            Ok(response)
        } else {
            Err(SyneviError::IntoInnerError)
        }
    }
}

impl IntoInner<AcceptResponse> for crate::network::BroadcastResponse {
    fn into_inner(self) -> Result<AcceptResponse, SyneviError> {
        if let crate::network::BroadcastResponse::Accept(response) = self {
            Ok(response)
        } else {
            Err(SyneviError::IntoInnerError)
        }
    }
}
impl IntoInner<CommitResponse> for crate::network::BroadcastResponse {
    fn into_inner(self) -> Result<CommitResponse, SyneviError> {
        if let crate::network::BroadcastResponse::Commit(response) = self {
            Ok(response)
        } else {
            Err(SyneviError::IntoInnerError)
        }
    }
}
impl IntoInner<ApplyResponse> for crate::network::BroadcastResponse {
    fn into_inner(self) -> Result<ApplyResponse, SyneviError> {
        if let crate::network::BroadcastResponse::Apply(response) = self {
            Ok(response)
        } else {
            Err(SyneviError::IntoInnerError)
        }
    }
}
impl IntoInner<RecoverResponse> for crate::network::BroadcastResponse {
    fn into_inner(self) -> Result<RecoverResponse, SyneviError> {
        if let crate::network::BroadcastResponse::Recover(response) = self {
            Ok(response)
        } else {
            Err(SyneviError::IntoInnerError)
        }
    }
}
