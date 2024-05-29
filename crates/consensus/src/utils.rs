use anyhow::anyhow;
use bytes::Bytes;
use consensus_transport::consensus_transport::{
    AcceptResponse, ApplyResponse, CommitResponse, Dependency, PreAcceptResponse,
};
use diesel_ulid::DieselUlid;
use std::collections::HashMap;

pub trait IntoInner<T> {
    fn into_inner(self) -> anyhow::Result<T>;
}

impl IntoInner<PreAcceptResponse> for crate::coordinator::ConsensusResponse {
    fn into_inner(self) -> anyhow::Result<PreAcceptResponse> {
        if let crate::coordinator::ConsensusResponse::PreAccept(response) = self {
            Ok(response)
        } else {
            Err(anyhow!("Invalid conversion"))
        }
    }
}
impl IntoInner<AcceptResponse> for crate::coordinator::ConsensusResponse {
    fn into_inner(self) -> anyhow::Result<AcceptResponse> {
        if let crate::coordinator::ConsensusResponse::Accept(response) = self {
            Ok(response)
        } else {
            Err(anyhow!("Invalid conversion"))
        }
    }
}
impl IntoInner<CommitResponse> for crate::coordinator::ConsensusResponse {
    fn into_inner(self) -> anyhow::Result<CommitResponse> {
        if let crate::coordinator::ConsensusResponse::Commit(response) = self {
            Ok(response)
        } else {
            Err(anyhow!("Invalid conversion"))
        }
    }
}
impl IntoInner<ApplyResponse> for crate::coordinator::ConsensusResponse {
    fn into_inner(self) -> anyhow::Result<ApplyResponse> {
        if let crate::coordinator::ConsensusResponse::Apply(response) = self {
            Ok(response)
        } else {
            Err(anyhow!("Invalid conversion"))
        }
    }
}

pub fn into_dependency(map: HashMap<DieselUlid, Bytes>) -> Vec<Dependency> {
    map.iter()
        .map(|(k, v)| Dependency {
            timestamp: k.as_byte_array().into(),
            event: v.to_vec(),
        })
        .collect()
}
