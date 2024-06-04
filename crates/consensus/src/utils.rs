use std::collections::HashMap;

use anyhow::{anyhow, Result};
use diesel_ulid::DieselUlid;

use consensus_transport::consensus_transport::{
    AcceptResponse, ApplyResponse, CommitResponse, Dependency, PreAcceptResponse,
};

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

pub fn into_dependency(map: HashMap<DieselUlid, DieselUlid>) -> Vec<Dependency> {
    map.iter()
        .map(|(t, t_zero)| Dependency {
            timestamp: t.as_byte_array().into(),
            timestamp_zero: t_zero.as_byte_array().into(),
        })
        .collect()
}

pub fn from_dependency(deps: Vec<Dependency>) -> Result<HashMap<DieselUlid, DieselUlid>> {
    let mut map = HashMap::new();
    for Dependency {
        timestamp,
        timestamp_zero,
    } in deps
    {
        map.insert(
            DieselUlid::try_from(timestamp.as_slice())?,
            DieselUlid::try_from(timestamp_zero.as_slice())?,
        );
    }
    Ok(map)
}
