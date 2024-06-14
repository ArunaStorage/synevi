use crate::coordinator::TransactionStateMachine;
use crate::event_store::Event;
use anyhow::{anyhow, Result};
use consensus_transport::consensus_transport::{
    AcceptResponse, ApplyResponse, CommitResponse, Dependency, PreAcceptResponse,
};
use monotime::MonoTime;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Notify;

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

pub fn into_dependency(map: HashMap<MonoTime, MonoTime>) -> Vec<Dependency> {
    map.iter()
        .map(|(t, t_zero)| Dependency {
            timestamp: (*t).into(),
            timestamp_zero: (*t_zero).into(),
        })
        .collect()
}

pub fn from_dependency(deps: Vec<Dependency>) -> Result<HashMap<MonoTime, MonoTime>> {
    deps.iter()
        .map(
            |Dependency {
                 timestamp,
                 timestamp_zero,
             }|
             -> Result<(MonoTime, MonoTime)> {
                Ok((
                    MonoTime::try_from(timestamp.as_slice())?,
                    MonoTime::try_from(timestamp_zero.as_slice())?,
                ))
            },
        )
        .collect()
}

impl From<&TransactionStateMachine> for Event {
    fn from(value: &TransactionStateMachine) -> Self {
        Event {
            t_zero: value.t_zero,
            t: value.t,
            state: value.state,
            event: value.transaction.clone(),
            dependencies: value.dependencies.clone(),
            commit_notify: Arc::new(Notify::new()),
            apply_notify: Arc::new(Notify::new()),
        }
    }
}
