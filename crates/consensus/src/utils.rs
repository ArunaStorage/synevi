use crate::coordinator::TransactionStateMachine;
use crate::event_store::Event;
use anyhow::{anyhow, Error, Result};
use consensus_transport::consensus_transport::{
    AcceptResponse, ApplyResponse, CommitResponse, Dependency, PreAcceptResponse, State,
};
use futures::Future;
use monotime::MonoTime;
use std::{collections::BTreeMap, time::Duration};
use tokio::{
    sync::watch::{self, Sender},
    time::timeout,
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

pub fn into_dependency(map: BTreeMap<MonoTime, MonoTime>) -> Vec<Dependency> {
    map.iter()
        .map(|(t, t_zero)| Dependency {
            timestamp: (*t).into(),
            timestamp_zero: (*t_zero).into(),
        })
        .collect()
}

pub fn from_dependency(deps: Vec<Dependency>) -> Result<BTreeMap<MonoTime, MonoTime>> {
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
        let (tx, _) = watch::channel(value.state);
        Event {
            t_zero: value.t_zero,
            t: value.t,
            state: tx,
            event: value.transaction.clone(),
            dependencies: value.dependencies.clone(),
        }
    }
}

const TIMEOUT: u64 = 100;

pub fn wait_for(
    sender: Sender<State>,
    expected_state: State,
) -> impl Future<Output = Result<(), Error>> {
    let mut rx = sender.subscribe();
    async move {
        let result = timeout(
            Duration::from_millis(TIMEOUT),
            rx.wait_for(|e| *e >= expected_state), // Wait for any state greater or equal to expected_state
        )
        .await;
        match result {
            Ok(e) => match e {
                Err(_) => Err(anyhow!("receive error")),
                Ok(_) => return Ok(()),
            },
            Err(_) => Err(anyhow!("TIMEOUT")),
        }
    }
}
