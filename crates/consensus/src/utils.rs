use crate::event_store::{Event, EventStore};
use crate::{coordinator::TransactionStateMachine, error::WaitError};
use anyhow::Result;
use consensus_transport::consensus_transport::{Dependency, State};
use futures::Future;
use monotime::MonoTime;
use std::sync::Arc;
use std::{collections::BTreeMap, ops::Deref, time::Duration};
use tokio::sync::Mutex;
use tokio::{
    sync::watch::{self, Sender},
    time::timeout,
};
use tracing::instrument;

#[derive(Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Copy)]
pub struct T0(pub MonoTime);

impl Deref for T0 {
    type Target = MonoTime;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<T0> for Vec<u8> {
    fn from(val: T0) -> Self {
        val.0.into()
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Copy)]
pub struct T(pub MonoTime);
impl Deref for T {
    type Target = MonoTime;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<T> for Vec<u8> {
    fn from(val: T) -> Self {
        val.0.into()
    }
}

pub fn into_dependency(map: BTreeMap<T, T0>) -> Vec<Dependency> {
    map.iter()
        .map(|(t, t_zero)| Dependency {
            timestamp: (**t).into(),
            timestamp_zero: (**t_zero).into(),
        })
        .collect()
}

pub fn from_dependency(deps: Vec<Dependency>) -> Result<BTreeMap<T, T0>> {
    deps.iter()
        .map(
            |Dependency {
                 timestamp,
                 timestamp_zero,
             }|
             -> Result<(T, T0)> {
                Ok((
                    T(MonoTime::try_from(timestamp.as_slice())?),
                    T0(MonoTime::try_from(timestamp_zero.as_slice())?),
                ))
            },
        )
        .collect()
}

impl From<&TransactionStateMachine> for Event {
    fn from(value: &TransactionStateMachine) -> Self {
        let (tx, _) = watch::channel((value.state, value.t));
        Event {
            t_zero: value.t_zero,
            t: value.t,
            state: tx,
            event: value.transaction.clone(),
            dependencies: value.dependencies.clone(),
        }
    }
}

#[instrument(level = "trace")]
pub async fn await_dependencies(
    store: Arc<Mutex<EventStore>>,
    dependencies: &BTreeMap<T, T0>,
    t: T,
) -> Result<()> {
    let mut handles = store
        .lock()
        .await
        .create_wait_handles(dependencies, t)
        .await?;

    while let Some(x) = handles.0.join_next().await {
        match x {
            Ok(Ok(_)) => {}
            Ok(Err(e)) => match e {
                WaitError::Timeout(_t0) => {
                    // Wait for a node specific timeout
                    // Await recovery for t0
                    // Retry from handles
                    Box::pin(await_dependencies(store.clone(), dependencies, t)).await?;
                }
                WaitError::SenderClosed => {
                    tracing::error!("Sender of transaction got closed")
                }
            },
            Err(_) => {
                tracing::error!("Join error")
            }
        }
    }
    Ok(())
}

const TIMEOUT: u64 = 100;

pub fn wait_for(
    t_request: T,
    dependency_t0: T0,
    sender: Sender<(State, T)>,
) -> impl Future<Output = Result<(), WaitError>> {
    let mut rx = sender.subscribe();
    async move {
        let result = timeout(
            Duration::from_millis(TIMEOUT),
            rx.wait_for(|(state, dep_t)| match state {
                State::Commited => &t_request < dep_t,
                State::Applied => true,
                _ => false,
            }), // Wait for any state greater or equal to expected_state
        )
        .await;
        match result {
            Ok(e) => match e {
                Err(_) => Err(WaitError::SenderClosed),
                Ok(_) => Ok(()),
            },
            Err(_) => Err(WaitError::Timeout(dependency_t0)),
        }
    }
}
