use crate::{coordinator::TransactionStateMachine, event_store::Event};
use ahash::RandomState;
use anyhow::Result;
use bytes::{BufMut, Bytes};
use monotime::MonoTime;
use std::{collections::HashSet, ops::Deref, time::{SystemTime, UNIX_EPOCH}};

#[derive(Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Copy)]
pub struct T0(pub MonoTime);

impl TryFrom<Bytes> for T0 {
    type Error = anyhow::Error;
    fn try_from(value: Bytes) -> Result<Self> {
        Ok(T0(MonoTime::try_from(value.as_ref())?))
    }
}

impl TryFrom<&[u8]> for T0 {
    type Error = anyhow::Error;
    fn try_from(value: &[u8]) -> Result<Self> {
        Ok(T0(MonoTime::try_from(value)?))
    }
}

impl From<T0> for Bytes {
    fn from(val: T0) -> Self {
        val.0.into()
    }
}

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

impl TryFrom<Bytes> for T {
    type Error = anyhow::Error;
    fn try_from(value: Bytes) -> Result<Self> {
        Ok(T(MonoTime::try_from(value.as_ref())?))
    }
}

impl TryFrom<&[u8]> for T {
    type Error = anyhow::Error;
    fn try_from(value: &[u8]) -> Result<Self> {
        Ok(T(MonoTime::try_from(value)?))
    }
}

impl From<T> for Bytes {
    fn from(val: T) -> Self {
        val.0.into()
    }
}

impl From<T> for Vec<u8> {
    fn from(val: T) -> Self {
        val.0.into()
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Copy)]
pub struct Ballot(pub MonoTime);
impl Deref for Ballot {
    type Target = MonoTime;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl TryFrom<Bytes> for Ballot {
    type Error = anyhow::Error;
    fn try_from(value: Bytes) -> Result<Self> {
        Ok(Ballot(MonoTime::try_from(value.as_ref())?))
    }
}

impl From<Ballot> for Bytes {
    fn from(val: Ballot) -> Self {
        val.0.into()
    }
}

impl TryFrom<&[u8]> for Ballot {
    type Error = anyhow::Error;
    fn try_from(value: &[u8]) -> Result<Self> {
        Ok(Ballot(MonoTime::try_from(value)?))
    }
}

impl From<Ballot> for Vec<u8> {
    fn from(val: Ballot) -> Self {
        val.0.into()
    }
}

pub fn into_dependency(map: &HashSet<T0, RandomState>) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(map.len() * 16);
    for t0 in map {
        bytes.put_u128(t0.0.into());
    }
    bytes
}

pub fn from_dependency(deps: Vec<u8>) -> Result<HashSet<T0, RandomState>> {
    let mut map = HashSet::default();
    for i in (0..deps.len()).step_by(16) {
        let t0 = T0(MonoTime::try_from(&deps[i..i + 16])?);
        map.insert(t0);
    }
    Ok(map)
}

impl From<&TransactionStateMachine> for Event {
    fn from(value: &TransactionStateMachine) -> Self {
        Event {
            t_zero: value.t_zero,
            t: value.t,
            state: value.state,
            event: value.transaction.clone(),
            dependencies: value.dependencies.clone(),
            ballot: value.ballot,
            ..Default::default()
        }
    }
}

//const MAX_RETRIES: u8 = 5;

// #[instrument(level = "trace")]
// pub async fn await_dependencies(
//     node: Arc<NodeInfo>,
//     store: Arc<Mutex<EventStore>>,
//     dependencies: &BTreeMap<T, T0>,
//     network_interface: Arc<dyn consensus_transport::network::NetworkInterface>,
//     t: T,
//     stats: Arc<Stats>,
//     is_coordinator: bool,
// ) -> Result<()> {
//     let mut backoff_counter = 0;
//     'outer: loop {
//         if backoff_counter > MAX_RETRIES {
//             return Err(anyhow::anyhow!("Node: {:?} reached max retries", node));
//         }
//         backoff_counter += 1;
//         let mut handles = store
//             .lock()
//             .await
//             .create_wait_handles(dependencies, t)
//             .await?;

//         while let Some(x) = handles.join_next().await {
//             match x {
//                 Ok(Ok(_)) => {}
//                 Ok(Err(e)) => match e {
//                     WaitError::Timeout(t0) => {
//                         // Wait for a node specific timeout
//                         // Await recovery for t0

//                         println!("Timeout for {:?} @ {}", t0, node.serial);
//                         CoordinatorIterator::recover(
//                             node.clone(),
//                             store.clone(),
//                             network_interface.clone(),
//                             t0,
//                             stats.clone(),
//                         )
//                         .await?;
//                         // Retry from handles
//                         continue 'outer;
//                     }
//                     WaitError::SenderClosed => {
//                         tracing::error!("Sender of transaction got closed");
//                         continue 'outer;
//                     }
//                 },
//                 Err(_) => {
//                     tracing::error!("Join error");
//                     continue 'outer;
//                 }
//             }
//         }

//         return Ok(());
//     }
// }

// const TIMEOUT: u64 = 1000; // TODO: Network dependent!! -> maximum latency

// pub fn wait_for(
//     t_request: T,
//     dependency_t0: T0,
//     sender: Sender<(State, T)>,
// ) -> impl Future<Output = Result<(), WaitError>> {
//     let mut rx = sender.subscribe();
//     async move {
//         let result = timeout(
//             Duration::from_millis(TIMEOUT),
//             rx.wait_for(|(state, dep_t)| match state {
//                 State::Commited => &t_request < dep_t,
//                 State::Applied => true,
//                 _ => false,
//             }), // Wait for any state greater or equal to expected_state
//         )
//         .await;
//         match result {
//             Ok(e) => match e {
//                 Err(_) => Err(WaitError::SenderClosed),
//                 Ok(_) => Ok(()),
//             },
//             Err(_) => Err(WaitError::Timeout(dependency_t0)),
//         }
//     }
// }
