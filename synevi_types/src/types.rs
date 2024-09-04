use ahash::RandomState;
use bytes::Bytes;
use monotime::MonoTime;
use serde::{Deserialize, Serialize};
use sha3::{Digest, Sha3_256};
use std::{
    collections::HashSet,
    ops::Deref,
    time::{SystemTime, UNIX_EPOCH},
};

use crate::{error::SyneviError, Executor, Transaction};

pub type SyneviResult<E> = Result<
    Result<<<E as Executor>::Tx as Transaction>::TxOk, <<E as Executor>::Tx as Transaction>::TxErr>,
    SyneviError,
>;

#[derive(
    Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Copy, Serialize, Deserialize,
)]
pub struct T0(pub MonoTime);

impl TryFrom<Bytes> for T0 {
    type Error = SyneviError;
    fn try_from(value: Bytes) -> Result<Self, Self::Error> {
        Ok(T0(MonoTime::try_from(value.as_ref())?))
    }
}

impl TryFrom<&[u8]> for T0 {
    type Error = SyneviError;
    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
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

#[derive(
    Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Copy, Serialize, Deserialize,
)]
pub struct T(pub MonoTime);

impl Deref for T {
    type Target = MonoTime;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl TryFrom<Bytes> for T {
    type Error = SyneviError;
    fn try_from(value: Bytes) -> Result<Self, SyneviError> {
        Ok(T(MonoTime::try_from(value.as_ref())?))
    }
}

impl TryFrom<&[u8]> for T {
    type Error = SyneviError;
    fn try_from(value: &[u8]) -> Result<Self, SyneviError> {
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

#[derive(
    Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Copy, Serialize, Deserialize,
)]
pub struct Ballot(pub MonoTime);
impl Deref for Ballot {
    type Target = MonoTime;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl TryFrom<Bytes> for Ballot {
    type Error = SyneviError;
    fn try_from(value: Bytes) -> Result<Self, SyneviError> {
        Ok(Ballot(MonoTime::try_from(value.as_ref())?))
    }
}

impl From<Ballot> for Bytes {
    fn from(val: Ballot) -> Self {
        val.0.into()
    }
}

impl TryFrom<&[u8]> for Ballot {
    type Error = SyneviError;
    fn try_from(value: &[u8]) -> Result<Self, SyneviError> {
        Ok(Ballot(MonoTime::try_from(value)?))
    }
}

impl From<Ballot> for Vec<u8> {
    fn from(val: Ballot) -> Self {
        val.0.into()
    }
}

#[derive(
    Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Copy, Serialize, Deserialize,
)]
pub enum State {
    #[default]
    Undefined = 0,
    PreAccepted = 1,
    Accepted = 2,
    Commited = 3,
    Applied = 4,
}

impl From<i32> for State {
    fn from(value: i32) -> Self {
        match value {
            1 => Self::PreAccepted,
            2 => Self::Accepted,
            3 => Self::Commited,
            4 => Self::Applied,
            _ => Self::Undefined,
        }
    }
}

impl From<State> for i32 {
    fn from(val: State) -> Self {
        match val {
            State::PreAccepted => 1,
            State::Accepted => 2,
            State::Commited => 3,
            State::Applied => 4,
            _ => 0,
        }
    }
}

#[derive(Debug, Default)]
pub struct RecoverDependencies {
    pub dependencies: HashSet<T0, RandomState>,
    pub wait: HashSet<T0, RandomState>,
    pub superseding: bool,
    pub timestamp: T,
}

#[derive(Debug, Default)]
pub struct RecoverEvent {
    pub id: u128,
    pub t_zero: T0,
    pub t: T,
    pub ballot: Ballot,
    pub state: State,
    pub transaction: Vec<u8>,
    pub dependencies: HashSet<T0, RandomState>,
}

#[derive(Debug)]
pub enum RecoveryState<R> {
    RestartRecovery,
    CompetingCoordinator,
    Recovered(R),
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Hashes {
    pub previous_hash: [u8; 32],
    pub transaction_hash: [u8; 32],
    pub execution_hash: [u8; 32],
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Event {
    pub id: u128,
    pub t_zero: T0,
    pub t: T,
    pub state: State,
    pub transaction: Vec<u8>,
    pub dependencies: HashSet<T0, RandomState>,
    pub ballot: Ballot,
    #[allow(dead_code)]
    pub(crate) last_updated: u128,
    pub hashes: Option<Hashes>,
}

#[derive(Clone, Debug, Default)]
pub struct UpsertEvent {
    pub id: u128,
    pub t_zero: T0,
    pub t: T,
    pub state: State,
    pub transaction: Option<Vec<u8>>,
    pub dependencies: Option<HashSet<T0, RandomState>>,
    pub ballot: Option<Ballot>,
    pub execution_hash: Option<[u8; 32]>,
}

impl Event {
    pub fn hash_event(&self, execution_hash: [u8; 32], previous_hash: [u8; 32]) -> Hashes {
        let mut hasher = Sha3_256::new();
        hasher.update(Vec::<u8>::from(self.id.to_be_bytes()).as_slice());
        hasher.update(Vec::<u8>::from(self.t_zero).as_slice());
        hasher.update(Vec::<u8>::from(self.t).as_slice());
        hasher.update(Vec::<u8>::from(self.ballot).as_slice());
        hasher.update(i32::from(self.state).to_be_bytes().as_slice());
        hasher.update(self.transaction.as_slice());
        hasher.update(previous_hash);
        // for dep in &self.dependencies {
        //     hasher.update(Vec::<u8>::from(*dep).as_slice());
        // }
        // Do we want to include ballots in our hash?
        // -> hasher.update(Vec::<u8>::from(self.ballot).as_slice());

        let event_hash = hasher.finalize().into();
        Hashes {
            previous_hash,
            transaction_hash: event_hash,
            execution_hash,
        }
    }
    pub fn is_update(&self, upsert_event: &UpsertEvent) -> bool {
        !(self.t_zero == upsert_event.t_zero
            && self.t == upsert_event.t
            && self.state == upsert_event.state
            && upsert_event
                .transaction
                .as_ref()
                .map(|tx| tx == &self.transaction)
                .unwrap_or(true)
            && upsert_event
                .dependencies
                .as_ref()
                .map(|dep| dep == &self.dependencies)
                .unwrap_or(true)
            && upsert_event
                .ballot
                .as_ref()
                .map(|ballot| ballot == &self.ballot)
                .unwrap_or(true))
    }

    pub fn update_t(&mut self, t: T) -> Option<T> {
        if self.t != t {
            let old = self.t;
            self.t = t;
            return Some(old);
        }
        None
    }

    pub fn get_latest_hash(&self) -> Option<[u8; 32]> {
        Some(self.hashes.as_ref()?.previous_hash)
    }
}

impl PartialEq for Event {
    fn eq(&self, other: &Self) -> bool {
        self.t_zero == other.t_zero
            && self.t == other.t
            && self.state == other.state
            && self.transaction == other.transaction
            && self.dependencies == other.dependencies
            && self.ballot == other.ballot
    }
}

impl From<UpsertEvent> for Event {
    fn from(value: UpsertEvent) -> Self {
        Event {
            id: value.id,
            t_zero: value.t_zero,
            t: value.t,
            state: value.state,
            transaction: value.transaction.unwrap_or_default(),
            dependencies: value.dependencies.unwrap_or_default(),
            ballot: value.ballot.unwrap_or_default(),
            hashes: None,
            last_updated: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap() // This must fail if the system clock is before the UNIX_EPOCH
                .as_nanos(),
        }
    }
}
