use crate::{error::SyneviError, Executor, Transaction};
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
use ulid::Ulid;

pub type SyneviResult<E> = Result<
    ExecutorResult<<E as Executor>::Tx>,
    //Result<<<E as Executor>::Tx as Transaction>::TxOk, <<E as Executor>::Tx as Transaction>::TxErr>,
    SyneviError,
>;

#[derive(Serialize)]
pub enum ExecutorResult<T: Transaction> {
    External(Result<T::TxOk, T::TxErr>),
    Internal(Result<InternalExecution, SyneviError>),
}

#[derive(Default, PartialEq, PartialOrd, Ord, Eq, Clone, Debug, Serialize)]
pub enum TransactionPayload<T: Transaction> {
    #[default]
    None,
    External(T),
    Internal(InternalExecution),
}

#[derive(Debug, Clone, Serialize, Eq, PartialEq, PartialOrd, Ord)]
pub enum InternalExecution {
    JoinElectorate { id: Ulid, serial: u16, host: String },
    ReadyElectorate { id: Ulid, serial: u16 },
}

// #[derive(Debug, Clone, Serialize, Eq, PartialEq, PartialOrd, Ord)]

impl<T: Transaction + Serialize> Transaction for TransactionPayload<T> {
    type TxErr = SyneviError;
    type TxOk = TransactionPayload<T>;

    fn as_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        match self {
            Self::None => bytes.push(0),
            Self::External(tx) => {
                bytes.push(1);
                bytes.extend_from_slice(&tx.as_bytes());
            }
            Self::Internal(member) => {
                bytes.push(2);
                bytes.extend_from_slice(&member.as_bytes());
            }
        }
        bytes
    }

    fn from_bytes(mut bytes: Vec<u8>) -> Result<Self, SyneviError>
    where
        Self: Sized,
    {
        let chunk = bytes.split_off(1);
        match bytes.first() {
            Some(0) => Ok(Self::None),
            Some(1) => Ok(Self::External(T::from_bytes(chunk)?)),
            Some(2) => Ok(Self::Internal(InternalExecution::from_bytes(chunk)?)),
            _ => Err(SyneviError::InvalidConversionFromBytes(
                "Invalid TransactionPayload variant".to_string(),
            )),
        }
    }
}

impl Transaction for InternalExecution {
    type TxErr = SyneviError;
    type TxOk = InternalExecution;
    fn as_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();

        match self {
            InternalExecution::JoinElectorate { id, serial, host } => {
                bytes.push(0);
                bytes.extend_from_slice(&id.to_bytes());
                bytes.extend_from_slice(&serial.to_be_bytes());
                bytes.extend_from_slice(&host.as_bytes());
            }
            InternalExecution::ReadyElectorate { id, serial } => {
                bytes.push(1);
                bytes.extend_from_slice(&id.to_bytes());
                bytes.extend_from_slice(&serial.to_be_bytes());
            }
        }

        bytes
    }

    fn from_bytes(bytes: Vec<u8>) -> Result<Self, SyneviError> {
        let (first, rest) = bytes.split_at(1);
        match first.first() {
            Some(0) => {
                let (id, rest) = rest.split_at(16);
                let id = Ulid::from_bytes(id.try_into()?);
                let (serial, host) = rest.split_at(2);
                let serial = u16::from_be_bytes(
                    serial
                        .try_into()
                        .map_err(|_| SyneviError::InvalidConversionFromBytes(String::new()))?,
                );
                let host = String::from_utf8(host.to_owned())
                    .map_err(|e| SyneviError::InvalidConversionFromBytes(e.to_string()))?;
                Ok(InternalExecution::JoinElectorate { id, serial, host })
            }
            Some(1) => {
                let (id, serial) = rest.split_at(16);
                let id = Ulid::from_bytes(id.try_into()?);
                let serial = u16::from_be_bytes(
                    serial
                        .try_into()
                        .map_err(|_| SyneviError::InvalidConversionFromBytes(String::new()))?,
                );
                Ok(InternalExecution::ReadyElectorate { id, serial })
            }
            _ => Err(SyneviError::InvalidConversionFromBytes(
                "Invalid InternalExecution variant".to_string(),
            )),
        }
    }
}

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
    pub fn hash_event(&self, previous_hash: [u8; 32]) -> Hashes {
        let mut hasher = Sha3_256::new();
        hasher.update(Vec::<u8>::from(self.id.to_be_bytes()).as_slice());
        hasher.update(Vec::<u8>::from(self.t_zero).as_slice());
        hasher.update(Vec::<u8>::from(self.t).as_slice());
        hasher.update(i32::from(self.state).to_be_bytes().as_slice());
        hasher.update(self.transaction.as_slice());
        hasher.update(previous_hash);

        let event_hash = hasher.finalize().into();
        Hashes {
            previous_hash,
            transaction_hash: event_hash,
            execution_hash: [0; 32],
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
            hashes: value.execution_hash.map(|hash| Hashes {
                previous_hash: [0; 32],
                transaction_hash:[0;32],
                execution_hash: hash,
            }),
            last_updated: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap() // This must fail if the system clock is before the UNIX_EPOCH
                .as_nanos(),
        }
    }
}

#[cfg(test)]
mod test {
    use crate::{types::TransactionPayload, Transaction};

    #[tokio::test]
    async fn test_conversion() {
        let transaction = TransactionPayload::External(b"abc".to_vec());
        let bytes = transaction.as_bytes();

        assert_eq!(TransactionPayload::from_bytes(bytes).unwrap(), transaction);

        let internal_join: TransactionPayload<Vec<u8>> =
            TransactionPayload::Internal(crate::types::InternalExecution::JoinElectorate {
                id: ulid::Ulid::new(),
                serial: 1,
                host: "http://test.org:1234".to_string(),
            });
        let bytes = internal_join.as_bytes();
        assert_eq!(
            TransactionPayload::from_bytes(bytes).unwrap(),
            internal_join
        );

        let internal_ready: TransactionPayload<Vec<u8>> =
            TransactionPayload::Internal(crate::types::InternalExecution::ReadyElectorate {
                id: ulid::Ulid::new(),
                serial: 1,
            });
        let bytes = internal_ready.as_bytes();
        assert_eq!(
            TransactionPayload::from_bytes(bytes).unwrap(),
            internal_ready
        )
    }
}
