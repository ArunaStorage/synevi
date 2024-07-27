use crate::{coordinator::TransactionStateMachine, event_store::Event};
use ahash::RandomState;
use anyhow::Result;
use bytes::{BufMut, Bytes};
use monotime::MonoTime;
use std::{collections::HashSet, ops::Deref};

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

impl<Tx> From<&TransactionStateMachine<Tx>> for Event
where
    Tx: Transaction,
{
    fn from(value: &TransactionStateMachine<Tx>) -> Self {
        Event {
            id: value.id,
            t_zero: value.t_zero,
            t: value.t,
            state: value.state,
            event: value.transaction.as_bytes(),
            dependencies: value.dependencies.clone(),
            ballot: value.ballot,
            ..Default::default()
        }
    }
}

pub trait Transaction: Default + std::fmt::Debug + Send {
    type ExecutionResult: Send;

    fn as_bytes(&self) -> Vec<u8>;
    fn from_bytes(bytes: Vec<u8>) -> Result<Self>
    where
        Self: Sized;

    fn execute(&self) -> Result<Self::ExecutionResult>;
}

pub trait Executor {
    type Tx: Transaction;

    fn execute(&self, transaction: Self::Tx) -> Result<<Self::Tx as Transaction>::ExecutionResult> {
        transaction.execute()
    }
}
