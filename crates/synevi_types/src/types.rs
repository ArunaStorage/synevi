use ahash::RandomState;
use anyhow::{bail, Result};
use bytes::Bytes;
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

#[derive(Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Copy)]
pub enum State {
    #[default]
    Undefined = 0,
    PreAccepted = 1,
    Accepted = 2,
    Commited = 3,
    Applied = 4,
}

impl TryFrom<i32> for State {
    type Error = anyhow::Error;
    fn try_from(value: i32) -> Result<Self> {
        match value {
            1 => Ok(State::PreAccepted),
            2 => Ok(State::Accepted),
            3 => Ok(State::Commited),
            4 => Ok(State::Applied),
            _ => bail!("Invalid state"),
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
}
