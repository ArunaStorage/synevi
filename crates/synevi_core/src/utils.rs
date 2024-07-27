use crate::{coordinator::TransactionStateMachine, event_store::Event};
use ahash::RandomState;
use anyhow::Result;
use bytes::{BufMut, Bytes};
use monotime::MonoTime;
use std::{collections::HashSet, ops::Deref};

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
