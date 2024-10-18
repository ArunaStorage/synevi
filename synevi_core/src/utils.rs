use crate::coordinator::TransactionStateMachine;
use ahash::RandomState;
use bytes::BufMut;
use monotime::MonoTime;
use serde::Serialize;
use std::collections::HashSet;
use synevi_types::{types::UpsertEvent, SyneviError, Transaction, T0};

pub fn into_dependency(map: &HashSet<T0, RandomState>) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(map.len() * 16);
    for t0 in map {
        bytes.put_u128(t0.0.into());
    }
    bytes
}

pub fn from_dependency(deps: Vec<u8>) -> Result<HashSet<T0, RandomState>, SyneviError> {
    let mut map = HashSet::default();
    for i in (0..deps.len()).step_by(16) {
        let t0 = T0(MonoTime::try_from(&deps[i..i + 16])?);
        map.insert(t0);
    }
    Ok(map)
}

impl<Tx> From<&TransactionStateMachine<Tx>> for UpsertEvent
where
    Tx: Transaction + Serialize,
{
    fn from(value: &TransactionStateMachine<Tx>) -> Self {
        UpsertEvent {
            id: value.id,
            t_zero: value.t_zero,
            t: value.t,
            state: value.state,
            transaction: Some(value.transaction.as_bytes()),
            dependencies: Some(value.dependencies.clone()),
            ballot: Some(value.ballot),
            hashes: value.hashes.clone(),
        }
    }
}
