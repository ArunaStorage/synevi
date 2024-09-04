use ahash::RandomState;
use serde::{Deserialize, Serialize};
use sha3::{Digest, Sha3_256};
use std::{
    collections::HashSet,
    time::{SystemTime, UNIX_EPOCH},
};
use synevi_types::{Ballot, State, T, T0};

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
    pub(crate) previous_hash: Option<[u8; 32]>,
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
}

impl Event {
    pub(crate) fn hash_event(&self) -> [u8; 32] {
        let mut hasher = Sha3_256::new();
        hasher.update(Vec::<u8>::from(self.id.to_be_bytes()).as_slice());
        hasher.update(Vec::<u8>::from(self.t_zero).as_slice());
        hasher.update(Vec::<u8>::from(self.t).as_slice());
        hasher.update(Vec::<u8>::from(self.ballot).as_slice());
        hasher.update(i32::from(self.state).to_be_bytes().as_slice());
        hasher.update(self.transaction.as_slice());
        if let Some(previous_hash) = self.previous_hash {
            hasher.update(previous_hash);
        }
        // for dep in &self.dependencies {
        //     hasher.update(Vec::<u8>::from(*dep).as_slice());
        // }
        // Do we want to include ballots in our hash?
        // -> hasher.update(Vec::<u8>::from(self.ballot).as_slice());

        hasher.finalize().into()
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
        self.previous_hash
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
            previous_hash: None,
            last_updated: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap() // This must fail if the system clock is before the UNIX_EPOCH
                .as_nanos(),
        }
    }
}
