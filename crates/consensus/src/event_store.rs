use crate::coordinator::Transaction;
use anyhow::{anyhow, Result};
use bytes::Bytes;
use consensus_transport::consensus_transport::{Dependency, State};
use diesel_ulid::DieselUlid;
use std::collections::{BTreeMap, HashSet};

pub struct EventStore {
    persisted: BTreeMap<DieselUlid, Event>,
    temporary: BTreeMap<DieselUlid, Event>, // key: t, Event has t0
}

#[derive(Clone, Debug)]
pub struct Event {
    pub t_zero: DieselUlid,
    pub state: State,
    pub event: Bytes,
    pub ballot_number: u32, // Ballot number is used for recovery assignments
    pub dependencies: HashSet<DieselUlid>,
}

impl EventStore {
    pub fn insert(&mut self, t: DieselUlid, event: Event) {
        self.temporary.insert(t, event);
    }
    pub fn update(&mut self, t: DieselUlid, transaction: &Transaction) -> Result<()> {
        if let Some(event) = self.temporary.remove(&transaction.t) {
            self.insert(
                t,
                Event {
                    t_zero: transaction.t_zero,
                    state: transaction.state,
                    event: transaction.transaction.clone(),
                    ballot_number: event.ballot_number,
                    dependencies: transaction.dependencies.clone(),
                },
            );
            Ok(())
        } else {
            Err(anyhow!("No entry found in temp"))
        }
    }
    pub fn persist(&mut self, t: &DieselUlid) -> Result<()> {
        if let Some(event) = self.temporary.remove(t) {
            self.persisted.insert(*t, event);
            Ok(())
        } else {
            Err(anyhow!("Event not found in temp"))
        }
    }
    pub fn search(&self, t: &DieselUlid) -> Option<Event> {
        match self.temporary.get(t) {
            Some(event) => Some(event.clone()),
            None => self.persisted.get(t).cloned(),
        }
    }
    pub fn update_state(&mut self, t: &DieselUlid, state: State) -> Result<()> {
        if let Some(event) = self.temporary.get_mut(t) {
            event.state = state;
            Ok(())
        } else {
            Err(anyhow!("Event not found"))
        }
    }

    pub fn get_dependencies(&mut self, t_zero: &DieselUlid) -> Vec<Dependency> {
        self.temporary
            .range(t_zero..)
            .map(|(k, v)| Dependency {
                timestamp: k.as_byte_array().into(),
            })
            .collect()
    }
}
