use crate::coordinator::{Transaction, MAX_RETRIES};
use anyhow::{anyhow, Result};
use bytes::Bytes;
use consensus_transport::consensus_transport::{Dependency, State};
use diesel_ulid::DieselUlid;
use std::collections::{BTreeMap, HashMap};
use std::time::Duration;

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
    pub dependencies: HashMap<DieselUlid, DieselUlid>, // t and t_zero
}

impl EventStore {
    pub fn insert(&mut self, t: DieselUlid, event: Event) {
        self.temporary.insert(t, event);
    }
    pub fn upsert(&mut self, t_old: DieselUlid, transaction: &Transaction) {
        if let Some(event) = self.temporary.remove(&t_old) {
            self.insert(
                transaction.t,
                Event {
                    t_zero: transaction.t_zero,
                    state: transaction.state,
                    event: transaction.transaction.clone(),
                    ballot_number: event.ballot_number,
                    dependencies: transaction.dependencies.clone(),
                },
            );
        } else {
            self.insert(
                transaction.t,
                Event {
                    t_zero: transaction.t_zero,
                    state: transaction.state,
                    event: transaction.transaction.clone(),
                    ballot_number: 0,
                    dependencies: transaction.dependencies.clone(),
                },
            );
        }
    }
    pub fn persist(&mut self, transaction: Transaction) {
        if let Some(mut event) = self.temporary.remove(&transaction.t) {
            event.state = State::Applied;
            self.persisted.insert(transaction.t, event);
        } else {
            self.persisted.insert(transaction.t, Event {
                t_zero: transaction.t_zero,
                state: State::Applied,
                event: transaction.transaction,
                ballot_number: 0,
                dependencies: transaction.dependencies,
            });
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

    pub fn last(&self) -> Option<(DieselUlid, Event)> {
        self.temporary.iter().last().map(|(k, v)| (*k, v.clone()))
    }

    pub fn get_dependencies(&mut self, t: DieselUlid) -> Vec<Dependency> {
        self.temporary
            .range(..t)
            .filter_map(|(k, v)| {
                if v.t_zero.timestamp() < t.timestamp() {
                    Some(Dependency {
                        timestamp: k.as_byte_array().into(),
                        timestamp_zero: v.t_zero.as_byte_array().into(),
                    })
                } else {
                    None
                }
            })
            .collect()
    }

    pub fn get_tmp_by_t_zero(&mut self, t_zero: DieselUlid) -> Option<(DieselUlid, Event)> {
        self.temporary.iter().find_map(|(k, v)| {
            if v.t_zero == t_zero {
                Some((*k, v.clone()))
            } else {
                None
            }
        })
    }
    pub fn get_applied_by_t_zero(&mut self, t_zero: DieselUlid) -> Option<(DieselUlid, Event)> {
        self.persisted.iter().find_map(|(k, v)| {
            if v.t_zero == t_zero {
                Some((*k, v.clone()))
            } else {
                None
            }
        })
    }

    pub async fn wait_for_dependencies(&mut self, transaction: &mut Transaction) -> Result<()> {
        let mut wait = true;
        let mut counter: u64 = 0;
        while wait {
            let dependencies = transaction.dependencies.clone();
            for (t, t_zero) in dependencies {
                if t_zero.timestamp() > transaction.t_zero.timestamp() {
                    // Wait for commit
                    if let Some((_, Event { state, .. })) = self.get_tmp_by_t_zero(t_zero) {
                        if matches!(state, State::Commited) {
                            transaction.dependencies.remove(&t);
                        }
                    }
                } else {
                    // Wait for commit
                    if let Some(_) = self.get_applied_by_t_zero(t_zero) {
                        // Every entry in event_store.persisted is applied
                        transaction.dependencies.remove(&t);
                    }
                }
            }
            if transaction.dependencies.is_empty() {
                wait = false;
            } else {
                counter += 1;
                tokio::time::sleep(Duration::from_millis(counter.pow(2))).await;
                if counter >= MAX_RETRIES {
                    return Err(anyhow!("Dependencies did not complete"));
                } else {
                    continue;
                }
            }
        }

        Ok(())
    }
}
