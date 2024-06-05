use crate::coordinator::{TransactionStateMachine, MAX_RETRIES};
use anyhow::{anyhow, Result};
use bytes::Bytes;
use consensus_transport::consensus_transport::{Dependency, State};
use diesel_ulid::DieselUlid;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;

#[derive(Debug)]
pub struct EventStore {
    persisted: Arc<Mutex<BTreeMap<DieselUlid, Event>>>,
    temporary: Arc<Mutex<BTreeMap<DieselUlid, Event>>>, // key: t, Event has t0
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
    pub fn init() -> Self {
        EventStore {
            persisted: Arc::new(Mutex::new(BTreeMap::new())),
            temporary: Arc::new(Mutex::new(BTreeMap::new())),
        } 
    }
    pub async fn insert(&self, t: DieselUlid, event: Event) {
        self.temporary.lock().await.insert(t, event);
    }
    pub async fn upsert(&self, t_old: DieselUlid, transaction: &TransactionStateMachine) {
        let event = self.temporary.lock().await.remove(&t_old).clone();
        if let Some(event) =  event {
            self.insert(
                transaction.t,
                Event {
                    t_zero: transaction.t_zero,
                    state: transaction.state,
                    event: transaction.transaction.clone(),
                    ballot_number: event.ballot_number,
                    dependencies: transaction.dependencies.clone(),
                },
            ).await;
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
            ).await;
        }
    }
    pub async fn persist(&self, transaction: TransactionStateMachine) {
        let event = self.temporary.lock().await.remove(&transaction.t).clone();
        if let Some(mut event) = event {
            event.state = State::Applied;
            self.persisted.lock().await.insert(transaction.t, event);
        } else {
            self.persisted.lock().await.insert(transaction.t, Event {
                t_zero: transaction.t_zero,
                state: State::Applied,
                event: transaction.transaction,
                ballot_number: 0,
                dependencies: transaction.dependencies,
            });
        }
    }
    pub async fn search(&self, t: &DieselUlid) -> Option<Event> {
        let event = self.temporary.lock().await.get(t).cloned();
        match event {
            Some(event) => Some(event.clone()),
            None => self.persisted.lock().await.get(t).cloned(),
        }
    }
    pub async fn update_state(&self, t: &DieselUlid, state: State) -> Result<()> {
        if let Some(event) = self.temporary.lock().await.get_mut(t) {
            event.state = state;
            Ok(())
        } else {
            Err(anyhow!("Event not found"))
        }
    }

    pub async fn last(&self) -> Option<(DieselUlid, Event)> {
        self.temporary.lock().await.iter().last().map(|(k, v)| (*k, v.clone()))
    }

    pub async fn get_dependencies(&self, t: DieselUlid) -> Vec<Dependency> {
        self.temporary
            .lock()
            .await
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

    pub async fn get_tmp_by_t_zero(&self, t_zero: DieselUlid) -> Option<(DieselUlid, Event)> {
        self.temporary.lock().await.iter().find_map(|(k, v)| {
            if v.t_zero == t_zero {
                Some((*k, v.clone()))
            } else {
                None
            }
        })
    }
    pub async fn get_applied_by_t_zero(&self, t_zero: DieselUlid) -> Option<(DieselUlid, Event)> {
        self.persisted.lock().await.iter().find_map(|(k, v)| {
            if v.t_zero == t_zero {
                Some((*k, v.clone()))
            } else {
                None
            }
        })
    }

    pub async fn wait_for_dependencies(&self, transaction: &mut TransactionStateMachine) -> Result<()> {
        let mut wait = true;
        let mut counter: u64 = 0;
        while wait {
            let dependencies = transaction.dependencies.clone();
            for (t, t_zero) in dependencies {
                if t_zero.timestamp() > transaction.t_zero.timestamp() {
                    // Wait for commit
                    if let Some((_, Event { state, .. })) = self.get_tmp_by_t_zero(t_zero).await {
                        if matches!(state, State::Commited) {
                            transaction.dependencies.remove(&t);
                        }
                    }
                } else {
                    // Wait for commit
                    if let Some(_) = self.get_applied_by_t_zero(t_zero).await {
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
