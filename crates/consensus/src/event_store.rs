use anyhow::Result;
use bytes::Bytes;
use consensus_transport::consensus_transport::{Dependency, PreAcceptRequest, State};
use monotime::MonoTime;
use std::collections::{BTreeMap, HashMap};
use tokio::sync::watch;
use tokio::task::JoinSet;
use tracing::instrument;

use crate::utils::{from_dependency, wait_for};

#[derive(Debug)]
pub struct EventStore {
    // Has both temp and persisted data
    // pros:
    //  - Only iterate once for deps
    //  - Reordering separate from events
    //  - RwLock allows less blocking for read deps
    // cons:
    //  - Updates need to consider both maps (when t is changing)
    //  - Events and mappings need clean up cron jobs and some form of consolidation
    pub events: HashMap<MonoTime, Event>, // Key: t0, value: Event
    mappings: BTreeMap<MonoTime, MonoTime>, // Key: t, value t0
    last_applied: MonoTime,               // t of last applied entry
}

#[derive(Clone, Debug)]
pub struct Event {
    pub t_zero: MonoTime,
    pub t: MonoTime, // maybe this should be changed to an actual timestamp
    // This holds the state and can be used by waiters to watch for a state change
    // In contrast to notify this can also be used if the state is already reached
    pub state: tokio::sync::watch::Sender<State>,
    pub event: Bytes,
    pub dependencies: BTreeMap<MonoTime, MonoTime>, // t and t_zero
}

impl PartialEq for Event {
    fn eq(&self, other: &Self) -> bool {
        self.t_zero == other.t_zero
            && self.t == other.t
            && *self.state.borrow() == *other.state.borrow()
            && self.event == other.event
            && self.dependencies == other.dependencies
    }
}

impl EventStore {
    #[instrument(level = "trace")]
    pub fn init() -> Self {
        EventStore {
            events: HashMap::default(),
            mappings: BTreeMap::default(),
            last_applied: MonoTime::default(),
        }
    }

    #[instrument(level = "trace")]
    async fn insert(&mut self, event: Event) {
        self.events.insert(event.t_zero, event.clone());
        self.mappings.insert(event.t, event.t_zero);
    }

    #[instrument(level = "trace")]
    pub async fn pre_accept(
        &mut self,
        request: PreAcceptRequest,
    ) -> Result<(Vec<Dependency>, MonoTime, MonoTime)> {
        // Parse t_zero
        let t_zero = MonoTime::try_from(request.timestamp_zero.as_slice())?;
        let (t, deps) = {
            let t = if let Some((last_t, _)) = self.mappings.last_key_value() {
                if last_t > &t_zero {
                    let t = t_zero.next_with_guard(last_t).into_time(); // This unwrap will not panic
                    t
                } else {
                    t_zero
                }
            } else {
                // No entries in the map -> insert the new event
                t_zero
            };
            self.mappings.insert(t, t_zero);
            // This might not be necessary to re-use the write lock here
            let deps: Vec<Dependency> = self.get_dependencies(&t).await;
            (t, deps)
        };

        let (tx, _) = watch::channel(State::PreAccepted);

        let event = Event {
            t_zero,
            t,
            state: tx,
            event: request.event.into(),
            dependencies: from_dependency(deps.clone())?,
        };
        self.events.insert(t_zero, event);
        Ok((deps, t_zero, t))
    }

    #[instrument(level = "trace")]
    pub async fn upsert(&mut self, event: Event) {
        let old_event = self.events.entry(event.t_zero).or_insert(event.clone());
        if old_event != &event {
            self.mappings.remove(&old_event.t);
            old_event.t = event.t;
            old_event.dependencies = event.dependencies;
            old_event.event = event.event;
            self.mappings.insert(event.t, event.t_zero);
            match *event.state.borrow() {
                State::Commited => {
                    old_event.state.send_replace(State::Commited);
                }
                State::Applied => {
                    self.last_applied = event.t;
                    old_event.state.send_replace(State::Applied);
                }
                _ => {}
            }
        } else {
            match *event.state.borrow() {
                State::Commited => {
                    old_event.state.send_replace(State::Commited);
                }
                State::Applied => {
                    self.last_applied = event.t;
                    old_event.state.send_replace(State::Applied);
                }
                _ => {}
            }
            self.mappings.insert(event.t, event.t_zero);
        };
    }

    #[instrument(level = "trace")]
    pub async fn get(&self, t_zero: &MonoTime) -> Option<Event> {
        self.events.get(t_zero).cloned()
    }

    #[instrument(level = "trace")]
    pub async fn last(&self) -> Option<Event> {
        if let Some((_, t_zero)) = self.mappings.last_key_value() {
            self.get(t_zero).await
        } else {
            None
        }
    }

    #[instrument(level = "trace")]
    pub async fn get_dependencies(&self, t: &MonoTime) -> Vec<Dependency> {
        if t < &self.last_applied {
            // If t is smaller than the last applied entry, return all dependencies
            println!("PROBLEM!!!!: T: {:?} < Last: {:?}", t, self.last_applied);
        }
        self.mappings
            .range(self.last_applied..*t)
            .map(|(k, v)| Dependency {
                timestamp: (*k).into(),
                timestamp_zero: (*v).into(),
            })
            .collect()
    }

    #[instrument(level = "trace")]
    pub async fn create_wait_handles(
        &mut self,
        dependencies: BTreeMap<MonoTime, MonoTime>,
        t_zero: MonoTime,
    ) -> Result<JoinSet<std::result::Result<(), anyhow::Error>>> {
        // TODO: Create custom error
        // Collect notifies
        let mut notifies = JoinSet::new();
        for (dep_t, dep_t_zero) in dependencies.iter() {
            let dep = self.events.get(dep_t_zero);

            // Wait for State::Commited if dep_t is larger than t_zero
            let wait_for_state = if dep_t > &t_zero {
                State::Commited
            } else {
                State::Applied
            };

            // Check if dep is known
            if let Some(dep) = dep {
                // Dependency known
                if *dep.state.borrow() < wait_for_state {
                    notifies.spawn(wait_for(dep.state.clone(), wait_for_state));
                }
            } else {
                // Dependency unknown
                let (tx, _) = watch::channel(State::Undefined);
                notifies.spawn(wait_for(tx.clone(), wait_for_state));
                self.insert(Event {
                    t_zero: *dep_t_zero,
                    t: *dep_t,
                    state: tx,
                    event: Default::default(),
                    dependencies: BTreeMap::default(),
                })
                .await;
            }
        }
        Ok(notifies)
    }
}
