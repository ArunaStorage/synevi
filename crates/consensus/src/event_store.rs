use crate::{
    coordinator::TransactionStateMachine,
    error::WaitError,
    utils::{from_dependency, Ballot, T, T0},
};
use anyhow::Result;
use bytes::{BufMut, Bytes, BytesMut};
use consensus_transport::consensus_transport::{Dependency, State};
use persistence::Database;
use std::collections::{BTreeMap, HashMap};
use tokio::sync::watch;
use tokio::task::JoinSet;
use tracing::instrument;

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
    pub events: BTreeMap<T0, Event>, // Key: t0, value: Event
    pub database: Option<Database>,
    pub(crate) mappings: BTreeMap<T, T0>, // Key: t, value t0
    pub last_applied: T,                  // t of last applied entry
    pub(crate) latest_t0: T0,             // last created or recognized t0
    pub node_serial: u16,
}

#[derive(Clone, Debug)]
pub struct Event {
    pub t_zero: T0,
    pub t: T,

    // This holds the state and can be used by waiters to watch for a state change
    // In contrast to notify this can also be used if the state is already reached
    pub state: State,
    pub event: Bytes,
    pub dependencies: HashMap<T0, T>, // t and t_zero
    pub ballot: Ballot,
}

impl Event {
    pub fn as_bytes(&self) -> Bytes {
        let mut new: BytesMut = BytesMut::new();

        new.put(<[u8; 16]>::from(*self.t).as_slice());
        let state: i32 = self.state.into();
        new.put(state.to_be_bytes().as_slice());
        new.put(<[u8; 16]>::from(*self.ballot).as_slice());

        for dep in &self.dependencies {
            new.put(<[u8; 16]>::from(**dep.0).as_slice());
            new.put(<[u8; 16]>::from(**dep.1).as_slice());
        }

        new.freeze()
    }
}

impl PartialEq for Event {
    fn eq(&self, other: &Self) -> bool {
        self.t_zero == other.t_zero
            && self.t == other.t
            && self.state == other.state
            && self.event == other.event
            && self.dependencies == other.dependencies
    }
}

type WaitHandleResult = Result<JoinSet<std::result::Result<(), WaitError>>>;

#[derive(Debug, Default)]
pub(crate) struct RecoverDependencies {
    pub dependencies: Vec<Dependency>,
    pub wait: Vec<Dependency>,
    pub superseding: Vec<Dependency>,
}

impl EventStore {
    #[instrument(level = "trace")]
    pub fn init(path: Option<String>, node_serial: u16) -> Self {
        EventStore {
            events: BTreeMap::default(),
            mappings: BTreeMap::default(),
            last_applied: T::default(),
            latest_t0: T0::default(),
            database: path.map(|p| Database::new(p).unwrap()),
            node_serial,
        }
    }

    #[instrument(level = "trace")]
    async fn insert(&mut self, event: Event) {
        self.events.insert(event.t_zero, event.clone());
        self.mappings.insert(event.t, event.t_zero);
    }

    #[instrument(level = "trace")]
    pub async fn init_transaction(
        &mut self,
        body: Bytes,
        node_serial: u16,
    ) -> TransactionStateMachine {
        let t0 = self.latest_t0.next_with_node(node_serial).into_time();
        TransactionStateMachine {
            state: State::PreAccepted,
            transaction: body,
            t_zero: T0(t0),
            t: T(t0),
            dependencies: HashMap::default(),
            ballot: Ballot::default(),
        }
    }

    #[instrument(level = "trace")]
    pub async fn get_event(&self, t_zero: T0) -> Option<Event> {
        self.events.get(&t_zero).cloned()
    }

    #[instrument(level = "trace")]
    pub async fn get_or_insert(&mut self, t_zero: T0) -> Event {
        let entry = self.events.entry(t_zero).or_insert(Event {
            t_zero,
            t: T(*t_zero),
            state: State::Undefined,
            event: Default::default(),
            dependencies: HashMap::default(),
            ballot: Ballot::default(),
        });
        entry.clone()
    }

    #[instrument(level = "trace")]
    pub async fn pre_accept(
        &mut self,
        t_zero: T0,
        transaction: Bytes,
    ) -> Result<(Vec<Dependency>, T)> {
        let (t, deps) = {
            let t = T(if let Some((last_t, _)) = self.mappings.last_key_value() {
                if **last_t > *t_zero {
                    // This unwrap will not panic
                    t_zero
                        .next_with_guard_and_node(last_t, self.node_serial)
                        .into_time()
                } else {
                    *t_zero
                }
            } else {
                // No entries in the map -> insert the new event
                *t_zero
            });
            // This might not be necessary to re-use the write lock here
            let deps: Vec<Dependency> = self.get_dependencies(&t, &t_zero).await;
            (t, deps)
        };

        // This is OK because on pre_accept T == T0

        let event = Event {
            t_zero,
            t,
            state: State::PreAccepted,
            event: transaction,
            dependencies: from_dependency(deps.clone())?,
            ballot: Ballot::default(),
        };
        self.upsert(event).await;
        Ok((deps, t))
    }

    #[instrument(level = "trace")]
    pub fn get_ballot(&self, t_zero: &T0) -> Ballot {
        self.events
            .get(t_zero)
            .map(|event| event.ballot)
            .unwrap_or_default()
    }
    #[instrument(level = "trace")]
    pub fn update_ballot(&mut self, t_zero: &T0, ballot: Ballot) {
        if let Some(event) = self.events.get_mut(t_zero) {
            event.ballot = ballot;
        }
    }

    #[instrument(level = "trace")]
    pub async fn upsert(&mut self, event: Event) {
        let old_event = self.events.entry(event.t_zero).or_insert(event.clone());
        if self.latest_t0 < event.t_zero {
            self.latest_t0 = event.t_zero;
        }

        if event.state < old_event.state {
            return;
        }

        // assert!(&old_event.t <= &event.t ); Can happen if minority is left behind
        let mut update = false;
        if old_event != &event {
            update = true;
            self.mappings.remove(&old_event.t);
            old_event.t = event.t;
            old_event.dependencies = event.dependencies;
            old_event.event = event.event;
            if event.ballot > old_event.ballot {
                old_event.ballot = event.ballot;
            }
        }

        self.mappings.insert(event.t, event.t_zero);
        if old_event.state == State::Applied {
            self.last_applied = event.t;
        }

        if let Some(db) = &self.database {
            let t: Vec<u8> = (*old_event.t).into();
            if update {
                db.update(t.into(), old_event.as_bytes()).await.unwrap()
            } else {
                db.init(t.into(), old_event.event.clone(), old_event.as_bytes())
                    .await
                    .unwrap()
            }
        }
    }

    #[instrument(level = "trace")]
    pub async fn get_dependencies(&self, t: &T, t_zero: &T0) -> Vec<Dependency> {
        if self.last_applied == T::default() {
            self.events
                .range(..&T0(**t))
                .filter_map(|(_, v)| {
                    if v.t_zero == *t_zero || v.state == State::Undefined {
                        None
                    } else {
                        Some(Dependency {
                            timestamp: (*v.t).into(),
                            timestamp_zero: (*v.t_zero).into(),
                        })
                    }
                })
                .collect()
        } else if let Some(last_t0) = self.mappings.get(&self.last_applied) {
            if **last_t0 == **t {
                vec![]
            } else {
                // Range from last applied t0 to T
                // -> Get all T0s that are before our T
                self.events
                    .range(last_t0..&T0(**t))
                    .filter_map(|(_, v)| {
                        if v.t_zero == *t_zero || v.state == State::Undefined {
                            None
                        } else {
                            Some(Dependency {
                                timestamp: (*v.t).into(),
                                timestamp_zero: (*v.t_zero).into(),
                            })
                        }
                    })
                    .collect()
            }
        } else {
            vec![]
        }
    }

    #[instrument(level = "trace")]
    pub async fn get_recover_deps(&self, t: &T, t_zero: &T0) -> Result<RecoverDependencies> {
        let mut recover_deps = RecoverDependencies::default();
        for (t_dep, t_zero_dep) in self.mappings.range(self.last_applied..) {
            let dep_event = self.events.get(t_zero_dep).ok_or_else(|| {
                anyhow::anyhow!("Dependency not found for t_zero: {:?}", t_zero_dep)
            })?;
            match dep_event.state {
                State::Accepted => {
                    if dep_event
                        .dependencies
                        .iter()
                        .any(|(t_zero_dep_dep, _)| t_zero == t_zero_dep_dep)
                    {
                        // Wait -> Accord p19 l7 + l9
                        if t_zero_dep < t_zero && **t_dep > **t_zero {
                            recover_deps.wait.push(Dependency {
                                timestamp: (*t_dep).into(),
                                timestamp_zero: (*t_zero_dep).into(),
                            });
                        }
                        // Superseding -> Accord: p19 l10
                        if t_zero_dep > t_zero {
                            recover_deps.superseding.push(Dependency {
                                timestamp: (*t_dep).into(),
                                timestamp_zero: (*t_zero_dep).into(),
                            });
                        }
                    }
                }
                State::Commited => {
                    if dep_event
                        .dependencies
                        .iter()
                        .any(|(t_zero_dep_dep, _)| t_zero == t_zero_dep_dep)
                    {
                        // Superseding -> Accord: p19 l11
                        if **t_dep > **t_zero {
                            recover_deps.superseding.push(Dependency {
                                timestamp: (*t_dep).into(),
                                timestamp_zero: (*t_zero_dep).into(),
                            });
                        }
                    }
                }
                _ => {}
            }
            // Collect "normal" deps -> Accord: p19 l16
            if t_zero_dep < t_zero {
                recover_deps.dependencies.push(Dependency {
                    timestamp: (*t_dep).into(),
                    timestamp_zero: (*t_zero_dep).into(),
                });
            }
        }
        Ok(recover_deps)
    }
}
