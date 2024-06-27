use anyhow::Result;
use bytes::Bytes;
use consensus_transport::consensus_transport::{Dependency, PreAcceptRequest, State};
use monotime::MonoTime;
use std::collections::BTreeMap;
use tokio::sync::watch;
use tokio::task::JoinSet;
use tracing::instrument;

use crate::{
    coordinator::TransactionStateMachine,
    error::WaitError,
    utils::{from_dependency, wait_for, T, T0},
};

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
    pub events: BTreeMap<T0, Event>,      // Key: t0, value: Event
    pub(crate) mappings: BTreeMap<T, T0>, // Key: t, value t0
    pub(crate) last_applied: T,           // t of last applied entry
    pub(crate) latest_t0: T0,             // last created or recognized t0
}

#[derive(Clone, Debug)]
pub struct Event {
    pub t_zero: T0,
    pub t: T, // maybe this should be changed to an actual timestamp
    // This holds the state and can be used by waiters to watch for a state change
    // In contrast to notify this can also be used if the state is already reached
    pub state: tokio::sync::watch::Sender<(State, T)>,
    pub event: Bytes,
    pub dependencies: BTreeMap<T, T0>, // t and t_zero
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

type WaitHandleResult = Result<(JoinSet<std::result::Result<(), WaitError>>, Vec<(T0, T)>)>;

impl EventStore {
    #[instrument(level = "trace")]
    pub fn init() -> Self {
        EventStore {
            events: BTreeMap::default(),
            mappings: BTreeMap::default(),
            last_applied: T::default(),
            latest_t0: T0::default(),
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
            dependencies: BTreeMap::default(),
            ballot: 0,
        }
    }

    #[instrument(level = "trace")]
    pub async fn pre_accept(&mut self, request: PreAcceptRequest) -> Result<(Vec<Dependency>, T)> {
        // Parse t_zero
        let t_zero = T0(MonoTime::try_from(request.timestamp_zero.as_slice())?);

        let (t, deps) = {
            let t = T(if let Some((last_t, _)) = self.mappings.last_key_value() {
                if **last_t > *t_zero {
                    // This unwrap will not panic
                    t_zero.next_with_guard(last_t).into_time()
                } else {
                    *t_zero
                }
            } else {
                // No entries in the map -> insert the new event
                *t_zero
            });
            self.mappings.insert(t, t_zero);
            // This might not be necessary to re-use the write lock here
            let deps: Vec<Dependency> = self.get_dependencies(&t, &t_zero).await;
            (t, deps)
        };

        // This is OK because on pre_accept T == T0
        let (tx, _) = watch::channel((State::PreAccepted, T(*t_zero)));

        let event = Event {
            t_zero,
            t,
            state: tx,
            event: request.event.into(),
            dependencies: from_dependency(deps.clone())?,
        };
        self.events.insert(t_zero, event);
        Ok((deps, t))
    }

    #[instrument(level = "trace")]
    pub async fn upsert(&mut self, event: Event) {
        let old_event = self.events.entry(event.t_zero).or_insert(event.clone());
        if self.latest_t0 < event.t_zero {
            self.latest_t0 = event.t_zero;
        }

        // assert!(&old_event.t <= &event.t ); Can happen if minority is left behind
        if old_event != &event {
            self.mappings.remove(&old_event.t);
            old_event.t = event.t;
            old_event.dependencies = event.dependencies;
            old_event.event = event.event;
        }

        self.mappings.insert(event.t, event.t_zero);
        let new_state: (State, T) = *event.state.borrow();
        old_event.state.send_modify(|old| *old = new_state)
    }

    #[instrument(level = "trace")]
    pub async fn get_dependencies(&self, t: &T, t_zero: &T0) -> Vec<Dependency> {
        if let Some(last_t0) = self.mappings.get(&self.last_applied) {
            return if **last_t0 == **t {
                vec![]
            } else {
                // Range from last applied t0 to T
                // -> Get all T0s that are before our T
                self.events
                    .range(last_t0..&T0(**t))
                    .filter_map(|(_, v)| {
                        if v.t_zero == *t_zero {
                            None
                        } else {
                            Some(Dependency {
                                timestamp: (*v.t).into(),
                                timestamp_zero: (*v.t_zero).into(),
                            })
                        }
                    })
                    .collect()
            };
        }
        vec![]
    }

    #[instrument(level = "trace")]
    pub async fn create_wait_handles(
        &mut self,
        dependencies: BTreeMap<T, T0>,
        t: T,
    ) -> WaitHandleResult {
        // TODO: Create custom error
        // Collect notifies
        let mut notifies = JoinSet::new();
        let mut result_vec = vec![];
        for (dep_t, dep_t_zero) in dependencies.iter() {
            let dep: Option<&Event> = self.events.get(dep_t_zero);

            // Check if dep is known
            if let Some(dep) = dep {
                // Dependency known
                if dep.state.borrow().0 != State::Applied {
                    result_vec.push((*dep_t_zero, *dep_t));
                    notifies.spawn(wait_for(t, dep.t_zero, dep.state.clone()));
                }
            } else {
                // Dependency unknown
                result_vec.push((*dep_t_zero, *dep_t));
                let (tx, _) = watch::channel((State::Undefined, *dep_t));
                notifies.spawn(wait_for(t, *dep_t_zero, tx.clone()));
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
        Ok((notifies, result_vec))
    }
}
