use anyhow::Result;
use bytes::Bytes;
use consensus_transport::consensus_transport::{Dependency, State};
use diesel_ulid::DieselUlid;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Notify, RwLock};
use tokio::task::JoinSet;
use tokio::time::timeout;
use tracing::instrument;

static TIMEOUT: u64 = 10;
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
    events: RwLock<HashMap<DieselUlid, Event>>,         // Key: t0, value: Event
    mappings: RwLock<BTreeMap<DieselUlid, DieselUlid>>, // Key: t, value t0
    last_applied: RwLock<DieselUlid>,                   // t of last applied entry
}

#[derive(Clone, Debug)]
pub struct Event {
    pub t_zero: DieselUlid,
    pub t: DieselUlid, // maybe this should be changed to an actual timestamp
    pub state: State,
    pub event: Bytes,
    pub dependencies: HashMap<DieselUlid, DieselUlid>, // t and t_zero
    pub ballot_number: u32, // Ballot number is used for recovery assignments
    pub commit_notify: Arc<Notify>,
    pub apply_notify: Arc<Notify>,
}

impl PartialEq for Event {
    fn eq(&self, other: &Self) -> bool {
        self.t_zero == other.t_zero
            && self.t == other.t
            && self.state == other.state
            && self.event == other.event
            && self.dependencies == other.dependencies
            && self.ballot_number == other.ballot_number
    }
}

impl EventStore {
    #[instrument(level = "trace")]
    pub fn init() -> Self {
        EventStore {
            events: RwLock::new(HashMap::default()),
            mappings: RwLock::new(BTreeMap::default()),
            last_applied: RwLock::new(DieselUlid::default()),
        }
    }


    #[instrument(level = "trace")]
    async fn insert(&self, event: Event) {
        self.events
            .write()
            .await
            .insert(event.t_zero, event.clone());
        self.mappings.write().await.insert(event.t, event.t_zero);
    }


    #[instrument(level = "trace")]
    pub async fn upsert(&self, event: Event) {
        let mut lock = self.events.write().await;
        //if let Some(old_event) = self.events.write().await.get_mut(&event.t_zero) {
        let old_event = lock.entry(event.t_zero).or_insert(event.clone());
        if old_event != &event {
            self.mappings.write().await.remove(&old_event.t);
            old_event.t = event.t;
            old_event.state = event.state;
            old_event.dependencies = event.dependencies;
            old_event.event = event.event;
            self.mappings.write().await.insert(event.t, event.t_zero);
            match event.state {
                State::Commited => old_event.commit_notify.notify_waiters(),
                State::Applied => {
                    let mut last = self.last_applied.write().await;
                    *last = event.t;
                    old_event.apply_notify.notify_waiters()
                }
                _ => {}
            }
        } else {
            match event.state {
                State::Commited => event.commit_notify.notify_waiters(),
                State::Applied => {
                    let mut last = self.last_applied.write().await;
                    *last = event.t;
                    event.apply_notify.notify_waiters()
                }
                _ => {}
            }
            // dbg!("[EVENT_STORE]: Insert");
            self.mappings.write().await.insert(event.t, event.t_zero);
            //self.insert(event).await;
            // dbg!("[EVENT_STORE]: Insert successful");
        };
    }


    #[instrument(level = "trace")]
    pub async fn get(&self, t_zero: &DieselUlid) -> Option<Event> {
        self.events.read().await.get(t_zero).cloned()
    }


    #[instrument(level = "trace")]
    pub async fn last(&self) -> Option<Event> {
        if let Some((_, t_zero)) = self.mappings.read().await.last_key_value() {
            self.get(t_zero).await
        } else {
            None
        }
    }


    #[instrument(level = "trace")]
    pub async fn get_dependencies(&self, t: &DieselUlid) -> Vec<Dependency> {
        let last = self.last_applied.read().await;
        if let Some(entry) = self.last().await {
            // Check if we can build a range over t -> If there is a newer timestamp than proposed t
            if entry.t.timestamp() <= t.timestamp() {
                // ... if not, return [last..end]
                self.mappings
                    .read()
                    .await
                    .range(*last..)
                    .map(|(k, v)| Dependency {
                        timestamp: k.as_byte_array().into(),
                        timestamp_zero: v.as_byte_array().into(),
                    })
                    .collect()
            } else {
                // ... if yes, return [last..proposed_t]
                self.mappings
                    .read()
                    .await
                    .range(*last..*t)
                    .map(|(k, v)| Dependency {
                        timestamp: k.as_byte_array().into(),
                        timestamp_zero: v.as_byte_array().into(),
                    })
                    .collect()
            }
        } else {
            Vec::new()
        }
    }


    #[instrument(level = "trace")]
    pub async fn wait_for_dependencies(
        &self,
        dependencies: HashMap<DieselUlid, DieselUlid>,
        t_zero: DieselUlid,
    ) -> Result<()> {
        // Collect notifies
        let mut notifies = JoinSet::new();
        for (dep_t, dep_t_zero) in dependencies.iter() {
            if dep_t_zero.timestamp() > t_zero.timestamp() {
                let dep = self.events.read().await.get(dep_t_zero).cloned();
                if let Some(event) = dep {
                    if matches!(event.state, State::Commited | State::Applied) {
                        let notify = event.commit_notify.clone();
                        notifies.spawn(async move {
                            timeout(Duration::from_millis(TIMEOUT), notify.notified()).await
                        });
                    }
                } else {
                    let commit_notify = Arc::new(Notify::new());
                    let commit_clone = commit_notify.clone();
                    notifies.spawn(async move {
                        timeout(Duration::from_millis(TIMEOUT), commit_clone.notified()).await
                    });
                    self.insert(Event {
                        t_zero: *dep_t_zero,
                        t: *dep_t,
                        state: State::Undefined,
                        event: Default::default(),
                        dependencies: HashMap::default(),
                        ballot_number: 0,
                        commit_notify,
                        apply_notify: Arc::new(Notify::new()),
                    })
                    .await;
                }
            } else if let Some(event) = self.events.read().await.get(dep_t_zero) {
                if event.state != State::Applied {
                    let apply_clone = event.apply_notify.clone();
                    notifies.spawn(async move {
                        timeout(Duration::from_millis(TIMEOUT), apply_clone.notified()).await
                    });
                }
            } else {
                let apply_notify = Arc::new(Notify::new());
                let apply_clone = apply_notify.clone();
                notifies.spawn(async move {
                    timeout(Duration::from_millis(TIMEOUT), apply_clone.notified()).await
                });
                self.insert(Event {
                    t_zero: *dep_t_zero,
                    t: *dep_t,
                    state: State::Undefined,
                    event: Default::default(),
                    dependencies: HashMap::default(),
                    ballot_number: 0,
                    commit_notify: Arc::new(Notify::new()),
                    apply_notify,
                })
                .await;
            }
        }
        while let Some(x) = notifies.join_next().await {
            x??
            // TODO: Recovery when timeout
        }
        Ok(())
    }
}