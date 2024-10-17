use ahash::RandomState;
use monotime::MonoTime;
use std::collections::{BTreeMap, HashSet};
use std::fmt::Debug;
use std::sync::{Arc, Mutex};
use synevi_types::error::SyneviError;
use synevi_types::traits::{Dependencies, Store};
use synevi_types::types::RecoverEvent;
use synevi_types::types::{Event, Hashes, RecoverDependencies, UpsertEvent};
use synevi_types::State;
use synevi_types::{Ballot, T, T0};
use tokio::sync::mpsc::{Receiver, Sender};
use tracing::instrument;

#[derive(Debug, Clone)]
struct InternalStore {
    pub events: BTreeMap<T0, Event>,      // Key: t0, value: Event
    pub(crate) mappings: BTreeMap<T, T0>, // Key: t, value t0
    pub last_applied: T,                  // t of last applied entry
    pub(crate) latest_time: MonoTime,     // last created or recognized time
    pub node_serial: u16,
    latest_hash: [u8; 32],
}

#[derive(Debug)]
pub struct MemStore {
    store: Arc<Mutex<InternalStore>>,
}

impl MemStore {
    #[instrument(level = "trace")]
    pub fn new(node_serial: u16) -> Result<Self, SyneviError> {
        let store = Arc::new(Mutex::new(InternalStore {
            events: BTreeMap::default(),
            mappings: BTreeMap::default(),
            last_applied: T::default(),
            latest_time: MonoTime::default(),
            node_serial,
            latest_hash: [0; 32],
        }));
        Ok(MemStore { store })
    }
}

impl Store for MemStore {
    fn init_t_zero(&self, node_serial: u16) -> T0 {
        self.store
            .lock()
            .expect("poisoned lock, aborting")
            .init_t_zero(node_serial)
    }

    fn pre_accept_tx(
        &self,
        id: u128,
        t_zero: T0,
        transaction: Vec<u8>,
    ) -> Result<(T, Dependencies), SyneviError> {
        self.store
            .lock()
            .expect("poisoned lock, aborting")
            .pre_accept_tx(id, t_zero, transaction)
    }

    fn get_tx_dependencies(&self, t: &T, t_zero: &T0) -> Dependencies {
        self.store
            .lock()
            .expect("poisoned lock, aborting")
            .get_tx_dependencies(t, t_zero)
    }

    fn get_recover_deps(&self, t_zero: &T0) -> Result<RecoverDependencies, SyneviError> {
        self.store
            .lock()
            .expect("poisoned lock, aborting")
            .get_recover_deps(t_zero)
    }

    fn recover_event(
        &self,
        t_zero_recover: &T0,
        node_serial: u16,
    ) -> Result<Option<RecoverEvent>, SyneviError> {
        self.store
            .lock()
            .expect("poisoned lock, aborting")
            .recover_event(t_zero_recover, node_serial)
    }

    fn accept_tx_ballot(&self, t_zero: &T0, ballot: Ballot) -> Option<Ballot> {
        self.store
            .lock()
            .expect("poisoned lock, aborting")
            .accept_tx_ballot(t_zero, ballot)
    }

    fn upsert_tx(&self, upsert_event: UpsertEvent) -> Result<(), SyneviError> {
        self.store
            .lock()
            .expect("poisoned lock, aborting")
            .upsert_tx(upsert_event)
    }

    fn get_event_state(&self, t_zero: &T0) -> Option<State> {
        self.store
            .lock()
            .expect("poisoned lock, aborting")
            .get_event_state(t_zero)
    }

    fn get_event_store(&self) -> BTreeMap<T0, Event> {
        self.store
            .lock()
            .expect("poisoned lock, aborting")
            .get_event_store()
    }

    fn last_applied(&self) -> (T, T0) {
        self.store
            .lock()
            .expect("poisoned lock, aborting")
            .last_applied()
    }

    fn get_events_after(
        &self,
        last_applied: T,
    ) -> Result<Receiver<Result<Event, SyneviError>>, SyneviError> {
        let (sdx, rcv) = tokio::sync::mpsc::channel(100);

        let store = self.store.clone();
        tokio::task::spawn_blocking(move || {
            store
                .lock()
                .expect("poisoned lock, aborting")
                .get_events_after(last_applied, sdx)?;
            Ok::<(), SyneviError>(())
        });

        Ok(rcv)
    }
    fn get_event(&self, t_zero: T0) -> Result<Option<Event>, SyneviError> {
        Ok(self
            .store
            .lock()
            .expect("poisoned lock, aborting")
            .events
            .get(&t_zero)
            .cloned())
    }

    fn get_and_update_hash(
        &self,
        t_zero: T0,
        execution_hash: [u8; 32],
    ) -> Result<Hashes, SyneviError> {
        let mut lock = self.store.lock().expect("poisoned lock, aborting");
        if let Some(event) = lock.events.get_mut(&t_zero) {
            let hashes = event
                .hashes
                .as_mut()
                .ok_or_else(|| SyneviError::MissingTransactionHash)?;
            hashes.execution_hash = execution_hash;
            Ok(hashes.clone())
        } else {
            Err(SyneviError::EventNotFound(t_zero.get_inner()))
        }
    }

    fn last_applied_hash(&self) -> Result<(T, [u8; 32]), SyneviError> {
        let lock = self.store.lock().expect("poisoned lock, aborting");
        let last = lock.last_applied;
        let last_t0 = lock
            .mappings
            .get(&last)
            .ok_or_else(|| SyneviError::EventNotFound(last.get_inner()))?;
        let hash = lock
            .events
            .get(last_t0)
            .cloned()
            .ok_or_else(|| SyneviError::EventNotFound(last.get_inner()))?
            .hashes
            .ok_or_else(|| SyneviError::MissingExecutionHash)?;
        Ok((last, hash.execution_hash))
    }

    fn inc_time_with_guard(&self, guard: T0) -> Result<(), SyneviError> {
        let mut lock = self.store.lock().expect("poisoned lock, aborting");
        lock.latest_time = lock
            .latest_time
            .next_with_guard_and_node(&guard, lock.node_serial)
            .into_time();
        Ok(())
    }
}

impl InternalStore {
    #[instrument(level = "trace")]
    fn init_t_zero(&mut self, node_serial: u16) -> T0 {
        let next_time = self.latest_time.next_with_node(node_serial).into_time();
        self.latest_time = next_time;
        T0(next_time)
    }

    #[instrument(level = "trace")]
    fn pre_accept_tx(
        &mut self,
        id: u128,
        t_zero: T0,
        transaction: Vec<u8>,
    ) -> Result<(T, HashSet<T0, RandomState>), SyneviError> {
        let (t, deps) = {
            let t = if self.latest_time > *t_zero {
                let new_time_t = t_zero
                    .next_with_guard_and_node(&self.latest_time, self.node_serial)
                    .into_time();

                self.latest_time = new_time_t;
                T(new_time_t)
            } else {
                T(*t_zero)
            };
            // This might not be necessary to re-use the write lock here
            let deps = self.get_tx_dependencies(&t, &t_zero);
            (t, deps)
        };

        let event = UpsertEvent {
            id,
            t_zero,
            t,
            state: State::PreAccepted,
            transaction: Some(transaction),
            dependencies: Some(deps.clone()),
            ..Default::default()
        };
        self.upsert_tx(event)?;
        Ok((t, deps))
    }

    #[instrument(level = "trace")]
    fn get_tx_dependencies(&self, t: &T, t_zero: &T0) -> HashSet<T0, RandomState> {
        if &self.last_applied == t {
            return HashSet::default();
        }
        assert!(self.last_applied < *t);
        // What about deps with dep_t0 < last_applied_t0 && dep_t > t?
        let mut deps = HashSet::default();

        // Dependencies are where any of these cases match:
        // - t_dep < t if not applied
        // - t0_dep < t0_last_applied, if t_dep > t0
        // - t_dep > t if t0_dep < t
        for (_, t0_dep) in self.mappings.range(self.last_applied..) {
            if t0_dep != t_zero && (t0_dep < &T0(**t)) {
                deps.insert(*t0_dep);
            }
        }
        deps
    }

    #[instrument(level = "trace")]
    fn accept_tx_ballot(&mut self, t_zero: &T0, ballot: Ballot) -> Option<Ballot> {
        let event = self.events.get_mut(t_zero)?;

        if event.ballot < ballot {
            event.ballot = ballot;
        }

        Some(event.ballot)
    }

    #[instrument(level = "trace")]
    fn upsert_tx(&mut self, upsert_event: UpsertEvent) -> Result<(), SyneviError> {
        // Update the latest time
        if self.latest_time < *upsert_event.t {
            self.latest_time = *upsert_event.t;
        }

        let Some(event) = self.events.get_mut(&upsert_event.t_zero) else {
            let mut event = Event::from(upsert_event.clone());
            if matches!(event.state, State::Applied) {
                self.mappings.insert(event.t, event.t_zero);
                if let Some(deps) = upsert_event.dependencies {
                    event.dependencies = deps;
                }
                if let Some(transaction) = upsert_event.transaction {
                    if event.transaction.is_empty() && !transaction.is_empty() {
                        event.transaction = transaction;
                    }
                }
                event.state = upsert_event.state;
                if let Some(ballot) = upsert_event.ballot {
                    if event.ballot < ballot {
                        event.ballot = ballot;
                    }
                }
                assert!(self.last_applied < event.t);
                self.last_applied = event.t;
                let hashes = event.hash_event(self.latest_hash);
                self.latest_hash = hashes.transaction_hash;
                event.hashes = Some(hashes);
                self.events.insert(upsert_event.t_zero, event);
            } else {
                self.events.insert(upsert_event.t_zero, event);
                self.mappings.insert(upsert_event.t, upsert_event.t_zero);
            }

            return Ok(());
        };

        // Do not update to a "lower" state
        if upsert_event.state < event.state {
            return Ok(());
        }

        // Event is already applied
        if event.state == State::Applied {
            return Ok(());
        }

        if event.is_update(&upsert_event) {
            if let Some(old_t) = event.update_t(upsert_event.t) {
                self.mappings.remove(&old_t);
                self.mappings.insert(event.t, event.t_zero);
            }
            if let Some(deps) = upsert_event.dependencies {
                event.dependencies = deps;
            }
            if let Some(transaction) = upsert_event.transaction {
                if event.transaction.is_empty() && !transaction.is_empty() {
                    event.transaction = transaction;
                }
            }
            event.state = upsert_event.state;
            if let Some(ballot) = upsert_event.ballot {
                if event.ballot < ballot {
                    event.ballot = ballot;
                }
            }

            if event.state == State::Applied {
                assert!(self.last_applied < event.t);
                self.last_applied = event.t;
                let hashes = event.hash_event(self.latest_hash);
                self.latest_hash = hashes.transaction_hash;
                event.hashes = Some(hashes);
            };

            Ok(())
        } else {
            Ok(())
        }
    }

    #[instrument(level = "trace")]
    fn get_recover_deps(&self, t_zero: &T0) -> Result<RecoverDependencies, SyneviError> {
        let mut recover_deps = RecoverDependencies {
            timestamp: self
                .events
                .get(t_zero)
                .map(|event| event.t)
                .ok_or_else(|| SyneviError::EventNotFound(t_zero.get_inner()))?,
            ..Default::default()
        };
        for (t_dep, t_zero_dep) in self.mappings.range(self.last_applied..) {
            let dep_event = self
                .events
                .get(t_zero_dep)
                .ok_or_else(|| SyneviError::DependencyNotFound(t_zero_dep.get_inner()))?;
            match dep_event.state {
                State::Accepted => {
                    if dep_event
                        .dependencies
                        .iter()
                        .any(|t_zero_dep_dep| t_zero == t_zero_dep_dep)
                    {
                        // Wait -> Accord p19 l7 + l9
                        if t_zero_dep < t_zero && **t_dep > **t_zero {
                            recover_deps.wait.insert(*t_zero_dep);
                        }
                        // Superseding -> Accord: p19 l10
                        if t_zero_dep > t_zero {
                            recover_deps.superseding = true;
                        }
                    }
                }
                State::Commited => {
                    if dep_event
                        .dependencies
                        .iter()
                        .any(|t_zero_dep_dep| t_zero == t_zero_dep_dep)
                    {
                        // Superseding -> Accord: p19 l11
                        if **t_dep > **t_zero {
                            recover_deps.superseding = true;
                        }
                    }
                }
                _ => {}
            }
            // Collect "normal" deps -> Accord: p19 l16
            if t_zero_dep < t_zero {
                recover_deps.dependencies.insert(*t_zero_dep);
            }
        }
        Ok(recover_deps)
    }

    fn get_event_state(&self, t_zero: &T0) -> Option<State> {
        self.events.get(t_zero).map(|event| event.state)
    }

    fn recover_event(
        &mut self,
        t_zero_recover: &T0,
        node_serial: u16,
    ) -> Result<Option<RecoverEvent>, SyneviError> {
        let Some(state) = self.get_event_state(t_zero_recover) else {
            return Ok(None);
        };
        if matches!(state, synevi_types::State::Undefined) {
            return Err(SyneviError::UndefinedRecovery);
        }

        if let Some(event) = self.events.get_mut(t_zero_recover) {
            event.ballot = Ballot(event.ballot.next_with_node(node_serial).into_time());

            Ok(Some(RecoverEvent {
                id: event.id,
                t_zero: event.t_zero,
                t: event.t,
                state,
                transaction: event.transaction.clone(),
                dependencies: event.dependencies.clone(),
                ballot: event.ballot,
            }))
        } else {
            Ok(None)
        }
    }

    fn get_event_store(&self) -> BTreeMap<T0, Event> {
        self.events.clone()
    }

    fn last_applied(&mut self) -> (T, T0) {
        let t0 = self
            .mappings
            .get(&self.last_applied)
            .cloned()
            .unwrap_or(T0::default());
        (self.last_applied, t0)
    }

    fn get_events_after(
        &self,
        last_applied: T,
        sdx: Sender<Result<Event, SyneviError>>,
    ) -> Result<(), SyneviError> {
        let last_applied_t0 = match self.mappings.get(&last_applied) {
            Some(t0) => *t0,
            None if last_applied == T::default() => T0::default(),
            _ => return Err(SyneviError::EventNotFound(last_applied.get_inner())),
        };
        for (_, event) in self.events.range(last_applied_t0..) {
            sdx.blocking_send(Ok(event.clone()))
                .map_err(|e| SyneviError::SendError(e.to_string()))?;
        }
        Ok(())
    }
}
