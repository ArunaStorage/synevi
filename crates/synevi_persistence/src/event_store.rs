use crate::event::Event;
use crate::event::UpsertEvent;
use crate::rocks_db::Database;
use ahash::RandomState;
use anyhow::Result;
use bytes::Bytes;
use std::collections::{BTreeMap, HashSet};
use std::fmt::Debug;
use synevi_types::types::RecoverDependencies;
use synevi_types::types::RecoverEvent;
use synevi_types::State;
use synevi_types::{Ballot, T, T0};
use tracing::instrument;

#[derive(Debug)]
pub struct EventStore {
    pub events: BTreeMap<T0, Event>, // Key: t0, value: Event
    pub database: Option<Database>,
    pub(crate) mappings: BTreeMap<T, T0>, // Key: t, value t0
    pub last_applied: T,                  // t of last applied entry
    pub(crate) latest_t0: T0,             // last created or recognized t0
    pub node_serial: u16,
    latest_hash: [u8; 32],
}

type Dependencies = HashSet<T0, RandomState>;

pub trait Store: Send + Sync + Sized + 'static {
    fn new(node_serial: u16) -> Result<Self>;
    // Initialize a new t0
    fn init_t_zero(&mut self, node_serial: u16) -> T0;
    // Pre-accept a transaction
    fn pre_accept_tx(
        &mut self,
        id: u128,
        t_zero: T0,
        transaction: Vec<u8>,
    ) -> Result<(T, Dependencies)>;
    // Get the dependencies for a transaction
    fn get_tx_dependencies(&self, t: &T, t_zero: &T0) -> Dependencies;
    // Get the recover dependencies for a transaction
    fn get_recover_deps(&self, t_zero: &T0) -> Result<RecoverDependencies>;
    // Tries to recover an unfinished event from the store
    fn recover_event(&mut self, t_zero_recover: &T0, node_serial: u16) -> Result<RecoverEvent>;
    // Check and update the ballot for a transaction
    // Returns true if the ballot was accepted (current <= ballot)
    fn accept_tx_ballot(&mut self, t_zero: &T0, ballot: Ballot) -> Option<Ballot>;
    // Update or insert a transaction, returns the hash of the transaction if applied
    fn upsert_tx(&mut self, upsert_event: UpsertEvent) -> Result<Option<[u8; 32]>>;

    fn get_event_state(&self, t_zero: &T0) -> Option<State>;

    fn get_event_store(&self) -> BTreeMap<T0, Event>;
}

impl Store for EventStore {
    fn new(node_serial: u16) -> Result<Self> {
        // match path {
        //     Some(path) => {
        //         // TODO: Read all from DB and fill event store
        //         let mut events = BTreeMap::default();
        //         let mut mappings = BTreeMap::default();
        //         let mut last_applied = T::default();
        //         let mut latest_t0 = T0::default();
        //         let db = Database::new(path)?;
        //         let result = db.read_all()?;
        //         for entry in result {
        //             let event = Event::from_bytes(entry)?;
        //             if event.state == State::Applied && event.t > last_applied {
        //                 last_applied = event.t;
        //             }
        //             if latest_t0 < event.t_zero {
        //                 latest_t0 = event.t_zero;
        //             }
        //             mappings.insert(event.t, event.t_zero);
        //             events.insert(event.t_zero, event);
        //         }
        //         Ok(EventStore {
        //             events,
        //             mappings,
        //             last_applied,
        //             latest_t0,
        //             database: Some(db),
        //             node_serial,
        //             latest_hash: [0; 32], // TODO: Read from DB
        //         })
        //     }
        //     None =>
        Ok(EventStore {
            events: BTreeMap::default(),
            mappings: BTreeMap::default(),
            last_applied: T::default(),
            latest_t0: T0::default(),
            database: None,
            node_serial,
            latest_hash: [0; 32],
        })
        // ),
        // }
    }

    #[instrument(level = "trace")]
    fn init_t_zero(&mut self, node_serial: u16) -> T0 {
        let t0 = T0(self.latest_t0.next_with_node(node_serial).into_time());
        self.latest_t0 = t0;
        t0
    }

    #[instrument(level = "trace")]
    fn pre_accept_tx(
        &mut self,
        id: u128,
        t_zero: T0,
        transaction: Vec<u8>,
    ) -> Result<(T, HashSet<T0, RandomState>)> {
        let (t, deps) = {
            let t = T(if let Some((last_t, _)) = self.mappings.last_key_value() {
                if **last_t > *t_zero {
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
    fn upsert_tx(&mut self, upsert_event: UpsertEvent) -> Result<Option<[u8; 32]>> {
        let Some(event) = self.events.get_mut(&upsert_event.t_zero) else {
            let event = Event::from(upsert_event.clone());
            self.events.insert(upsert_event.t_zero, event);
            self.mappings.insert(upsert_event.t, upsert_event.t_zero);
            return Ok(None);
        };

        // Update the latest t0
        if self.latest_t0 < event.t_zero {
            self.latest_t0 = event.t_zero;
        }

        // Do not update to a "lower" state
        if upsert_event.state < event.state {
            return Ok(None);
        }

        // Event is already applied
        if event.state == State::Applied {
            return Ok(Some(event.hash_event()));
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

            let hash = if event.state == State::Applied {
                self.last_applied = event.t;
                event.previous_hash = Some(self.latest_hash);
                self.latest_hash = event.hash_event();
                Some(self.latest_hash)
            } else {
                None
            };

            if let Some(db) = &self.database {
                let t: Vec<u8> = (*event.t).into();
                db.update_object(t.into(), event.as_bytes())?
            }
            Ok(hash)
        } else {
            if let Some(db) = &self.database {
                let t: Vec<u8> = (*event.t).into();
                db.init_object(
                    t.into(),
                    Bytes::from(event.transaction.clone()),
                    event.as_bytes(),
                )?;
            }
            Ok(None)
        }
    }

    #[instrument(level = "trace")]
    fn get_recover_deps(&self, t_zero: &T0) -> Result<RecoverDependencies> {
        let mut recover_deps = RecoverDependencies {
            timestamp: self
                .events
                .get(t_zero)
                .map(|event| event.t)
                .ok_or_else(|| anyhow::anyhow!("Event not found for t_zero: {:?}", t_zero))?,
            ..Default::default()
        };
        for (t_dep, t_zero_dep) in self.mappings.range(self.last_applied..) {
            let dep_event = self.events.get(t_zero_dep).ok_or_else(|| {
                anyhow::anyhow!("Dependency not found for t_zero: {:?}", t_zero_dep)
            })?;
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

    fn recover_event(&mut self, t_zero_recover: &T0, node_serial: u16) -> Result<RecoverEvent> {
        let Some(state) = self.get_event_state(t_zero_recover) else {
            return Err(anyhow::anyhow!(
                "No state found for t0 {:?}",
                t_zero_recover
            ));
        };
        if matches!(state, synevi_types::State::Undefined) {
            return Err(anyhow::anyhow!("Undefined recovery"));
        }

        if let Some(event) = self.events.get_mut(t_zero_recover) {
            event.ballot = Ballot(event.ballot.next_with_node(node_serial).into_time());

            Ok(RecoverEvent {
                id: event.id,
                t_zero: event.t_zero,
                t: event.t,
                state,
                transaction: event.transaction.clone(),
                dependencies: event.dependencies.clone(),
                ballot: event.ballot,
            })
        } else {
            Err(anyhow::anyhow!(
                "Event not found for t0 {:?}",
                t_zero_recover
            ))
        }
    }

    fn get_event_store(&self) -> BTreeMap<T0, Event> {
        self.events.clone()
    }
}

impl EventStore {
    pub fn init(path: Option<String>, node_serial: u16) -> Result<Self> {
        match path {
            Some(path) => {
                // TODO: Read all from DB and fill event store
                let mut events = BTreeMap::default();
                let mut mappings = BTreeMap::default();
                let mut last_applied = T::default();
                let mut latest_t0 = T0::default();
                let db = Database::new(path)?;
                let result = db.read_all()?;
                for entry in result {
                    let event = Event::from_bytes(entry)?;
                    if event.state == State::Applied && event.t > last_applied {
                        last_applied = event.t;
                    }
                    if latest_t0 < event.t_zero {
                        latest_t0 = event.t_zero;
                    }
                    mappings.insert(event.t, event.t_zero);
                    events.insert(event.t_zero, event);
                }
                Ok(EventStore {
                    events,
                    mappings,
                    last_applied,
                    latest_t0,
                    database: Some(db),
                    node_serial,
                    latest_hash: [0; 32], // TODO: Read from DB
                })
            }
            None => Ok(EventStore {
                events: BTreeMap::default(),
                mappings: BTreeMap::default(),
                last_applied: T::default(),
                latest_t0: T0::default(),
                database: None,
                node_serial,
                latest_hash: [0; 32],
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::event_store::Event;
    use crate::rocks_db::SplitEvent;
    use bytes::Bytes;
    use monotime::MonoTime;
    use std::collections::HashSet;
    use std::time::{SystemTime, UNIX_EPOCH};
    use synevi_types::{Ballot, State, T, T0};

    #[test]
    fn event_conversion() {
        let mut dependencies = HashSet::default();
        for i in 0..3 {
            dependencies.insert(T0(MonoTime::new(0, i)));
        }
        let t_zero = T0(MonoTime::new(1, 1));
        let t = T(t_zero.next().into_time());

        let event = Event {
            id: 0,
            t_zero,
            t,
            state: State::Commited,
            transaction: Vec::from(b"this is a test transaction"),
            dependencies,
            ballot: Ballot(MonoTime::new(1, 1)),
            last_updated: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap() // This must fail if the system clock is before the UNIX_EPOCH
                .as_nanos(),
            previous_hash: None,
        };
        let key = Bytes::from(t_zero.0);
        let payload = Bytes::from(event.transaction.clone());
        let state = event.as_bytes();
        let event_to_bytes = SplitEvent {
            key,
            event: payload,
            state,
        };

        let back_to_struct = Event::from_bytes(event_to_bytes).unwrap();

        assert_eq!(event, back_to_struct)
    }
}
