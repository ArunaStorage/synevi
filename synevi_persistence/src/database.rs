use ahash::RandomState;
use heed::{
    byteorder::BigEndian,
    types::{SerdeBincode, U128},
    Database, Env, EnvOpenOptions,
};
use std::collections::{BTreeMap, HashSet};
use synevi_types::{
    error::SyneviError,
    traits::Store,
    types::{Event, Hashes, RecoverDependencies, RecoverEvent, UpsertEvent},
    Ballot, State, T, T0,
};
use tracing::instrument;

const EVENT_DB_NAME: &str = "events";
type EventDb = Database<U128<BigEndian>, SerdeBincode<Event>>;

#[derive(Clone, Debug)]
pub struct PersistentStore {
    db: Env,
    pub(crate) mappings: BTreeMap<T, T0>, // Key: t, value t0
    pub last_applied: T,                  // t of last applied entry
    pub(crate) latest_t0: T0,             // last created or recognized t0
    pub node_serial: u16,
    latest_hash: [u8; 32],
}

impl PersistentStore {
    pub fn new(path: String, node_serial: u16) -> Result<PersistentStore, SyneviError> {
        let env = unsafe { EnvOpenOptions::new().open(path)? };
        let env_clone = env.clone();
        let read_txn = env.read_txn()?;
        let events_db: Option<EventDb> = env.open_database(&read_txn, Some(EVENT_DB_NAME))?;
        match events_db {
            Some(db) => {
                let result = db
                    .iter(&read_txn)?
                    .filter_map(|e| {
                        if let Ok((_, event)) = e {
                            Some(event)
                        } else {
                            None
                        }
                    })
                    .collect::<Vec<_>>();

                let mut mappings = BTreeMap::default();
                let mut last_applied = T::default();
                let mut latest_t0 = T0::default();
                let mut latest_hash: [u8; 32] = [0; 32];
                for event in result {
                    mappings.insert(event.t, event.t_zero);
                    if event.state == State::Applied && event.t > last_applied {
                        last_applied = event.t;
                        latest_hash = if let Some(hashes) = event.hashes {
                            hashes.transaction_hash
                        } else {
                            return Err(SyneviError::MissingTransactionHash);
                        };
                    }
                    if event.t_zero > latest_t0 {
                        latest_t0 = event.t_zero;
                    }
                }

                Ok(PersistentStore {
                    db: env_clone,
                    mappings,
                    last_applied,
                    latest_t0,
                    node_serial,
                    latest_hash,
                })
            }
            None => Ok(PersistentStore {
                db: env_clone,
                mappings: BTreeMap::default(),
                last_applied: T::default(),
                latest_t0: T0::default(),
                node_serial,
                latest_hash: [0; 32],
            }),
        }
    }

    pub fn read_all(&self) -> Result<Vec<Event>, SyneviError> {
        let wtxn = self.db.read_txn()?;
        let events_db: Database<U128<BigEndian>, SerdeBincode<Event>> = self
            .db
            .open_database(&wtxn, Some(EVENT_DB_NAME))?
            .ok_or_else(|| SyneviError::DatabaseNotFound(EVENT_DB_NAME))?;
        let result = events_db
            .iter(&wtxn)?
            .filter_map(|e| {
                if let Ok((_, event)) = e {
                    Some(event)
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();
        Ok(result)
    }

    pub fn upsert_object(&self, event: Event) -> Result<(), SyneviError> {
        let mut wtxn = self.db.write_txn()?;
        let events_db: Database<U128<BigEndian>, SerdeBincode<Event>> =
            self.db.create_database(&mut wtxn, Some(EVENT_DB_NAME))?;
        events_db.put(&mut wtxn, &event.t_zero.get_inner(), &event)?;
        wtxn.commit()?;
        Ok(())
    }
}

impl Store for PersistentStore {
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
    ) -> Result<(T, HashSet<T0, RandomState>), SyneviError> {
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
        let mut write_txn = self.db.write_txn().ok()?;
        let events_db: EventDb = self
            .db
            .open_database(&write_txn, Some(EVENT_DB_NAME))
            .ok()??;
        let mut event = events_db.get(&write_txn, &t_zero.get_inner()).ok()??;

        if event.ballot < ballot {
            event.ballot = ballot;
            let _ = events_db.put(&mut write_txn, &t_zero.get_inner(), &event);
        }

        Some(event.ballot)
    }

    #[instrument(level = "trace")]
    fn upsert_tx(&mut self, upsert_event: UpsertEvent) -> Result<Option<Hashes>, SyneviError> {
        let mut write_txn = self.db.write_txn()?;
        let events_db: EventDb = self
            .db
            .open_database(&write_txn, Some(EVENT_DB_NAME))?
            .ok_or_else(|| SyneviError::DatabaseNotFound(EVENT_DB_NAME))?;
        let event = events_db.get(&write_txn, &upsert_event.t_zero.get_inner())?;
        let Some(mut event) = event else {
            let event = Event::from(upsert_event.clone());
            events_db.put(&mut write_txn, &upsert_event.t_zero.get_inner(), &event)?;
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
            return Ok(event.hashes.clone());
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
                self.last_applied = event.t;
                let hashes = event.hash_event(
                    upsert_event
                        .execution_hash
                        .ok_or_else(|| SyneviError::MissingExecutionHash)?,
                    self.latest_hash,
                );
                self.latest_hash = hashes.transaction_hash;
                event.hashes = Some(hashes);
            };
            events_db.put(&mut write_txn, &upsert_event.t_zero.get_inner(), &event)?;
            Ok(event.hashes.clone())
        } else {
            Ok(None)
        }
    }

    #[instrument(level = "trace")]
    fn get_recover_deps(&self, t_zero: &T0) -> Result<RecoverDependencies, SyneviError> {
        let read_txn = self.db.read_txn()?;
        let db: EventDb = self
            .db
            .open_database(&read_txn, Some(EVENT_DB_NAME))?
            .ok_or_else(|| SyneviError::DatabaseNotFound(EVENT_DB_NAME))?;
        let timestamp = db
            .get(&read_txn, &t_zero.get_inner())?
            .ok_or_else(|| SyneviError::EventNotFound(t_zero.get_inner()))?
            .t;
        let mut recover_deps = RecoverDependencies {
            timestamp,
            ..Default::default()
        };
        for (t_dep, t_zero_dep) in self.mappings.range(self.last_applied..) {
            let dep_event = db
                .get(&read_txn, &t_zero_dep.get_inner())?
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
        let read_txn = self.db.read_txn().ok()?;
        let db: EventDb = self
            .db
            .open_database(&read_txn, Some(EVENT_DB_NAME))
            .ok()??;
        Some(
            db.get(&read_txn, &t_zero.get_inner())
                .ok()?
                .ok_or_else(|| SyneviError::EventNotFound(t_zero.get_inner()))
                .ok()?
                .state,
        )
    }

    fn recover_event(
        &mut self,
        t_zero_recover: &T0,
        node_serial: u16,
    ) -> Result<RecoverEvent, SyneviError> {
        let Some(state) = self.get_event_state(t_zero_recover) else {
            return Err(SyneviError::EventNotFound(t_zero_recover.get_inner()));
        };
        if matches!(state, synevi_types::State::Undefined) {
            return Err(SyneviError::UndefinedRecovery);
        }

        let mut write_txn = self.db.write_txn()?;
        let db: EventDb = self
            .db
            .open_database(&write_txn, Some(EVENT_DB_NAME))?
            .ok_or_else(|| SyneviError::DatabaseNotFound(EVENT_DB_NAME))?;
        let event = db
            .get(&write_txn, &t_zero_recover.get_inner())?;

        if let Some(mut event) = event {
            event.ballot = Ballot(event.ballot.next_with_node(node_serial).into_time());
            db.put(&mut write_txn, &t_zero_recover.get_inner(), &event)?;

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
            Err(SyneviError::EventNotFound(t_zero_recover.get_inner()))
        }
    }

    fn get_event_store(&self) -> BTreeMap<T0, Event> {
        //self.events.clone()
        todo!()
    }

    fn last_applied(&mut self) -> T {
        self.last_applied
    }
}

#[cfg(test)]
mod tests {

    #[test]
    fn test_db() {
        // TODO
        //let db = Database::new("../../tests/database".to_string()).unwrap();
        //db.init(Bytes::from("key"), Bytes::from("value"))
        //    .unwrap()
    }
}
