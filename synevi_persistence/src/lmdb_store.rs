use ahash::RandomState;
use heed::{
    byteorder::BigEndian,
    types::{SerdeBincode, U128},
    Database, Env, EnvOpenOptions,
};
use monotime::MonoTime;
use std::collections::{BTreeMap, HashSet};
use synevi_types::{
    error::SyneviError,
    traits::Store,
    types::{Event, Hashes, RecoverDependencies, RecoverEvent, UpsertEvent},
    Ballot, State, T, T0,
};
use tokio::sync::{mpsc::Receiver, Mutex};
use tracing::instrument;

const EVENT_DB_NAME: &str = "events";
type EventDb = Database<U128<BigEndian>, SerdeBincode<Event>>;

#[derive(Debug)]
pub struct PersistentStore {
    data: Mutex<MutableData>,
}

#[derive(Clone, Debug)]
struct MutableData {
    db: Env,
    pub(crate) mappings: BTreeMap<T, T0>, // Key: t, value t0
    pub last_applied: T,                  // t of last applied entry
    pub(crate) latest_time: MonoTime,             // last created or recognized t0
    pub node_serial: u16,
    latest_hash: [u8; 32],
}

impl PersistentStore {
    pub fn new(path: String, node_serial: u16) -> Result<PersistentStore, SyneviError> {
        let env = unsafe {
            EnvOpenOptions::new()
                .map_size(1024 * 1024 * 1024)
                .max_dbs(16)
                .open(path)?
        };
        let env_clone = env.clone();
        let mut write_txn = env.write_txn()?;
        let events_db: Option<EventDb> = env.open_database(&write_txn, Some(EVENT_DB_NAME))?;
        match events_db {
            Some(db) => {
                let result = db
                    .iter(&write_txn)?
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
                let mut latest_time = MonoTime::default();
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
                    if *event.t > latest_time {
                        latest_time = *event.t;
                    }
                }
                write_txn.commit()?;
                Ok(PersistentStore {
                    //db: env_clone,
                    data: Mutex::new(MutableData {
                        db: env_clone,
                        mappings,
                        last_applied,
                        latest_time,
                        node_serial,
                        latest_hash,
                    }),
                })
            }
            None => {
                let _: EventDb = env.create_database(&mut write_txn, Some(EVENT_DB_NAME))?;
                write_txn.commit()?;
                Ok(PersistentStore {
                    data: Mutex::new(MutableData {
                        db: env_clone,
                        mappings: BTreeMap::default(),
                        last_applied: T::default(),
                        latest_time: MonoTime::default(),
                        node_serial,
                        latest_hash: [0; 32],
                    }),
                })
            }
        }
    }
}

#[async_trait::async_trait]
impl Store for PersistentStore {
    #[instrument(level = "trace")]
    async fn init_t_zero(&self, node_serial: u16) -> T0 {
        self.data.lock().await.init_t_zero(node_serial).await
    }

    #[instrument(level = "trace")]
    async fn pre_accept_tx(
        &self,
        id: u128,
        t_zero: T0,
        transaction: Vec<u8>,
    ) -> Result<(T, HashSet<T0, RandomState>), SyneviError> {
        self.data
            .lock()
            .await
            .pre_accept_tx(id, t_zero, transaction)
            .await
    }

    #[instrument(level = "trace")]
    async fn get_tx_dependencies(&self, t: &T, t_zero: &T0) -> HashSet<T0, RandomState> {
        self.data.lock().await.get_tx_dependencies(t, t_zero).await
    }

    #[instrument(level = "trace")]
    async fn accept_tx_ballot(&self, t_zero: &T0, ballot: Ballot) -> Option<Ballot> {
        self.data
            .lock()
            .await
            .accept_tx_ballot(t_zero, ballot)
            .await
    }

    #[instrument(level = "trace", skip(self))]
    async fn upsert_tx(&self, upsert_event: UpsertEvent) -> Result<(), SyneviError> {
        self.data.lock().await.upsert_tx(upsert_event).await
    }

    #[instrument(level = "trace")]
    async fn get_recover_deps(&self, t_zero: &T0) -> Result<RecoverDependencies, SyneviError> {
        self.data.lock().await.get_recover_deps(t_zero).await
    }

    #[instrument(level = "trace")]
    async fn get_event_state(&self, t_zero: &T0) -> Option<State> {
        self.data.lock().await.get_event_state(t_zero).await
    }

    #[instrument(level = "trace")]
    async fn recover_event(
        &self,
        t_zero_recover: &T0,
        node_serial: u16,
    ) -> Result<Option<RecoverEvent>, SyneviError> {
        self.data
            .lock()
            .await
            .recover_event(t_zero_recover, node_serial)
            .await
    }

    #[instrument(level = "trace")]
    async fn get_event_store(&self) -> BTreeMap<T0, Event> {
        self.data.lock().await.get_event_store().await
    }

    #[instrument(level = "trace")]
    async fn last_applied(&self) -> (T, T0) {
        self.data.lock().await.last_applied().await
    }

    #[instrument(level = "trace")]
    async fn get_events_after(
        &self,
        last_applied: T,
        self_event: u128,
    ) -> Result<Receiver<Result<Event, SyneviError>>, SyneviError> {
        self.data
            .lock()
            .await
            .get_events_after(last_applied, self_event)
            .await
    }

    #[instrument(level = "trace", skip(self))]
    async fn get_event(&self, t_zero: T0) -> Result<Option<Event>, SyneviError> {
        self.data.lock().await.get_event(t_zero).await
    }

    async fn get_and_update_hash(
        &self,
        t_zero: T0,
        execution_hash: [u8; 32],
    ) -> Result<Hashes, SyneviError> {
        self.data
            .lock()
            .await
            .get_and_update_hash(t_zero, execution_hash)
            .await
    }

    #[instrument(level = "trace", skip(self))]
    async fn last_applied_hash(&self) -> Result<(T, [u8; 32]), SyneviError> {
        self.data.lock().await.last_applied_hash().await
    }

    async fn inc_time_with_guard(&self, guard: T0) -> Result<(), SyneviError> {
        let mut lock = self.data.lock().await;
        lock.latest_time = lock.latest_time.next_with_guard_and_node(&guard, lock.node_serial).into_time();
        Ok(())
    }
}
impl MutableData {
    #[instrument(level = "trace")]
    async fn init_t_zero(&mut self, node_serial: u16) -> T0 {
        let next_time = self.latest_time.next_with_node(node_serial).into_time();
        self.latest_time = next_time;
        T0(next_time)
    }

    #[instrument(level = "trace")]
    async fn pre_accept_tx(
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
            }else{
                T(*t_zero)
            };
            // This might not be necessary to re-use the write lock here
            let deps = self.get_tx_dependencies(&t, &t_zero).await;
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
        self.upsert_tx(event).await?;
        Ok((t, deps))
    }

    #[instrument(level = "trace")]
    async fn get_tx_dependencies(&self, t: &T, t_zero: &T0) -> HashSet<T0, RandomState> {
        if self.last_applied == *t {
            return HashSet::default();
        }
        assert!(self.last_applied < *t);
        let mut deps = HashSet::default();
        if let Some(last_applied_t0) = self.mappings.get(&self.last_applied) {
            if last_applied_t0 != &T0::default() {
                deps.insert(*last_applied_t0);
            }
        }
        // What about deps with dep_t0 < last_applied_t0 && dep_t > t?

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
    async fn accept_tx_ballot(&self, t_zero: &T0, ballot: Ballot) -> Option<Ballot> {
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
        write_txn.commit().ok()?;

        Some(event.ballot)
    }

    #[instrument(level = "trace", skip(self))]
    async fn upsert_tx(&mut self, upsert_event: UpsertEvent) -> Result<(), SyneviError> {
        //let db = self.db.clone();

        // Update the latest time
        if self.latest_time < *upsert_event.t {
            self.latest_time = *upsert_event.t;
        }

        let mut write_txn = self.db.write_txn()?;
        let events_db: EventDb = self
            .db
            .open_database(&write_txn, Some(EVENT_DB_NAME))?
            .ok_or_else(|| SyneviError::DatabaseNotFound(EVENT_DB_NAME))?;
        let event = events_db.get(&write_txn, &upsert_event.t_zero.get_inner())?;

        let Some(mut event) = event else {
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

                let last_t = self.last_applied;
                // Safeguard
                assert!(last_t < event.t);

                self.last_applied = event.t;
                let hashes = event.hash_event(self.latest_hash);
                self.latest_hash = hashes.transaction_hash;
                event.hashes = Some(hashes.clone());

                events_db.put(&mut write_txn, &upsert_event.t_zero.get_inner(), &event)?;
            } else {
                events_db.put(&mut write_txn, &upsert_event.t_zero.get_inner(), &event)?;
                self.mappings.insert(upsert_event.t, upsert_event.t_zero);
            }
            write_txn.commit()?;
            return Ok(());
        };

        // Do not update to a "lower" state
        if upsert_event.state < event.state {
            write_txn.commit()?;
            return Ok(());
        }

        // Event is already applied
        if event.state == State::Applied {
            write_txn.commit()?;
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
                let last_t = self.last_applied;
                // Safeguard
                assert!(last_t < event.t);

                self.last_applied = event.t;
                let hashes = event.hash_event(self.latest_hash);
                self.latest_hash = hashes.transaction_hash;
                event.hashes = Some(hashes.clone());
            };
            events_db.put(&mut write_txn, &upsert_event.t_zero.get_inner(), &event)?;
            write_txn.commit()?;
            Ok(())
        } else {
            write_txn.commit()?;
            Ok(())
        }
    }

    #[instrument(level = "trace")]
    async fn get_recover_deps(&self, t_zero: &T0) -> Result<RecoverDependencies, SyneviError> {
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
        read_txn.commit()?;
        Ok(recover_deps)
    }

    async fn get_event_state(&self, t_zero: &T0) -> Option<State> {
        let read_txn = self.db.read_txn().ok()?;
        let db: EventDb = self
            .db
            .open_database(&read_txn, Some(EVENT_DB_NAME))
            .ok()??;
        let state = db
            .get(&read_txn, &t_zero.get_inner())
            .ok()?
            .ok_or_else(|| SyneviError::EventNotFound(t_zero.get_inner()))
            .ok()?
            .state;
        read_txn.commit().ok()?;
        Some(state)
    }

    async fn recover_event(
        &self,
        t_zero_recover: &T0,
        node_serial: u16,
    ) -> Result<Option<RecoverEvent>, SyneviError> {
        let Some(state) = self.get_event_state(t_zero_recover).await else {
            return Ok(None);
        };
        if matches!(state, synevi_types::State::Undefined) {
            return Err(SyneviError::UndefinedRecovery);
        }

        let mut write_txn = self.db.write_txn()?;
        let db: EventDb = self
            .db
            .open_database(&write_txn, Some(EVENT_DB_NAME))?
            .ok_or_else(|| SyneviError::DatabaseNotFound(EVENT_DB_NAME))?;
        let event = db.get(&write_txn, &t_zero_recover.get_inner())?;

        if let Some(mut event) = event {
            event.ballot = Ballot(event.ballot.next_with_node(node_serial).into_time());
            db.put(&mut write_txn, &t_zero_recover.get_inner(), &event)?;
            write_txn.commit()?;

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

    async fn get_event_store(&self) -> BTreeMap<T0, Event> {
        // TODO: Remove unwrap and change trait result
        let read_txn = self.db.read_txn().unwrap();
        let events_db: Database<U128<BigEndian>, SerdeBincode<Event>> = self
            .db
            .open_database(&read_txn, Some(EVENT_DB_NAME))
            .unwrap()
            .ok_or_else(|| SyneviError::DatabaseNotFound(EVENT_DB_NAME))
            .unwrap();
        let result = events_db
            .iter(&read_txn)
            .unwrap()
            .filter_map(|e| {
                if let Ok((t0, event)) = e {
                    Some((T0(MonoTime::from(t0)), event))
                } else {
                    None
                }
            })
            .collect::<BTreeMap<T0, Event>>();
        read_txn.commit().unwrap();
        result
    }

    async fn last_applied(&self) -> (T, T0) {
        let t = self.last_applied.clone();
        let t0 = self.mappings.get(&t).cloned().unwrap_or(T0::default());
        (t, t0)
    }

    async fn get_events_after(
        &self,
        last_applied: T,
        _self_event: u128,
    ) -> Result<Receiver<Result<Event, SyneviError>>, SyneviError> {
        let (sdx, rcv) = tokio::sync::mpsc::channel(200);
        let db = self.db.clone();
        let last_applied_t0 = match self.mappings.get(&last_applied) {
            Some(t0) => *t0,
            None if last_applied == T::default() => T0::default(),
            _ => return Err(SyneviError::EventNotFound(last_applied.get_inner())),
        };
        tokio::task::spawn_blocking(move || {
            let read_txn = db.read_txn()?;
            let range = last_applied_t0.get_inner()..;
            let events_db: EventDb = db
                .open_database(&read_txn, Some(EVENT_DB_NAME))?
                .ok_or_else(|| SyneviError::DatabaseNotFound(EVENT_DB_NAME))?;

            for result in events_db.range(&read_txn, &range)? {
                let (_t0, event) = result?;
                sdx.blocking_send(Ok(event))
                    .map_err(|e| SyneviError::SendError(e.to_string()))?;
            }
            read_txn.commit()?;
            Ok::<(), SyneviError>(())
        });
        Ok(rcv)
    }

    async fn get_event(&self, t_zero: T0) -> Result<Option<Event>, SyneviError> {
        let read_txn = self.db.read_txn()?;
        let db: EventDb = self
            .db
            .open_database(&read_txn, Some(EVENT_DB_NAME))?
            .ok_or_else(|| SyneviError::DatabaseNotFound(EVENT_DB_NAME))?;
        let event = db.get(&read_txn, &t_zero.get_inner())?;
        read_txn.commit()?;
        Ok(event)
    }

    async fn get_and_update_hash(
        &self,
        t_zero: T0,
        execution_hash: [u8; 32],
    ) -> Result<Hashes, SyneviError> {
        let t_zero = t_zero.get_inner();
        let mut write_txn = self.db.write_txn()?;
        let db: EventDb = self
            .db
            .open_database(&write_txn, Some(EVENT_DB_NAME))?
            .ok_or_else(|| SyneviError::DatabaseNotFound(EVENT_DB_NAME))?;
        let Some(mut event) = db.get(&write_txn, &t_zero)? else {
            return Err(SyneviError::EventNotFound(t_zero));
        };
        let Some(mut hashes) = event.hashes else {
            return Err(SyneviError::MissingTransactionHash);
        };
        hashes.execution_hash = execution_hash;
        event.hashes = Some(hashes.clone());

        db.put(&mut write_txn, &t_zero, &event)?;
        write_txn.commit()?;
        Ok(hashes)
    }

    async fn last_applied_hash(&self) -> Result<(T, [u8; 32]), SyneviError> {
        let last = self.last_applied;
        let last_t0 = self
            .mappings
            .get(&last)
            .ok_or_else(|| SyneviError::EventNotFound(last.get_inner()))?;
        let read_txn = self.db.read_txn()?;
        let db: EventDb = self
            .db
            .open_database(&read_txn, Some(EVENT_DB_NAME))?
            .ok_or_else(|| SyneviError::DatabaseNotFound(EVENT_DB_NAME))?;
        let event = db
            .get(&read_txn, &last_t0.get_inner())?
            .ok_or_else(|| SyneviError::EventNotFound(last_t0.get_inner()))?
            .hashes
            .ok_or_else(|| SyneviError::MissingExecutionHash)?;
        Ok((last, event.execution_hash))
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
