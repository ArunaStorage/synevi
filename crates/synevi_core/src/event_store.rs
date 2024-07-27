use crate::{
    coordinator::TransactionStateMachine,
    utils::{from_dependency, Ballot, Transaction, T, T0},
};
use ahash::RandomState;
use anyhow::Result;
use bytes::{BufMut, Bytes, BytesMut};
use monotime::MonoTime;
use sha3::Digest;
use sha3::Sha3_256;
use std::{
    collections::{BTreeMap, HashSet},
    time::{SystemTime, UNIX_EPOCH},
};
use std::{fmt::Debug, sync::Arc};
use synevi_network::consensus_transport::State;
use synevi_persistence::{Database, SplitEvent};
use tokio::sync::{oneshot, Notify};
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
    sender: tokio::sync::mpsc::Sender<(u128, Vec<u8>, oneshot::Sender<Option<Vec<u8>>>)>,
}

#[derive(Clone, Debug, Default)]
pub struct Event {
    pub id: u128,
    pub t_zero: T0,
    pub t: T,
    pub state: State,
    pub event: Vec<u8>,
    pub dependencies: HashSet<T0, RandomState>, // t_zero
    pub ballot: Ballot,
    pub previous_hash: Option<[u8; 32]>,
    pub last_updated: u128,
}

impl Event {
    fn hash_event(&self) -> [u8; 32] {
        let mut hasher = Sha3_256::new();
        hasher.update(Vec::<u8>::from(self.id.to_be_bytes()).as_slice());
        hasher.update(Vec::<u8>::from(self.t_zero).as_slice());
        hasher.update(Vec::<u8>::from(self.t).as_slice());
        hasher.update(Vec::<u8>::from(self.ballot).as_slice());
        hasher.update(i32::from(self.state).to_be_bytes().as_slice());
        hasher.update(self.event.as_slice());
        if let Some(previous_hash) = self.previous_hash {
            hasher.update(previous_hash);
        }
        // for dep in &self.dependencies {
        //     hasher.update(Vec::<u8>::from(*dep).as_slice());
        // }
        // Do we want to include ballots in our hash?
        // -> hasher.update(Vec::<u8>::from(self.ballot).as_slice());

        hasher.finalize().into()
    }

    pub fn as_bytes(&self) -> Bytes {
        let mut new: BytesMut = BytesMut::new();

        new.put(self.id.to_be_bytes().as_slice());
        new.put(<[u8; 16]>::from(*self.t).as_slice());
        let state: i32 = self.state.into();
        new.put(state.to_be_bytes().as_slice()); // -> [u8: 4]
        new.put(<[u8; 16]>::from(*self.ballot).as_slice());

        for dep in &self.dependencies {
            new.put::<Bytes>((*dep).into());
        }

        new.freeze()
    }
    pub fn from_bytes(input: SplitEvent) -> Result<Self> {
        let mut state = input.state;
        let mut event = Event::default();
        event.t_zero = T0(MonoTime::try_from(input.key.iter().as_slice())?);
        event.event = input.event.into();
        event.id = u128::from_be_bytes(<[u8; 16]>::try_from(state.split_to(16).iter().as_slice())?);
        event.t = T::try_from(state.split_to(16))?;
        event.state = State::try_from(i32::from_be_bytes(<[u8; 4]>::try_from(
            state.split_to(4).iter().as_slice(),
        )?))?;
        event.ballot = Ballot::try_from(state.split_to(16))?;
        while !state.is_empty() {
            let dep = state.split_to(16);
            let t0_dep = T0::try_from(dep)?;
            event.dependencies.insert(t0_dep);
        }
        Ok(event)
    }
}

impl PartialEq for Event {
    fn eq(&self, other: &Self) -> bool {
        self.t_zero == other.t_zero
            && self.t == other.t
            && self.state == other.state
            && self.event == other.event
            && self.dependencies == other.dependencies
            && self.ballot == other.ballot
    }
}

#[derive(Debug, Default)]
pub(crate) struct RecoverDependencies {
    pub dependencies: HashSet<T0, RandomState>,
    pub wait: HashSet<T0, RandomState>,
    pub superseding: bool,
}

impl EventStore {
    #[instrument(level = "trace")]
    pub fn init(
        path: Option<String>,
        node_serial: u16,
        sender: tokio::sync::mpsc::Sender<(u128, Vec<u8>, oneshot::Sender<Option<Vec<u8>>>)>,
    ) -> Result<Self> {
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
                    sender,
                    //last_applied_series: vec![],
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
                sender,
                //last_applied_series: vec![],
            }),
        }
    }

    #[instrument(level = "trace")]
    async fn insert(&mut self, event: Event) {
        self.events.insert(event.t_zero, event.clone());
        self.mappings.insert(event.t, event.t_zero);
    }

    #[instrument(level = "trace", skip(transaction))]
    pub async fn init_transaction<Tx: Transaction>(
        &mut self,
        transaction: Tx,
        node_serial: u16,
        id: u128,
    ) -> TransactionStateMachine<Tx> {
        let t0 = self.latest_t0.next_with_node(node_serial).into_time();
        TransactionStateMachine {
            id,
            state: State::PreAccepted,
            transaction,
            t_zero: T0(t0),
            t: T(t0),
            dependencies: HashSet::default(),
            ballot: Ballot::default(),
            tx_result: None,
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
            last_updated: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap() // This must fail if the system clock is before the UNIX_EPOCH
                .as_nanos(),
            ..Default::default()
        });
        entry.clone()
    }

    #[instrument(level = "trace")]
    pub async fn pre_accept(
        &mut self,
        t_zero: T0,
        transaction: Vec<u8>,
        id: u128,
    ) -> Result<(Vec<u8>, T)> {
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
            let deps: Vec<u8> = self.get_dependencies(&t, &t_zero).await;
            (t, deps)
        };

        // This is OK because on pre_accept T == T0

        let event = Event {
            id,
            t_zero,
            t,
            state: State::PreAccepted,
            event: transaction,
            dependencies: from_dependency(deps.clone())?,
            ..Default::default()
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

        //println!("T0: {:?}, Old: {:?} new: {:?} @ {}", event.t_zero, old_event.state, event.state, self.node_serial);
        if event.state < old_event.state
            || (event.state == State::Applied && old_event.state == State::Applied)
        {
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
            old_event.state = event.state;
            if event.ballot > old_event.ballot {
                old_event.ballot = event.ballot;
            }
        }

        old_event.last_updated = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap() // This must fail if the system clock is before the UNIX_EPOCH
            .as_nanos();

        self.mappings.insert(event.t, event.t_zero);

        if old_event.state == State::Applied {
            //self.last_applied_series.push(event.t);
            self.last_applied = event.t;
            old_event.previous_hash = Some(self.latest_hash);
            self.latest_hash = old_event.hash_event();
            let payload = old_event.event.clone();
            let notify = Arc::new(Notify::new());
            let notify_clone = notify.clone();
            let notified = notify_clone.notified();
            self.sender
                .send((old_event.id, payload, notify))
                .await
                .unwrap();
            notified.await;
        }

        if let Some(db) = &self.database {
            let t: Vec<u8> = (*old_event.t).into();
            if update {
                db.update_object(t.into(), old_event.as_bytes())
                    .await
                    .unwrap()
            } else {
                db.init_object(
                    t.into(),
                    Bytes::from(old_event.event.clone()),
                    old_event.as_bytes(),
                )
                .await
                .unwrap()
            }
        }
    }

    #[instrument(level = "trace")]
    pub async fn get_dependencies(&self, t: &T, t_zero: &T0) -> Vec<u8> {
        if &self.last_applied == t {
            return vec![];
        }
        assert!(self.last_applied < *t);
        // What about deps with dep_t0 < last_applied_t0 && dep_t > t?
        let mut deps = Vec::new();

        // Dependencies are where any of these cases match:
        // - t_dep < t if not applied
        // - t0_dep < t0_last_applied, if t_dep > t0
        // - t_dep > t if t0_dep < t
        for (_, t0_dep) in self.mappings.range(self.last_applied..) {
            if t0_dep != t_zero && (t0_dep < &T0(**t)) {
                deps.put::<Bytes>((*t0_dep).into());
            }
        }
        deps
    }

    #[instrument(level = "trace")]
    pub async fn get_recover_deps(&self, _t: &T, t_zero: &T0) -> Result<RecoverDependencies> {
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
}

#[cfg(test)]
mod tests {
    use crate::event_store::Event;
    use crate::utils::{Ballot, T, T0};
    use bytes::Bytes;
    use monotime::MonoTime;
    use std::collections::HashSet;
    use std::time::{SystemTime, UNIX_EPOCH};
    use synevi_network::consensus_transport::State;
    use synevi_persistence::SplitEvent;

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
            event: Vec::from(b"this is a test transaction"),
            dependencies,
            ballot: Ballot(MonoTime::new(1, 1)),
            last_updated: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap() // This must fail if the system clock is before the UNIX_EPOCH
                .as_nanos(),
            previous_hash: None,
        };
        let key = Bytes::from(t_zero.0);
        let payload = Bytes::from(event.event.clone());
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
