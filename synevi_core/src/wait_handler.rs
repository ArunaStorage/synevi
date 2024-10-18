use ahash::RandomState;
use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex},
};
use synevi_types::{traits::Store, types::RecoverEvent, State, T, T0};
use tokio::{sync::oneshot, time::Instant};

pub struct Waiter {
    waited_since: Instant,
    finished_dependencies: HashSet<T0, RandomState>,
    sender: Vec<oneshot::Sender<()>>,
}

pub enum CheckResult {
    NoRecovery,
    RecoverEvent(RecoverEvent),
    RecoverUnknown(T0),
}

impl CheckResult {
    pub fn replace_if_smaller(&mut self, other: CheckResult) {
        match (&self, &other) {
            (CheckResult::NoRecovery, _) => *self = other,
            (
                CheckResult::RecoverEvent(recover_event_existing),
                CheckResult::RecoverEvent(recover_event),
            ) => {
                if recover_event.t_zero < recover_event_existing.t_zero {
                    *self = other;
                }
            }
            (
                CheckResult::RecoverEvent(recover_event_existing),
                CheckResult::RecoverUnknown(t0),
            ) => {
                if *t0 < recover_event_existing.t_zero {
                    *self = other;
                }
            }
            (
                CheckResult::RecoverUnknown(t0_existing),
                CheckResult::RecoverEvent(recover_event),
            ) => {
                if recover_event.t_zero < *t0_existing {
                    *self = other;
                }
            }
            (CheckResult::RecoverUnknown(t0_existing), CheckResult::RecoverUnknown(t0)) => {
                if t0 < t0_existing {
                    *self = other;
                }
            }
            _ => (),
        }
    }
}

pub struct WaitHandler<S>
where
    S: Store,
{
    waiters: Mutex<HashMap<T0, Waiter, RandomState>>,
    store: Arc<S>,
}

impl<S> WaitHandler<S>
where
    S: Store,
{
    pub fn new(store: Arc<S>, _serial: u16) -> Self {
        Self {
            waiters: Mutex::new(HashMap::default()),
            store,
        }
    }

    pub fn get_waiter(&self, t0: &T0) -> Option<oneshot::Receiver<()>> {
        let (sdx, rcv) = oneshot::channel();
        let mut waiter_lock = self.waiters.lock().expect("Locking waiters failed");

        let Some(event) = self.store.get_event(*t0).ok().flatten() else {
            tracing::error!("Unexpected state in wait_handler: Event not found in store");
            return None;
        };

        let waiter = waiter_lock.entry(*t0).or_insert(Waiter {
            waited_since: Instant::now(),
            finished_dependencies: HashSet::default(),
            sender: Vec::new(),
        });
        waiter.waited_since = Instant::now();

        for dep_t0 in event.dependencies.iter() {
            let Some(dep_event) = self.store.get_event(*dep_t0).ok().flatten() else {
                continue;
            };

            match dep_event.state {
                State::Committed if dep_event.t > event.t => {
                    waiter.finished_dependencies.insert(*dep_t0);
                }
                State::Applied => {
                    waiter.finished_dependencies.insert(*dep_t0);
                }
                _ => {}
            }
        }

        if waiter.finished_dependencies.len() >= event.dependencies.len() {
            return None;
        }

        waiter.sender.push(sdx);
        Some(rcv)
    }

    pub fn notify_commit(&self, t0_commit: &T0, t_commit: &T) {
        let mut waiter_lock = self.waiters.lock().expect("Locking waiters failed");
        waiter_lock.retain(|t0_waiting, waiter| {
            let Some(event) = self.store.get_event(*t0_waiting).ok().flatten() else {
                tracing::error!("Unexpected state in wait_handler: Event not found in store");
                return true;
            };
            if event.dependencies.contains(t0_commit) {
                if t_commit > &event.t {
                    waiter.finished_dependencies.insert(*t0_commit);
                    waiter.waited_since = Instant::now();
                    if waiter.finished_dependencies.len() >= event.dependencies.len() {
                        for sdx in waiter.sender.drain(..) {
                            let _ = sdx.send(());
                        }
                        return false;
                    }
                }
            }
            true
        });
    }

    pub fn notify_apply(&self, t0_commit: &T0) {
        let mut waiter_lock = self.waiters.lock().expect("Locking waiters failed");
        waiter_lock.retain(|t0_waiting, waiter| {
            let Some(event) = self.store.get_event(*t0_waiting).ok().flatten() else {
                tracing::error!("Unexpected state in wait_handler: Event not found in store");
                return true;
            };
            if event.dependencies.contains(t0_commit) {
                waiter.finished_dependencies.insert(*t0_commit);
                waiter.waited_since = Instant::now();
                if waiter.finished_dependencies.len() >= event.dependencies.len() {
                    for sdx in waiter.sender.drain(..) {
                        let _ = sdx.send(());
                    }
                    return false;
                }
            }
            true
        });
    }

    pub fn check_recovery(&self) -> CheckResult {
        let mut waiter_lock = self.waiters.lock().expect("Locking waiters failed");
        let mut smallest_hanging_dep = CheckResult::NoRecovery;
        for (t0, waiter) in waiter_lock.iter_mut() {
            if waiter.waited_since.elapsed().as_millis() > 100 {
                // Get deps and find smallest dep that is not committed / applied
                let Some(event) = self.store.get_event(*t0).ok().flatten() else {
                    tracing::error!(
                        "Unexpected state in wait_handler: Event timed out, but not found in store"
                    );
                    continue;
                };
                for dep in event.dependencies.iter() {
                    let Some(event_dep) = self.store.get_event(*dep).ok().flatten() else {
                        smallest_hanging_dep.replace_if_smaller(CheckResult::RecoverUnknown(*dep));
                        continue;
                    };
                    if event_dep.t_zero > event.t_zero {
                        tracing::error!("Error: Dependency is newer than event");
                        continue;
                    }
                    match event_dep.state {
                        State::Committed => {
                            if event_dep.t > event.t {
                                // Dependency is newer than event (and already commited)
                                continue;
                            }
                            smallest_hanging_dep
                                .replace_if_smaller(CheckResult::RecoverEvent(event_dep.into()));
                        }
                        State::Applied => {
                            // Already applied (no problem)
                            continue;
                        }
                        _ => {
                            smallest_hanging_dep
                                .replace_if_smaller(CheckResult::RecoverEvent(event_dep.into()));
                        }
                    }
                }
                if !matches!(smallest_hanging_dep, CheckResult::NoRecovery) {
                    waiter.waited_since = Instant::now();
                    return smallest_hanging_dep;
                }
            }
        }
        CheckResult::NoRecovery
    }
}

// Tx1 = dep[Tx0]

// -> Tx0 commit
//  -> for each waiter: is tx0 in deps?
//  -> if yes! -> is t(tx0) > t(tx1)
//        -> y -> do nothing
//        -> n -> increase dep_state +1
//           -> if dep_state == dep.len() -> send signal to waiter
//
//
//loop {
//  if waiter.waited_since > 10s -> Find inital tx everyone is waiting for ->
//
//}
