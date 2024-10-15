use ahash::RandomState;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};
use synevi_types::{traits::Store, T, T0};
use tokio::{sync::oneshot, time::Instant};

pub struct Waiter {
    waited_since: Instant,
    dependency_states: u64,
    sender: Vec<oneshot::Sender<()>>,
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
    pub fn new(store: Arc<S>) -> Self {
        Self {
            waiters: Mutex::new(HashMap::default()),
            store,
        }
    }

    pub fn get_waiter(&self, t0: &T0) -> oneshot::Receiver<()> {
        let (sdx, rcv) = oneshot::channel();
        let mut waiter_lock = self.waiters.lock().expect("Locking waiters failed");
        let waiter = waiter_lock.entry(*t0).or_insert(Waiter {
            waited_since: Instant::now(),
            dependency_states: 0,
            sender: Vec::new(),
        });
        waiter.sender.push(sdx);
        rcv
    }

    pub fn commit(&self, t0_commit: &T0, t_commit: &T) {
        let mut waiter_lock = self.waiters.lock().expect("Locking waiters failed");
        waiter_lock.retain(|t0_waiting, waiter| {
            let event = self.store.get_event(*t0_waiting).unwrap().unwrap(); // TODO: Remove unwrap
            if event.dependencies.contains(t0_commit) {
                if t_commit > &event.t {
                    waiter.dependency_states += 1;
                    waiter.waited_since = Instant::now();
                    if waiter.dependency_states >= event.dependencies.len() as u64 {
                        for sdx in waiter.sender.drain(..) {
                            sdx.send(()).unwrap(); // TODO: Remove unwrap
                        }
                        return false;
                    }
                }
            }
            true
        });
    }

    pub fn apply(&self, t0_commit: &T0) {
        let mut waiter_lock = self.waiters.lock().expect("Locking waiters failed");
        waiter_lock.retain(|t0_waiting, waiter| {
            let event = self.store.get_event(*t0_waiting).unwrap().unwrap(); // TODO: Remove unwrap
            if event.dependencies.contains(t0_commit) {
                waiter.dependency_states += 1;
                waiter.waited_since = Instant::now();
                if waiter.dependency_states >= event.dependencies.len() as u64 {
                    for sdx in waiter.sender.drain(..) {
                        sdx.send(()).unwrap(); // TODO: Remove unwrap
                    }
                    return false;
                }
            }
            true
        });
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
