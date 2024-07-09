use crate::{
    event_store::{Event, EventStore},
    utils::{Ballot, T, T0},
};
use ahash::RandomState;
use anyhow::Result;
use async_channel::{Receiver, Sender};
use bytes::Bytes;
use consensus_transport::{consensus_transport::State, network::Network};
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::{
    sync::{oneshot, Mutex},
    time::timeout,
};

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum WaitAction {
    CommitBefore,
    ApplyAfter,
}

#[derive(Debug)]
pub struct WaitMessage {
    t_zero: T0,
    t: T,
    deps: HashSet<T0, RandomState>,
    event: Vec<u8>,
    action: WaitAction,
    notify: Option<oneshot::Sender<()>>,
}

#[derive(Clone, Debug)]
pub struct WaitHandler {
    sender: Sender<WaitMessage>,
    receiver: Receiver<WaitMessage>,
    event_store: Arc<Mutex<EventStore>>,
    _network: Arc<dyn Network + Send + Sync>,
}

#[derive(Debug)]
struct WaitDependency {
    wait_message: Option<WaitMessage>,
    deps: HashSet<T0, RandomState>,
    _started_at: Instant,
}

struct WaiterState {
    events: HashMap<T0, WaitDependency, RandomState>,
    committed: HashMap<T0, T, RandomState>,
    applied: HashSet<T0, RandomState>,
}

impl WaitHandler {
    pub fn new(
        event_store: Arc<Mutex<EventStore>>,
        network: Arc<dyn Network + Send + Sync>,
    ) -> Arc<Self> {
        let (sender, receiver) = async_channel::bounded(1000);
        Arc::new(Self {
            sender,
            receiver,
            event_store,
            _network: network,
        })
    }

    pub async fn send_msg(
        &self,
        t_zero: T0,
        t: T,
        deps: HashSet<T0, RandomState>,
        event: Vec<u8>,
        action: WaitAction,
        notify: oneshot::Sender<()>,
    ) -> Result<()> {
        Ok(self
            .sender
            .send(WaitMessage {
                t_zero,
                t,
                deps,
                event,
                action,
                notify: Some(notify),
            })
            .await?)
    }

    pub async fn run(&self) -> Result<()> {
        // HashMap<T0_dep waiting_for, Vec<T0_transaction waiting>>

        let mut waiter_state = WaiterState::new();

        loop {
            match timeout(Duration::from_millis(50), self.receiver.recv()).await {
                Ok(Ok(msg)) => match msg.action {
                    WaitAction::CommitBefore => {
                        //println!("CommitBefore");
                        self.upsert_event(&msg).await;
                        waiter_state
                            .committed
                            .insert(msg.t_zero.clone(), msg.t.clone());
                        let mut to_apply =
                            waiter_state.remove_from_waiter_commit(&msg.t_zero, &msg.t);
                        while let Some(mut apply) = to_apply.pop() {
                            apply.action = WaitAction::ApplyAfter;
                            self.upsert_event(&apply).await;
                            waiter_state.applied.insert(apply.t_zero);
                            if let Some(notify) = apply.notify.take() {
                                let _ = notify.send(());
                            }
                            waiter_state.remove_from_waiter_apply(&apply.t_zero, &mut to_apply);
                        }
                        waiter_state.insert_commit(msg);
                    }
                    WaitAction::ApplyAfter => {
                        if let Some(mut msg) = waiter_state.insert_apply(msg) {
                            self.upsert_event(&msg).await;
                            if let Some(notify) = msg.notify.take() {
                                let _ = notify.send(());
                            }
                            waiter_state.applied.insert(msg.t_zero);
                            let mut to_apply = Vec::new();
                            waiter_state.remove_from_waiter_apply(&msg.t_zero, &mut to_apply);
                            while let Some(mut apply) = to_apply.pop() {
                                apply.action = WaitAction::ApplyAfter;
                                self.upsert_event(&apply).await;
                                waiter_state.applied.insert(apply.t_zero);
                                if let Some(notify) = apply.notify.take() {
                                    let _ = notify.send(());
                                }
                                waiter_state.remove_from_waiter_apply(&apply.t_zero, &mut to_apply);
                            }
                        }
                    }
                },
                _ => {
                    //println!("{:?}", waiter_state.events)
                }
            }
        }
    }

    async fn upsert_event(
        &self,
        WaitMessage {
            t_zero,
            t,
            action,
            deps,
            event,
            ..
        }: &WaitMessage,
    ) {
        let state = match action {
            WaitAction::CommitBefore => State::Commited,
            WaitAction::ApplyAfter => State::Applied,
        };
        self.event_store
            .lock()
            .await
            .upsert(Event {
                t_zero: *t_zero,
                t: *t,
                state,
                event: event.clone(),
                dependencies: deps.clone(),
                ballot: Ballot::default(),
            })
            .await;
    }
}

impl WaiterState {
    fn new() -> Self {
        Self {
            events: HashMap::default(),
            committed: HashMap::default(),
            applied: HashSet::default(),
        }
    }

    fn remove_from_waiter_commit(&mut self, t0_dep: &T0, t_dep: &T) -> Vec<WaitMessage> {
        let mut apply_deps = Vec::new();
        self.events.retain(|_, event| {
            if let Some(msg) = &mut event.wait_message {
                if msg.t_zero == *t0_dep {
                    return true;
                }

                if t_dep < &msg.t {
                    // Cannot remove must wait for apply -> retain
                    return true;
                }
                event.deps.remove(t0_dep);
                if event.deps.is_empty() {
                    if msg.action != WaitAction::ApplyAfter {
                        if let Some(sender) = msg.notify.take() {
                            let _ = sender.send(());
                        }
                    } else {
                        if let Some(msg) = event.wait_message.take() {
                            apply_deps.push(msg);
                        }
                    }
                    return false;
                }
            }
            true
        });
        apply_deps
    }

    fn remove_from_waiter_apply(&mut self, t0_dep: &T0, to_apply: &mut Vec<WaitMessage>) {
        self.events.retain(|_, event| {
            event.deps.remove(t0_dep);
            for wait_dep in to_apply.iter() {
                event.deps.remove(&wait_dep.t_zero);
            }

            if let Some(msg) = &mut event.wait_message {
                if event.deps.is_empty() {
                    if msg.action != WaitAction::ApplyAfter {
                        if let Some(sender) = msg.notify.take() {
                            let _ = sender.send(());
                        }
                    } else {
                        if let Some(msg) = event.wait_message.take() {
                            to_apply.push(msg);
                        }
                    }
                    return false;
                }
            }
            true
        });
    }

    fn insert_commit(&mut self, mut wait_message: WaitMessage) {
        if self.applied.contains(&wait_message.t_zero) {
            if let Some(sender) = wait_message.notify.take() {
                let _ = sender.send(());
            }
            return;
        }
        let mut wait_dep = WaitDependency {
            wait_message: Some(wait_message),
            deps: HashSet::default(),
            _started_at: Instant::now(),
        };
        if let Some(wait_message) = &mut wait_dep.wait_message {
            for dep_t0 in wait_message.deps.iter() {
                if !self.applied.contains(&dep_t0) {
                    if let Some(stored_t) = self.committed.get(&dep_t0) {
                        // Your T is lower than the dep commited t -> no wait necessary
                        if &wait_message.t < stored_t {
                            continue;
                        }
                    }
                    wait_dep.deps.insert(*dep_t0);
                }
            }

            if wait_dep.deps.is_empty() {
                if let Some(sender) = wait_message.notify.take() {
                    let _ = sender.send(());
                }
                return;
            }

            if let Some(existing) = self.events.get_mut(&wait_message.t_zero) {
                if let Some(existing_wait_message) = &mut existing.wait_message {
                    if let Some(sender) = existing_wait_message.notify.take() {
                        let _ = sender.send(());
                        return;
                    }
                }
            }
            self.events.insert(wait_message.t_zero, wait_dep);
        }
    }

    fn insert_apply(&mut self, mut wait_message: WaitMessage) -> Option<WaitMessage> {
        if self.applied.contains(&wait_message.t_zero) {
            if let Some(sender) = wait_message.notify.take() {
                let _ = sender.send(());
            }
            return None;
        }
        let mut wait_dep = WaitDependency {
            wait_message: Some(wait_message),
            deps: HashSet::default(),
            _started_at: Instant::now(),
        };
        if let Some(wait_message) = &wait_dep.wait_message {
            for dep_t0 in wait_message.deps.iter() {
                if !self.applied.contains(&dep_t0) {
                    if let Some(stored_t) = self.committed.get(&dep_t0) {
                        // Your T is lower than the dep commited t -> no wait necessary
                        if &wait_message.t < stored_t {
                            continue;
                        }
                    }
                    wait_dep.deps.insert(*dep_t0);
                }
            }

            if wait_dep.deps.is_empty() {
                if let Some(wait_msg) = wait_dep.wait_message.take() {
                    return Some(wait_msg);
                }
            } else {
                self.events.insert(wait_message.t_zero, wait_dep);
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use monotime::MonoTime;

    use crate::wait_handler::*;

    #[tokio::test]
    async fn test_wait_handler() {
        let (sender, receiver): (Sender<WaitMessage>, Receiver<WaitMessage>) =
            async_channel::unbounded();
        let wait_handler = WaitHandler {
            sender,
            receiver,
            event_store: Arc::new(Mutex::new(EventStore::init(None, 0))),
            _network: Arc::new(crate::tests::NetworkMock::default()),
        };

        let (sx11, rx11) = tokio::sync::oneshot::channel();
        let (sx12, rx12) = tokio::sync::oneshot::channel();
        let (sx21, _rx21) = tokio::sync::oneshot::channel();

        // let notify_2_1_future = notify_2_1.notified();
        // let notify_2_2_future = notify_2_2.notified();

        let t0_1 = T0(MonoTime::new_with_time(1u128, 0, 0));
        let t0_2 = T0(MonoTime::new_with_time(2u128, 0, 0));
        let t_1 = T(MonoTime::new_with_time(1u128, 0, 0));
        let t_2 = T(MonoTime::new_with_time(2u128, 0, 0));
        let deps_2 = HashSet::from_iter([t0_1.clone()]);
        wait_handler
            .send_msg(
                t0_2.clone(),
                t_2.clone(),
                deps_2.clone(),
                Vec::new(),
                WaitAction::CommitBefore,
                sx11,
            )
            .await
            .unwrap();
        wait_handler
            .send_msg(
                t0_1,
                t_1,
                HashSet::default(),
                Vec::new(),
                WaitAction::CommitBefore,
                sx12,
            )
            .await
            .unwrap();

        wait_handler
            .send_msg(
                t0_1,
                t_1,
                HashSet::default(),
                Vec::new(),
                WaitAction::ApplyAfter,
                sx21,
            )
            .await
            .unwrap();
        // wait_handler
        //     .send_msg(
        //         t0_2.clone(),
        //         t_2.clone(),
        //         deps_2.clone(),
        //         Bytes::new(),
        //         WaitAction::CommitBefore,
        //         notify_2_1.clone(),
        //     )
        //     .await
        //     .unwrap();
        // wait_handler
        //     .send_msg(
        //         t0_1,
        //         t_1,
        //         HashMap::new(),
        //         Bytes::new(),
        //         WaitAction::ApplyAfter,
        //         notify_1_2.clone(),
        //     )
        //     .await
        //     .unwrap();

        tokio::spawn(async move { wait_handler.run().await.unwrap() });
        timeout(Duration::from_millis(10), rx11)
            .await
            .unwrap()
            .unwrap();
        timeout(Duration::from_millis(10), rx12)
            .await
            .unwrap()
            .unwrap();
        // timeout(Duration::from_millis(10), notify_2_1_future)
        //     .await
        //     .unwrap();
        // timeout(Duration::from_millis(10), notify_2_2_future)
        //     .await
        //     .unwrap();
    }
}
