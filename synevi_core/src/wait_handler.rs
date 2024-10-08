use crate::coordinator::Coordinator;
use crate::node::Node;
use ahash::RandomState;
use async_channel::{Receiver, Sender};
use std::collections::BTreeMap;
use std::panic;
use std::sync::atomic::AtomicU8;
use std::{
    collections::{BTreeSet, HashMap, HashSet},
    sync::Arc,
    time::{Duration, Instant},
};
use synevi_network::network::Network;
use synevi_types::traits::Store;
use synevi_types::types::UpsertEvent;
use synevi_types::{Executor, State, SyneviError, T, T0};
use tokio::{sync::oneshot, time::timeout};

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum WaitAction {
    CommitBefore,
    ApplyAfter,
}

#[derive(Debug)]
pub struct WaitMessage {
    id: u128,
    t_zero: T0,
    t: T,
    deps: HashSet<T0, RandomState>,
    transaction: Vec<u8>,
    action: WaitAction,
    notify: Option<oneshot::Sender<()>>,
}

#[derive(Clone)]
pub struct WaitHandler<N, E, S>
where
    N: Network + Send + Sync,
    E: Executor + Send + Sync,
    S: Store + Send + Sync,
{
    sender: Sender<WaitMessage>,
    receiver: Receiver<WaitMessage>,
    node: Arc<Node<N, E, S>>,
}

#[derive(Debug)]
struct WaitDependency {
    wait_message: Option<WaitMessage>,
    deps: HashSet<T0, RandomState>,
    started_at: Instant,
}

struct WaiterState {
    events: HashMap<T0, WaitDependency, RandomState>,
    committed: HashMap<T0, T, RandomState>,
    applied: HashSet<T0, RandomState>,
}

static RECOVERY_CYCLE: AtomicU8 = AtomicU8::new(0);

impl<N, E, S> WaitHandler<N, E, S>
where
    N: Network + Send + Sync,
    E: Executor + Send + Sync,
    S: Store + Send + Sync,
{
    pub fn new(node: Arc<Node<N, E, S>>) -> Arc<Self> {
        let (sender, receiver) = async_channel::bounded(1000);
        Arc::new(Self {
            sender,
            receiver,
            node,
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn send_msg(
        &self,
        t_zero: T0,
        t: T,
        deps: HashSet<T0, RandomState>,
        transaction: Vec<u8>,
        action: WaitAction,
        notify: oneshot::Sender<()>,
        id: u128,
    ) -> Result<(), SyneviError> {
        self.sender
            .send(WaitMessage {
                id,
                t_zero,
                t,
                deps,
                transaction,
                action,
                notify: Some(notify),
            })
            .await
            .map_err(|e| SyneviError::SendError(e.to_string()))
    }

    pub async fn run(self: Arc<Self>) -> Result<(), SyneviError> {
        // HashMap<T0_dep waiting_for, Vec<T0_transaction waiting>>

        let mut waiter_state = WaiterState::new();
        let mut recovering = BTreeSet::new();

        loop {
            let future = async {
                let msg = self.receiver.recv().await;
                //if self.node.info.serial == 6 {
                //    dbg!(&msg);
                //}
                msg
            };
            match timeout(Duration::from_millis(50), future).await {
                Ok(Ok(msg)) => match msg.action {
                    WaitAction::CommitBefore => {
                        //if self.node.info.serial == 6 {
                        //                             println!(
                        //                                 "COMMIT BEFORE
                        // t0: {:?}
                        // t: {:?}
                        // deps: {:?}",
                        //                                 &msg.t_zero, &msg.t, &msg.deps
                        //                             );
                        //}

                        if let Err(err) = self.commit_action(msg, &mut waiter_state).await {
                            tracing::error!("Error commit event: {:?}", err);
                            println!("Error commit event: {:?}", err);
                            continue;
                        };
                        //println!(
                        //    "t0: {:?}, t: {:?}, deps: {:?}",
                        //    &msg.t_zero, &msg.t, &msg.deps
                        //);
                        // if let Err(e) = self.upsert_event(&msg).await {
                        //     tracing::error!("Error upserting event: {:?}", e);
                        //     println!("Error upserting event: {:?}", e);
                        //     continue;
                        // };
                        // waiter_state.committed.insert(msg.t_zero, msg.t);
                        // let mut to_apply =
                        //     waiter_state.remove_from_waiter_commit(&msg.t_zero, &msg.t);
                        // while let Some(mut apply) = to_apply.pop_first() {
                        //     apply.1.action = WaitAction::ApplyAfter;
                        //     if let Err(e) = self.upsert_event(&msg).await {
                        //         tracing::error!("Error upserting event: {:?}", e);
                        //         println!("Error upserting event: {:?}", e);
                        //         continue;
                        //     };
                        //     waiter_state.applied.insert(apply.1.t_zero);
                        //     if let Some(notify) = apply.1.notify.take() {
                        //         let _ = notify.send(());
                        //     }
                        //     waiter_state.remove_from_waiter_apply(&apply.1.t_zero, &mut to_apply);
                        // }
                        // waiter_state.insert_commit(msg);
                    }
                    WaitAction::ApplyAfter => {
                        //                        if self.node.info.serial == 6 {
                        //                            println!(
                        //                                "APPLY AFTER
                        //t0: {:?}
                        //t: {:?}
                        //deps: {:?}",
                        //                                &msg.t_zero, &msg.t, &msg.deps
                        //                            );
                        //                        }
                        match &self.node.event_store.get_event(msg.t_zero).await? {
                            Some(event) if event.state < State::Commited => {
                                //println!("{:?} NOT COMMITTED", &msg.t_zero);
                                if let Err(err) = self
                                    .commit_action(
                                        WaitMessage {
                                            id: msg.id,
                                            t_zero: msg.t_zero,
                                            t: msg.t,
                                            deps: msg.deps.clone(),
                                            transaction: msg.transaction.clone(),
                                            action: WaitAction::CommitBefore,
                                            notify: None,
                                        },
                                        &mut waiter_state,
                                    )
                                    .await
                                {
                                    tracing::error!(
                                        "Error committing event before apply: {:?}",
                                        err
                                    );
                                    println!("Error committing event bevore apply: {:?}", err);
                                    continue;
                                };
                                //                                if let Err(e) = self.upsert_event(&msg).await {
                                //                                    tracing::error!("Error upserting event: {:?}", e);
                                //                                    println!("Error upserting event: {:?}", e);
                                //                                    continue;
                                //                                };
                                //                                waiter_state.committed.insert(msg.t_zero, msg.t);
                                //                                let mut to_apply =
                                //                                    waiter_state.remove_from_waiter_commit(&msg.t_zero, &msg.t);
                                //                                while let Some(mut apply) = to_apply.pop_first() {
                                //                                    apply.1.action = WaitAction::ApplyAfter;
                                //                                    if let Err(e) = self.upsert_event(&msg).await {
                                //                                        tracing::error!("Error upserting event: {:?}", e);
                                //                                        println!("Error upserting event: {:?}", e);
                                //                                        continue;
                                //                                    };
                                //                                    waiter_state.applied.insert(apply.1.t_zero);
                                //                                    if let Some(notify) = apply.1.notify.take() {
                                //                                        let _ = notify.send(());
                                //                                    }
                                //                                    waiter_state
                                //                                        .remove_from_waiter_apply(&apply.1.t_zero, &mut to_apply);
                                //                                }
                                //                                waiter_state.insert_commit(WaitMessage {
                                //                                    id: msg.id,
                                //                                    t_zero: msg.t_zero,
                                //                                    t: msg.t,
                                //                                    deps: msg.deps.clone(),
                                //                                    transaction: msg.transaction.clone(),
                                //                                    action: WaitAction::CommitBefore,
                                //                                    notify: None,
                                //                                });
                            }
                            None => {
                                //println!("{:?} NOT COMMITTED", &msg.t_zero);
                                if let Err(err) = self
                                    .commit_action(
                                        WaitMessage {
                                            id: msg.id,
                                            t_zero: msg.t_zero,
                                            t: msg.t,
                                            deps: msg.deps.clone(),
                                            transaction: msg.transaction.clone(),
                                            action: WaitAction::CommitBefore,
                                            notify: None,
                                        },
                                        &mut waiter_state,
                                    )
                                    .await
                                {
                                    tracing::error!(
                                        "Error committing event before apply: {:?}",
                                        err
                                    );
                                    println!("Error committing event before apply: {:?}", err);
                                    continue;
                                };
                                //                                if let Err(e) = self.upsert_event(&msg).await {
                                //                                    tracing::error!("Error upserting event: {:?}", e);
                                //                                    println!("Error upserting event: {:?}", e);
                                //                                    continue;
                                //                                };
                                //                                waiter_state.committed.insert(msg.t_zero, msg.t);
                                //                                let mut to_apply =
                                //                                    waiter_state.remove_from_waiter_commit(&msg.t_zero, &msg.t);
                                //                                while let Some(mut apply) = to_apply.pop_first() {
                                //                                    apply.1.action = WaitAction::ApplyAfter;
                                //                                    if let Err(e) = self.upsert_event(&msg).await {
                                //                                        tracing::error!("Error upserting event: {:?}", e);
                                //                                        println!("Error upserting event: {:?}", e);
                                //                                        continue;
                                //                                    };
                                //                                    waiter_state.applied.insert(apply.1.t_zero);
                                //                                    if let Some(notify) = apply.1.notify.take() {
                                //                                        let _ = notify.send(());
                                //                                    }
                                //                                    waiter_state
                                //                                        .remove_from_waiter_apply(&apply.1.t_zero, &mut to_apply);
                                //                                }
                                //                                waiter_state.insert_commit(WaitMessage {
                                //                                    id: msg.id,
                                //                                    t_zero: msg.t_zero,
                                //                                    t: msg.t,
                                //                                    deps: msg.deps.clone(),
                                //                                    transaction: msg.transaction.clone(),
                                //                                    action: WaitAction::CommitBefore,
                                //                                    notify: None,
                                //                                });
                            }
                            _ => (),
                        }

                        if let Some(mut msg) = waiter_state.insert_apply(msg) {
                            if let Err(e) = self.upsert_event(&msg).await {
                                tracing::error!("Error upserting event: {:?}", e);
                                println!("Error upserting event: {:?}", e);
                                continue;
                            };
                            if let Some(notify) = msg.notify.take() {
                                let _ = notify.send(()).unwrap();
                            }
                            waiter_state.applied.insert(msg.t_zero);
                            let mut to_apply = BTreeMap::new();
                            waiter_state.remove_from_waiter_apply(&msg.t_zero, &mut to_apply);
                            while let Some(mut apply) = to_apply.pop_first() {
                                apply.1.action = WaitAction::ApplyAfter;
                                if let Err(e) = self.upsert_event(&apply.1).await {
                                    tracing::error!("Error upserting event: {:?}", e);
                                    println!("Error upserting event: {:?}", e);
                                    continue;
                                };
                                waiter_state.applied.insert(apply.1.t_zero);
                                if let Some(notify) = apply.1.notify.take() {
                                    let _ = notify.send(()).unwrap();
                                }
                                waiter_state
                                    .remove_from_waiter_apply(&apply.1.t_zero, &mut to_apply);
                            }
                        }
                    }
                },
                _ => {
                    if let Some(t0_recover) = self.check_recovery(&mut waiter_state) {
                        //if self.node.info.serial == 6 {
                        //    dbg!("Recovering", t0_recover);
                        //}
                        if !recovering.insert(t0_recover) {
                            //dbg!("Inserting existing recovery");
                        };
                        //panic!("Starting recovery");
                        //println!("Recovering: {:?}", t0_recover);
                        let recover_t0 = recovering.pop_first().unwrap_or(t0_recover);
                        let wait_handler = self.clone();
                        wait_handler.recover(recover_t0, &mut waiter_state).await;
                    }
                }
            }
        }
    }

    async fn commit_action(
        &self,
        msg: WaitMessage,
        waiter_state: &mut WaiterState,
    ) -> Result<(), SyneviError> {
        self.upsert_event(&msg).await?;
        waiter_state.committed.insert(msg.t_zero, msg.t);
        let mut to_apply = waiter_state.remove_from_waiter_commit(&msg.t_zero, &msg.t);
        while let Some(mut apply) = to_apply.pop_first() {
            apply.1.action = WaitAction::ApplyAfter;
            if let Err(e) = self.upsert_event(&apply.1).await {
                tracing::error!("Error upserting event: {:?}", e);
                println!("Error upserting event: {:?}", e);
                continue;
            };
            waiter_state.applied.insert(apply.1.t_zero);
            if let Some(notify) = apply.1.notify.take() {
                let _ = notify.send(()).unwrap();
            }
            waiter_state.remove_from_waiter_apply(&apply.1.t_zero, &mut to_apply);
        }
        waiter_state.insert_commit(msg);
        Ok(())
    }

    async fn upsert_event(
        &self,
        WaitMessage {
            id,
            t_zero,
            t,
            action,
            deps,
            transaction,
            ..
        }: &WaitMessage,
    ) -> Result<(), SyneviError> {
        let state = match action {
            WaitAction::CommitBefore => State::Commited,
            WaitAction::ApplyAfter => State::Applied,
        };
        self.node
            .event_store
            .upsert_tx(UpsertEvent {
                id: *id,
                t_zero: *t_zero,
                t: *t,
                state,
                transaction: Some(transaction.clone()),
                dependencies: Some(deps.clone()),
                ..Default::default()
            })
            .await?;

        Ok(())
    }

    async fn recover(self: Arc<Self>, t0_recover: T0, waiter_state: &mut WaiterState) {
        //if self.node.info.serial == 6 {
        //    //dbg!("Started recovery of", t0_recover, &self.node.info);
        //    panic!("STARTED_RECOVERY");
        //}
        //if RECOVERY_CYCLE.fetch_add(1, std::sync::atomic::Ordering::Relaxed) > 1 {
        //    panic!("Reached recovery cycle threshold");
        //}
        if let Some(event) = waiter_state.events.get_mut(&t0_recover) {
            event.started_at = Instant::now();
            if let Some(msg) = &event.wait_message {
                //if self.node.info.serial == 6 {
                //    dbg!(msg.t);
                //}
            }
            // if RECOVERY_CYCLE.fetch_add(1, std::sync::atomic::Ordering::Relaxed) > 2 {
            //     panic!("Reached recovery cycle threshold");
            // }
        } else {
            if self.node.info.serial == 6 {
                if let Ok(None) = &self.node.event_store.get_event(t0_recover).await {
                    //let msgs = self.receiver.clone().collect::<Vec<WaitMessage>>();
                    //self.sender.close();
                    //dbg!("IN FLIGHT MSGS", msgs.await);
                    //dbg!(&self.node.stats.total_recovers);
                    //panic!("Nothing found in event store");
                    //dbg!("Nothing found in event store");
                    //return ();
                }
            }
        }

        let node = self.node.clone();
        tokio::spawn(async move {
            if let Err(err) = Coordinator::recover(node, t0_recover).await {
                if self.node.info.serial == 6 {
                    dbg!(err);
                }
            }
        });
        //panic!("Started recovery");
    }

    fn check_recovery(&self, waiter_state: &mut WaiterState) -> Option<T0> {
        //println!("CHECK RECOVERY");
        for (
            t0,
            WaitDependency {
                deps, started_at, ..
            },
        ) in waiter_state.events.iter_mut()
        {
            if started_at.elapsed() > Duration::from_secs(1) {
                let sorted_deps: BTreeSet<T0> = deps.iter().cloned().collect();

                let mut min_dep = None;
                for t0_dep in sorted_deps {
                    if let Some(t_dep) = waiter_state.committed.get(&t0_dep) {
                        // Check if lowest t0 is committed
                        // If yes -> Recover dep with lowest T
                        if let Some((t0_min, t_min)) = min_dep.as_mut() {
                            if t_dep < t_min {
                                *t0_min = t0_dep;
                                *t_min = *t_dep;
                            }
                        } else {
                            min_dep = Some((t0_dep, *t_dep));
                        }
                    } else {
                        if self.node.info.serial == 6 {
                            //dbg!("Recover deps from:", &t0);
                            // dbg!(&waiter_state.events));
                            // dbg!(&waiter_state.committed);
                            // dbg!(&waiter_state.applied);
                        }
                        // Lowest T0 is not commited -> Recover lowest t0 to ensure commit
                        // Recover t0_dep

                        *started_at = Instant::now();
                        return Some(t0_dep);
                    }
                }

                // Recover min_dep
                if let Some((t0_dep, _)) = min_dep {
                    //if self.node.info.serial == 6 {
                    //dbg!("Recover deps from:", &t0);
                    //}

                    *started_at = Instant::now();
                    return Some(t0_dep);
                }
            }
        }
        None
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

    fn remove_from_waiter_commit(&mut self, t0_dep: &T0, t_dep: &T) -> BTreeMap<T, WaitMessage> {
        let mut apply_deps = BTreeMap::default();
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
                            let _ = sender.send(()).unwrap();
                        }
                    } else if let Some(msg) = event.wait_message.take() {
                        apply_deps.insert(msg.t, msg);
                    }
                    return false;
                }
            }
            true
        });
        apply_deps
    }

    fn remove_from_waiter_apply(&mut self, t0_dep: &T0, to_apply: &mut BTreeMap<T, WaitMessage>) {
        self.events.retain(|_, event| {
            event.deps.remove(t0_dep);
            for wait_dep in to_apply.iter() {
                event.deps.remove(&wait_dep.1.t_zero);
            }

            if let Some(msg) = &mut event.wait_message {
                if event.deps.is_empty() {
                    if msg.action != WaitAction::ApplyAfter {
                        if let Some(sender) = msg.notify.take() {
                            let _ = sender.send(()).unwrap();
                        }
                    } else if let Some(msg) = event.wait_message.take() {
                        to_apply.insert(msg.t, msg);
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
                let _ = sender.send(()).unwrap();
            }
            return;
        }
        let mut wait_dep = WaitDependency {
            wait_message: Some(wait_message),
            deps: HashSet::default(),
            started_at: Instant::now(),
        };
        if let Some(wait_message) = &mut wait_dep.wait_message {
            for dep_t0 in wait_message.deps.iter() {
                if !self.applied.contains(dep_t0) {
                    if let Some(stored_t) = self.committed.get(dep_t0) {
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
                    let _ = sender.send(()).unwrap();
                }
                return;
            }

            if let Some(existing) = self.events.get_mut(&wait_message.t_zero) {
                if let Some(existing_wait_message) = &mut existing.wait_message {
                    if let Some(sender) = existing_wait_message.notify.take() {
                        let _ = sender.send(()).unwrap();
                        return;
                    }
                }
            }
            self.events.insert(wait_message.t_zero, wait_dep);
        }
    }

    fn insert_apply(&mut self, mut wait_message: WaitMessage) -> Option<WaitMessage> {
        let (t0, t) = (wait_message.t_zero, wait_message.t);
        if self.applied.contains(&wait_message.t_zero) {
            if let Some(sender) = wait_message.notify.take() {
                let _ = sender.send(()).unwrap();
            }
            return None;
        }
        let debug_deps = wait_message.deps.clone();
        let mut deps = HashSet::default();
        let mut wait_dep = WaitDependency {
            wait_message: Some(wait_message),
            deps: HashSet::default(),
            started_at: Instant::now(),
        };

        if let Some(wait_message) = &wait_dep.wait_message {
            for dep_t0 in wait_message.deps.iter() {
                if !self.applied.contains(dep_t0) {
                    if let Some(stored_t) = self.committed.get(dep_t0) {
                        // Your T is lower than the dep commited t -> no wait necessary
                        if &wait_message.t < stored_t {
                            //                            println!(
                            //                                "SKIP EVENT WITH
                            //T0:  {:?}
                            //T:    {:?}",
                            //                                dep_t0, stored_t
                            //                            );
                            continue;
                        }
                    }
                    // if not applied and not comitted with lower t
                    deps.insert(*dep_t0);
                }
            }

            if deps.is_empty() {
                if let Some(wait_msg) = wait_dep.wait_message.take() {
                    //                    println!(
                    //                        "NO DEPS FOUND FOR
                    //T0:  {:?}
                    //T:    {:?},
                    //DEPS: {:?},
                    ///////////////////////////////
                    //{:?}
                    //{:?}",
                    //                        &t0, &t, &debug_deps, &self.committed, &self.applied
                    //                    );

                    return Some(wait_msg);
                }
            } else {
                wait_dep.deps = deps.clone();
                self.events.insert(wait_message.t_zero, wait_dep);
            }
        }
        //        println!(
        //            "
        //NOT APPLYING BECAUSE OF DEPENDENCIES:
        //{:?}
        //{:?}
        //{:?}
        ////////////////////////////////////////
        //{:?}
        //{:?}
        //",
        //            &t0, &t, deps, &self.committed, &self.applied
        //        );
        None
    }
}

#[cfg(test)]
mod tests {
    use monotime::MonoTime;
    use ulid::Ulid;

    use crate::{
        tests::{DummyExecutor, NetworkMock},
        wait_handler::*,
    };

    #[tokio::test]
    async fn test_wait_handler() {
        let (sender, receiver): (Sender<WaitMessage>, Receiver<WaitMessage>) =
            async_channel::unbounded();

        let node = Node::new_with_network_and_executor(
            Ulid::new(),
            1,
            NetworkMock::default(),
            DummyExecutor,
        )
        .await
        .unwrap();
        let wait_handler = WaitHandler {
            sender,
            receiver,
            node,
        };

        let (sx11, rx11) = tokio::sync::oneshot::channel();
        let (sx12, rx12) = tokio::sync::oneshot::channel();
        let (sx21, _rx21) = tokio::sync::oneshot::channel();

        // let notify_2_1_future = notify_2_1.notified();
        // let notify_2_2_future = notify_2_2.notified();

        let id_1 = u128::from_be_bytes(Ulid::new().to_bytes());
        let _id_2 = u128::from_be_bytes(Ulid::new().to_bytes());
        let t0_1 = T0(MonoTime::new_with_time(1u128, 0, 0));
        let t0_2 = T0(MonoTime::new_with_time(2u128, 0, 0));
        let t_1 = T(MonoTime::new_with_time(1u128, 0, 0));
        let t_2 = T(MonoTime::new_with_time(2u128, 0, 0));
        let deps_2 = HashSet::from_iter([t0_1]);
        wait_handler
            .send_msg(
                t0_2,
                t_2,
                deps_2.clone(),
                Vec::new(),
                WaitAction::CommitBefore,
                sx11,
                id_1,
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
                id_1,
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
                id_1,
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

        let wait_handler = Arc::new(wait_handler);

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
