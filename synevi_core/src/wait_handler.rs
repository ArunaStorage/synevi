use crate::coordinator::Coordinator;
use crate::node::Node;
use ahash::RandomState;
use async_channel::{Receiver, Sender};
use std::collections::BTreeMap;
use std::sync::atomic::AtomicU8;
use std::{
    collections::{BTreeSet, HashMap, HashSet},
    sync::Arc,
    time::{Duration, Instant},
};
use synevi_network::network::{Network, NetworkInterface};
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

static _RECOVERY_CYCLE: AtomicU8 = AtomicU8::new(0);

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
        let mut waiter_state = WaiterState::new();
        let mut recovering = BTreeSet::new();

        loop {
            match timeout(Duration::from_millis(50), self.receiver.recv()).await {
                Ok(Ok(msg)) => match msg.action {
                    WaitAction::CommitBefore => {
                        if let Err(err) = self.commit_action(msg, &mut waiter_state).await {
                            tracing::error!("Error commit event: {:?}", err);
                            println!("Error commit event: {:?}", err);
                            continue;
                        };
                    }
                    WaitAction::ApplyAfter => {
                        match &self.node.event_store.get_event(msg.t_zero).await? {
                            Some(event) if event.state < State::Commited => {
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
                            }
                            None => {
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
                                let _ = notify.send(());
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
                                    let _ = notify.send(());
                                }
                                waiter_state
                                    .remove_from_waiter_apply(&apply.1.t_zero, &mut to_apply);
                            }
                        }
                    }
                },
                _ => {
                    if let Some(t0_recover) = self.check_recovery(&mut waiter_state) {
                        recovering.insert(t0_recover);
                        let recover_t0 = recovering.pop_first().unwrap_or(t0_recover);
                        let wait_handler = self.clone();
                        if let Err(err) = wait_handler.recover(recover_t0, &mut waiter_state).await
                        {
                            tracing::error!("Error recovering event: {:?}", err);
                            println!("Error recovering event: {:?}", err);
                        };
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
                let _ = notify.send(());
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

    async fn recover(
        self: Arc<Self>,
        t0_recover: T0,
        waiter_state: &mut WaiterState,
    ) -> Result<(), SyneviError> {
        if let Some(event) = waiter_state.events.get_mut(&t0_recover) {
            event.started_at = Instant::now();
        }
        if let Some(recover_event) = self
            .node
            .event_store
            .recover_event(&t0_recover, self.node.get_info().serial)
            .await?
        {
            let node = self.node.clone();
            tokio::spawn(async move {
                if let Err(err) = Coordinator::recover(node, recover_event).await {
                    tracing::error!("Error recovering event: {:?}", err);
                    println!("Error recovering event: {:?}", err);
                }
            });
        } else {
            let interface = self.node.network.get_interface().await;
            match interface.broadcast_recovery(t0_recover).await {
                Ok(true) => (),
                Ok(false) => {

                    // Remove from
                    // SAFETY: No majority has witnessed this Tx and no fast path is possible
                    // -> Can be removed from waiter_state because a recovery will enforce a slow path / new t
                    let mut to_apply = BTreeMap::new();
                    waiter_state.remove_from_waiter_apply(&t0_recover, &mut to_apply);
                    while let Some(mut apply) = to_apply.pop_first() {
                        apply.1.action = WaitAction::ApplyAfter;
                        if let Err(e) = self.upsert_event(&apply.1).await {
                            tracing::error!("Error upserting event: {:?}", e);
                            println!("Error upserting event: {:?}", e);
                            continue;
                        };
                        waiter_state.applied.insert(apply.1.t_zero);
                        if let Some(notify) = apply.1.notify.take() {
                            let _ = notify.send(());
                        }
                        waiter_state
                            .remove_from_waiter_apply(&apply.1.t_zero, &mut to_apply);
                    }
                
                }
                Err(err) => {
                    tracing::error!("Error broadcasting recovery: {:?}", err);
                    // TODO: Graceful restart ?
                    panic!("Error broadcasting recovery: {:?}", err);
                }
            }
        };
        Ok(())
    }

    fn check_recovery(&self, waiter_state: &mut WaiterState) -> Option<T0> {
        for (
            _t0,
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
                        // Lowest T0 is not commited -> Recover lowest t0 to ensure commit
                        // Recover t0_dep
                        *started_at = Instant::now();
                        return Some(t0_dep);
                    }
                }

                // Recover min_dep
                if let Some((t0_dep, _)) = min_dep {
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
                            let _ = sender.send(());
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
                            let _ = sender.send(());
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
                let _ = sender.send(());
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
            started_at: Instant::now(),
        };

        if let Some(wait_message) = &wait_dep.wait_message {
            for dep_t0 in wait_message.deps.iter() {
                if !self.applied.contains(dep_t0) {
                    if let Some(stored_t) = self.committed.get(dep_t0) {
                        // Your T is lower than the dep commited t -> no wait necessary
                        if &wait_message.t < stored_t {
                            continue;
                        }
                    }
                    // if not applied and not comitted with lower t
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
