use crate::{
    event_store::{Event, EventStore}, utils::{Ballot, T, T0}
};
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
    sync::{Mutex, Notify},
    time::timeout,
};

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum WaitAction {
    CommitBefore,
    ApplyAfter,
}

#[derive(Clone, Debug)]
pub struct WaitMessage {
    t_zero: T0,
    t: T,
    deps: HashMap<T0, T>,
    event: Bytes,
    action: WaitAction,
    notify: Arc<Notify>,
}

#[derive(Clone, Debug)]
pub struct WaitHandler {
    sender: Sender<WaitMessage>,
    receiver: Receiver<WaitMessage>,
    event_store: Arc<Mutex<EventStore>>,
    network: Arc<dyn Network + Send + Sync>,
}

#[derive(Clone, Debug)]
struct WaitDependency {
    wait_message: WaitMessage,
    deps: HashSet<T0>,
    started_at: Instant,
}

struct WaiterState {
    events: HashMap<T0, WaitDependency>,
    committed: HashMap<T0, T>,
    applied: HashSet<T0>,
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
            network,
        })
    }

    pub async fn send_msg(
        &self,
        t_zero: T0,
        t: T,
        deps: HashMap<T0, T>,
        event: Bytes,
        action: WaitAction,
        notify: Arc<Notify>,
    ) -> Result<()> {

        println!("Send msg {:?}, {:?}, deps: {:?}", t_zero, t, deps);

        Ok(self
            .sender
            .send(WaitMessage {
                t_zero,
                t,
                deps,
                event,
                action,
                notify,
            })
            .await?)
    }

    pub async fn run(&self) -> Result<()> {
        // HashMap<T0_dep waiting_for, Vec<T0_transaction waiting>>

        let mut waiter_state = WaiterState::new();

        loop {
            match timeout(Duration::from_millis(50), self.receiver.recv()).await {
                Ok(Ok(msg)) => 
                
                match msg.action {
                    WaitAction::CommitBefore => {
                        //println!("CommitBefore");
                        self.upsert_event(&msg).await;
                        waiter_state.committed.insert(msg.t_zero.clone(), msg.t.clone());
                        let mut to_apply = waiter_state.remove_from_waiter_commit(&msg.t_zero, &msg.t);
                        while let Some(mut apply) = to_apply.pop() {
                            apply.wait_message.action = WaitAction::ApplyAfter;
                            self.upsert_event(&apply.wait_message).await;
                            waiter_state.applied.insert(apply.wait_message.t_zero);
                            apply.wait_message.notify.notify_waiters();
                            waiter_state.remove_from_waiter_apply(&apply.wait_message.t_zero, &mut to_apply);
                        }
                        waiter_state.insert_commit(msg);
                    }
                    WaitAction::ApplyAfter => { 
                        if let Some(msg) = waiter_state.insert_apply(msg) {
                           // println!("ApplyAfter {:?}", msg);
                            self.upsert_event(&msg).await;
                            println!("ApplyAfter {:?}", msg.t_zero);
                            msg.notify.notify_waiters();
                            waiter_state.applied.insert(msg.t_zero);
                            let mut to_apply = Vec::new();
                            waiter_state.remove_from_waiter_apply(&msg.t_zero, &mut to_apply);
                            while let Some(mut apply) = to_apply.pop() {
                                apply.wait_message.action = WaitAction::ApplyAfter;
                                self.upsert_event(&apply.wait_message).await;
                                waiter_state.applied.insert(apply.wait_message.t_zero);
                                apply.wait_message.notify.notify_waiters();
                                waiter_state.remove_from_waiter_apply(&apply.wait_message.t_zero, &mut to_apply);
                            }
                        }
                    }
                },
                _ => {
                    //println!("Receive failed");
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
            events: HashMap::new(),
            committed: HashMap::new(),
            applied: HashSet::new(),
        }
    }

    fn remove_from_waiter_commit(&mut self, t0_dep: &T0, t_dep: &T) -> Vec<WaitDependency>{
        let mut apply_deps = Vec::new();
        self.events.retain(|_, event| {
            if t_dep < &event.wait_message.t{
                // Cannot remove must wait for apply -> retain
                return true
            }
            event.deps.remove(t0_dep);
            if event.deps.is_empty() {
                if event.wait_message.action == WaitAction::ApplyAfter {
                    apply_deps.push(event.clone());
                }else{
                    event.wait_message.notify.notify_waiters();
                }
                return false
            }
            true
        });
        apply_deps
    }

    fn remove_from_waiter_apply(&mut self, t0_dep: &T0, to_apply: &mut Vec<WaitDependency>){
        self.events.retain(|_, event| {
            event.deps.remove(t0_dep);
            for wait_dep in to_apply.iter() {
                event.deps.remove(&wait_dep.wait_message.t_zero);
            }
            if event.deps.is_empty() {
                if event.wait_message.action == WaitAction::ApplyAfter {
                    to_apply.push(event.clone());
                }else{
                    event.wait_message.notify.notify_waiters();
                }
                return false
            }
            true
        });
    }

    fn insert_commit(&mut self,
        wait_message: WaitMessage,
    ) {
        if self.applied.contains(&wait_message.t_zero) {
            wait_message.notify.notify_waiters();
            return;
        }
        let mut wait_dep = WaitDependency {
            wait_message,
            deps: HashSet::new(),
            started_at: Instant::now(),
        };
        for (dep_t0, _) in wait_dep.wait_message.deps.iter() {
            if !self.applied.contains(&dep_t0) {
                if let Some(stored_t) = self.committed.get(&dep_t0) {
                    // Your T is lower than the dep commited t -> no wait necessary
                    if &wait_dep.wait_message.t < stored_t {
                        continue;
                    }
                }
                wait_dep.deps.insert(*dep_t0);
            }
        }
    
        if wait_dep.deps.is_empty() {
            wait_dep.wait_message.notify.notify_waiters();
            return;
        }

        let waiter = wait_dep.wait_message.notify.clone();
    
        let entry = self.events.entry(wait_dep.wait_message.t_zero).or_insert(wait_dep);
        if entry.wait_message.action == WaitAction::ApplyAfter {
            waiter.notify_waiters();
        }
    }


    fn insert_apply(&mut self, wait_message: WaitMessage) -> Option<WaitMessage> {
        if self.applied.contains(&wait_message.t_zero) {
            wait_message.notify.notify_waiters();
            return None;
        }
        let mut wait_dep = WaitDependency {
            wait_message,
            deps: HashSet::new(),
            started_at: Instant::now(),
        };
        for (dep_t0, _) in wait_dep.wait_message.deps.iter() {
            if !self.applied.contains(&dep_t0) {
                if let Some(stored_t) = self.committed.get(&dep_t0) {
                    // Your T is lower than the dep commited t -> no wait necessary
                    if &wait_dep.wait_message.t < stored_t {
                        continue;
                    }
                }
                wait_dep.deps.insert(*dep_t0);
            }
        }
    
        println!("Wait deps {:?}", wait_dep.deps);
        if wait_dep.deps.is_empty() {
            return Some(wait_dep.wait_message);
        }
    
        self.events.insert(wait_dep.wait_message.t_zero, wait_dep);
        None
    }
    
}



#[cfg(test)]
mod tests {
    use monotime::MonoTime;

    use crate::wait_handler::*;

    #[tokio::test]
    async fn test_wait_handler() {
        let (sender, receiver): (Sender<WaitMessage>, Receiver<WaitMessage>) = async_channel::unbounded();
        let wait_handler = WaitHandler {
            sender,
            receiver,
            event_store: Arc::new(Mutex::new(EventStore::init(None, 0))),
            network: Arc::new(crate::tests::NetworkMock::default()),
        };

        let notify_1_1 = Arc::new(Notify::new());
        let notify_1_2 = Arc::new(Notify::new());
        let notify_2_1 = Arc::new(Notify::new());
        let notify_2_2 = Arc::new(Notify::new());
        let notify_1_1_future = notify_1_1.notified();
        let notify_1_2_future = notify_1_2.notified();
        let notify_2_1_future = notify_2_1.notified();
        let notify_2_2_future = notify_2_2.notified();

        let t0_1 = T0(MonoTime::new_with_time(1u128, 0, 0));
        let t0_2 = T0(MonoTime::new_with_time(2u128, 0, 0));
        let t_1 = T(MonoTime::new_with_time(1u128, 0, 0));
        let t_2 = T(MonoTime::new_with_time(2u128, 0, 0));
        let deps_2 = HashMap::from_iter([
            (t0_1.clone(), t_1.clone())
        ]);
        wait_handler.send_msg(t0_1, t_1, HashMap::new(), Bytes::new(), WaitAction::CommitBefore, notify_1_1.clone()).await.unwrap();
        wait_handler.send_msg(t0_2.clone(), t_2.clone(), deps_2.clone(), Bytes::new(), WaitAction::ApplyAfter, notify_2_2.clone()).await.unwrap();
        wait_handler.send_msg(t0_2.clone(), t_2.clone(), deps_2.clone(), Bytes::new(), WaitAction::CommitBefore, notify_2_1.clone()).await.unwrap();
        wait_handler.send_msg(t0_1, t_1, HashMap::new(), Bytes::new(), WaitAction::ApplyAfter, notify_1_2.clone()).await.unwrap();
        
        tokio::spawn(async move {wait_handler.run().await.unwrap()});
        timeout(Duration::from_millis(10), notify_1_1_future).await.unwrap();
        timeout(Duration::from_millis(10), notify_1_2_future).await.unwrap();
        timeout(Duration::from_millis(10), notify_2_1_future).await.unwrap();
        timeout(Duration::from_millis(10), notify_2_2_future).await.unwrap();
    }


}