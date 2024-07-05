use crate::{
    event_store::{Event, EventStore},
    utils::{Ballot, T, T0},
};
use anyhow::Result;
use async_channel::{Receiver, Sender};
use bytes::Bytes;
use consensus_transport::{consensus_transport::State, network::Network};
use std::{collections::HashMap, sync::Arc, time::Duration};
use tokio::{
    sync::{Mutex, Notify},
    time::timeout,
};

pub enum WaitAction {
    CommitBefore,
    ApplyAfter,
}

pub struct WaitMessage {
    t_zero: T0,
    t: T,
    deps: HashMap<T0, T>,
    event: Bytes,
    action: WaitAction,
    notify: Arc<Notify>,
    global_latency: u64,
}

#[derive(Clone, Debug)]
pub struct WaitHandler {
    sender: Sender<WaitMessage>,
    receiver: Receiver<WaitMessage>,
    event_store: Arc<Mutex<EventStore>>,
    network: Arc<dyn Network + Send + Sync>,
}

impl WaitHandler {
    pub fn new(event_store: Arc<Mutex<EventStore>>, network: Arc<dyn Network + Send + Sync>) -> Arc<Self> {
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
        global_latency: u64,
    ) -> Result<()> {
        Ok(self
            .sender
            .send(WaitMessage {
                t_zero,
                t,
                deps,
                event,
                action,
                notify,
                global_latency,
            })
            .await?)
    }

    pub async fn run(&self) -> Result<()> {
        loop {
            match timeout(Duration::from_millis(50), self.receiver.recv()).await {
                Ok(Ok(ref msg @ WaitMessage { ref action, .. })) => match action {
                    WaitAction::CommitBefore => {}
                    WaitAction::ApplyAfter => {}
                },
                _ => {}
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
    ) -> Result<()> {
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
        Ok(())
    }
}
