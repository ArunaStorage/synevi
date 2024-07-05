use crate::{
    event_store::EventStore,
    utils::{T, T0},
};
use anyhow::Result;
use async_channel::{Receiver, Sender};
use bytes::Bytes;
use consensus_transport::consensus_transport::Dependency;
use std::{
    collections::BTreeMap,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::{
    sync::{oneshot, Mutex},
    time::timeout,
};

pub struct ReorderMessage {
    pub t0: T0,
    pub event: Bytes,
    pub notify: oneshot::Sender<(T, Vec<Dependency>)>,
    pub latency: u64,
}

#[derive(Clone, Debug)]
pub struct ReorderBuffer {
    sender: Sender<ReorderMessage>,
    receiver: Receiver<ReorderMessage>,
    event_store: Arc<Mutex<EventStore>>,
}

impl ReorderBuffer {
    pub fn new(event_store: Arc<Mutex<EventStore>>) -> Arc<Self> {
        let (sender, receiver) = async_channel::bounded(1000);
        Arc::new(Self {
            sender,
            receiver,
            event_store,
        })
    }

    pub async fn send_msg(
        &self,
        t0: T0,
        notify: oneshot::Sender<(T, Vec<Dependency>)>,
        event: Bytes,
        latency: u64,
    ) -> Result<()> {
        Ok(self
            .sender
            .send(ReorderMessage {
                t0,
                notify,
                event,
                latency,
            })
            .await?)
    }

    pub async fn run(&self) -> Result<()> {
        let mut buffer = BTreeMap::new();
        let mut current_transaction = (Instant::now(), T0::default());
        let mut next_latency = 300;
        loop {
            match timeout(
                Duration::from_micros(next_latency * 2),
                self.receiver.recv(),
            )
            .await
            {
                Ok(Ok(ReorderMessage {
                    t0,
                    notify,
                    event,
                    latency,
                })) => {
                    //println!("Received message: {:?} latency: {}", t0, latency);
                    let now = Instant::now();
                    buffer.insert(t0, (notify, event));
                    if current_transaction.1 == T0::default() {
                        current_transaction = (now, t0);
                        next_latency = latency;
                    }
                    // TODO: Put "real" latency here
                    if (current_transaction.0.elapsed().as_micros() as u64) < next_latency {
                        continue;
                    }

                    while let Some(entry) = buffer.first_entry() {
                        if entry.key() <= &current_transaction.1 {
                            let (t0_buffer, (notify, event)) = entry.remove_entry();

                            let (deps, t) = self
                                .event_store
                                .lock()
                                .await
                                .pre_accept(t0_buffer, event)
                                .await?;
                            let _ = notify.send((t, deps));
                        } else {
                            break;
                        }
                    }
                    current_transaction = (now, t0);
                    next_latency = latency;
                }
                Ok(Err(e)) => {
                    println!("Error receiving message {e}")
                }
                Err(_) => {
                    // Elapsed more than 1.2x average (TODO) latency
                    if (current_transaction.0.elapsed().as_micros() as u64) > next_latency {
                        while let Some(entry) = buffer.first_entry() {
                            let (t0_buffer, (notify, event)) = entry.remove_entry();
                            let (deps, t) = self
                                .event_store
                                .lock()
                                .await
                                .pre_accept(t0_buffer, event)
                                .await?;
                            let _ = notify.send((t, deps));
                        }
                    }
                    continue;
                }
            }
        }
    }
}
