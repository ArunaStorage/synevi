use async_channel::{Receiver, Sender};
use std::{
    collections::BTreeMap,
    sync::Arc,
    time::{Duration, Instant},
};
use synevi_network::network::Network;
use synevi_persistence::event_store::Store;
use synevi_types::{Executor, SyneviError, T, T0};
use tokio::{sync::oneshot, time::timeout};

use crate::{node::Node, utils::into_dependency};

pub struct ReorderMessage {
    pub id: u128,
    pub t0: T0,
    pub event: Vec<u8>,
    pub notify: oneshot::Sender<(T, Vec<u8>)>,
    pub latency: u64,
}

#[derive(Clone)]
pub struct ReorderBuffer<N, E, S>
where
    N: Network,
    E: Executor,
    S: Store,
{
    #[allow(dead_code)]
    sender: Sender<ReorderMessage>,
    receiver: Receiver<ReorderMessage>,
    node: Arc<Node<N, E, S>>,
}

impl<N, E, S> ReorderBuffer<N, E, S>
where
    N: Network,
    E: Executor,
    S: Store,
{
    pub fn new(node: Arc<Node<N, E, S>>) -> Arc<Self> {
        let (sender, receiver) = async_channel::bounded(1000);
        Arc::new(Self {
            sender,
            receiver,
            node,
        })
    }

    #[allow(dead_code)]
    pub async fn send_msg(
        &self,
        t0: T0,
        notify: oneshot::Sender<(T, Vec<u8>)>,
        event: Vec<u8>,
        latency: u64,
        id: u128,
    ) -> Result<(), SyneviError> {
        Ok(self
            .sender
            .send(ReorderMessage {
                t0,
                notify,
                event,
                latency,
                id,
            })
            .await
            .map_err(|e| SyneviError::SendError(e.to_string()))?)
    }

    pub async fn run(&self) -> Result<(), SyneviError> {
        let mut buffer = BTreeMap::new();
        let mut current_transaction = (Instant::now(), T0::default());
        let mut next_latency = 500;
        loop {
            match timeout(
                Duration::from_micros(next_latency * 2),
                self.receiver.recv(),
            )
            .await
            {
                Ok(Ok(ReorderMessage {
                    id,
                    t0,
                    notify,
                    event,
                    latency,
                })) => {
                    //println!("Received message: {:?} latency: {}", t0, latency);
                    let now = Instant::now();
                    buffer.insert(t0, (notify, event, id));
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
                            let (t0_buffer, (notify, event, id)) = entry.remove_entry();

                            let (t, deps) = self
                                .node
                                .event_store
                                .lock()
                                .await
                                .pre_accept_tx(id, t0_buffer, event)?;
                            let _ = notify.send((t, into_dependency(&deps)));
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
                            let (t0_buffer, (notify, event, id)) = entry.remove_entry();
                            let (t, deps) = self
                                .node
                                .event_store
                                .lock()
                                .await
                                .pre_accept_tx(id, t0_buffer, event)?;
                            let _ = notify.send((t, into_dependency(&deps)));
                        }
                    }
                    continue;
                }
            }
        }
    }
}
