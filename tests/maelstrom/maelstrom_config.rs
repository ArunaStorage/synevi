use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use ahash::RandomState;
use anyhow::{anyhow, Result};
use diesel_ulid::DieselUlid;
use tokio::sync::mpsc::Sender;
use tokio::sync::Mutex;

use monotime::MonoTime;
use synevi_consensus::replica::ReplicaConfig;
use synevi_kv::error::KVError;
use synevi_kv::kv_store::KVStore;
use synevi_network::consensus_transport::{
    AcceptRequest, AcceptResponse, ApplyRequest, ApplyResponse, CommitRequest, CommitResponse,
    PreAcceptRequest, PreAcceptResponse, RecoverRequest, RecoverResponse,
};
use synevi_network::network::BroadcastResponse;
use synevi_network::replica::Replica;

use crate::messages::{Body, Message, MessageType};
use crate::messages::MessageType::WriteOk;
use crate::network::GLOBAL_COUNTER;
use crate::protocol::MessageHandler;

type ResponseHandler = Arc<
    Mutex<
        HashMap<
            MonoTime,
            (
                tokio::sync::broadcast::Sender<BroadcastResponse>,
                tokio::sync::broadcast::Receiver<BroadcastResponse>,
            ),
            RandomState,
        >,
    >,
>;
#[derive(Debug)]
pub struct MaelstromConfig {
    pub members: Vec<String>,
    pub node_id: String,
    pub message_handler: Arc<Mutex<MessageHandler>>,
    pub broadcast_responses: ResponseHandler, // in response to : broadcast responses
}

impl MaelstromConfig {
    pub async fn new(node_id: String, members: Vec<String>) -> Self {
        MaelstromConfig {
            members,
            node_id,
            message_handler: Arc::new(Mutex::new(MessageHandler)),
            broadcast_responses: Arc::new(Mutex::new(HashMap::default())),
        }
    }
    pub async fn init() -> Result<(KVStore, Arc<Self>, Arc<ReplicaConfig>)> {
        let mut handler = MessageHandler;

        let (kv_store, network) = if let Some(msg) = handler.next() {
            if let MessageType::Init {
                ref node_id,
                ref node_ids,
            } = msg.body.msg_type
            {
                let mut node_ids = node_ids.clone();
                node_ids.retain(|n| n != node_id);
                let id: u32 = node_id.chars().last().unwrap().into();

                let mut parsed_nodes = Vec::new();
                for node in &node_ids {
                    let node_id: u32 = node.chars().last().unwrap().into();
                    parsed_nodes.push((DieselUlid::generate(), node_id as u16, node.clone()));
                }
                let network =
                    Arc::new(MaelstromConfig::new(node_id.clone(), node_ids.clone()).await);

                GLOBAL_COUNTER.fetch_add(1, Ordering::Relaxed);

                let reply = msg.reply(Body {
                    msg_type: MessageType::InitOk,
                    ..Default::default()
                });
                MessageHandler::send(reply)?;
                (
                    KVStore::init_maelstrom(
                        DieselUlid::generate(),
                        id as u16,
                        network.clone(),
                        parsed_nodes,
                        None,
                    )
                    .await
                    .unwrap(),
                    network,
                )
            } else {
                eprintln!("Unexpected message type: {:?}", msg.body.msg_type);
                return Err(anyhow!("Unexpected message type: {:?}", msg.body.msg_type));
            }
        } else {
            eprintln!("No init message received");
            return Err(anyhow!("No init message received"));
        };

        Ok((kv_store.0, network, kv_store.1))
    }

    pub(crate) async fn kv_dispatch(
        &self,
        kv_store: &mut KVStore,
        msg: Message,
        responder: Arc<Sender<Message>>,
    ) -> Result<()> {
        let reply = match msg.body.msg_type {
            MessageType::Read { ref key } => {
                match kv_store.read(key.to_string()).await {
                    Ok(value) => {
                        msg.reply(Body {
                            msg_type: MessageType::ReadOk {
                                value: value.parse()?,
                            },
                            ..Default::default()
                        })
                    }
                    Err(err) => {
                        msg.reply(Body {
                            msg_type: MessageType::Error {
                                code: 20,
                                text: format!("{err}"),
                            },
                            ..Default::default()
                        })
                    }
                }
            }
            MessageType::Write { ref key, ref value } => {
                kv_store.write(key.to_string(), value.to_string()).await?;
                eprintln!("WRITE OK REACHED");
                msg.reply(Body {
                    msg_type: WriteOk,
                    ..Default::default()
                })
            }
            MessageType::Cas {
                ref key,
                ref from,
                ref to,
            } => {
                match kv_store
                    .cas(key.to_string(), from.to_string(), to.to_string())
                    .await
                {
                    Ok(_) => msg.reply(Body {
                        msg_type: MessageType::CasOk,
                        ..Default::default()
                    }),

                    Err(err) => match err {
                        KVError::KeyNotFound => msg.reply(Body {
                            msg_type: MessageType::Error {
                                code: 20,
                                text: format!("{err}"),
                            },
                            ..Default::default()
                        }),
                        KVError::MismatchError => msg.reply(Body {
                            msg_type: MessageType::Error {
                                code: 22,
                                text: format!("{err}"),
                            },
                            ..Default::default()
                        }),
                        _ => {
                            eprintln!("Error: {err}");
                            msg.reply(Body {
                                msg_type: MessageType::CasOk,
                                ..Default::default()
                            })
                        }
                    },
                }
                
            }
            err => {
                return Err(anyhow!("{err:?}"));
            }
        };
        if let Err(err) = responder.send(reply).await {
            eprintln!("Error sending reply : {err:?}");
        }
        Ok(())
    }

    pub(crate) async fn replica_dispatch(
        &self,
        replica_config: Arc<ReplicaConfig>,
        msg: Message,
        responder: Arc<Sender<Message>>,
    ) -> Result<()> {
        if msg.dest != self.node_id {
            eprintln!("Wrong msg");
            return Ok(());
        }
        match msg.body.msg_type {
            MessageType::PreAccept {
                ref id,
                ref event,
                ref t0,
            } => {
                let node: u32 = msg.dest.chars().last().unwrap().into();
                let response = replica_config
                    .pre_accept(
                        PreAcceptRequest {
                            id: id.clone(),
                            event: event.clone(),
                            timestamp_zero: t0.clone(),
                        },
                        node as u16,
                    )
                    .await
                    .unwrap();

                let reply = msg.reply(Body {
                    msg_type: MessageType::PreAcceptOk {
                        t0: t0.clone(),
                        t: response.timestamp,
                        deps: response.dependencies,
                        nack: response.nack,
                    },
                    ..Default::default()
                });
                if let Err(err) = responder.send(reply).await {
                    eprintln!("Error sending reply: {err:?}");
                }
            }
            MessageType::Accept {
                ref id,
                ref ballot,
                ref event,
                ref t0,
                ref t,
                ref deps,
            } => {
                let response = replica_config
                    .accept(AcceptRequest {
                        id: id.clone(),
                        ballot: ballot.clone(),
                        event: event.clone(),
                        timestamp_zero: t0.clone(),
                        timestamp: t.clone(),
                        dependencies: deps.clone(),
                    })
                    .await?;

                let reply = msg.reply(Body {
                    msg_type: MessageType::AcceptOk {
                        t0: t0.clone(),
                        deps: response.dependencies,
                        nack: response.nack,
                    },
                    ..Default::default()
                });
                if let Err(err) = responder.send(reply).await {
                    eprintln!("Error sending reply: {err:?}");
                }
            }
            MessageType::Commit {
                ref id,
                ref event,
                ref t0,
                ref t,
                ref deps,
            } => {
                replica_config
                    .commit(CommitRequest {
                        id: id.clone(),
                        event: event.clone(),
                        timestamp_zero: t0.clone(),
                        timestamp: t.clone(),
                        dependencies: deps.clone(),
                    })
                    .await?;

                let reply = msg.reply(Body {
                    msg_type: MessageType::CommitOk { t0: t0.clone() },
                    ..Default::default()
                });
                if let Err(err) = responder.send(reply).await {
                    eprintln!("Error sending reply: {err:?}");
                }
            }
            MessageType::Apply {
                ref id,
                ref event,
                ref t0,
                ref t,
                ref deps,
            } => {
                replica_config
                    .apply(ApplyRequest {
                        id: id.clone(),
                        event: event.clone(),
                        timestamp_zero: t0.clone(),
                        timestamp: t.clone(),
                        dependencies: deps.clone(),
                    })
                    .await?;

                let reply = msg.reply(Body {
                    msg_type: MessageType::ApplyOk { t0: t0.clone() },
                    ..Default::default()
                });
                if let Err(err) = responder.send(reply).await {
                    eprintln!("Error sending reply: {err:?}");
                }
            }
            MessageType::Recover {
                ref id,
                ref ballot,
                ref event,
                ref t0,
            } => {
                let result = replica_config
                    .recover(RecoverRequest {
                        id: id.clone(),
                        ballot: ballot.clone(),
                        event: event.clone(),
                        timestamp_zero: t0.clone(),
                    })
                    .await?;

                let reply = msg.reply(Body {
                    msg_type: MessageType::RecoverOk {
                        t0: t0.clone(),
                        local_state: result.local_state,
                        wait: result.wait,
                        superseding: result.superseding,
                        deps: result.dependencies,
                        t: result.timestamp,
                        nack: result.nack,
                    },
                    ..Default::default()
                });
                if let Err(err) = responder.send(reply).await {
                    eprintln!("Error sending reply: {err:?}");
                }
            }
            err => {
                return Err(anyhow!("{err:?}"));
            }
        }
        Ok(())
    }

    pub(crate) async fn broadcast_collect(&self, msg: Message) -> Result<()> {
        if msg.dest != self.node_id {
            eprintln!("Wrong msg");
            return Ok(());
        }

        match msg.body.msg_type {
            MessageType::PreAcceptOk {
                ref t0,
                ref t,
                ref deps,
                ref nack,
            } => {
                let key = MonoTime::try_from(t0.as_slice())?;
                let mut lock = self.broadcast_responses.lock().await;
                if let Some(entry) = lock.get(&key) {
                    entry
                        .0
                        .send(BroadcastResponse::PreAccept(PreAcceptResponse {
                            timestamp: t.clone(),
                            dependencies: deps.clone(),
                            nack: *nack,
                        }))?;
                } else {
                    let channel = tokio::sync::broadcast::channel(100);
                    lock.insert(key, channel);
                }
                drop(lock);
            }
            MessageType::AcceptOk {
                t0,
                ref deps,
                ref nack,
            } => {
                let key = MonoTime::try_from(t0.as_slice())?;
                let mut lock = self.broadcast_responses.lock().await;
                if let Some(entry) = lock.get(&key) {
                    entry.0.send(BroadcastResponse::Accept(AcceptResponse {
                        dependencies: deps.clone(),
                        nack: *nack,
                    }))?;
                } else {
                    let channel = tokio::sync::broadcast::channel(100);
                    lock.insert(key, channel);
                }
                drop(lock);
            }
            MessageType::CommitOk { t0 } => {
                let key = MonoTime::try_from(t0.as_slice())?;
                let mut lock = self.broadcast_responses.lock().await;
                if let Some(entry) = lock.get(&key) {
                    entry.0.send(BroadcastResponse::Commit(CommitResponse {}))?;
                } else {
                    let channel = tokio::sync::broadcast::channel(100);
                    lock.insert(key, channel);
                }
                drop(lock);
            }
            MessageType::ApplyOk { t0 } => {
                let key = MonoTime::try_from(t0.as_slice())?;
                let mut lock = self.broadcast_responses.lock().await;
                if let Some(entry) = lock.get(&key) {
                    entry.0.send(BroadcastResponse::Apply(ApplyResponse {}))?;
                } else {
                    let channel = tokio::sync::broadcast::channel(100);
                    lock.insert(key, channel);
                }
                drop(lock);
            }
            MessageType::RecoverOk {
                t0,
                ref local_state,
                ref wait,
                ref superseding,
                ref deps,
                ref t,
                ref nack,
            } => {
                let key = MonoTime::try_from(t0.as_slice())?;
                let mut lock = self.broadcast_responses.lock().await;
                if let Some(entry) = lock.get(&key) {
                    entry.0.send(BroadcastResponse::Recover(RecoverResponse {
                        local_state: *local_state,
                        wait: wait.clone(),
                        superseding: *superseding,
                        dependencies: deps.clone(),
                        timestamp: t.clone(),
                        nack: nack.clone(),
                    }))?;
                } else {
                    let channel = tokio::sync::broadcast::channel(100);
                    lock.insert(key, channel);
                }
                drop(lock);
            }
            err => {
                return Err(anyhow! {"{err:?}"});
            }
        };
        Ok(())
    }
}
