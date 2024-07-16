use crate::messages::AdditionalFields::{AcceptOk, ApplyOk, CommitOk, PreAcceptOk, RecoverOk};
use crate::messages::{AdditionalFields, Body, Message, MessageType};
use crate::protocol::MessageHandler;
use ahash::RandomState;
use anyhow::{anyhow, Result};
use diesel_ulid::DieselUlid;
use monotime::MonoTime;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::broadcast::{Sender, Receiver};
use synevi_kv::KVStore;
use synevi_network::consensus_transport::{
    AcceptRequest, AcceptResponse, ApplyRequest, ApplyResponse, CommitRequest, CommitResponse,
    PreAcceptRequest, PreAcceptResponse, RecoverRequest, RecoverResponse,
};
use synevi_network::network::BroadcastResponse;
use synevi_network::replica::Replica;
use tokio::sync::Mutex;
use synevi_consensus::replica::ReplicaConfig;

#[derive(Debug)]
pub struct MaelstromConfig {
    pub members: Vec<String>,
    pub node_id: String,
    pub message_handler: Arc<Mutex<MessageHandler>>,
    pub broadcast_responses: Arc<Mutex<HashMap<MonoTime, (Sender<BroadcastResponse>, Receiver<BroadcastResponse>), RandomState>>>, // in response to : broadcast responses
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
            if matches!(msg.body.msg_type, MessageType::Init) {
                let Some(AdditionalFields::Init {
                    ref node_id,
                    ref node_ids,
                }) = msg.body.additional_fields
                else {
                    eprintln!("Invalid message: {:?}", msg);
                    return Err(anyhow!("Invalid message"));
                };

                let id: u32 = node_id.chars().last().unwrap().into();

                let mut parsed_nodes = Vec::new();
                for node in node_ids {
                    let node_id: u32 = node_id.chars().last().unwrap().into();
                    parsed_nodes.push((DieselUlid::generate(), node_id as u16, node.clone()));
                }
                let network = Arc::new(MaelstromConfig::new(node_id.clone(), node_ids.clone()).await);

                let reply = msg.reply(Body {
                    msg_type: MessageType::InitOk,
                    ..Default::default()
                });
                MessageHandler::send(reply)?;
                (
                    KVStore::init(
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

    pub(crate) async fn kv_dispatch(&self, kv_store: &mut KVStore, msg: Message) -> Result<()> {
        match msg.body.additional_fields {
            Some(AdditionalFields::Read { ref key }) => {
                match kv_store.read(key.to_string()).await {
                    Ok(value) => {
                        let reply = msg.reply(Body {
                            msg_type: MessageType::ReadOk,
                            additional_fields: Some(AdditionalFields::ReadOk {
                                key: *key,
                                value,
                            }),
                            ..Default::default()
                        });
                        MessageHandler::send(reply)?;
                    },
                    Err(err) => {
                        let reply = msg.reply(Body {
                            msg_type: MessageType::Error,
                            additional_fields: Some(AdditionalFields::Error {
                                code: 20,
                                text: format!("{err}"),
                            }),
                            ..Default::default()
                        });
                        MessageHandler::send(reply)?;
                    }
                    
                }; 
                
            }
            Some(AdditionalFields::Write { ref key, ref value }) => {
                kv_store.write(key.to_string(), value.clone()).await?;
                let reply = msg.reply(Body {
                    msg_type: MessageType::WriteOk,
                    additional_fields: None,
                    ..Default::default()
                });
                MessageHandler::send(reply)?;
            }
            err => {
                return Err(anyhow!("{err:?}"));
            }
        }
        Ok(())
    }

    pub(crate) async fn replica_dispatch(&self, replica_config: Arc<ReplicaConfig>, msg: Message) -> Result<()> {
        match msg.body.additional_fields {
            Some(AdditionalFields::PreAccept { ref event, ref t0 }) => {
                if msg.dest != self.node_id {
                    return Ok(());
                }

                let node: u32 = msg.dest.chars().last().unwrap().into();
                let response = replica_config
                    .pre_accept(
                        PreAcceptRequest {
                            event: event.clone(),
                            timestamp_zero: t0.clone(),
                        },
                        node as u16,
                    )
                    .await
                    .unwrap();

                let reply = msg.reply(Body {
                    msg_type: MessageType::PreAcceptOk,
                    additional_fields: Some(PreAcceptOk {
                        t0: t0.clone(),
                        t: response.timestamp,
                        deps: response.dependencies,
                        nack: response.nack,
                    }),
                    ..Default::default()
                });
                MessageHandler::send(reply)?;
            }
            Some(AdditionalFields::Accept {
                ref ballot,
                ref event,
                ref t0,
                ref t,
                ref deps,
            }) => {
                if msg.dest != self.node_id {
                    return Ok(());
                }
                let response = replica_config
                    .accept(AcceptRequest {
                        ballot: ballot.clone(),
                        event: event.clone(),
                        timestamp_zero: t0.clone(),
                        timestamp: t.clone(),
                        dependencies: deps.clone(),
                    })
                    .await?;

                let reply = msg.reply(Body {
                    msg_type: MessageType::AcceptOk,
                    additional_fields: Some(AcceptOk {
                        t0: t0.clone(),
                        deps: response.dependencies,
                        nack: response.nack,
                    }),
                    ..Default::default()
                });
                MessageHandler::send(reply)?;
            }
            Some(AdditionalFields::Commit {
                ref event,
                ref t0,
                ref t,
                ref deps,
            }) => {
                if msg.dest != self.node_id {
                    return Ok(());
                }
                replica_config.commit(CommitRequest {
                    event: event.clone(),
                    timestamp_zero: t0.clone(),
                    timestamp: t.clone(),
                    dependencies: deps.clone(),
                })
                .await?;

                let reply = msg.reply(Body {
                    msg_type: MessageType::CommitOk,
                    additional_fields: Some(CommitOk { t0: t0.clone() }),
                    ..Default::default()
                });
                MessageHandler::send(reply)?;
            }
            Some(AdditionalFields::Apply {
                ref event,
                ref t0,
                ref t,
                ref deps,
            }) => {
                if msg.dest != self.node_id {
                    return Ok(());
                }
                replica_config.apply(ApplyRequest {
                    event: event.clone(),
                    timestamp_zero: t0.clone(),
                    timestamp: t.clone(),
                    dependencies: deps.clone(),
                })
                .await?;

                let reply = msg.reply(Body {
                    msg_type: MessageType::ApplyOk,
                    additional_fields: Some(ApplyOk { t0: t0.clone() }),
                    ..Default::default()
                });
                MessageHandler::send(reply)?;
            }
            Some(AdditionalFields::Recover {
                ref ballot,
                ref event,
                ref t0,
            }) => {
                if msg.dest != self.node_id {
                    return Ok(());
                }
                let result = replica_config
                    .recover(RecoverRequest {
                        ballot: ballot.clone(),
                        event: event.clone(),
                        timestamp_zero: t0.clone(),
                    })
                    .await?;

                let reply = msg.reply(Body {
                    msg_type: MessageType::RecoverOk,
                    additional_fields: Some(RecoverOk {
                        t0: t0.clone(),
                        local_state: result.local_state,
                        wait: result.wait,
                        superseding: result.superseding,
                        deps: result.dependencies,
                        t: result.timestamp,
                        nack: result.nack,
                    }),
                    ..Default::default()
                });
                MessageHandler::send(reply)?;
            }
            err => {
                return Err(anyhow!("{err:?}"));
            }
        }
        Ok(())
    }

    pub(crate) async fn broadcast_collect(&self, msg: Message) -> Result<()> {
        match msg.body.additional_fields {
            Some(PreAcceptOk {
                ref t0,
                ref t,
                ref deps,
                ref nack,
            }) => {
                if msg.dest != self.node_id {
                    return Ok(());
                }

                let key = MonoTime::try_from(t0.as_slice())?;
                let mut lock = self.broadcast_responses.lock().await;
                if let Some(entry)= lock.get(&key){
                    entry.0.send(BroadcastResponse::PreAccept(PreAcceptResponse {
                        timestamp: t.clone(),
                        dependencies: deps.clone(),
                        nack: *nack,
                    }))?;
                } else {
                    let channel = tokio::sync::broadcast::channel(self.members.len()*5);
                    lock.insert(key, channel);
                }
                drop(lock);
            }
            Some(AcceptOk {
                t0,
                ref deps,
                ref nack,
            }) => {
                if msg.dest != self.node_id {
                    return Ok(());
                }

                let key = MonoTime::try_from(t0.as_slice())?;
                let mut lock = self.broadcast_responses.lock().await;
                if let Some(entry)= lock.get(&key){
                    entry.0.send(BroadcastResponse::Accept(AcceptResponse {
                    dependencies: deps.clone(),
                    nack: *nack,
                }))?;
                } else {
                    let channel = tokio::sync::broadcast::channel(self.members.len()*5);
                    lock.insert(key, channel);
                }
                drop(lock);
            }
            Some(CommitOk { t0 }) => {
                if msg.dest != self.node_id {
                    return Ok(());
                }
                let key = MonoTime::try_from(t0.as_slice())?;
                let mut lock = self.broadcast_responses.lock().await;
                if let Some(entry)= lock.get(&key){
                    entry.0.send(BroadcastResponse::Commit(CommitResponse {}))?;
                } else {
                    let channel = tokio::sync::broadcast::channel(self.members.len()*5);
                    lock.insert(key, channel);
                }
                drop(lock);
            }
            Some(ApplyOk { t0 }) => {
                if msg.dest != self.node_id {
                    return Ok(());
                }
                let key = MonoTime::try_from(t0.as_slice())?;
                let mut lock = self.broadcast_responses.lock().await;
                if let Some(entry)= lock.get(&key){
                    entry.0.send(BroadcastResponse::Apply(ApplyResponse {}))?;
                } else {
                    let channel = tokio::sync::broadcast::channel(self.members.len()*5);
                    lock.insert(key, channel);
                }
                drop(lock);
            }
            Some(RecoverOk {
                t0,
                ref local_state,
                ref wait,
                ref superseding,
                ref deps,
                ref t,
                ref nack,
            }) => {
                if msg.dest != self.node_id {
                    return Ok(());
                }

                let key = MonoTime::try_from(t0.as_slice())?;
                let mut lock = self.broadcast_responses.lock().await;
                if let Some(entry)= lock.get(&key){
                    entry.0.send(BroadcastResponse::Recover(RecoverResponse {
                    local_state: *local_state,
                    wait: wait.clone(),
                    superseding: *superseding,
                    dependencies: deps.clone(),
                    timestamp: t.clone(),
                    nack: nack.clone(),
                }))?;
                } else {
                    let channel = tokio::sync::broadcast::channel(self.members.len()*5);
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
