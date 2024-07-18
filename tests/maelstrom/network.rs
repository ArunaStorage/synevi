use crate::maelstrom_config::MaelstromConfig;
use crate::messages::{Body, Message, MessageType};
use crate::protocol::MessageHandler;
use async_trait::async_trait;
use diesel_ulid::DieselUlid;
use monotime::MonoTime;
use std::sync::Arc;
use synevi_network::consensus_transport::{
    AcceptRequest, AcceptResponse, ApplyRequest, ApplyResponse, CommitRequest, CommitResponse,
    PreAcceptRequest, PreAcceptResponse, RecoverRequest, RecoverResponse,
};
use synevi_network::error::BroadCastError;
use synevi_network::network::{BroadcastRequest, BroadcastResponse, Network, NetworkInterface};
use synevi_network::replica::Replica;

#[async_trait]
impl Network for MaelstromConfig {
    async fn add_members(&self, members: Vec<(DieselUlid, u16, String)>) {
        for (id, serial, host) in members {
            self.add_member(id, serial, host).await.unwrap()
        }
    }

    async fn add_member(&self, _id: DieselUlid, _serial: u16, _host: String) -> anyhow::Result<()> {
        Ok(())
    }

    async fn spawn_server(&self, _server: Arc<dyn Replica>) -> anyhow::Result<()> {
        let (mut kv_store, network, replica) = MaelstromConfig::init().await?;
        let handler_clone = self.message_handler.clone();

        tokio::spawn(async move {
            loop {
                let msg = handler_clone.lock().await.next();
                eprintln!("GOT: {msg:?}");
                if let Some(msg) = msg {
                    match msg.body.msg_type {
                        MessageType::Read{..} | MessageType::Write { .. } => {
                            if let Err(err) = network.kv_dispatch(&mut kv_store, msg.clone()).await
                            {
                                eprintln!("{err:?}");
                                continue;
                            };
                        }
                            MessageType::PreAccept{..}
                            | MessageType::Commit {..}
                            | MessageType::Accept {..}
                            | MessageType::Apply  {..}
                            | MessageType::Recover{..}
                         => {
                            if let Err(err) =
                                network.replica_dispatch(replica.clone(), msg.clone()).await
                            {
                                eprintln!("{err:?}");
                                continue;
                            }
                        }
                            MessageType::PreAcceptOk{..} 
                            | MessageType::AcceptOk {..}
                            | MessageType::CommitOk {..}
                            | MessageType::ApplyOk  {..}
                            | MessageType::RecoverOk{..}
                        => {
                            if let Err(err) = network.broadcast_collect(msg.clone()).await {
                                eprintln!("{err:?}");
                                continue;
                            }
                        }
                        err => {
                            eprintln!("Unexpected message type {:?}", err);
                            continue;
                        }
                    }
                }
            }
        });
        Ok(())
    }

    async fn get_interface(&self) -> Arc<dyn NetworkInterface> {
        let clone = MaelstromConfig {
            members: self.members.clone(),
            node_id: self.node_id.clone(),
            message_handler: self.message_handler.clone(),
            broadcast_responses: self.broadcast_responses.clone(),
        };
        Arc::new(clone)
    }

    async fn get_waiting_time(&self, _node_serial: u16) -> u64 {
        todo!()
    }
}

#[async_trait]
impl NetworkInterface for MaelstromConfig {
    async fn broadcast(
        &self,
        request: BroadcastRequest,
    ) -> anyhow::Result<Vec<BroadcastResponse>, BroadCastError> {
        let mut await_majority = true;
        let mut broadcast_all = false;
        let mut rcv = match &request {
            BroadcastRequest::PreAccept(req, serial) => {
                let t0 = MonoTime::try_from(req.timestamp_zero.as_slice()).unwrap();
                let mut lock = self.broadcast_responses.lock().await;
                let entry = lock
                    .entry(t0)
                    .or_insert(tokio::sync::broadcast::channel(self.members.len() * 5));
                let rcv = entry.0.subscribe();
                drop(lock);
                for replica in &self.members {
                    if let Err(err) = MessageHandler::send(Message {
                        src: self.node_id.clone(),
                        dest: replica.clone(),
                        body: Body {
                            msg_id: None,
                            in_reply_to: None,
                            msg_type: MessageType::PreAccept {
                                id: req.id.clone(),
                                event: req.event.clone(),
                                t0: req.timestamp_zero.clone(),
                            },
                        },
                        ..Default::default()
                    }) {
                        eprintln!("{err:?}");
                        continue;
                    };
                }
                rcv
            }
            BroadcastRequest::Accept(req) => {
                let t0 = MonoTime::try_from(req.timestamp_zero.as_slice()).unwrap();
                let mut lock = self.broadcast_responses.lock().await;
                let entry = lock
                    .entry(t0)
                    .or_insert(tokio::sync::broadcast::channel(self.members.len() * 5));
                let rcv = entry.0.subscribe();
                drop(lock);
                for replica in &self.members {
                    if let Err(err) = MessageHandler::send(Message {
                        src: self.node_id.clone(),
                        dest: replica.clone(),
                        body: Body {
                            
                            msg_id: None,
                            in_reply_to: None,
                            msg_type: MessageType::Accept {
                                id: req.id.clone(),
                                ballot: req.ballot.clone(),
                                event: req.event.clone(),
                                t0: req.timestamp_zero.clone(),
                                t: req.timestamp.clone(),
                                deps: req.dependencies.clone(),
                            },
                        },
                        ..Default::default()
                    }) {
                        eprintln!("{err:?}");
                        continue;
                    };
                }
                rcv
            }
            BroadcastRequest::Commit(req) => {
                let t0 = MonoTime::try_from(req.timestamp_zero.as_slice()).unwrap();
                let mut lock = self.broadcast_responses.lock().await;
                let entry = lock
                    .entry(t0)
                    .or_insert(tokio::sync::broadcast::channel(self.members.len() * 5));
                let rcv = entry.0.subscribe();
                drop(lock);
                for replica in &self.members {
                    if let Err(err) = MessageHandler::send(Message {
                        src: self.node_id.clone(),
                        dest: replica.clone(),
                        body: Body {
                            msg_id: None,
                            in_reply_to: None,

                            msg_type: MessageType::Commit {
                                id: req.id.clone(),
                                event: req.event.clone(),
                                t0: req.timestamp_zero.clone(),
                                t: req.timestamp.clone(),
                                deps: req.dependencies.clone(),
                            },
                        },
                        ..Default::default()
                    }) {
                        eprintln!("{err:?}");
                        continue;
                    };
                }
                rcv
            }
            BroadcastRequest::Apply(req) => {
                let t0 = MonoTime::try_from(req.timestamp_zero.as_slice()).unwrap();
                let mut lock = self.broadcast_responses.lock().await;
                let entry = lock
                    .entry(t0)
                    .or_insert(tokio::sync::broadcast::channel(self.members.len() * 5));
                let rcv = entry.0.subscribe();
                drop(lock);
                await_majority = false;
                for replica in &self.members {
                    if let Err(err) = MessageHandler::send(Message {
                        src: self.node_id.clone(),
                        dest: replica.clone(),
                        body: Body {
                            msg_id: None,
                            in_reply_to: None,

                            msg_type: MessageType::Apply {
                                id: req.id.clone(),
                                event: req.event.clone(),
                                t0: req.timestamp_zero.clone(),
                                t: req.timestamp.clone(),
                                deps: req.dependencies.clone(),
                            },
                        },
                        ..Default::default()
                    }) {
                        eprintln!("{err:?}");
                        continue;
                    };
                }
                rcv
            }
            BroadcastRequest::Recover(req) => {
                let t0 = MonoTime::try_from(req.timestamp_zero.as_slice()).unwrap();
                await_majority = false;
                broadcast_all = true;
                let mut lock = self.broadcast_responses.lock().await;
                let entry = lock
                    .entry(t0)
                    .or_insert(tokio::sync::broadcast::channel(self.members.len() * 5));
                let rcv = entry.0.subscribe();
                drop(lock);
                for replica in &self.members {
                    if let Err(err) = MessageHandler::send(Message {
                        src: self.node_id.clone(),
                        dest: replica.clone(),
                        body: Body {
                            msg_id: None,
                            in_reply_to: None,

                            msg_type: MessageType::Recover { 
                                id: req.id.clone(),
                                ballot: req.ballot.clone(),
                                event: req.event.clone(),
                                t0: req.timestamp_zero.clone(),
                            },
                        },
                        ..Default::default()
                    }) {
                        eprintln!("{err:?}");
                        continue;
                    };
                }
                rcv
            }
        };

        let majority = (self.members.len() / 2) + 1;
        let mut counter = 0_usize;
        let mut result = Vec::new();

        // Poll majority
        // TODO: Electorates for PA ?
        if await_majority {
            while let Ok(message) = rcv.recv().await {
                match (&request, message) {
                    (
                        &BroadcastRequest::PreAccept(..),
                        response @ BroadcastResponse::PreAccept(_),
                    ) => {
                        result.push(response);
                    }
                    (&BroadcastRequest::Accept(_), response @ BroadcastResponse::Accept(_)) => {
                        result.push(response);
                    }
                    (&BroadcastRequest::Commit(_), response @ BroadcastResponse::Commit(_)) => {
                        result.push(response);
                    }
                    (&BroadcastRequest::Apply(_), response @ BroadcastResponse::Apply(_)) => {
                        result.push(response);
                    }
                    (&BroadcastRequest::Recover(_), response @ BroadcastResponse::Recover(_)) => {
                        result.push(response);
                    }
                    _ => continue,
                }
                counter += 1;
                if counter >= majority {
                    break;
                }
            }
        } else {
            // TODO: Differentiate between push and forget and wait for all response
            // -> Apply vs Recover
            while let Ok(message) = rcv.recv().await {
                match (&request, message) {
                    (
                        &BroadcastRequest::PreAccept(..),
                        response @ BroadcastResponse::PreAccept(_),
                    ) => {
                        result.push(response);
                    }
                    (&BroadcastRequest::Accept(_), response @ BroadcastResponse::Accept(_)) => {
                        result.push(response);
                    }
                    (&BroadcastRequest::Commit(_), response @ BroadcastResponse::Commit(_)) => {
                        result.push(response);
                    }
                    (&BroadcastRequest::Apply(_), response @ BroadcastResponse::Apply(_)) => {
                        result.push(response);
                    }
                    (&BroadcastRequest::Recover(_), response @ BroadcastResponse::Recover(_)) => {
                        result.push(response);
                    }
                    _ => continue,
                };
            }
        }

        if result.len() < majority {
            println!("Majority not reached: {:?}", result);
            return Err(BroadCastError::MajorityNotReached);
        }
        Ok(result)
    }
}
#[async_trait]
impl Replica for MaelstromConfig {
    async fn pre_accept(
        &self,
        _request: PreAcceptRequest,
        _node_serial: u16,
    ) -> anyhow::Result<PreAcceptResponse> {
        todo!()
    }

    async fn accept(&self, _request: AcceptRequest) -> anyhow::Result<AcceptResponse> {
        todo!()
    }

    async fn commit(&self, _request: CommitRequest) -> anyhow::Result<CommitResponse> {
        todo!()
    }

    async fn apply(&self, _request: ApplyRequest) -> anyhow::Result<ApplyResponse> {
        todo!()
    }

    async fn recover(&self, _request: RecoverRequest) -> anyhow::Result<RecoverResponse> {
        todo!()
    }
}
