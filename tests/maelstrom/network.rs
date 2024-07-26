use crate::maelstrom_config::MaelstromConfig;
use crate::messages::{Body, Message, MessageType};
use crate::protocol::MessageHandler;
use async_trait::async_trait;
use diesel_ulid::DieselUlid;
use lazy_static::lazy_static;
use monotime::MonoTime;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use synevi_network::consensus_transport::{
    AcceptRequest, AcceptResponse, ApplyRequest, ApplyResponse, CommitRequest, CommitResponse,
    PreAcceptRequest, PreAcceptResponse, RecoverRequest, RecoverResponse,
};
use synevi_network::error::BroadCastError;
use synevi_network::network::{BroadcastRequest, BroadcastResponse, Network, NetworkInterface};
use synevi_network::replica::Replica;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::sync::mpsc::{Receiver, Sender};

lazy_static! {
    pub(crate) static ref GLOBAL_COUNTER: Arc<AtomicU64> = Arc::new(AtomicU64::default());
}

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

        let (kv_send, mut kv_rcv) = tokio::sync::mpsc::channel(100);
        let (replica_send, mut replica_rcv) = tokio::sync::mpsc::channel(100);
        let (response_send, mut response_rcv) = tokio::sync::mpsc::channel(100);
        let (responder_send, mut responder_rcv): (Sender<Message>, Receiver<Message>) =
            tokio::sync::mpsc::channel(100);
        let responder_send = Arc::new(responder_send);
        let responder = tokio::spawn(async move {
            while let Some(reply) = responder_rcv.recv().await {
                if let Err(err) = MessageHandler::send(reply) {
                    eprintln!("ERROR sending reply: {err}");
                }
            }
        });
        let stdin_receiver = tokio::spawn(async move {
            //let mut stdin_lock = tokio::io::stdin().read_line();
            loop {
                //let mut lock = handler_clone.lock().await;
                let mut buffer = String::new();
                while let Ok(x) = BufReader::new(tokio::io::stdin())
                    .read_line(&mut buffer)
                    .await
                {
                    if x != 0 {
                        eprintln!("GOT: {}", buffer);
                        let msg: Message = serde_json::from_str(&buffer).unwrap();
                        GLOBAL_COUNTER.fetch_add(1, Ordering::Relaxed);
                        match msg.body.msg_type {
                            MessageType::Read { .. }
                            | MessageType::Write { .. }
                            | MessageType::Cas { .. } => {
                                if let Err(err) = kv_send.send(msg.clone()).await {
                                    eprintln!("Send failed");
                                    buffer.clear();
                                    continue;
                                };
                                eprintln!("Next");
                            }
                            MessageType::PreAccept { .. }
                            | MessageType::Commit { .. }
                            | MessageType::Accept { .. }
                            | MessageType::Apply { .. }
                            | MessageType::Recover { .. } => {
                                if let Err(err) = replica_send.send(msg.clone()).await {
                                    eprintln!("Send failed");
                                    buffer.clear();
                                    continue;
                                };
                                eprintln!("Next");
                            }
                            MessageType::PreAcceptOk { .. }
                            | MessageType::AcceptOk { .. }
                            | MessageType::CommitOk { .. }
                            | MessageType::ApplyOk { .. }
                            | MessageType::RecoverOk { .. } => {
                                if let Err(err) = response_send.send(msg.clone()).await {
                                    eprintln!("{err:?}");
                                    buffer.clear();
                                    continue;
                                };
                                eprintln!("Next");
                            }
                            err => {
                                eprintln!("Unexpected message type {:?}", err);
                                buffer.clear();
                                continue;
                            }
                        }
                        buffer.clear();
                    }
                }
            }
        });

        let network_clone = network.clone();
        let responder_clone = responder_send.clone();
        let lin_kv_handler = tokio::spawn(async move {
            while let Some(msg) = kv_rcv.recv().await {
                eprintln!("KV: {msg:?}");
                if let Err(err) = network_clone
                    .kv_dispatch(&mut kv_store, msg.clone(), responder_clone.clone())
                    .await
                {
                    eprintln!("{err:?}");
                }
            }
        });
        let network_clone = network.clone();
        let replica_handler = tokio::spawn(async move {
            while let Some(msg) = replica_rcv.recv().await {
                eprintln!("REPLICA: {msg:?}");
                if let Err(err) = network_clone
                    .replica_dispatch(replica.clone(), msg.clone(), responder_send.clone())
                    .await
                {
                    eprintln!("{err:?}");
                    continue;
                }
            }
        });
        let response_handler = tokio::spawn(async move {
            while let Some(msg) = response_rcv.recv().await {
                if let Err(err) = network.broadcast_collect(msg.clone()).await {
                    eprintln!("{err:?}");
                    continue;
                }
            }
        });
        let (responder, stdin, kv, rep, resp) = tokio::join!(
            responder,
            stdin_receiver,
            lin_kv_handler,
            replica_handler,
            response_handler
        );
        responder?;
        stdin?;
        kv?;
        rep?;
        resp?;
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
        let counter = GLOBAL_COUNTER.load(Ordering::Relaxed);
        let mut await_majority = true;
        let mut broadcast_all = false;
        let mut rcv = match &request {
            BroadcastRequest::PreAccept(req, _serial) => {
                let t0 = MonoTime::try_from(req.timestamp_zero.as_slice()).unwrap();
                let mut lock = self.broadcast_responses.lock().await;
                let entry = lock
                    .entry(t0)
                    .or_insert(tokio::sync::broadcast::channel(100));
                let rcv = entry.0.subscribe();
                drop(lock);
                for replica in &self.members {
                    if let Err(err) = MessageHandler::send(Message {
                        src: self.node_id.clone(),
                        dest: replica.clone(),
                        body: Body {
                            msg_id: Some(counter),
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
                    .or_insert(tokio::sync::broadcast::channel(100));
                let rcv = entry.0.subscribe();
                drop(lock);
                for replica in &self.members {
                    if let Err(err) = MessageHandler::send(Message {
                        src: self.node_id.clone(),
                        dest: replica.clone(),
                        body: Body {
                            msg_id: Some(counter),
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
                    .or_insert(tokio::sync::broadcast::channel(100));
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
                    .or_insert(tokio::sync::broadcast::channel(100));
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
                    .or_insert(tokio::sync::broadcast::channel(100));
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
                counter += 1;
                if counter >= self.members.len() {
                    break;
                }
            }
        }

        if result.len() < majority {
            eprintln!("Majority not reached: {:?}", result);
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
