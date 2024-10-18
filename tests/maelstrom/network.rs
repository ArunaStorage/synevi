use crate::messages::{Body, Message, MessageType};
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::Duration;
use synevi::network::requests::{
    AcceptRequest, AcceptResponse, ApplyRequest, ApplyResponse, BroadcastRequest,
    BroadcastResponse, CommitRequest, CommitResponse, PreAcceptRequest, PreAcceptResponse,
    RecoverRequest, RecoverResponse,
};
use synevi::network::{Network, NetworkInterface, Replica};
use synevi::{State, SyneviError, T0};
use synevi_network::configure_transport::GetEventResponse;
use synevi_network::network::{MemberWithLatency, NodeStatus};
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::Mutex;
use tokio::task::JoinSet;
use ulid::Ulid;

pub struct MaelstromNetwork {
    pub node_id: String,
    pub members: RwLock<Vec<String>>,
    pub self_arc: RwLock<Option<Arc<MaelstromNetwork>>>,
    pub message_sender: async_channel::Sender<Message>,
    pub message_receiver: async_channel::Receiver<Message>,
    pub kv_sender: KVSend,
    pub broadcast_responses: Mutex<HashMap<(State, T0), Sender<BroadcastResponse>>>,
    pub join_set: Arc<Mutex<JoinSet<anyhow::Result<()>>>>,
}

type KVSend = Sender<Message>;
type KVReceive = Receiver<Message>;

impl MaelstromNetwork {
    #[allow(dead_code)]
    pub fn new(
        node_id: String,
        message_sender: async_channel::Sender<Message>,
        message_receiver: async_channel::Receiver<Message>,
    ) -> (Arc<Self>, KVReceive) {
        let (kv_send, kv_rcv) = tokio::sync::mpsc::channel(100);

        let network = Arc::new(MaelstromNetwork {
            node_id,
            members: RwLock::new(Vec::new()),
            self_arc: RwLock::new(None),
            message_sender,
            message_receiver,
            broadcast_responses: Mutex::new(HashMap::new()),
            kv_sender: kv_send.clone(),
            join_set: Arc::new(Mutex::new(JoinSet::new())),
        });

        network.self_arc.write().unwrap().replace(network.clone());
        (network, kv_rcv)
    }

    #[allow(dead_code)]
    pub fn get_join_set(&self) -> Arc<Mutex<JoinSet<anyhow::Result<()>>>> {
        self.join_set.clone()
    }
}

#[async_trait]
impl Network for MaelstromNetwork {
    type Ni = MaelstromNetwork;

    async fn add_members(&self, members: Vec<(Ulid, u16, String)>) {
        for (id, serial, host) in members {
            self.add_member(id, serial, host, true).await.unwrap()
        }
    }

    fn get_node_status(&self) -> Arc<NodeStatus> {
        todo!()
    }

    async fn add_member(
        &self,
        _id: Ulid,
        _serial: u16,
        host: String,
        _ready: bool,
    ) -> Result<(), SyneviError> {
        self.members.write().unwrap().push(host);
        Ok(())
    }

    async fn spawn_server<R: Replica + 'static>(&self, server: R) -> Result<(), SyneviError> {
        eprintln!("Spawning network handler");
        let (response_send, mut response_rcv) = tokio::sync::mpsc::channel::<Message>(100);
        let (replica_send, mut replica_rcv) = tokio::sync::mpsc::channel::<Message>(100);
        // Receive messages from STDIN
        // 3 channels: KV, Replica, Response
        let message_receiver = self.message_receiver.clone();
        let message_sender = self.message_sender.clone();

        let kv_sender = self.kv_sender.clone();
        self.join_set.lock().await.spawn(async move {
            loop {
                let msg = match message_receiver.recv().await {
                    Ok(msg) => msg,
                    Err(e) => {
                        eprintln!("{e:?}");
                        continue;
                    }
                };

                match msg.body.msg_type {
                    MessageType::Read { .. }
                    | MessageType::Write { .. }
                    | MessageType::Cas { .. } => {
                        if let Err(err) = kv_sender.send(msg).await {
                            eprintln!("Send failed {err}");
                        };
                    }
                    MessageType::PreAccept { .. }
                    | MessageType::Commit { .. }
                    | MessageType::Accept { .. }
                    | MessageType::Apply { .. }
                    | MessageType::Recover { .. } => {
                        if let Err(err) = replica_send.try_send(msg) {
                            eprintln!("Replica send failed {err}");
                        };
                    }
                    MessageType::PreAcceptOk { .. }
                    | MessageType::AcceptOk { .. }
                    | MessageType::CommitOk { .. }
                    | MessageType::ApplyOk { .. }
                    | MessageType::RecoverOk { .. } => {
                        if let Err(err) = response_send.send(msg).await {
                            eprintln!("{err:?}");
                        };
                    }
                    err => {
                        eprintln!("Unexpected message type {:?}", err);
                    }
                }
            }
        });

        let self_clone = self.self_arc.read().unwrap().clone().unwrap();
        self.join_set.lock().await.spawn(async move {
            while let Some(msg) = response_rcv.recv().await {
                if let Err(err) = self_clone.broadcast_collect(msg.clone()).await {
                    eprintln!("{err:?}");
                    continue;
                }
            }
            Ok(())
        });

        self.join_set.lock().await.spawn(async move {
            let server = Arc::new(server);
            while let Some(msg) = replica_rcv.recv().await {
                tokio::spawn(replica_dispatch(
                    server.clone(),
                    msg,
                    message_sender.clone(),
                ));
            }
            Ok(())
        });

        Ok(())
    }

    async fn get_interface(&self) -> Arc<MaelstromNetwork> {
        self.self_arc.read().unwrap().clone().unwrap()
    }

    async fn get_waiting_time(&self, _node_serial: u16) -> u64 {
        todo!()
    }

    async fn ready_member(&self, _id: Ulid, _serial: u16) -> Result<(), SyneviError> {
        todo!()
    }

    async fn ready_electorate(&self, _host: String) -> Result<(), SyneviError> {
        todo!()
    }

    async fn get_stream_events(
        &self,
        _last_applied: Vec<u8>,
    ) -> Result<tokio::sync::mpsc::Receiver<GetEventResponse>, SyneviError> {
        todo!()
    }

    async fn join_electorate(&self, _host: String) -> Result<u32, SyneviError> {
        todo!()
    }

    async fn get_members(&self) -> Vec<Arc<MemberWithLatency>> {
        todo!()
    }

    async fn report_config(&self, _host: String) -> Result<(), SyneviError> {
        todo!()
    }
}

#[async_trait]
impl NetworkInterface for MaelstromNetwork {
    async fn broadcast(
        &self,
        request: BroadcastRequest,
    ) -> anyhow::Result<Vec<BroadcastResponse>, SyneviError> {
        let await_majority = true;
        //let broadcast_all = false;
        let (sx, mut rcv) = tokio::sync::mpsc::channel(50);
        let members = self.members.read().unwrap().clone();
        let t0 = match &request {
            BroadcastRequest::PreAccept(req, _serial) => {
                let t0 = T0::try_from(req.timestamp_zero.as_slice()).unwrap();
                self.broadcast_responses
                    .lock()
                    .await
                    .insert((State::PreAccepted, t0), sx);
                for replica in members {
                    if let Err(err) = self
                        .message_sender
                        .send(Message {
                            src: self.node_id.clone(),
                            dest: replica.clone(),
                            body: Body {
                                in_reply_to: None,
                                msg_type: MessageType::PreAccept {
                                    id: req.id.clone(),
                                    event: req.event.clone(),
                                    t0: req.timestamp_zero.clone(),
                                    last_applied: req.last_applied.clone(),
                                },
                                ..Default::default()
                            },
                        })
                        .await
                    {
                        eprintln!("Message sender error: {err:?}");
                        continue;
                    };
                }
                (State::PreAccepted, t0)
            }
            BroadcastRequest::Accept(req) => {
                let t0 = T0::try_from(req.timestamp_zero.as_slice()).unwrap();
                self.broadcast_responses
                    .lock()
                    .await
                    .insert((State::Accepted, t0), sx);
                for replica in members {
                    if let Err(err) = self
                        .message_sender
                        .send(Message {
                            src: self.node_id.clone(),
                            dest: replica.clone(),
                            body: Body {
                                in_reply_to: None,
                                msg_type: MessageType::Accept {
                                    id: req.id.clone(),
                                    ballot: req.ballot.clone(),
                                    event: req.event.clone(),
                                    t0: req.timestamp_zero.clone(),
                                    t: req.timestamp.clone(),
                                    deps: req.dependencies.clone(),
                                    last_applied: req.last_applied.clone(),
                                },
                                ..Default::default()
                            },
                        })
                        .await
                    {
                        eprintln!("Message sender error: {err:?}");
                        continue;
                    };
                }
                (State::Accepted, t0)
            }
            BroadcastRequest::Commit(req) => {
                let t0 = T0::try_from(req.timestamp_zero.as_slice()).unwrap();
                self.broadcast_responses
                    .lock()
                    .await
                    .insert((State::Committed, t0), sx);
                for replica in members {
                    if let Err(err) = self
                        .message_sender
                        .send(Message {
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
                        })
                        .await
                    {
                        eprintln!("Message sender error: {err:?}");
                        continue;
                    };
                }
                (State::Committed, t0)
            }
            BroadcastRequest::Apply(req) => {
                let t0 = T0::try_from(req.timestamp_zero.as_slice()).unwrap();
                self.broadcast_responses
                    .lock()
                    .await
                    .insert((State::Applied, t0), sx);
                for replica in members {
                    if let Err(err) = self
                        .message_sender
                        .send(Message {
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
                                    transaction_hash: req.transaction_hash.clone(),
                                    execution_hash: req.execution_hash.clone(),
                                },
                            },
                        })
                        .await
                    {
                        eprintln!("Message sender error: {err:?}");
                        continue;
                    };
                }
                (State::Applied, t0)
            }
            BroadcastRequest::Recover(req) => {
                let t0 = T0::try_from(req.timestamp_zero.as_slice()).unwrap();
                self.broadcast_responses
                    .lock()
                    .await
                    .insert((State::Undefined, t0), sx);
                for replica in members {
                    if let Err(err) = self
                        .message_sender
                        .send(Message {
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
                        })
                        .await
                    {
                        eprintln!("Message sender error: {err:?}");
                        continue;
                    };
                }
                (State::Undefined, t0)
            }
        };

        let majority = (self.members.read().unwrap().len() / 2) + 1;
        let mut counter = 0_usize;
        let mut result = Vec::new();

        // Poll majority
        // TODO: Electorates for PA ?
        if await_majority {
            while let Ok(Some(message)) =
                tokio::time::timeout(Duration::from_millis(50), rcv.recv()).await
            {
                result.push(message);
                counter += 1;
                if counter >= majority {
                    break;
                }
            }
        } else {
            // TODO: Differentiate between push and forget and wait for all response
            // -> Apply vs Recover
            while let Ok(Some(message)) =
                tokio::time::timeout(Duration::from_millis(50), rcv.recv()).await
            {
                result.push(message);
                counter += 1;
                if counter >= self.members.read().unwrap().len() {
                    break;
                }
            }
        }

        if result.len() < majority {
            eprintln!("Majority not reached: {:?}", result);
            return Err(SyneviError::MajorityNotReached);
        }

        self.broadcast_responses.lock().await.remove(&t0);

        Ok(result)
    }
    async fn broadcast_recovery(&self, _t0: T0) -> Result<bool, SyneviError> {
        todo!()
    }
}

pub(crate) async fn replica_dispatch<R: Replica + 'static>(
    replica: Arc<R>,
    msg: Message,
    responder: async_channel::Sender<Message>,
) -> anyhow::Result<()> {
    match msg.body.msg_type {
        MessageType::PreAccept {
            ref id,
            ref event,
            ref t0,
            ref last_applied,
        } => {
            let node: u32 = msg.dest.chars().last().unwrap().into();
            let response = replica
                .pre_accept(
                    PreAcceptRequest {
                        id: id.clone(),
                        event: event.clone(),
                        timestamp_zero: t0.clone(),
                        last_applied: last_applied.clone(),
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
            ref last_applied,
        } => {
            let response = replica
                .accept(AcceptRequest {
                    id: id.clone(),
                    ballot: ballot.clone(),
                    event: event.clone(),
                    timestamp_zero: t0.clone(),
                    timestamp: t.clone(),
                    dependencies: deps.clone(),
                    last_applied: last_applied.clone(),
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
            replica
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
            ref transaction_hash,
            ref execution_hash,
        } => {
            eprintln!("Replica dispatch apply {:?}", t0);
            replica
                .apply(ApplyRequest {
                    id: id.clone(),
                    event: event.clone(),
                    timestamp_zero: t0.clone(),
                    timestamp: t.clone(),
                    dependencies: deps.clone(),
                    transaction_hash: transaction_hash.clone(),
                    execution_hash: execution_hash.clone(),
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
            let result = replica
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
            return Err(anyhow::anyhow!("{err:?}"));
        }
    }
    Ok(())
}

impl MaelstromNetwork {
    pub(crate) async fn broadcast_collect(&self, msg: Message) -> anyhow::Result<()> {
        if msg.dest != self.node_id {
            eprintln!(
                "Wrong msg_dest: {}, {}, msg: {:?}",
                msg.dest, self.node_id, msg
            );
            return Ok(());
        }

        match msg.body.msg_type {
            MessageType::PreAcceptOk {
                ref t0,
                ref t,
                ref deps,
                ref nack,
            } => {
                let key = T0::try_from(t0.as_slice())?;
                let lock = self.broadcast_responses.lock().await;
                if let Some(entry) = lock.get(&(State::PreAccepted, key)) {
                    entry
                        .send(BroadcastResponse::PreAccept(PreAcceptResponse {
                            timestamp: t.clone(),
                            dependencies: deps.clone(),
                            nack: *nack,
                        }))
                        .await?;
                }
            }
            MessageType::AcceptOk {
                t0,
                ref deps,
                ref nack,
            } => {
                let key = T0::try_from(t0.as_slice())?;
                let lock = self.broadcast_responses.lock().await;
                if let Some(entry) = lock.get(&(State::Accepted, key)) {
                    entry
                        .send(BroadcastResponse::Accept(AcceptResponse {
                            dependencies: deps.clone(),
                            nack: *nack,
                        }))
                        .await?;
                }
            }
            MessageType::CommitOk { t0 } => {
                let key = T0::try_from(t0.as_slice())?;
                let lock = self.broadcast_responses.lock().await;
                if let Some(entry) = lock.get(&(State::Committed, key)) {
                    entry
                        .send(BroadcastResponse::Commit(CommitResponse {}))
                        .await?;
                }
            }
            MessageType::ApplyOk { t0 } => {
                let key = T0::try_from(t0.as_slice())?;
                let lock = self.broadcast_responses.lock().await;
                if let Some(entry) = lock.get(&(State::Applied, key)) {
                    entry
                        .send(BroadcastResponse::Apply(ApplyResponse {}))
                        .await?;
                }
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
                let key = T0::try_from(t0.as_slice())?;
                let lock = self.broadcast_responses.lock().await;
                if let Some(entry) = lock.get(&(State::Undefined, key)) {
                    entry
                        .send(BroadcastResponse::Recover(RecoverResponse {
                            local_state: *local_state,
                            wait: wait.clone(),
                            superseding: *superseding,
                            dependencies: deps.clone(),
                            timestamp: t.clone(),
                            nack: nack.clone(),
                        }))
                        .await?;
                }
            }
            err => {
                return Err(anyhow::anyhow! {"{err:?}"});
            }
        };
        Ok(())
    }
}
