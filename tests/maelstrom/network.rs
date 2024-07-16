use async_trait::async_trait;
use diesel_ulid::DieselUlid;
use std::sync::Arc;
use anyhow::anyhow;
use futures::lock::Mutex;
use synevi_kv::KVStore;
use synevi_network::consensus_transport::{
    AcceptRequest, AcceptResponse, ApplyRequest, ApplyResponse, CommitRequest, CommitResponse,
    PreAcceptRequest, PreAcceptResponse, RecoverRequest, RecoverResponse,
};
use synevi_network::error::BroadCastError;
use synevi_network::network::{BroadcastRequest, BroadcastResponse, Network, NetworkInterface};
use synevi_network::replica::Replica;
use crate::messages;
use crate::messages::{AdditionalFields, Body, MessageType};
use crate::protocol::MessageHandler;

#[derive(Debug)]
pub struct MaelstromConfig {
    pub members: Vec<String>,
    pub node_id: String,
    pub message_handler: Arc<Mutex<MessageHandler>>,
}

impl MaelstromConfig {
    async fn new(node_id: String, members: Vec<String>) -> Self {
        MaelstromConfig { members, node_id, message_handler: Arc::new(Mutex::new(MessageHandler)) }
    }
    pub async fn init() -> anyhow::Result<(KVStore, Arc<Self>)> {
        let mut handler = MessageHandler;

        let  (kv_store, network) = if let Some(msg) = handler.next() {
            if matches!(msg.body.msg_type, messages::MessageType::Init) {
                let Some(AdditionalFields::Init {
                             ref node_id,
                             ref nodes,
                         }) = msg.body.additional_fields
                else {
                    eprintln!("Invalid message: {:?}", msg);
                    return Err(anyhow!("Invalid message"));
                };

                let id: u32 = node_id.chars().last().unwrap().into();

                let mut parsed_nodes = Vec::new();
                for node in nodes {
                    let node_id: u32 = node_id.chars().last().unwrap().into();
                    parsed_nodes.push((DieselUlid::generate(), node_id as u16, node.clone()));
                }
                let network = Arc::new(MaelstromConfig::new(node_id.clone(), nodes.clone()).await);

                let reply = msg.reply(Body {
                    msg_type: messages::MessageType::InitOk,
                    ..Default::default()
                });
                MessageHandler::send(reply)?;
                (KVStore::init(
                    DieselUlid::generate(),
                    id as u16,
                    network.clone(),
                    parsed_nodes,
                    None,
                )
                     .await
                     .unwrap(), network)
            } else {
                eprintln!("Unexpected message type: {:?}", msg.body.msg_type);
                return Err(anyhow!("Unexpected message type: {:?}", msg.body.msg_type));
            }
        } else {
            eprintln!("No init message received");
            return Err(anyhow!("No init message received"));
        };

        Ok((
            kv_store,
            network,
        ))
    }
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

    async fn spawn_server(&self, server: Arc<dyn Replica>) -> anyhow::Result<()> {
        
        
        tokio::spawn(async move {
            loop {
                let msg = handler_clone.lock().await.next();
                if let Some(msg) = msg {
                    match msg.body.msg_type {
                        MessageType::Read | MessageType::Write => {
                            todo!("Let kv store handle these");
                        },
                        MessageType::PreAccept |
                        MessageType::Commit |
                        MessageType::Accept |
                        MessageType::Apply |
                        MessageType::Recover
                         => {
                            todo!("Let replica handle these");
                        }
                        MessageType::PreAcceptOk |
                        MessageType::AcceptOk |
                        MessageType::CommitOk |
                        MessageType::ApplyOk | 
                        MessageType::RecoverOk
                        => {
                            todo!("Collect for broadcast results");
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
        todo!()
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
        todo!()
    }
}

#[async_trait]
impl Replica for MaelstromConfig {
    async fn pre_accept(&self, _request: PreAcceptRequest, _node_serial: u16) -> anyhow::Result<PreAcceptResponse> {
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