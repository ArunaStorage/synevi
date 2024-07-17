use crate::maelstrom_config::MaelstromConfig;
use crate::messages::{AdditionalFields, Body, MessageType};
use crate::protocol::MessageHandler;
use anyhow::{anyhow, Result};
use diesel_ulid::DieselUlid;
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::Arc;
use synevi_kv::KVStore;
use synevi_network::network::Network;

mod maelstrom_config;
mod messages;
mod network;
mod protocol;

#[tokio::main]
pub async fn main() -> Result<()> {
    //maelstrom_only_setup().await
    maelstrom_kv_setup().await
}

pub async fn maelstrom_only_setup() -> Result<()> {
    let config = MaelstromConfig::new("dummy1".to_string(), vec![]).await;
    let replica = Arc::new(MaelstromConfig::new("dummy2".to_string(), vec![]).await);
    config.spawn_server(replica).await
}

pub async fn maelstrom_kv_setup() -> Result<()> {
    let mut handler = MessageHandler;

    if let Some(msg) = handler.next() {
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
                if node == node_id {
                    continue;
                }
                let node_id: u32 = node.chars().last().unwrap().into();
                let host = format!("http://localhost:{}", 11000 + node_id);
                eprintln!("{host}");
                parsed_nodes.push((DieselUlid::generate(), node_id as u16, host));
            }
            let socket_addr = SocketAddr::from_str(&format!("0.0.0.0:{}", 11000 + id)).unwrap();
            eprintln!("{socket_addr}");
            let network = Arc::new(synevi_network::network::NetworkConfig::new(socket_addr));

            let reply = msg.reply(Body {
                msg_type: MessageType::InitOk,
                ..Default::default()
            });
            MessageHandler::send(reply)?;
            let mut kv_store = KVStore::init(
                DieselUlid::generate(),
                id as u16,
                network.clone(),
                parsed_nodes,
                None,
            )
            .await
            .unwrap();

            while let Some(msg) = handler.next() {
                eprintln!("{msg:?}");
                match msg.body.msg_type {
                    MessageType::Read => {
                        if let Some(AdditionalFields::Read { key }) = msg.body.additional_fields {
                            match kv_store.read(key.to_string()).await {
                                Ok(value) => {
                                    let reply = msg.reply(Body {
                                        msg_type: MessageType::ReadOk,
                                        additional_fields: Some(AdditionalFields::ReadOk {
                                            key,
                                            value,
                                        }),
                                        ..Default::default()
                                    });
                                    MessageHandler::send(reply)?;
                                }
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
                    }
                    MessageType::Write => {
                        if let Some(AdditionalFields::Write { ref key, ref value }) =
                            msg.body.additional_fields
                        {
                            kv_store.write(key.to_string(), value.clone()).await?;
                            let reply = msg.reply(Body {
                                msg_type: MessageType::WriteOk,
                                additional_fields: None,
                                ..Default::default()
                            });
                            MessageHandler::send(reply)?;
                        }
                    }
                    MessageType::Cas => {
                        eprintln!("Cas not implemented yet")
                    }
                    msg => eprintln!("Unexpected msg type {msg:?}"),
                }
            }
        } else {
            eprintln!("Unexpected message type: {:?}", msg.body.msg_type);
            return Err(anyhow!("Unexpected message type: {:?}", msg.body.msg_type));
        }
    } else {
        eprintln!("No init message received");
        return Err(anyhow!("No init message received"));
    };

    todo!()
}
