use crate::messages::MessageType::WriteOk;
use crate::messages::{Body, Message, MessageType};
use crate::network::MaelstromNetwork;
use crate::protocol::MessageHandler;
use anyhow::{anyhow, Result};
use diesel_ulid::DieselUlid;
use std::sync::Arc;
use synevi_kv::error::KVError;
use synevi_kv::kv_store::KVStore;
use tokio::task::JoinHandle;

pub struct MaelstromServer;

impl MaelstromServer {
    pub async fn spawn() -> Result<(
        JoinHandle<()>,
        Arc<tokio::sync::Mutex<tokio::task::JoinSet<std::result::Result<(), anyhow::Error>>>>,
    )> {
        let (rx, sx) = MessageHandler::spawn_handler();

        eprintln!("Spawning maelstrom server");
        let init_msg = rx.recv().await.unwrap();
        let mut node_info: (DieselUlid, u16, String) = (DieselUlid::generate(), 0, "".to_string());
        let mut members: Vec<(DieselUlid, u16, String)> = Vec::new();
        if let MessageType::Init {
            ref node_id,
            ref node_ids,
        } = &init_msg.body.msg_type
        {
            for (num, node) in node_ids.iter().enumerate() {
                if node == node_id {
                    node_info = (DieselUlid::generate(), num as u16, node.clone());
                    continue;
                }
                members.push((DieselUlid::generate(), num as u16, node.clone()));
            }
        } else {
            eprintln!("Unexpected message type: {:?}", init_msg);
            return Err(anyhow!(
                "Unexpected message type: {:?}",
                init_msg.body.msg_type
            ));
        }

        sx.send(init_msg.reply(Body {
            msg_type: MessageType::InitOk,
            ..Default::default()
        }))
        .await
        .unwrap();

        let (network, mut kv_receiver) = MaelstromNetwork::new(node_info.2, sx.clone(), rx);
        let set: Arc<
            tokio::sync::Mutex<tokio::task::JoinSet<std::result::Result<(), anyhow::Error>>>,
        > = network.get_join_set();
        eprintln!("Network created");

        let kv_store = KVStore::init(node_info.0, node_info.1, network, members).await?;
        eprintln!("KV store created");
        let store = kv_store.clone();

        let joinhandle = tokio::spawn(async move {
            let sx = sx;
            while let Some(msg) = kv_receiver.recv().await {
                kv_dispatch(&store, msg, &sx).await.unwrap();
            }
        });

        Ok((joinhandle, set))
    }
}

pub(crate) async fn kv_dispatch(
    kv_store: &KVStore<Arc<MaelstromNetwork>>,
    msg: Message,
    responder: &async_channel::Sender<Message>,
) -> Result<()> {
    let reply = match msg.body.msg_type {
        MessageType::Read { ref key } => match kv_store.read(key.to_string()).await {
            Ok(value) => msg.reply(Body {
                msg_type: MessageType::ReadOk {
                    value: value.parse()?,
                },
                ..Default::default()
            }),
            Err(KVError::KeyNotFound) => msg.reply(Body {
                msg_type: MessageType::Error {
                    code: 20,
                    text: format!("Key not found"),
                },
                ..Default::default()
            }),
            Err(err) => {
                eprintln!("Error: {err}");
                msg.reply(Body {
                    msg_type: MessageType::Error {
                        code: 14,
                        text: format!("{err}"),
                    },
                    ..Default::default()
                })
            }
        },
        MessageType::Write { ref key, ref value } => {
            match kv_store.write(key.to_string(), value.to_string()).await {
                Ok(_) => msg.reply(Body {
                    msg_type: WriteOk,
                    ..Default::default()
                }),
                Err(err) => {
                    eprintln!("Error: {err}");
                    msg.reply(Body {
                        msg_type: MessageType::Error {
                            code: 14,
                            text: format!("{err}"),
                        },
                        ..Default::default()
                    })
                }
            }
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
                            msg_type: MessageType::Error {
                                code: 14,
                                text: format!("{err}"),
                            },
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
