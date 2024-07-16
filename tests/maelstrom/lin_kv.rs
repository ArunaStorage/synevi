use crate::network::MaelstromConfig;
use anyhow::{anyhow, Result};
use diesel_ulid::DieselUlid;
use messages::{AdditionalFields, Body};
use protocol::MessageHandler;
use std::sync::Arc;
use synevi_kv::KVStore;
use synevi_network::consensus_transport::{AcceptRequest, PreAcceptRequest};
use synevi_network::network::Network;
use synevi_network::replica::Replica;
use crate::messages::AdditionalFields::{AcceptOk, PreAcceptOk};
use crate::messages::Message;

mod messages;
mod network;
mod protocol;



#[tokio::main]
pub async fn main() -> Result<()> {
    let maelstrom_store = MaelstromStore::init().await?;
    if let Err(err) =  maelstrom_store.start().await {
        eprintln!("Error: {err}");
    };
    Ok(())
}


    // async fn read(&mut self, msg: &Message) -> Result<()> {
    //     let Some(AdditionalFields::Read { ref key }) = msg.body.additional_fields  
    //     else {
    //         eprintln!("Invalid message: {:?}", msg);
    //         return Err(anyhow!("Invalid message"))
    //     };

    //     let value = self.kv_store.read(key.clone()).await?;

    //     let mut reply = msg.reply(Body {
    //         msg_type: messages::MessageType::ReadOk,
    //         additional_fields: Some(AdditionalFields::ReadOk {
    //             key: key.clone(),
    //             value,
    //         }),
    //         ..Default::default()
    //     });
    //     reply.dest = msg.src.clone();
    //     reply.src = self.network.node_id.clone();
    //     MessageHandler::send(reply)?;
    //     Ok(())
    // }
    // async fn write(&mut self, msg: &Message) -> Result<()> {
    //     let Some(AdditionalFields::Write { ref key, ref value }) =
    //         msg.body.additional_fields
    //     else {
    //         eprintln!("Invalid message: {:?}", msg);
    //         return Err(anyhow!("Invalid message"))

    //     };

    //     self.kv_store.write(key.clone(), value.clone()).await?;

    //     let mut reply = msg.reply(Body {
    //         msg_type: messages::MessageType::WriteOk,
    //         additional_fields: None,
    //         ..Default::default()
    //     });
    //     reply.dest = msg.src.clone();
    //     reply.src = self.network.node_id.clone();
    //     MessageHandler::send(reply)?;       
    //     Ok(())
    // }
    // async fn pre_accept(&mut self, msg: &Message) -> Result<()> {
    //     if msg.dest != self.network.node_id {
    //         return Ok(())
    //     }
    //     let Some(AdditionalFields::PreAccept { event, t0}) =
    //         msg.body.additional_fields.clone()
    //     else {
    //         eprintln!("Invalid message: {:?}", msg);
    //         return Err(anyhow!("Invalid message"));
    //     };

    //     let node: u32 = msg.dest.chars().last().unwrap().into();
    //     let response = self.network.pre_accept(PreAcceptRequest{event, timestamp_zero: t0}, node as u16).await.unwrap();

    //     let reply = msg.reply(Body {
    //         msg_type: messages::MessageType::PreAcceptOk,
    //         additional_fields: Some(PreAcceptOk {
    //             t: response.timestamp,
    //             deps: response.dependencies,
    //             nack: response.nack,
    //         }),
    //         ..Default::default()
    //     });
    //     MessageHandler::send(reply)?;       
    //     Ok(())
    // }
    // async fn accept(&mut self, msg: &Message) -> Result<()> {
    //     if msg.dest != self.network.node_id {
    //         return Ok(())
    //     }
    //     let Some(AdditionalFields::Accept { ballot, event, t0, t, deps }) =
    //         msg.body.additional_fields.clone()
    //     else {
    //         eprintln!("Invalid message: {:?}", msg);
    //         return Err(anyhow!("Invalid message"));
    //     };
    //     let response = self.network.accept(AcceptRequest{ ballot, event, timestamp_zero: t0, timestamp: t, dependencies: deps}).await.unwrap();

    //     let reply = msg.reply(Body {
    //         msg_type: messages::MessageType::AcceptOk,
    //         additional_fields: Some(AcceptOk {
    //             deps: response.dependencies,
    //             nack: response.nack,
    //         }),
    //         ..Default::default()
    //     });
    //     MessageHandler::send(reply)?;
    //     Ok(())
    // }
}