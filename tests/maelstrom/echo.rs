use anyhow::Result;
use messages::{AdditionalFields, Body};
use protocol::MessageHandler;
mod maelstrom_config;
mod messages;
mod network;
mod protocol;

pub fn main() -> Result<()> {
    let mut handler = protocol::MessageHandler;

    while let Some(msg) = handler.next() {
        eprintln!("Received message: {:?}", msg);
        match msg.body.msg_type {
            messages::MessageType::Echo => {
                let Some(AdditionalFields::Echo { ref echo }) = msg.body.additional_fields else {
                    eprintln!("Invalid message: {:?}", msg);
                    continue;
                };
                let reply = msg.reply(Body {
                    msg_type: messages::MessageType::EchoOk,
                    additional_fields: Some(messages::AdditionalFields::EchoOk {
                        echo: echo.to_string(),
                    }),
                    ..Default::default()
                });
                MessageHandler::send(reply)?;
            }
            messages::MessageType::Init => {
                let reply = msg.reply(Body {
                    msg_type: messages::MessageType::InitOk,
                    ..Default::default()
                });
                MessageHandler::send(reply)?;
            }
            _ => {
                eprintln!("Unknown message type: {:?}", msg.body.msg_type);
            }
        }
    }
    Ok(())
}
