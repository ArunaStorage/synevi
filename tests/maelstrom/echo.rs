use crate::messages::MessageType;
use anyhow::Result;
use messages::Body;
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
            MessageType::Echo { ref echo, .. } => {
                let reply = msg.reply(Body {
                    msg_type: MessageType::EchoOk {
                        echo: echo.to_string(),
                    },
                    ..Default::default()
                });
                MessageHandler::send(reply)?;
            }
            MessageType::Init { .. } => {
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
