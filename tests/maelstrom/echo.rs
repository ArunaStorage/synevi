use crate::messages::MessageType;
use anyhow::Result;
use messages::Body;
mod messages;
mod network;
mod protocol;

pub fn main() -> Result<()> {
    let (mut rx, tx) = protocol::MessageHandler::new();

    while let Some(msg) = rx.blocking_recv() {
        eprintln!("Received message: {:?}", msg);
        match msg.body.msg_type {
            MessageType::Echo { ref echo, .. } => {
                let reply = msg.reply(Body {
                    msg_type: MessageType::EchoOk {
                        echo: echo.to_string(),
                    },
                    ..Default::default()
                });
                tx.blocking_send(reply)?;
            }
            MessageType::Init { .. } => {
                let reply = msg.reply(Body {
                    msg_type: messages::MessageType::InitOk,
                    ..Default::default()
                });
                tx.blocking_send(reply)?;
            }
            _ => {
                eprintln!("Unknown message type: {:?}", msg.body.msg_type);
            }
        }
    }
    Ok(())
}
