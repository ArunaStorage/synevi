use crate::messages::MessageType;
use anyhow::Result;
use messages::Body;
mod messages;
mod network;
mod protocol;

#[tokio::main]
pub async fn main() -> Result<()> {
    let (rx, tx) = protocol::MessageHandler::spawn_handler();

    while let Ok(msg) = rx.recv_blocking() {
        eprintln!("Received message: {:?}", msg);
        match msg.body.msg_type {
            MessageType::Echo { ref echo, .. } => {
                let reply = msg.reply(Body {
                    msg_type: MessageType::EchoOk {
                        echo: echo.to_string(),
                    },
                    ..Default::default()
                });
                tx.send_blocking(reply)?;
            }
            MessageType::Init { .. } => {
                let reply = msg.reply(Body {
                    msg_type: messages::MessageType::InitOk,
                    ..Default::default()
                });
                tx.send_blocking(reply)?;
            }
            _ => {
                eprintln!("Unknown message type: {:?}", msg.body.msg_type);
            }
        }
    }
    Ok(())
}
