use crate::messages::Message;
use crate::network::GLOBAL_COUNTER;
use std::io::{self, Write};
use std::sync::atomic::Ordering;

#[derive(Debug)]
pub struct MessageHandler;

impl Iterator for MessageHandler {
    type Item = Message;

    fn next(&mut self) -> Option<Self::Item> {
        let mut buffer = String::new();
        while let Ok(x) = io::stdin().read_line(&mut buffer) {
            if x != 0 {
                eprintln!("GOT: {}", buffer);
                return serde_json::from_str(&buffer).unwrap();
            }
        }
        None
    }
}

impl MessageHandler {
    pub fn send(mut message: Message) -> Result<(), io::Error> {
        //message.id = GLOBAL_COUNTER.fetch_add(1, Ordering::Relaxed);
        let mut serialized = serde_json::to_string(&message)?;
        serialized.push('\n');
        io::stdout().write_all(serialized.as_bytes())?;
        io::stdout().flush()
    }
}
