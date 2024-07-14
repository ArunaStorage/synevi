use crate::messages::Message;
use std::io::{self, Write};

pub struct MessageHandler;

impl Iterator for MessageHandler {
    type Item = Message;

    fn next(&mut self) -> Option<Self::Item> {
        let mut buffer = String::new();
        while let Ok(x) = io::stdin().read_line(&mut buffer) {
            if x != 0 {
                return serde_json::from_str(&buffer).unwrap();
            }
        }
        None
    }
}

impl MessageHandler {
    pub fn send(message: Message) -> Result<(), io::Error> {
        let mut serialized = serde_json::to_string(&message)?;
        serialized.push('\n');
        io::stdout().write_all(serialized.as_bytes())?;
        eprintln!("Replied message: {:?}", message);
        io::stdout().flush()
    }
}
