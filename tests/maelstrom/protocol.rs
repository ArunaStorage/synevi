use crate::messages::Message;
use std::io::{self, Write};
use async_trait::async_trait;
use synevi_network::consensus_transport::{AcceptRequest, AcceptResponse, ApplyRequest, ApplyResponse, CommitRequest, CommitResponse, PreAcceptRequest, PreAcceptResponse, RecoverRequest, RecoverResponse};
use synevi_network::replica::Replica;

#[derive(Debug)]
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

#[async_trait]
impl Replica for MessageHandler {
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