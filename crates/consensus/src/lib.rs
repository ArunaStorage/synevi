mod coordinator;
pub(crate) mod error;
mod event_store;
pub mod node;
mod replica;
pub mod utils;

#[cfg(test)]
pub mod tests {
    use anyhow::Result;
    use consensus_transport::network::BroadcastRequest;
    use consensus_transport::network::BroadcastResponse;
    use consensus_transport::network::NetworkInterface;

    #[derive(Debug)]
    pub struct NetworkMock {}

    #[async_trait::async_trait]
    impl NetworkInterface for NetworkMock {
        async fn broadcast(&self, _request: BroadcastRequest) -> Result<Vec<BroadcastResponse>> {
            unimplemented!()
        }
    }
}
