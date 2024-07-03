mod coordinator;
pub(crate) mod error;
mod event_store;
pub mod node;
mod replica;
pub mod utils;

#[cfg(test)]
pub mod tests {
    use std::sync::Arc;
    use anyhow::Result;
    use diesel_ulid::DieselUlid;
    use consensus_transport::network::{BroadcastRequest, Member, Network};
    use consensus_transport::network::BroadcastResponse;
    use consensus_transport::network::NetworkInterface;
    use consensus_transport::replica::Replica;

    #[derive(Debug)]
    pub struct NetworkMock {}

    #[async_trait::async_trait]
    impl NetworkInterface for NetworkMock {
        async fn broadcast(&self, request: BroadcastRequest) -> Result<Vec<BroadcastResponse>> {
            println!("{:?}", request);
            Ok(vec![])
        }
    }
    
    #[async_trait::async_trait]
    impl Network for NetworkMock {
        async fn add_members(&mut self, _members: Vec<Arc<Member>>) {
        }

        async fn add_member(&mut self, _id: DieselUlid, _serial: u16, _host: String) -> Result<()> {
            Ok(())
        }

        async fn spawn_server(&mut self, _server: Arc<dyn Replica>) -> Result<()> {
            Ok(())
        }

        fn get_interface(&self) -> Arc<dyn NetworkInterface> {
            Arc::new(NetworkMock{})
        }
    }
    
}
