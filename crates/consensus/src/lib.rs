mod coordinator;
pub(crate) mod error;
mod event_store;
pub mod node;
mod replica;
pub mod utils;

#[cfg(test)]
pub mod tests {
    use anyhow::Result;
    use consensus_transport::error::BroadCastError;
    use consensus_transport::network::BroadcastResponse;
    use consensus_transport::network::NetworkInterface;
    use consensus_transport::network::{BroadcastRequest, Network};
    use consensus_transport::replica::Replica;
    use diesel_ulid::DieselUlid;
    use std::sync::Arc;
    use tokio::sync::Mutex;

    #[derive(Debug, Default)]
    pub struct NetworkMock {
        got_requests: Arc<Mutex<Vec<BroadcastRequest>>>,
    }

    impl NetworkMock {
        pub async fn get_requests(&self) -> Vec<BroadcastRequest> {
            self.got_requests.lock().await.to_vec()
        }
    }

    #[async_trait::async_trait]
    impl NetworkInterface for NetworkMock {
        async fn broadcast(&self, request: BroadcastRequest) -> Result<Vec<BroadcastResponse>, BroadCastError> {
            self.got_requests.lock().await.push(request);
            Ok(vec![])
        }
    }

    #[async_trait::async_trait]
    impl Network for NetworkMock {
        async fn add_members(&mut self, _members: Vec<(DieselUlid, u16, String)>) {}

        async fn add_member(&mut self, _id: DieselUlid, _serial: u16, _host: String) -> Result<()> {
            Ok(())
        }

        async fn spawn_server(&mut self, _server: Arc<dyn Replica>) -> Result<()> {
            Ok(())
        }

        fn get_interface(&self) -> Arc<dyn NetworkInterface> {
            Arc::new(NetworkMock {
                got_requests: self.got_requests.clone(),
            })
        }
    }
}
