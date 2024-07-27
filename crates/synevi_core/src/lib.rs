mod coordinator;
pub(crate) mod error;
pub mod node;
pub mod reorder_buffer;
pub mod replica;
pub mod utils;
mod wait_handler;

#[cfg(test)]
pub mod tests {
    use anyhow::Result;
    use diesel_ulid::DieselUlid;
    use std::sync::Arc;
    use synevi_network::error::BroadCastError;
    use synevi_network::network::BroadcastResponse;
    use synevi_network::network::NetworkInterface;
    use synevi_network::network::{BroadcastRequest, Network};
    use synevi_network::replica::Replica;
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
        async fn broadcast(
            &self,
            request: BroadcastRequest,
        ) -> Result<Vec<BroadcastResponse>, BroadCastError> {
            self.got_requests.lock().await.push(request);
            Ok(vec![])
        }
    }

    #[async_trait::async_trait]
    impl Network for NetworkMock {
        async fn add_members(&self, _members: Vec<(DieselUlid, u16, String)>) {}

        async fn add_member(&self, _id: DieselUlid, _serial: u16, _host: String) -> Result<()> {
            Ok(())
        }

        async fn spawn_server(&self, _server: Arc<dyn Replica>) -> Result<()> {
            Ok(())
        }

        async fn get_interface(&self) -> Arc<dyn NetworkInterface> {
            Arc::new(NetworkMock {
                got_requests: self.got_requests.clone(),
            })
        }

        async fn get_waiting_time(&self, _node_serial: u16) -> u64 {
            0
        }
    }
}
