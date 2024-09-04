mod coordinator;
pub mod node;
pub mod reorder_buffer;
pub mod replica;
pub mod utils;
mod wait_handler;

pub mod tests {
    use std::sync::Arc;
    use synevi_network::network::BroadcastResponse;
    use synevi_network::network::NetworkInterface;
    use synevi_network::network::{BroadcastRequest, Network};
    use synevi_network::replica::Replica;
    use synevi_types::types::SyneviResult;
    use synevi_types::Executor;
    use synevi_types::SyneviError;
    use tokio::sync::Mutex;
    use ulid::Ulid;

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
        ) -> Result<Vec<BroadcastResponse>, SyneviError> {
            self.got_requests.lock().await.push(request);
            Ok(vec![])
        }
    }

    #[async_trait::async_trait]
    impl Network for NetworkMock {
        type Ni = Self;
        async fn add_members(&self, _members: Vec<(Ulid, u16, String)>) {}

        async fn add_member(
            &self,
            _id: Ulid,
            _serial: u16,
            _host: String,
        ) -> Result<(), SyneviError> {
            Ok(())
        }

        async fn spawn_server<R: Replica>(&self, _server: R) -> Result<(), SyneviError> {
            Ok(())
        }

        async fn get_interface(&self) -> Arc<NetworkMock> {
            Arc::new(NetworkMock {
                got_requests: self.got_requests.clone(),
            })
        }

        async fn get_waiting_time(&self, _node_serial: u16) -> u64 {
            0
        }
    }

    pub struct DummyExecutor;

    #[async_trait::async_trait]
    impl Executor for DummyExecutor {
        type Tx = Vec<u8>;

        async fn execute(&self, data: Vec<u8>) -> SyneviResult<Self> {
            Ok(Ok(data))
        }
    }
}
