mod coordinator;
pub mod node;
pub mod reorder_buffer;
pub mod replica;
pub mod utils;
mod wait_handler;

pub mod tests {
    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::AtomicU32;
    use std::sync::Arc;
    use synevi_network::configure_transport::GetEventResponse;
    use synevi_network::network::BroadcastResponse;
    use synevi_network::network::MemberWithLatency;
    use synevi_network::network::NetworkInterface;
    use synevi_network::network::NodeInfo;
    use synevi_network::network::NodeStatus;
    use synevi_network::network::{BroadcastRequest, Network};
    use synevi_network::replica::Replica;
    use synevi_types::types::SyneviResult;
    use synevi_types::Executor;
    use synevi_types::SyneviError;
    use synevi_types::T0;
    use tokio::sync::mpsc::Receiver;
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
        async fn broadcast_recovery(&self, _t0: T0) -> Result<bool, SyneviError> {
            Ok(true)
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
            _ready: bool,
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
        async fn get_members(&self) -> Vec<Arc<MemberWithLatency>> {
            vec![]
        }

        fn get_node_status(&self) -> Arc<NodeStatus> {
            Arc::new(NodeStatus {
                info: NodeInfo {
                    id: Ulid::new(),
                    serial: 0,
                    host: "localhost".to_string(),
                    ready: AtomicBool::new(false),
                },
                members_responded: AtomicU32::new(0),
                has_members: AtomicBool::new(false),
            })
        }

        async fn join_electorate(&self, _host: String) -> Result<u32, SyneviError> {
            Ok(0)
        }
        async fn get_stream_events(
            &self,
            _last_applied: Vec<u8>,
        ) -> Result<Receiver<GetEventResponse>, SyneviError> {
            let (_, rcv) = tokio::sync::mpsc::channel(1);
            Ok(rcv)
        }
        async fn ready_electorate(&self, _host: String) -> Result<(), SyneviError> {
            Ok(())
        }

        async fn ready_member(&self, _id: Ulid, _serial: u16) -> Result<(), SyneviError> {
            Ok(())
        }

        async fn report_config(&self, _host: String) -> Result<(), SyneviError> {
            Ok(())
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
