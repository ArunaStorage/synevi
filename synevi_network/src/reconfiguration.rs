use crate::{
    configure_transport::{
        GetEventRequest, GetEventResponse,
        JoinElectorateRequest, JoinElectorateResponse, ReadyElectorateRequest,
        ReadyElectorateResponse, ReportLastAppliedRequest, ReportLastAppliedResponse,
    },
    consensus_transport::{
        ApplyRequest, CommitRequest,
    },
};
use std::{collections::BTreeMap, sync::Arc};
use synevi_types::{SyneviError, T, T0};
use tokio::sync::{
    mpsc::{channel, Receiver, Sender},
    Mutex,
};
use ulid::Ulid;

pub struct ReplicaBuffer {
    inner: Arc<Mutex<BTreeMap<T0, BufferedMessage>>>,
    _notifier: Sender<Report>,
}

impl ReplicaBuffer {
    pub fn new(sdx: Sender<Report>) -> Self {
        ReplicaBuffer {
            inner: Arc::new(Mutex::new(BTreeMap::new())),
            _notifier: sdx,
        }
    }

    pub async fn send_buffered(
        &self,
    ) -> Result<Receiver<Option<(T0, BufferedMessage)>>, SyneviError> {
        let (sdx, rcv) = channel(100);
        let inner = self.inner.clone();
        tokio::spawn(async move {
            loop {
                let mut lock = inner.lock().await;
                if let Some(event) = lock.pop_first() {
                    sdx.send(Some(event)).await.map_err(|_| {
                        SyneviError::SendError(
                            "Channel for receiving buffered messages closed".to_string(),
                        )
                    })?;
                } else {
                    sdx.send(None).await.map_err(|_| {
                        SyneviError::SendError(
                            "Channel for receiving buffered messages closed".to_string(),
                        )
                    })?;
                    break;
                }
            }
            Ok::<(), SyneviError>(())
        });
        Ok(rcv)
    }
}

#[async_trait::async_trait]
pub trait Reconfiguration {
    // Existing nodes
    async fn join_electorate(
        &self,
        request: JoinElectorateRequest,
    ) -> Result<JoinElectorateResponse, SyneviError>;
    async fn get_events(
        &self,
        request: GetEventRequest,
    ) -> tokio::sync::mpsc::Receiver<Result<GetEventResponse, SyneviError>>;
    async fn ready_electorate(
        &self,
        request: ReadyElectorateRequest,
    ) -> Result<ReadyElectorateResponse, SyneviError>;

    // Joining node
    async fn report_last_applied(
        &self,
        request: ReportLastAppliedRequest,
    ) -> Result<ReportLastAppliedResponse, SyneviError>;
}

#[derive(Debug, Clone)]
pub enum BufferedMessage {
    Commit(CommitRequest),
    Apply(ApplyRequest),
}

#[derive(Debug)]
pub struct Report {
    pub node_id: Ulid,
    pub node_serial: u16,
    pub node_host: String,
    pub last_applied: T,
    pub last_applied_hash: [u8; 32],
}
