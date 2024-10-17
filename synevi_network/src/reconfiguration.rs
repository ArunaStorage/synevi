use crate::configure_transport::{
    GetEventRequest, GetEventResponse, JoinElectorateRequest, JoinElectorateResponse,
    ReadyElectorateRequest, ReadyElectorateResponse, ReportElectorateRequest,
    ReportElectorateResponse,
};
use synevi_types::{SyneviError, T};
use ulid::Ulid;

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
    ) -> Result<tokio::sync::mpsc::Receiver<Result<GetEventResponse, SyneviError>>, SyneviError>;
    async fn ready_electorate(
        &self,
        request: ReadyElectorateRequest,
    ) -> Result<ReadyElectorateResponse, SyneviError>;

    // Joining node
    async fn report_electorate(
        &self,
        request: ReportElectorateRequest,
    ) -> Result<ReportElectorateResponse, SyneviError>;
}

#[derive(Debug)]
pub struct Report {
    pub node_id: Ulid,
    pub node_serial: u16,
    pub node_host: String,
    pub last_applied: T,
    pub last_applied_hash: [u8; 32],
}
