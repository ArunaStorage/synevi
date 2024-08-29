use thiserror::Error;
use tokio::task::JoinError;

#[derive(Error, Debug)]
pub enum BroadCastError {
    #[error("IntoInnerError")]
    IntoInnerError,
    #[error("JoinError")]
    JoinError(#[from] JoinError),
    #[error("TonicStatusError")]
    TonicStatusError(#[from] tonic::Status),
    #[error("Majority not reached")]
    MajorityNotReached,
    #[error("TonicMetadataError")]
    TonicMetadataError(#[from] tonic::metadata::errors::InvalidMetadataKey),
}

#[derive(Error, Debug)]
pub enum NetworkError {
    #[error("BroadCastError")]
    BroadCastError(#[from] BroadCastError),
    #[error("Invalid uri")]
    InvalidUri(#[from] http::uri::InvalidUri),
    #[error("Tonic transport error {0}")]
    TonicTransportError(#[from] tonic::transport::Error),
    #[error("Latency error {0}")]
    LatencyError(#[from] LatencyError),
}

#[derive(Error, Debug)]
pub enum LatencyError {
    #[error("Invalid conversion int: {0}")]
    InvalidConversionInt(#[from] std::num::TryFromIntError),
    #[error("Invalid conversion slice: {0}")]
    InvalidConversionSlice(#[from] std::array::TryFromSliceError),
}

#[derive(Error, Debug)]
pub enum ConsensusError<E> {
    #[error("Majority not reached")]
    BroadCastError(#[from] BroadCastError),
    #[error("Competing coordinator")]
    CompetingCoordinator,
    #[error("Internal error: {0}")]
    InternalError(&'static str),
    #[error("Invalid serialization")]
    InvalidSerialization(#[from] DeserializeError),
    #[error(transparent)]
    ExecutorError(E),
    #[error("Coordinator error {0}")]
    CoordinatorError(#[from] CoordinatorError),
    #[error("Persistence error {0}")]
    PersistenceError(#[from] PersistenceError),
}

#[derive(Error, Debug)]
pub enum PersistenceError{
    #[error("Database error {0}")]
    DatabaseError(#[from] heed::Error),
    #[error("Database {0} not found")]
    DatabaseNotFound(&'static str),
    #[error("Event {0} not found")]
    EventNotFound(u128),
    #[error("Dependency {0} not found")]
    DependencyNotFound(u128),
    #[error("Undefined recovery")]
    UndefinedRecovery,
}

#[derive(Error, Debug)]
pub enum CoordinatorError {
    #[error("Monotime deserialize error {0}")]
    MonotimeDeserializeError(#[from] DeserializeError),
    #[error("Persistence error {0}")]
    PersistenceError(#[from] PersistenceError),
    #[error("Missing wait handler")]
    MissingWaitHandler,
    #[error("Transaction not found")]
    TransactionNotFound,
}

#[derive(Error, Debug)]
#[error("Invalid serialization")]
pub struct DeserializeError(#[from] monotime::error::MonotimeError);
