use serde::Serialize;
use thiserror::Error;
use tokio::task::JoinError;

#[derive(Error, Debug)]
pub enum SyneviError {
    #[error("IntoInnerError")]
    IntoInnerError,
    #[error("JoinError")]
    JoinError(#[from] JoinError),
    #[error("Send to channel error {0}")]
    SendError(String),
    #[error("Receive from channel error {0}")]
    ReceiveError(String),
    #[error("TonicStatusError")]
    TonicStatusError(#[from] tonic::Status),
    #[error("Tonic transport error {0}")]
    TonicTransportError(#[from] tonic::transport::Error),
    #[error("Majority not reached")]
    MajorityNotReached,
    #[error("TonicMetadataError")]
    TonicMetadataError(#[from] tonic::metadata::errors::InvalidMetadataKey),
    #[error("Acquire semaphore error {0}")]
    AcquireSemaphoreError(#[from] tokio::sync::AcquireError),
    #[error("Missing wait handler")]
    MissingWaitHandler,
    #[error("Arc dropped for weak")]
    ArcDropped,

    #[error("Invalid uri")]
    InvalidUri(#[from] http::uri::InvalidUri),
    #[error("Invalid conversion from_bytes: {0}")]
    InvalidConversionFromBytes(String),
    #[error("Invalid conversion int: {0}")]
    InvalidConversionInt(#[from] std::num::TryFromIntError),
    #[error("Invalid conversion slice: {0}")]
    InvalidConversionSlice(#[from] std::array::TryFromSliceError),
    #[error("Invalid conversion serde_json: {0}")]
    InvalidConversionSerdeJson(#[from] serde_json::Error),
    #[error("Invalid conversion serde_postcard: {0}")]
    InvalidConversionSerdePostcard(#[from] postcard::Error),
    #[error("Invalid serialization for monotime {0}")]
    InvalidMonotime(#[from] monotime::error::MonotimeError),
    #[error("Invalid conversion from request: {0}")]
    InvalidConversionRequest(String),

    #[error("Database error {0}")]
    DatabaseError(#[from] heed::Error),
    #[error("Database {0} not found")]
    DatabaseNotFound(&'static str),
    #[error("Event {0} not found")]
    EventNotFound(u128),
    #[error("Transaction not found")]
    TransactionNotFound,
    #[error("Dependency {0} not found")]
    DependencyNotFound(u128),
    #[error("Missing execution hash")]
    MissingExecutionHash,
    #[error("Missing transaction hash")]
    MissingTransactionHash,
    #[error("Competing coordinator")]
    CompetingCoordinator,
    #[error("Undefined recovery")]
    UndefinedRecovery,
    #[error("No members found")]
    NoMembersFound,
    #[error("Not ready for transactions")]
    NotReady,
    #[error("Mismatched execution hashes")]
    MismatchedExecutionHashes,
    #[error("Mismatched transaction hashes")]
    MismatchedTransactionHashes,
    #[error("Unrecoverable transaction")]
    UnrecoverableTransaction,
    #[error("Expected external transaction: {0}")]
    InternalTransaction(String),
}

impl Serialize for SyneviError {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.to_string().serialize(serializer)
    }
}
