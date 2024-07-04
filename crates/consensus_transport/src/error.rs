use thiserror::Error;
use tokio::task::JoinError;

#[derive(Error, Debug)]
pub enum BroadCastError {
    #[error("JoinError")]
    JoinError(#[from] JoinError),
    #[error("TonicStatusError")]
    TonicStatusError(#[from] tonic::Status),
    #[error("Majority not reached")]
    MajorityNotReached,
    #[error("TonicMetadataError")]
    TonicMetadataError(#[from] tonic::metadata::errors::InvalidMetadataKey),
}
