use thiserror::Error;
use tokio::task::JoinError;


#[derive(Error, Debug)]
pub enum BroadCastError {
    #[error("JoinError")]
    JoinError(#[from] JoinError),
    #[error("TonicError")]
    TonicError(#[from] tonic::Status),
    #[error("Majority not reached")]
    MajorityNotReached,
}
