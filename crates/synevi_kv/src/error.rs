use thiserror::Error;

#[derive(Error, Debug)]
pub enum KVError {
    #[error("Key not found")]
    KeyNotFound,
    #[error("From value mismatch")]
    MismatchError,
    #[error("Protocol error")]
    ProtocolError(#[from] anyhow::Error),
    #[error("Receive error")]
    RcvError(#[from] tokio::sync::oneshot::error::RecvError),
}
