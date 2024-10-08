use serde::Serialize;
use synevi_types::SyneviError;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum KVError {
    #[error("Key not found")]
    KeyNotFound,
    #[error("From value mismatch")]
    MismatchError,
    #[error("Protocol error {0}")]
    ProtocolError(#[from] SyneviError),
    #[error("Receive error")]
    RcvError(#[from] tokio::sync::oneshot::error::RecvError),
}

impl Serialize for KVError {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.to_string().serialize(serializer)
    }
}
