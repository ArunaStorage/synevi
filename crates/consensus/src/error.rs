use crate::utils::T0;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum WaitError {
    #[error("Timeout waiting for event")]
    Timeout(T0),
    #[error("Sender got closed")]
    SenderClosed,
}
