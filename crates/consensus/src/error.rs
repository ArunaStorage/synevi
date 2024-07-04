use crate::utils::T0;
use consensus_transport::error::BroadCastError;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum WaitError {
    #[error("Timeout waiting for event")]
    Timeout(T0),
    #[error("Sender got closed")]
    SenderClosed,
}

#[derive(Error, Debug)]
pub enum ConsensusError {
    #[error("Majority not reached")]
    BroadCastError(#[from] BroadCastError),
    #[error("Competing coordinator")]
    CompetingCoordinator,
    #[error("Anyhow error")]
    AnyhowError(#[from] anyhow::Error),
}
