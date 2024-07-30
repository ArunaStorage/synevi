use synevi_network::error::BroadCastError;
use thiserror::Error;
use crate::traits::ExecutorError;

#[derive(Error, Debug)]
pub enum ConsensusError<E: ExecutorError> {
    #[error("Majority not reached")]
    BroadCastError(#[from] BroadCastError),
    #[error("Competing coordinator")]
    CompetingCoordinator,
    #[error("Anyhow error")]
    AnyhowError(#[from] anyhow::Error),
    #[error("Error from executor")]
    ExecutorError(#[from] E),
}
