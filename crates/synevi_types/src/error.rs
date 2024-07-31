use synevi_network::error::BroadCastError;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum ConsensusError<E> {
    #[error("Majority not reached")]
    BroadCastError(#[from] BroadCastError),
    #[error("Competing coordinator")]
    CompetingCoordinator,
    #[error("Anyhow error")]
    AnyhowError(#[from] anyhow::Error),
    #[error(transparent)]
    ExecutorError(E),
}
