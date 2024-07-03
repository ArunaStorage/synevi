use crate::utils::T0;
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
    MajorityNotReached,
    #[error("Competing coordinator")]
    CompetingCoordinator,
}
