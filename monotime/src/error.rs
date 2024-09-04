use thiserror::Error;

#[derive(Debug, Error)]
pub enum MonotimeError {
    #[error("Invalid length")]
    InvalidLength,
    #[error("From slice serialization error")]
    FromSliceSerializationError(#[from] std::array::TryFromSliceError),
}
