use anyhow::Result;

pub trait Transaction: Default + std::fmt::Debug + Send {
    type ExecutionResult: Send;

    fn as_bytes(&self) -> Vec<u8>;
    fn from_bytes(bytes: Vec<u8>) -> Result<Self>
    where
        Self: Sized;
}

pub trait Executor: Send + Sync + 'static {
    type Tx: Transaction;
    fn execute(&self, transaction: Self::Tx) -> Result<<Self::Tx as Transaction>::ExecutionResult>;
}
