use std::error::Error;

use anyhow::Result;

pub trait Transaction: std::fmt::Debug + Send {
    fn as_bytes(&self) -> Vec<u8>;
    fn from_bytes(bytes: Vec<u8>) -> Result<Self>
    where
        Self: Sized;
}

pub trait ExecutorError: Error {}

impl Transaction for Vec<u8> {
    fn as_bytes(&self) -> Vec<u8> {
        self.clone()
    }

    fn from_bytes(bytes: Vec<u8>) -> Result<Self> {
        Ok(bytes)
    }
}

pub trait Executor: Send + Sync + 'static {
    type Tx: Transaction;
    type TxOk: Send;
    type TxErr: Error + Send;
    // Executor expects a type with interior mutability
    fn execute(&self, transaction: Self::Tx) -> Result<Self::TxOk, Self::TxErr>;
}
