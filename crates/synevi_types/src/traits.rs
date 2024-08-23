use std::sync::{Arc, Weak};

use crate::ConsensusError;
use anyhow::{anyhow, Result};

pub trait Transaction: std::fmt::Debug + Clone + Send {
    fn as_bytes(&self) -> Vec<u8>;
    fn from_bytes(bytes: Vec<u8>) -> Result<Self>
    where
        Self: Sized;
}

impl Transaction for Vec<u8> {
    fn as_bytes(&self) -> Vec<u8> {
        self.clone()
    }

    fn from_bytes(bytes: Vec<u8>) -> Result<Self> {
        Ok(bytes)
    }
}

#[async_trait::async_trait]
pub trait Executor: Send + Sync + 'static {
    type Tx: Transaction;
    type TxOk: Send;
    type TxErr: Send;
    // Executor expects a type with interior mutability
    async fn execute(
        &self,
        transaction: Self::Tx,
    ) -> Result<Self::TxOk, ConsensusError<Self::TxErr>>;
}

#[async_trait::async_trait]
impl<E> Executor for Arc<E>
where
    E: Executor,
{
    type Tx = E::Tx;
    type TxOk = E::TxOk;
    type TxErr = E::TxErr;

    async fn execute(
        &self,
        transaction: Self::Tx,
    ) -> Result<Self::TxOk, ConsensusError<Self::TxErr>> {
        self.as_ref().execute(transaction).await
    }
}

#[async_trait::async_trait]
impl<E> Executor for Weak<E>
where
    E: Executor,
{
    type Tx = E::Tx;
    type TxOk = E::TxOk;
    type TxErr = E::TxErr;

    async fn execute(
        &self,
        transaction: Self::Tx,
    ) -> Result<Self::TxOk, ConsensusError<Self::TxErr>> {
        self.upgrade()
            .ok_or_else(|| anyhow!("Arc dropped"))?
            .as_ref()
            .execute(transaction)
            .await
    }
}
