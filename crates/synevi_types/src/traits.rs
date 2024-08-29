use std::sync::{Arc, Weak};

use crate::ConsensusError;

pub trait Transaction: std::fmt::Debug + Clone + Send {
    type TxErr: Send;
    type TxOk: Send;
    fn as_bytes(&self) -> Vec<u8>;
    fn from_bytes(bytes: Vec<u8>) -> Result<Self, ConsensusError<Self::TxErr>>
    where
        Self: Sized;
}

impl Transaction for Vec<u8> {
    type TxErr = ();
    type TxOk = Vec<u8>;
    fn as_bytes(&self) -> Vec<u8> {
        self.clone()
    }

    fn from_bytes(bytes: Vec<u8>) -> Result<Self, ConsensusError<Self::TxErr>> {
        Ok(bytes)
    }
}

#[async_trait::async_trait]
pub trait Executor: Send + Sync + 'static {
    type Tx: Transaction;
    // Executor expects a type with interior mutability
    async fn execute(
        &self,
        transaction: Self::Tx,
    ) -> Result<<Self::Tx as Transaction>::TxOk, ConsensusError<<Self::Tx as Transaction>::TxErr>>;
}

#[async_trait::async_trait]
impl<E> Executor for Arc<E>
where
    E: Executor,
{
    type Tx = E::Tx;
    async fn execute(
        &self,
        transaction: Self::Tx,
    ) -> Result<<Self::Tx as Transaction>::TxOk, ConsensusError<<Self::Tx as Transaction>::TxErr>>
    {
        self.as_ref().execute(transaction).await
    }
}

#[async_trait::async_trait]
impl<E> Executor for Weak<E>
where
    E: Executor,
{
    type Tx = E::Tx;

    async fn execute(
        &self,
        transaction: Self::Tx,
    ) -> Result<<Self::Tx as Transaction>::TxOk, ConsensusError<<Self::Tx as Transaction>::TxErr>>
    {
        self.upgrade()
            .ok_or_else(|| ConsensusError::InternalError("Arc dropped for weak"))?
            .as_ref()
            .execute(transaction)
            .await
    }
}
