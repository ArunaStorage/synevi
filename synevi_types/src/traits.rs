use std::sync::{Arc, Weak};

use crate::{types::SyneviResult, SyneviError};

pub trait Transaction: std::fmt::Debug + Clone + Send {
    type TxErr: Send;
    type TxOk: Send;
    fn as_bytes(&self) -> Vec<u8>;
    fn from_bytes(bytes: Vec<u8>) -> Result<Self, SyneviError>
    where
        Self: Sized;
}

impl Transaction for Vec<u8> {
    type TxErr = ();
    type TxOk = Vec<u8>;
    fn as_bytes(&self) -> Vec<u8> {
        self.clone()
    }

    fn from_bytes(bytes: Vec<u8>) -> Result<Self, SyneviError> {
        Ok(bytes)
    }
}

#[async_trait::async_trait]
pub trait Executor: Send + Sync + 'static {
    type Tx: Transaction;
    // Executor expects a type with interior mutability
    async fn execute(&self, transaction: Self::Tx) -> SyneviResult<Self>;
}

#[async_trait::async_trait]
impl<E> Executor for Arc<E>
where
    E: Executor,
{
    type Tx = E::Tx;
    async fn execute(&self, transaction: Self::Tx) -> SyneviResult<Self> {
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
    ) -> Result<
        Result<<Self::Tx as Transaction>::TxOk, <Self::Tx as Transaction>::TxErr>,
        SyneviError,
    > {
        self.upgrade()
            .ok_or_else(|| SyneviError::ArcDropped)?
            .as_ref()
            .execute(transaction)
            .await
    }
}
