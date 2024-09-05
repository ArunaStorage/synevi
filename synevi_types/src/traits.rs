use ahash::RandomState;
use serde::Serialize;
use std::{
    collections::{BTreeMap, HashSet},
    sync::{Arc, Weak},
};

use crate::{
    types::{Event, Hashes, RecoverDependencies, RecoverEvent, SyneviResult, UpsertEvent},
    Ballot, State, SyneviError, T, T0,
};

pub trait Transaction: std::fmt::Debug + Clone + Send {
    type TxErr: Send + Serialize;
    type TxOk: Send + Serialize;
    fn as_bytes(&self) -> Vec<u8>;
    fn from_bytes(bytes: Vec<u8>) -> Result<Self, SyneviError>
    where
        Self: Sized;
}

impl Transaction for Vec<u8> {
    type TxErr = Vec<u8>;
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
    type Tx: Transaction + Serialize;
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

pub type Dependencies = HashSet<T0, RandomState>;

pub trait Store: Send + Sync + Sized + 'static {
    fn new(node_serial: u16) -> Result<Self, SyneviError>;
    // Initialize a new t0
    fn init_t_zero(&mut self, node_serial: u16) -> T0;
    // Pre-accept a transaction
    fn pre_accept_tx(
        &mut self,
        id: u128,
        t_zero: T0,
        transaction: Vec<u8>,
    ) -> Result<(T, Dependencies), SyneviError>;
    // Get the dependencies for a transaction
    fn get_tx_dependencies(&self, t: &T, t_zero: &T0) -> Dependencies;
    // Get the recover dependencies for a transaction
    fn get_recover_deps(&self, t_zero: &T0) -> Result<RecoverDependencies, SyneviError>;
    // Tries to recover an unfinished event from the store
    fn recover_event(
        &mut self,
        t_zero_recover: &T0,
        node_serial: u16,
    ) -> Result<RecoverEvent, SyneviError>;
    // Check and update the ballot for a transaction
    // Returns true if the ballot was accepted (current <= ballot)
    fn accept_tx_ballot(&mut self, t_zero: &T0, ballot: Ballot) -> Option<Ballot>;
    // Update or insert a transaction, returns the hash of the transaction if applied
    fn upsert_tx(&mut self, upsert_event: UpsertEvent) -> Result<Option<Hashes>, SyneviError>;

    fn get_event_state(&self, t_zero: &T0) -> Option<State>;

    fn get_event_store(&self) -> BTreeMap<T0, Event>;
}
