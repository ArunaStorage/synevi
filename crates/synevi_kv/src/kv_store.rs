use crate::error::KVError;
use ahash::RandomState;
use anyhow::{anyhow, Result};
use diesel_ulid::DieselUlid;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::{Arc, Mutex, RwLock};
use synevi_core::node::Node;
use synevi_network::network::Network;
use synevi_types::{ConsensusError, Executor};

pub struct KVStore<N>
where
    N: Network,
{
    store: Mutex<HashMap<String, String, RandomState>>,
    node: RwLock<Option<Arc<Node<N, ArcStore<N>>>>>,
}

impl<N> Default for KVStore<N>
where
    N: Network,
{
    fn default() -> Self {
        KVStore {
            store: Mutex::new(HashMap::default()),
            node: RwLock::new(None),
        }
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub enum Transaction {
    Read {
        key: String,
    },
    Write {
        key: String,
        value: String,
    },
    Cas {
        key: String,
        from: String,
        to: String,
    },
}

impl synevi_types::Transaction for Transaction {
    fn as_bytes(&self) -> Vec<u8> {
        serde_json::to_vec(self).unwrap()
    }

    fn from_bytes(bytes: Vec<u8>) -> Result<Self>
    where
        Self: Sized,
    {
        serde_json::from_slice(&bytes).map_err(Into::into)
    }
}

struct ArcStore<N: Network>(Arc<KVStore<N>>);

impl<N> Clone for ArcStore<N>
where
    N: Network,
{
    fn clone(&self) -> Self {
        ArcStore(self.0.clone())
    }
}

impl<N> Executor for ArcStore<N>
where
    N: Network,
{
    type Tx = Transaction;
    type TxOk = String;
    type TxErr = KVError;
    fn execute(&self, transaction: Self::Tx) -> Result<String, ConsensusError<Self::TxErr>> {
        Ok(match transaction {
            Transaction::Read { key } => self
                .0
                .store
                .lock()
                .unwrap()
                .get(&key)
                .cloned()
                .unwrap_or_default(),
            Transaction::Write { key, value } => {
                self.0
                    .store
                    .lock()
                    .unwrap()
                    .insert(key.clone(), value.clone());
                value
            }
            Transaction::Cas { key, from, to } => {
                let mut store = self.0.store.lock().unwrap();

                let entry = store
                    .get_mut(&key)
                    .ok_or_else(|| ConsensusError::ExecutorError(KVError::KeyNotFound))?;

                if entry == &from {
                    *entry = to.clone();
                    to
                } else {
                    return Err(ConsensusError::ExecutorError(KVError::MismatchError));
                }
            }
        })
    }
}

impl<N> ArcStore<N>
where
    N: Network,
{
    pub fn new() -> Self {
        ArcStore(Arc::new(KVStore::default()))
    }
    pub async fn init(
        id: DieselUlid,
        serial: u16,
        network: N,
        members: Vec<(DieselUlid, u16, String)>,
    ) -> Result<Self, KVError> {
        let arc_store = ArcStore::new();

        let node =
            Node::new_with_network_and_executor(id, serial, network, arc_store.clone()).await?;
        for (ulid, id, host) in members {
            node.add_member(ulid, id, host).await?;
        }
        arc_store.0.node.write().unwrap().replace(node);
        Ok(arc_store)
    }

    async fn transaction(
        &self,
        id: DieselUlid,
        transaction: Transaction,
    ) -> Result<String, KVError> {
        let lock = self
            .0
            .node
            .read()
            .unwrap();
        let node = lock.clone()
            .ok_or_else(|| KVError::ProtocolError(anyhow!("Node not set")))?;
        let response = node
            .transaction(u128::from_be_bytes(id.as_byte_array()), transaction)
            .await
            .map_err(|e| match e {
                ConsensusError::BroadCastError(e) => {
                    KVError::ProtocolError(anyhow!("Broadcast error: {e}"))
                }
                ConsensusError::CompetingCoordinator => {
                    KVError::ProtocolError(anyhow!("Competing coordinator"))
                }
                ConsensusError::AnyhowError(e) => KVError::ProtocolError(e),
                ConsensusError::ExecutorError(e) => e,
            })?;
        Ok(response)
    }

    pub async fn read(&self, key: String) -> Result<String, KVError> {
        eprintln!("STARTED READ with {key}");
        let id = DieselUlid::generate();
        let log = format!("READ: {id} - {key}");
        eprintln!("{log}");
        let payload = Transaction::Read { key: key.clone() };
        let response = self.transaction(id, payload.into()).await?;
        Ok(response)
    }

    pub async fn write(&mut self, key: String, value: String) -> Result<(), KVError> {
        let id = DieselUlid::generate();
        let log = format!("WRITE: {id} - {key}: {value}");
        eprintln!("{log}");
        let payload = Transaction::Write { key, value };
        let _response = self.transaction(id, payload.into()).await?;
        Ok(())
    }
    pub async fn cas(&mut self, key: String, from: String, to: String) -> Result<(), KVError> {
        let id = DieselUlid::generate();
        let log = format!("CAS: {id} - {key}: from: {from} to: {to}");
        eprintln!("{log}");
        let payload = Transaction::Cas { key, from, to };
        let _response = self.transaction(id, payload.into()).await?;
        Ok(())
    }
}
