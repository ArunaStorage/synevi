use crate::error::KVError;
use ahash::RandomState;
use anyhow::{anyhow, Result};
use diesel_ulid::DieselUlid;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::{Arc, Mutex};
use synevi_core::node::Node;
use synevi_network::network::Network;
use synevi_types::{ConsensusError, Executor};

#[derive(Clone)]
pub struct KVExecutor {
    store: Arc<Mutex<HashMap<String, String, RandomState>>>,
}

#[derive(Clone)]
pub struct KVStore<N>
where
    N: Network,
{
    node: Arc<Node<N, KVExecutor>>,
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

#[async_trait::async_trait]
impl Executor for KVExecutor {
    type Tx = Transaction;
    type TxOk = String;
    type TxErr = KVError;
    async fn execute(&self, transaction: Self::Tx) -> Result<String, ConsensusError<Self::TxErr>> {
        Ok(match transaction {
            Transaction::Read { key } => {
                let Some(key) = self.store.lock().unwrap().get(&key).cloned() else {
                    return Err(ConsensusError::ExecutorError(KVError::KeyNotFound));
                };
                key
            }
            Transaction::Write { key, value } => {
                self.store
                    .lock()
                    .unwrap()
                    .insert(key.clone(), value.clone());
                value
            }
            Transaction::Cas { key, from, to } => {
                let mut store = self.store.lock().unwrap();

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

impl<N> KVStore<N>
where
    N: Network,
{
    pub async fn init(
        id: DieselUlid,
        serial: u16,
        network: N,
        members: Vec<(DieselUlid, u16, String)>,
    ) -> Result<Self, KVError> {
        let executor = KVExecutor {
            store: Arc::new(Mutex::new(HashMap::default())),
        };

        let node =
            Node::new_with_network_and_executor(id, serial, network, executor.clone()).await?;
        for (ulid, id, host) in members {
            node.add_member(ulid, id, host).await?;
        }

        Ok(KVStore { node })
    }

    async fn transaction(
        &self,
        id: DieselUlid,
        transaction: Transaction,
    ) -> Result<String, KVError> {
        let node = self.node.clone();
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
        let id = DieselUlid::generate();
        let payload = Transaction::Read { key: key.clone() };
        let response = self.transaction(id, payload).await?;
        Ok(response)
    }

    pub async fn write(&self, key: String, value: String) -> Result<(), KVError> {
        let id = DieselUlid::generate();
        let payload = Transaction::Write { key, value };
        let _response = self.transaction(id, payload).await?;
        Ok(())
    }
    pub async fn cas(&self, key: String, from: String, to: String) -> Result<(), KVError> {
        let id = DieselUlid::generate();
        let payload = Transaction::Cas { key, from, to };
        let _response = self.transaction(id, payload).await?;
        Ok(())
    }
}
