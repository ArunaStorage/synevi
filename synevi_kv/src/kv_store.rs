use crate::error::KVError;
use ahash::RandomState;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::{Arc, Mutex};
use synevi_core::node::Node;
use synevi_network::network::Network;
use synevi_types::types::{ExecutorResult, SyneviResult};
use synevi_types::{error::SyneviError, Executor};
use ulid::Ulid;

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
    type TxErr = KVError;
    type TxOk = String;

    fn as_bytes(&self) -> Vec<u8> {
        serde_json::to_vec(self).unwrap()
    }

    fn from_bytes(bytes: Vec<u8>) -> Result<Self, SyneviError>
    where
        Self: Sized,
    {
        Ok(serde_json::from_slice(&bytes)?)
    }
}

#[async_trait::async_trait]
impl Executor for KVExecutor {
    type Tx = Transaction;
    async fn execute(&self, transaction: Self::Tx) -> SyneviResult<Self> {
        Ok(match transaction {
            Transaction::Read { key } => {
                let Some(key) = self.store.lock().unwrap().get(&key).cloned() else {
                    return Ok(ExecutorResult::External(Err(KVError::KeyNotFound)));
                };
                ExecutorResult::External(Ok(key))
            }
            Transaction::Write { key, value } => {
                self.store
                    .lock()
                    .unwrap()
                    .insert(key.clone(), value.clone());
                ExecutorResult::External(Ok(value))
            }
            Transaction::Cas { key, from, to } => {
                let mut store = self.store.lock().unwrap();

                let Some(entry) = store.get_mut(&key) else {
                    return Ok(ExecutorResult::External(Err(KVError::KeyNotFound)));
                };

                if entry == &from {
                    *entry = to.clone();
                    ExecutorResult::External(Ok(to))
                } else {
                    return Ok(ExecutorResult::External(Err(KVError::MismatchError)));
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
        id: Ulid,
        serial: u16,
        network: N,
        members: Vec<(Ulid, u16, String)>,
    ) -> Result<Self, KVError> {
        let executor = KVExecutor {
            store: Arc::new(Mutex::new(HashMap::default())),
        };

        let node =
            Node::new_with_network_and_executor(id, serial, network, executor.clone()).await?;
        for (ulid, id, host) in members {
            node.add_member(ulid, id, host, true).await?;
        }

        Ok(KVStore { node })
    }

    async fn transaction(&self, id: Ulid, transaction: Transaction) -> Result<String, KVError> {
        let node = self.node.clone();
        match node
            .transaction(
                u128::from_be_bytes(id.to_bytes()),
                transaction,
            )
            .await?
        {
            ExecutorResult::External(result) => result,
            _ => Err(KVError::MismatchError), // TODO: Make a new error for this case
        }
    }

    pub async fn read(&self, key: String) -> Result<String, KVError> {
        let id = Ulid::new();
        let payload = Transaction::Read { key: key.clone() };
        let response = self.transaction(id, payload).await?;
        Ok(response)
    }

    pub async fn write(&self, key: String, value: String) -> Result<(), KVError> {
        let id = Ulid::new();
        let payload = Transaction::Write { key, value };
        let _response = self.transaction(id, payload).await?;
        Ok(())
    }
    pub async fn cas(&self, key: String, from: String, to: String) -> Result<(), KVError> {
        let id = Ulid::new();
        let payload = Transaction::Cas { key, from, to };
        let _response = self.transaction(id, payload).await?;
        Ok(())
    }
}
