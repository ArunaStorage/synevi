use ahash::RandomState;
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use diesel_ulid::DieselUlid;
use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;
use synevi_consensus::node::Execute;
use synevi_consensus::node::Node;
use synevi_consensus::replica::ReplicaConfig;
use synevi_network::network::Network;
use tokio::sync::Mutex;

pub struct KVStore {
    pub kv_store: Arc<KVMap>,
    pub node: Node,
}

#[derive(Debug)]
pub struct KVMap(pub Mutex<HashMap<String, String, RandomState>>);

impl KVMap {
    pub fn new() -> Self {
        KVMap(Mutex::new(HashMap::default()))
    }
}

impl KVStore {
    pub async fn init_maelstrom(
        id: DieselUlid,
        serial: u16,
        network: Arc<dyn Network + Send + Sync>,
        members: Vec<(DieselUlid, u16, String)>,
        path: Option<String>,
    ) -> Result<(Self, Arc<ReplicaConfig>)> {
        let kv_map = Arc::new(KVMap::new());
        let mut node =
            Node::new_with_parameters_and_replica(id, serial, network, path, kv_map.clone())
                .await?;
        for (ulid, id, host) in members {
            node.0.add_member(ulid, id, host).await?;
        }
        Ok((
            KVStore {
                kv_store: kv_map,
                node: node.0,
            },
            node.1,
        ))
    }

    pub async fn init(
        id: DieselUlid,
        serial: u16,
        network: Arc<dyn Network + Send + Sync>,
        members: Vec<(DieselUlid, u16, String)>,
        path: Option<String>,
    ) -> Result<Self> {
        let kv_map = Arc::new(KVMap::new());
        let mut node = Node::new_with_parameters(id, serial, network, path, kv_map.clone()).await?;
        for (ulid, id, host) in members {
            node.add_member(ulid, id, host).await?;
        }
        Ok(KVStore {
            kv_store: kv_map,
            node,
        })
    }

    pub async fn read(&self, key: String) -> Result<String> {
        let log = format!("READ: {key}");
        eprintln!("{log}; {:?}", self.kv_store);
        let payload = Transaction::Read { key: key.clone() };
        if let Err(err) = self.node.transaction(payload.into()).await {
            eprintln!("{err:?}");
        };
        self.kv_store
            .0
            .lock()
            .await
            .get(&key)
            .ok_or_else(|| anyhow!("Key not found"))
            .cloned()
    }

    pub async fn write(&mut self, key: String, value: String) -> Result<()> {
        let log = format!("WRITE: {key}: {value}");
        eprintln!("{log}");
        let payload = Transaction::Write { key, value };
        self.node.transaction(payload.into()).await?;
        Ok(())
    }
    pub async fn cas(&mut self, key: String, from: String, to: String) -> Result<()> {
        let log = format!("CAS: {key}: from: {from} to: {to}");
        eprintln!("{log}");
        let payload = Transaction::Cas { key, from, to };
        self.node.transaction(payload.into()).await?;
        Ok(())
    }
}

impl Debug for KVStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KVStore")
            .field("kv_store", &self.kv_store)
            .finish()
    }
}

#[async_trait]
impl Execute for KVMap {
    async fn execute(&self, payload: Vec<u8>) -> Result<()> {
        let transaction: Transaction = payload.try_into()?;
        match transaction {
            Transaction::Read { .. } => {
                eprintln!("READ EXEC");
            }
            Transaction::Write { key, value } => {
                eprintln!("WRITE EXEC");
                self.0.lock().await.insert(key, value);
            }
            Transaction::Cas { key, from, to } => {
                eprintln!("CAS EXEC");
                if let Some(entry) = self.0.lock().await.get_mut(&key) {
                    if entry == &from {
                        *entry = to;
                    }
                };
            }
        }

        Ok(())
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum Transaction {
    Read { key: String },
    Write { key: String, value: String },
    Cas {key: String, from: String, to: String},
}

impl TryFrom<Vec<u8>> for Transaction {
    type Error = anyhow::Error;
    fn try_from(value: Vec<u8>) -> std::result::Result<Self, Self::Error> {
        let (state, payload) = value.split_at(1);
        match state {
            [0u8] => Ok(Transaction::Read {
                key: String::from_utf8(payload.to_vec())?,
            }),
            [1u8] => {
                let (len, key_val) = payload.split_at(8);
                let len = u64::from_be_bytes(<[u8; 8]>::try_from(len)?);
                let (key, val) = key_val.split_at(len.try_into()?);

                Ok(Transaction::Write {
                    key: String::from_utf8(key.to_vec())?,
                    value: String::from_utf8(val.to_vec())?,
                })
            }
            [2u8] => {
                // Get key len and parse key
                let (key_len, key_from_to) = payload.split_at(8);
                let key_len = u64::from_be_bytes(<[u8; 8]>::try_from(key_len)?);
                let (key, from_to) = key_from_to.split_at(key_len.try_into()?);

                // Get from len and parse from
                let (from_len, from_to) = from_to.split_at(8);
                let from_len = u64::from_be_bytes(<[u8; 8]>::try_from(from_len)?);
                let (from, to) = from_to.split_at(from_len.try_into()?);

                Ok(Transaction::Cas {
                    key: String::from_utf8(key.to_vec())?,
                    from: String::from_utf8(from.to_vec())?,
                    to: String::from_utf8(to.to_vec())?,
                })
            }
            _ => Err(anyhow!("Invalid state")),
        }
    }
}

impl From<Transaction> for Vec<u8> {
    fn from(value: Transaction) -> Self {
        let mut result = Vec::new();
        match value {
            Transaction::Read { key } => {
                result.push(0);
                result.extend(key.into_bytes());
            }
            Transaction::Write { key, value } => {
                result.push(1);
                let key = key.into_bytes();
                let len = key.len() as u64;
                result.extend(len.to_be_bytes());
                result.extend(key);
                result.extend(value.into_bytes());
            }
            Transaction::Cas {key, from, to} => {
                result.push(2);
                let key = key.into_bytes();
                let key_len = key.len() as u64;
                result.extend(key_len.to_be_bytes());
                result.extend(key);
                let from = from.into_bytes();
                let from_len = from.len() as u64;
                result.extend(from_len.to_be_bytes());
                result.extend(from);
                result.extend(to.into_bytes());
            }
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use crate::Transaction;

    #[test]
    fn test_conversion() {
        let transaction_read = Transaction::Read {
            key: "1".to_string(),
        };
        let transaction_write = Transaction::Write {
            key: "1".to_string(),
            value: "world".to_string(),
        };
        let transaction_cas = Transaction::Cas {
            key: "1".to_string(),
            from: "2".to_string(),
            to: "3".to_string(),
        };
        let convert_read: Vec<u8> = transaction_read.clone().into();
        let back_converted_read = convert_read.try_into().unwrap();
        assert_eq!(transaction_read, back_converted_read);

        let convert_write: Vec<u8> = transaction_write.clone().into();
        let back_converted_write = convert_write.try_into().unwrap();
        assert_eq!(transaction_write, back_converted_write);

        let convert_cas: Vec<u8> = transaction_cas.clone().into();
        let back_converted_cas = convert_cas.try_into().unwrap();
        assert_eq!(transaction_cas, back_converted_cas);
    }
}
