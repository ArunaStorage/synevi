use ahash::{HashSet, RandomState};
use anyhow::Result;
use diesel_ulid::DieselUlid;
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;
use std::time::Duration;
use synevi_core::node::Node;
use synevi_network::network::Network;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::sync::{oneshot, Mutex, Notify};
use tokio::time::timeout;

use crate::error::KVError;

pub struct KVStore {
    pub store: HashMap<String, String, RandomState>,
    pub transactions: HashSet<DieselUlid>,
    pub node: Node,
}

#[derive(Debug, PartialEq, Eq, Clone)]
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

impl KVStore {
    pub async fn init(
        id: DieselUlid,
        serial: u16,
        network: Arc<dyn Network + Send + Sync>,
        members: Vec<(DieselUlid, u16, String)>,
        path: Option<String>,
    ) -> Result<Arc<Mutex<Self>>, KVError> {
        let store = Arc::new(Mutex::new(HashMap::default()));
        let transactions = Arc::new(Mutex::new(HashMap::default()));

        let (sdx, rcv) = tokio::sync::mpsc::channel(100);
        let store_clone = store.clone();
        let transaction_clone = transactions.clone();

        let mut node = Node::new_with_parameters(id, serial, network, path, sdx).await?;
        for (ulid, id, host) in members {
            node.add_member(ulid, id, host).await?;
        }
        let store = Arc::new(Mutex::new(KVStore {
            store: HashMap::default(),
            transactions: HashSet::default(),
            node,
        }));

        let store_clone = store.clone();

        tokio::spawn(async move { KVStore::execute(store_clone, rcv) });

        Ok(store)
    }

    pub async fn read(&self, key: String) -> Result<String, KVError> {
        eprintln!("STARTED READ with {key}");
        let id = DieselUlid::generate();
        let log = format!("READ: {id} - {key}");
        eprintln!("{log}");
        self.transactions.insert(id);
        let payload = Transaction::Read { key: key.clone() };
        self.node
            .transaction(u128::from_be_bytes(id.as_byte_array()), payload.into())
            .await?;
        let response = timeout(Duration::from_secs(5), rcv)
            .await
            .unwrap()?
            .map(|(_k, v)| v);
        eprintln!("GOT READ RESPONSE with key {key} and {response:?}");
        response
    }

    pub async fn write(&mut self, key: String, value: String) -> Result<(), KVError> {
        let id = DieselUlid::generate();
        let log = format!("WRITE: {id} - {key}: {value}");
        eprintln!("{log}");
        let (sdx, rcv) = oneshot::channel();
        self.transactions.lock().await.insert(id, sdx);
        let payload = Transaction::Write { key, value };
        self.node
            .transaction(u128::from_be_bytes(id.as_byte_array()), payload.into())
            .await?;
        rcv.await??;
        Ok(())
    }
    pub async fn cas(&mut self, key: String, from: String, to: String) -> Result<(), KVError> {
        let id = DieselUlid::generate();
        let log = format!("CAS: {id} - {key}: from: {from} to: {to}");
        let (sdx, rcv) = oneshot::channel();
        self.transactions.lock().await.insert(id, sdx);
        eprintln!("{log}");
        let payload = Transaction::Cas { key, from, to };
        self.node
            .transaction(u128::from_be_bytes(id.as_byte_array()), payload.into())
            .await?;
        rcv.await??;
        Ok(())
    }

    async fn execute(
        store: Arc<Mutex<KVStore>>,
        mut rcv: Receiver<(u128, Vec<u8>, Arc<Notify>)>,
    ) -> Result<()> {
        loop {
            while let Some((id, transaction, notify)) = rcv.recv().await {
                let transaction: Transaction = transaction.try_into()?;
                let id = DieselUlid::from(id.to_be_bytes());
                match transaction {
                    Transaction::Read { key } => {
                        eprintln!("READ EXEC");
                        if let Some(sdx) = transactions.lock().await.remove(&id) {
                            eprintln!("READ LOCK ACQUIRED");
                            let lock = store.lock().await;
                            eprintln!("STORE LOCK ACQUIRED");
                            if let Some(value) = lock.get(&key).cloned() {
                                if let Err(err) = sdx.send(Ok((key.clone(), value.clone()))) {
                                    eprintln!("Receiver dropped: {err:?}");
                                } else {
                                    eprintln!("Sent message with id {id:?}");
                                };
                            } else if let Err(err) = sdx.send(Err(KVError::KeyNotFound)) {
                                eprintln!("Receiver dropped: {err:?}");
                            } else {
                                eprintln!("Sent message with id {id:?}");
                            };
                        }
                    }
                    Transaction::Write { key, value } => {
                        eprintln!("WRITE EXEC");
                        store.lock().await.insert(key.clone(), value.clone());
                        if let Some(sdx) = transactions.lock().await.remove(&id) {
                            if let Err(err) = sdx.send(Ok((key, value))) {
                                eprintln!("Receiver dropped: {err:?}");
                            };
                        }
                    }
                    Transaction::Cas { key, from, to } => {
                        eprintln!("CAS EXEC");

                        match transactions.lock().await.remove(&id) {
                            None => {
                                if let Some(entry) = store.lock().await.get_mut(&key) {
                                    if entry == &from {
                                        *entry = to;
                                    }
                                };
                            }
                            Some(sdx) => {
                                if let Some(entry) = store.lock().await.get_mut(&key) {
                                    if entry == &from {
                                        *entry = to.clone();
                                        if let Err(err) = sdx.send(Ok((entry.clone(), to))) {
                                            eprintln!("Receiver dropped: {err:?}");
                                        }
                                    } else if let Err(err) = sdx.send(Err(KVError::MismatchError)) {
                                        eprintln!("Receiver dropped: {err:?}");
                                    }
                                } else if let Err(err) = sdx.send(Err(KVError::KeyNotFound)) {
                                    eprintln!("Receiver dropped: {err:?}");
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}