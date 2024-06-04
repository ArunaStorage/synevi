use crate::event_store::{Event, EventStore};
use crate::utils::from_dependency;
use bytes::Bytes;
use consensus_transport::consensus_transport::consensus_transport_server::ConsensusTransport;
use consensus_transport::consensus_transport::*;
use diesel_ulid::DieselUlid;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::Mutex;
use tonic::transport::Channel;
use tonic::{Request, Response, Status};

pub static MAX_RETRIES: u64 = 10;

pub struct Replica {
    pub node: String,
    pub members: Vec<crate::coordinator::Member>,
    pub event_store: Arc<Mutex<EventStore>>,
}

#[derive(Clone, Debug)]
pub struct Member {
    pub host: String,
    pub node: String,
    pub channel: Channel,
}

#[derive(Clone, Debug)]
pub struct Transaction {
    pub state: State,
    pub transaction: Bytes,
    pub t_zero: DieselUlid,
    pub t: DieselUlid,
    pub dependencies: HashSet<DieselUlid>,
}

#[tonic::async_trait]
impl ConsensusTransport for Replica {
    async fn pre_accept(
        &self,
        request: Request<PreAcceptRequest>,
    ) -> Result<Response<PreAcceptResponse>, Status> {
        let request = request.into_inner();
        let t_zero = DieselUlid::try_from(request.timestamp_zero.as_slice())
            .map_err(|e| Status::invalid_argument(e.to_string()))?;

        // check for entries in temp
        if let Some(entry) = self.event_store.lock().await.last() {
            let (latest, _) = entry;

            // If there is a newer timestamp, propose new
            let t = if latest.timestamp() >= t_zero.timestamp() {
                DieselUlid::generate()
            } else {
                t_zero
            };

            // Get all dependencies in temp
            let dependencies = self.event_store.lock().await.get_dependencies(t);

            // Insert event into temp
            self.event_store.lock().await.insert(
                t,
                Event {
                    t_zero,
                    state: State::PreAccepted,
                    event: request.event.into(),
                    ballot_number: 0,
                    dependencies: from_dependency(dependencies.clone())
                        .map_err(|e| Status::internal(e.to_string()))?,
                },
            );
            Ok(Response::new(PreAcceptResponse {
                node: self.node.to_string(),
                timestamp: t.as_byte_array().into(),
                dependencies,
            }))
        } else {
            // If no entries are found in temp just insert and PreAccept msg
            self.event_store.lock().await.insert(
                t_zero,
                Event {
                    t_zero,
                    state: State::PreAccepted,
                    event: request.event.into(),
                    ballot_number: 0,
                    dependencies: HashMap::default(),
                },
            );
            Ok(Response::new(PreAcceptResponse {
                node: self.node.to_string(),
                timestamp: t_zero.as_byte_array().into(),
                dependencies: vec![],
            }))
        }
    }
    async fn commit(
        &self,
        request: Request<CommitRequest>,
    ) -> Result<Response<CommitResponse>, Status> {
        let request = request.into_inner();
        let t_zero = DieselUlid::try_from(request.timestamp_zero.as_slice())
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
        let t = DieselUlid::try_from(request.timestamp_zero.as_slice())
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
        let dependencies = from_dependency(request.dependencies.clone())
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
        self.event_store.lock().await.upsert(
            t,
            &crate::coordinator::Transaction {
                state: State::Commited,
                transaction: request.event.clone().into(),
                t_zero,
                t,
                dependencies: dependencies.clone(),
            },
        );

        self.event_store
            .lock()
            .await
            .wait_for_dependencies(&mut crate::coordinator::Transaction {
                state: State::Commited,
                transaction: request.event.into(),
                t_zero,
                t,
                dependencies,
            })
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        Ok(Response::new(CommitResponse {
            node: self.node.to_string(),
            results: Vec::new(),
        }))
    }
    async fn accept(
        &self,
        request: Request<AcceptRequest>,
    ) -> Result<Response<AcceptResponse>, Status> {
        let request = request.into_inner();
        let t_zero = DieselUlid::try_from(request.timestamp_zero.as_slice())
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
        let t = DieselUlid::try_from(request.timestamp_zero.as_slice())
            .map_err(|e| Status::invalid_argument(e.to_string()))?;

        // We need to get the entry by `t_zero`, because there is no way to know if this replica `t` was accepted
        if let Some(entry) = self.event_store.lock().await.get_tmp_by_t_zero(t_zero) {
            self.event_store.lock().await.upsert(
                entry.0,
                &crate::coordinator::Transaction {
                    state: State::Accepted,
                    transaction: request.event.into(),
                    t_zero,
                    t,
                    dependencies: from_dependency(request.dependencies.clone())
                        .map_err(|e| Status::invalid_argument(e.to_string()))?,
                },
            );
        } else {
            // This is possible because either the replica was not included in any majority or via recovery
            self.event_store.lock().await.insert(
                t,
                Event {
                    t_zero,
                    state: State::Accepted,
                    event: request.event.into(),
                    ballot_number: 0,
                    dependencies: from_dependency(request.dependencies.clone())
                        .map_err(|e| Status::invalid_argument(e.to_string()))?,
                },
            );
        }
        let dependencies = self.event_store.lock().await.get_dependencies(t);
        Ok(Response::new(AcceptResponse {
            node: self.node.to_string(),
            dependencies,
        }))
    }

    async fn apply(
        &self,
        request: Request<ApplyRequest>,
    ) -> Result<Response<ApplyResponse>, Status> {
        let request = request.into_inner();
        
        let transaction: Bytes = request.event.into();
        
        let t_zero = DieselUlid::try_from(request.timestamp_zero.as_slice())
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
        let t = DieselUlid::try_from(request.timestamp.as_slice())
            .map_err(|e| Status::invalid_argument(e.to_string()))?;

        let dependencies = from_dependency(request.dependencies.clone())
            .map_err(|e| Status::invalid_argument(e.to_string()))?;

        self.event_store
            .lock()
            .await
            .wait_for_dependencies(&mut crate::coordinator::Transaction {
                state: State::Commited,
                transaction: transaction.clone(),
                t_zero,
                t,
                dependencies: dependencies.clone(),
            })
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        self.event_store.lock().await.persist(crate::coordinator::Transaction {
                state: State::Commited,
                transaction,
                t_zero,
                t,
                dependencies,
            });

        Ok(Response::new(ApplyResponse {}))
    }

    async fn recover(
        &self,
        request: Request<RecoverRequest>,
    ) -> Result<Response<RecoverResponse>, Status> {
        todo!()
    }
}
