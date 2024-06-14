use crate::event_store::{Event, EventStore};
use crate::utils::from_dependency;
use bytes::Bytes;
use consensus_transport::consensus_transport::consensus_transport_server::ConsensusTransport;
use consensus_transport::consensus_transport::*;
use diesel_ulid::DieselUlid;
use monotime::MonoTime;
use rusty_ulid::Ulid;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Notify;
use tonic::{Request, Response, Status};
use tracing::instrument;

pub struct Replica {
    pub node: Arc<String>,
    pub event_store: Arc<EventStore>,
}

#[tonic::async_trait]
impl ConsensusTransport for Replica {

    #[instrument(level = "trace", skip(self))]
    async fn pre_accept(
        &self,
        request: Request<PreAcceptRequest>,
    ) -> Result<Response<PreAcceptResponse>, Status> {

        let request = request.into_inner();
        let t_zero = MonoTime::try_from(request.timestamp_zero.as_slice())
            .map_err(|e| Status::invalid_argument(e.to_string()))?;

        // check for entries in temp
        if let Some(entry) = self.event_store.last().await {
            let latest = entry.t;

            // If there is a newer timestamp, propose new
            // Compare the full ULID, not just the timestamp
            let t = if latest > t_zero {
                let mut t = MonoTime::generate();
                if t < t_zero {
                    t = DieselUlid::from(Ulid::from_timestamp_with_rng(t.timestamp() + 1, &mut rand::thread_rng()));
                }
                t
            } else {
                t_zero
            };

            // Safeguard to ensure that t_zero <= t
            assert!(t_zero <= t, "{:?} <= {:?}", t_zero, t);

            // Get all dependencies in temp
            let dependencies = self.event_store.get_dependencies(&t).await;

            // Insert event into temp
            self.event_store
                .upsert(Event {
                    t_zero,
                    t,
                    state: State::PreAccepted,
                    event: request.event.into(),
                    ballot_number: 0,
                    dependencies: from_dependency(dependencies.clone())
                        .map_err(|e| Status::internal(e.to_string()))?,
                    commit_notify: Arc::new(Notify::new()),
                    apply_notify: Arc::new(Notify::new()),
                })
                .await;
            Ok(Response::new(PreAcceptResponse {
                node: self.node.to_string(),
                timestamp: t.as_byte_array().into(),
                dependencies,
            }))
        } else {
            // dbg!("[PRE_ACCEPT]: No entries, insert into empty event_store");
            // If no entries are found in temp just insert and PreAccept msg
            // dbg!("[PRE_ACCEPT]:", &self.event_store);
            self.event_store
                .upsert(Event {
                    t_zero,
                    t: t_zero,
                    state: State::PreAccepted,
                    event: request.event.into(),
                    ballot_number: 0,
                    dependencies: HashMap::default(),
                    commit_notify: Arc::new(Notify::new()),
                    apply_notify: Arc::new(Notify::new()),
                })
                .await;

            // dbg!("[PRE_ACCEPT]: Sending response");
            Ok(Response::new(PreAcceptResponse {
                node: self.node.to_string(),
                timestamp: t_zero.as_byte_array().into(),
                dependencies: vec![],
            }))
        }
    }
    #[instrument(level = "trace", skip(self))]
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
        self.event_store
            .upsert(Event {
                t_zero,
                t,
                state: State::Commited,
                event: request.event.clone().into(),
                dependencies: dependencies.clone(),
                ballot_number: 0,
                commit_notify: Arc::new(Notify::new()),
                apply_notify: Arc::new(Notify::new()),
            })
            .await;

        self.event_store
            .wait_for_dependencies(dependencies, t_zero)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        Ok(Response::new(CommitResponse {
            node: self.node.to_string(),
            results: Vec::new(),
        }))
    }
    #[instrument(level = "trace", skip(self))]
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
        if let Some(_entry) = self.event_store.get(&t_zero).await {
            self.event_store
                .upsert(Event {
                    t_zero,
                    t,
                    state: State::Accepted,
                    event: request.event.into(),
                    dependencies: from_dependency(request.dependencies.clone())
                        .map_err(|e| Status::invalid_argument(e.to_string()))?,
                    ballot_number: 0,
                    commit_notify: Arc::new(Notify::new()),
                    apply_notify: Arc::new(Notify::new()),
                })
                .await;
        } else {
            // This is possible because either the replica was not included in any majority or via recovery
            self.event_store
                .upsert(Event {
                    t_zero,
                    t,
                    state: State::Accepted,
                    event: request.event.into(),
                    ballot_number: 0,
                    dependencies: from_dependency(request.dependencies.clone())
                        .map_err(|e| Status::invalid_argument(e.to_string()))?,
                    commit_notify: Arc::new(Notify::new()),
                    apply_notify: Arc::new(Notify::new()),
                })
                .await;
        }
        // Should this be saved to event_store before it is finalized in commit?
        let dependencies = self.event_store.get_dependencies(&t).await;
        Ok(Response::new(AcceptResponse {
            node: self.node.to_string(),
            dependencies,
        }))
    }

    #[instrument(level = "trace", skip(self))]
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
            .wait_for_dependencies(dependencies.clone(), t_zero)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        self.event_store
            .upsert(Event {
                t_zero,
                t,
                state: State::Applied,
                event: transaction,
                dependencies,
                ballot_number: 0,
                commit_notify: Arc::new(Notify::new()),
                apply_notify: Arc::new(Notify::new()),
            })
            .await;

        Ok(Response::new(ApplyResponse {}))
    }

    #[instrument(level = "trace", skip(self))]
    async fn recover(
        &self,
        _request: Request<RecoverRequest>,
    ) -> Result<Response<RecoverResponse>, Status> {
        todo!()
    }
}
