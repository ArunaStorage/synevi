use crate::event_store::{Event, EventStore};
use crate::utils::from_dependency;
use bytes::Bytes;
use consensus_transport::consensus_transport::consensus_transport_server::ConsensusTransport;
use consensus_transport::consensus_transport::*;
use monotime::MonoTime;
use std::sync::Arc;
use tokio::sync::{watch, Mutex};
use tonic::{Request, Response, Status};
use tracing::instrument;

pub struct Replica {
    pub node: Arc<String>,
    pub event_store: Arc<Mutex<EventStore>>,
}

#[tonic::async_trait]
impl ConsensusTransport for Replica {
    #[instrument(level = "trace", skip(self))]
    async fn pre_accept(
        &self,
        request: Request<PreAcceptRequest>,
    ) -> Result<Response<PreAcceptResponse>, Status> {
        let request = request.into_inner();

        let (deps, t_zero, t) = self
            .event_store
            .lock()
            .await
            .pre_accept(request)
            .await
            .map_err(|e| Status::invalid_argument(e.to_string()))?;

        Ok(Response::new(PreAcceptResponse {
            node: self.node.to_string(),
            timestamp_zero: t_zero.into(),
            timestamp: t.into(),
            dependencies: deps,
        }))
    }

    #[instrument(level = "trace", skip(self))]
    async fn accept(
        &self,
        request: Request<AcceptRequest>,
    ) -> Result<Response<AcceptResponse>, Status> {
        let request = request.into_inner();
        let t_zero = MonoTime::try_from(request.timestamp_zero.as_slice())
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
        let t = MonoTime::try_from(request.timestamp_zero.as_slice())
            .map_err(|e| Status::invalid_argument(e.to_string()))?;

        let (tx, _) = watch::channel(State::Accepted);
        self.event_store
            .lock()
            .await
            .upsert(Event {
                t_zero,
                t,
                state: tx,
                event: request.event.into(),
                dependencies: from_dependency(request.dependencies.clone())
                    .map_err(|e| Status::invalid_argument(e.to_string()))?,
            })
            .await;

        // Should this be saved to event_store before it is finalized in commit?
        // TODO: Check if the deps do not need unification
        let dependencies = self.event_store.lock().await.get_dependencies(&t).await;
        Ok(Response::new(AcceptResponse {
            node: self.node.to_string(),
            dependencies,
        }))
    }

    #[instrument(level = "trace", skip(self))]
    async fn commit(
        &self,
        request: Request<CommitRequest>,
    ) -> Result<Response<CommitResponse>, Status> {
        let request = request.into_inner();
        let t_zero = MonoTime::try_from(request.timestamp_zero.as_slice())
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
        let t = MonoTime::try_from(request.timestamp_zero.as_slice())
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
        let dependencies = from_dependency(request.dependencies.clone())
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
        let (tx, _) = watch::channel(State::Commited);

        let mut handles = self
            .event_store
            .lock()
            .await
            .create_wait_handles(dependencies.clone(), t_zero)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        let initial_len = handles.len();
        let mut counter = 0;
        while let Some(x) = handles.join_next().await {
            if let Err(_) = x.unwrap() {
                let store = &self.event_store.lock().await.events;
                //
                let store = store
                    .iter()
                    .filter_map(|(k, v)| {
                        if dependencies.contains_key(&v.t) {
                            Some((k, v.state.borrow().clone()))
                        } else {
                            None
                        }
                    })
                    .collect::<Vec<_>>();
                println!(
                    "PANIC: T0: {:?}, T: {:?} deps: {:?}, store: {:?} | {:?} / {}",
                    t_zero, t, dependencies, store, initial_len, counter
                );
                panic!()
            }
            counter += 1;
            // TODO: Recovery when timeout
        }
        self.event_store
            .lock()
            .await
            .upsert(Event {
                t_zero,
                t,
                state: tx,
                event: request.event.clone().into(),
                dependencies: dependencies.clone(),
            })
            .await;

        Ok(Response::new(CommitResponse {
            node: self.node.to_string(),
            results: Vec::new(),
        }))
    }

    #[instrument(level = "trace", skip(self))]
    async fn apply(
        &self,
        request: Request<ApplyRequest>,
    ) -> Result<Response<ApplyResponse>, Status> {
        let request = request.into_inner();

        let transaction: Bytes = request.event.into();

        let t_zero = MonoTime::try_from(request.timestamp_zero.as_slice())
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
        let t = MonoTime::try_from(request.timestamp.as_slice())
            .map_err(|e| Status::invalid_argument(e.to_string()))?;

        let dependencies = from_dependency(request.dependencies.clone())
            .map_err(|e| Status::invalid_argument(e.to_string()))?;

        let mut handles = self
            .event_store
            .lock()
            .await
            .create_wait_handles(dependencies.clone(), t_zero)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        while let Some(x) = handles.join_next().await {
            x.unwrap().unwrap()
            // TODO: Recovery when timeout
        }
        let (tx, _) = watch::channel(State::Applied);
        self.event_store
            .lock()
            .await
            .upsert(Event {
                t_zero,
                t,
                state: tx,
                event: transaction,
                dependencies,
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
