use crate::event_store::{Event, EventStore};
use crate::utils::{await_dependencies, from_dependency, T, T0};
use anyhow::Result;
use bytes::Bytes;
use consensus_transport::consensus_transport::*;
use consensus_transport::network::{Network, NodeInfo};
use consensus_transport::replica::Replica;
use monotime::MonoTime;
use std::sync::Arc;
use tokio::sync::{watch, Mutex};
use tracing::instrument;

#[derive(Debug)]
pub struct ReplicaConfig {
    pub node_info: Arc<NodeInfo>,
    pub network: Arc<Mutex<dyn Network + Send + Sync>>,
    pub event_store: Arc<Mutex<EventStore>>,
}

#[async_trait::async_trait]
impl Replica for ReplicaConfig {
    #[instrument(level = "trace", skip(self))]
    async fn pre_accept(&self, request: PreAcceptRequest) -> Result<PreAcceptResponse> {
        let (deps, t) = self
            .event_store
            .lock()
            .await
            .pre_accept(request, self.node_info.serial)
            .await?;

        Ok(PreAcceptResponse {
            timestamp: t.into(),
            dependencies: deps,
        })
    }

    #[instrument(level = "trace", skip(self))]
    async fn accept(&self, request: AcceptRequest) -> Result<AcceptResponse> {
        let t_zero = T0(MonoTime::try_from(request.timestamp_zero.as_slice())?);
        let t = T(MonoTime::try_from(request.timestamp.as_slice())?);

        let (tx, _) = watch::channel((State::Accepted, t));

        let dependencies = {
            let mut store = self.event_store.lock().await;
            if request.ballot < store.get_ballot(&t_zero) {
                return Ok(AcceptResponse {
                    dependencies: vec![],
                    nack: true,
                });
            }
            store
                .upsert(Event {
                    t_zero,
                    t,
                    state: tx,
                    event: request.event.into(),
                    dependencies: from_dependency(request.dependencies.clone())?,
                    ballot: request.ballot,
                })
                .await;

            store.get_dependencies(&t, &t_zero).await
        };
        Ok(AcceptResponse {
            dependencies,
            nack: false,
        })
    }

    #[instrument(level = "trace", skip(self))]
    async fn commit(&self, request: CommitRequest) -> Result<CommitResponse> {
        let t_zero = T0(MonoTime::try_from(request.timestamp_zero.as_slice())?);
        let t = T(MonoTime::try_from(request.timestamp.as_slice())?);
        let dependencies = from_dependency(request.dependencies.clone())?;
        let (tx, _) = watch::channel((State::Commited, t));

        self.event_store
            .lock()
            .await
            .upsert(Event {
                t_zero,
                t,
                state: tx,
                event: request.event.clone().into(),
                dependencies: dependencies.clone(),
                ballot: 0, // Will keep the highest ballot
            })
            .await;
        await_dependencies(
            self.node_info.clone(),
            self.event_store.clone(),
            &dependencies,
            self.network.lock().await.get_interface(),
            t,
        )
        .await?;

        Ok(CommitResponse {})
    }

    #[instrument(level = "trace", skip(self))]
    async fn apply(&self, request: ApplyRequest) -> Result<ApplyResponse> {
        let transaction: Bytes = request.event.into();

        let t_zero = T0(MonoTime::try_from(request.timestamp_zero.as_slice())?);
        let t = T(MonoTime::try_from(request.timestamp.as_slice())?);

        let dependencies = from_dependency(request.dependencies.clone())?;

        await_dependencies(
            self.node_info.clone(),
            self.event_store.clone(),
            &dependencies,
            self.network.lock().await.get_interface(),
            t,
        )
        .await?;

        let (tx, _) = watch::channel((State::Applied, t));
        self.event_store
            .lock()
            .await
            .upsert(Event {
                t_zero,
                t,
                state: tx,
                event: transaction,
                dependencies,
                ballot: 0,
            })
            .await;

        Ok(ApplyResponse {})
    }

    #[instrument(level = "trace", skip(self))]
    async fn recover(&self, _request: RecoverRequest) -> Result<RecoverResponse> {
        todo!()
    }
}
