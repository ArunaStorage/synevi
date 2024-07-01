use crate::event_store::{Event, EventStore};
use crate::node::Stats;
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

// todo!()
pub struct ReplicaConfig {
    pub node_info: Arc<NodeInfo>,
    pub network: Arc<Mutex<dyn Network + Send + Sync>>,
    pub event_store: Arc<Mutex<EventStore>>,
    pub stats: Arc<Stats>,
    //pub reorder_buffer: Arc<Mutex<BTreeMap<T0, (Bytes, watch::Sender<Option<T0>>)>>>,
}

#[async_trait::async_trait]
impl Replica for ReplicaConfig {
    #[instrument(level = "trace", skip(self))]
    async fn pre_accept(&self, request: PreAcceptRequest) -> Result<PreAcceptResponse> {
        //self.reorder_buffer.lock().await.pop_first()

        // Put request into_reorder buffer
        // Wait (max latency of majority + skew) - (latency from node)/2
        // Calculate deps -> all from event_store + deps that would result from the reorder buffer

        // t0-2, t0-4, t-06, t-01, t-03, t-05, t-07
        //

        // timeout || wait_for(|inner| inner.is_some())
        // ? timeout oder aufgeweckt..
        // Wenn timeout -> Wecke buffer[0] und sag dem ich wars (T0)
        // wait_for(|inner| inner.is_some())

        let (deps, t) = self
            .event_store
            .lock()
            .await
            .pre_accept(request, self.node_info.serial)
            .await?;

        // Remove mich aus buffer
        // wenn ich geweckt wurde && T0 != Ich -> buffer[0].awake

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
            self.stats.clone(),
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
            self.stats.clone(),
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
    async fn recover(&self, request: RecoverRequest) -> Result<RecoverResponse> {
        let t_zero = T0(MonoTime::try_from(request.timestamp_zero.as_slice())?);
        let mut event_store_lock = self.event_store.lock().await;
        if let Some(mut event) = event_store_lock.get_event(t_zero).await {
            // If another coordinator has started recovery with a higher ballot
            // Return NACK with the higher ballot number
            if event.ballot > request.ballot {
                return Ok(RecoverResponse {
                    nack: event.ballot,
                    ..Default::default()
                });
            }

            if matches!(event.state.borrow().0, State::Undefined) {
                event.t = event_store_lock
                        .pre_accept(
                            PreAcceptRequest {
                                timestamp_zero: request.timestamp_zero.clone(),
                                event: request.event.clone(),
                            },
                            self.node_info.serial,
                        )
                        .await?.1;
            };
            let recover_deps = event_store_lock.get_recover_deps(&event.t, &t_zero).await?;

            Ok(RecoverResponse {
                local_state: event.state.borrow().0.clone().into(),
                wait: recover_deps.wait,
                superseding: recover_deps.superseding,
                dependencies: recover_deps.dependencies,
                timestamp: event.t.into(),
                nack: 0,
            })
        } else {
            let (_, t) = event_store_lock
                .pre_accept(
                    PreAcceptRequest {
                        timestamp_zero: request.timestamp_zero.clone(),
                        event: request.event.clone(),
                    },
                    self.node_info.serial,
                )
                .await?;
            let recover_deps = event_store_lock.get_recover_deps(&t, &t_zero).await?;
            Ok(RecoverResponse {
                local_state: State::PreAccepted.into(),
                wait: recover_deps.wait,
                superseding: recover_deps.superseding,
                dependencies: recover_deps.dependencies,
                timestamp: t.into(),
                nack: 0,
            })
        }
    }
}
