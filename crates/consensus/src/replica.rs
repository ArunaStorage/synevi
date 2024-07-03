use crate::event_store::{Event, EventStore};
use crate::node::Stats;
use crate::utils::{await_dependencies, from_dependency, Ballot, T, T0};
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
        let request_ballot = Ballot(MonoTime::try_from(request.ballot.as_slice())?);

        let (tx, _) = watch::channel((State::Accepted, t));

        let dependencies = {
            let mut store = self.event_store.lock().await;
            if request_ballot < store.get_ballot(&t_zero) {
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
                    ballot: request_ballot,
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
        let network_interface = self.network.lock().await.get_interface();

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
                ballot: Ballot::default(), // Will keep the highest ballot
            })
            .await;
        await_dependencies(
            self.node_info.clone(),
            self.event_store.clone(),
            &dependencies,
            network_interface,
            t,
            self.stats.clone(),
            false,
        )
        .await?;

        Ok(CommitResponse {})
    }

    #[instrument(level = "trace", skip(self))]
    async fn apply(&self, request: ApplyRequest) -> Result<ApplyResponse> {
        let network_interface = self.network.lock().await.get_interface();
        let transaction: Bytes = request.event.into();

        let t_zero = T0(MonoTime::try_from(request.timestamp_zero.as_slice())?);
        let t = T(MonoTime::try_from(request.timestamp.as_slice())?);

        let dependencies = from_dependency(request.dependencies.clone())?;

        await_dependencies(
            self.node_info.clone(),
            self.event_store.clone(),
            &dependencies,
            network_interface,
            t,
            self.stats.clone(),
            false,
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
                ballot: Ballot::default(),
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
            let request_ballot = Ballot(MonoTime::try_from(request.ballot.as_slice())?);
            if event.ballot > request_ballot {
                return Ok(RecoverResponse {
                    nack: event.ballot.into(),
                    ..Default::default()
                });
            }

            event_store_lock.update_ballot(&t_zero, request_ballot);

            if matches!(event.state.borrow().0, State::Undefined) {
                event.t = event_store_lock
                    .pre_accept(
                        PreAcceptRequest {
                            timestamp_zero: request.timestamp_zero.clone(),
                            event: request.event.clone(),
                        },
                        self.node_info.serial,
                    )
                    .await?
                    .1;
            };
            let recover_deps = event_store_lock.get_recover_deps(&event.t, &t_zero).await?;

            Ok(RecoverResponse {
                local_state: event.state.borrow().0.into(),
                wait: recover_deps.wait,
                superseding: recover_deps.superseding,
                dependencies: recover_deps.dependencies,
                timestamp: event.t.into(),
                nack: Ballot::default().into(),
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
                nack: Ballot::default().into(),
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::event_store::{Event, EventStore};
    use crate::replica::ReplicaConfig;
    use crate::tests;
    use crate::utils::{Ballot, T, T0};
    use consensus_transport::consensus_transport::{CommitRequest, Dependency, State};
    use consensus_transport::network::NodeInfo;
    use consensus_transport::replica::Replica;
    use monotime::MonoTime;
    use std::collections::BTreeMap;
    use std::sync::Arc;
    use tokio::sync::watch::Sender;
    use tokio::sync::Mutex;

    #[tokio::test]
    async fn start_recovery() {
        let event_store = Arc::new(Mutex::new(EventStore::init(None)));

        let conflicting_t0 = MonoTime::new(0, 0);
        event_store
            .lock()
            .await
            .upsert(Event {
                t_zero: T0(conflicting_t0),
                t: T(conflicting_t0),
                state: Sender::new((State::PreAccepted, T(conflicting_t0))),
                event: Default::default(),
                dependencies: BTreeMap::new(), // No deps
                ballot: Ballot::default(),
            })
            .await;

        let network = Arc::new(Mutex::new(tests::NetworkMock::default()));

        let replica = ReplicaConfig {
            node_info: Arc::new(NodeInfo {
                id: Default::default(),
                serial: 0,
            }),
            network: network.clone(),
            event_store: event_store.clone(),
            stats: Arc::new(Default::default()),
        };

        let t0 = conflicting_t0.next().into_time();
        let request = CommitRequest {
            event: vec![],
            timestamp_zero: t0.into(),
            timestamp: t0.into(),
            dependencies: vec![Dependency {
                timestamp: conflicting_t0.into(),
                timestamp_zero: conflicting_t0.into(),
            }],
        };

        replica.commit(request).await.unwrap();
        assert!(matches!(
            network.lock().await.get_requests().await.first().unwrap(),
            consensus_transport::network::BroadcastRequest::Recover(_)
        ));
    }
}
