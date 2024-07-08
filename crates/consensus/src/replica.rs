use crate::event_store::{Event, EventStore};
use crate::node::Stats;
use crate::reorder_buffer::ReorderBuffer;
use crate::utils::{from_dependency, Ballot, T, T0};
use crate::wait_handler::{WaitAction, WaitHandler};
use anyhow::Result;
use bytes::Bytes;
use consensus_transport::consensus_transport::*;
use consensus_transport::network::{Network, NodeInfo};
use consensus_transport::replica::Replica;
use monotime::MonoTime;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::{oneshot, Mutex};
use tracing::instrument;

#[derive(Debug)]
pub struct ReplicaConfig {
    pub _node_info: Arc<NodeInfo>, // For tracing
    pub network: Arc<dyn Network + Send + Sync>,
    pub event_store: Arc<Mutex<EventStore>>,
    pub stats: Arc<Stats>,
    pub reorder_buffer: Arc<ReorderBuffer>,
    pub wait_handler: Arc<WaitHandler>,
}

#[async_trait::async_trait]
impl Replica for ReplicaConfig {
    #[instrument(level = "trace", skip(self))]
    async fn pre_accept(
        &self,
        request: PreAcceptRequest,
        node_serial: u16,
    ) -> Result<PreAcceptResponse> {
        let t0 = T0(MonoTime::try_from(request.timestamp_zero.as_slice())?);

        //println!("PreAccept: {:?} @ {:?}", t0, self.node_info.serial);

        // TODO(perf): Remove the lock here
        // Creates contention on the event store
        let ballot = self.event_store.lock().await.get_ballot(&t0);
        if ballot != Ballot::default() {
            return Ok(PreAcceptResponse {
                nack: true,
                ..Default::default()
            });
        }

        let waiting_time = self.network.get_waiting_time(node_serial).await;

        let (sx, rx) = oneshot::channel();

        self.reorder_buffer
            .send_msg(t0, sx, request.event.into(), waiting_time)
            .await?;

        let (t, deps) = rx.await?;

        // Remove mich aus buffer
        // wenn ich geweckt wurde && T0 != Ich -> buffer[0].awake

        Ok(PreAcceptResponse {
            timestamp: t.into(),
            dependencies: deps,
            nack: false,
        })
    }

    #[instrument(level = "trace", skip(self))]
    async fn accept(&self, request: AcceptRequest) -> Result<AcceptResponse> {
        let t_zero = T0(MonoTime::try_from(request.timestamp_zero.as_slice())?);
        //println!("Accept: {:?} @ {:?}", t_zero, self.node_info.serial);

        let t = T(MonoTime::try_from(request.timestamp.as_slice())?);
        let request_ballot = Ballot(MonoTime::try_from(request.ballot.as_slice())?);

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
                    state: State::Accepted,
                    event: request.event.into(),
                    dependencies: from_dependency(&request.dependencies)?,
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
        let t_zero = T0(MonoTime::try_from(request.timestamp_zero.as_slice())?);
        //println!("Commit: {:?} @ {:?}", t_zero, self.node_info.serial);
        let t = T(MonoTime::try_from(request.timestamp.as_slice())?);
        let deps = from_dependency(&request.dependencies)?;
        let (sx, rx) = tokio::sync::oneshot::channel();
        self.wait_handler
            .send_msg(
                t_zero,
                t,
                deps,
                request.event.into(),
                WaitAction::CommitBefore,
                sx,
            )
            .await?;
        let _ = rx.await;
        Ok(CommitResponse {})
    }

    #[instrument(level = "trace", skip(self))]
    async fn apply(&self, request: ApplyRequest) -> Result<ApplyResponse> {
        let transaction: Bytes = request.event.into();

        let t_zero = T0(MonoTime::try_from(request.timestamp_zero.as_slice())?);
        let t = T(MonoTime::try_from(request.timestamp.as_slice())?);
        let deps = from_dependency(&request.dependencies)?;

        let (sx, rx) = tokio::sync::oneshot::channel();

        self.wait_handler
            .send_msg(t_zero, t, deps, transaction, WaitAction::ApplyAfter, sx)
            .await?;
        let _ = rx.await;

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

            if matches!(event.state, State::Undefined) {
                event.t = event_store_lock
                    .pre_accept(t_zero, request.event.into())
                    .await?
                    .1;
            };
            let recover_deps = event_store_lock.get_recover_deps(&event.t, &t_zero).await?;

            self.stats.total_recovers.fetch_add(1, Ordering::Relaxed);

            Ok(RecoverResponse {
                local_state: event.state.into(),
                wait: recover_deps.wait,
                superseding: recover_deps.superseding,
                dependencies: recover_deps.dependencies,
                timestamp: event.t.into(),
                nack: Ballot::default().into(),
            })
        } else {
            let (_, t) = event_store_lock
                .pre_accept(t_zero, request.event.into())
                .await?;
            let recover_deps = event_store_lock.get_recover_deps(&t, &t_zero).await?;
            self.stats.total_recovers.fetch_add(1, Ordering::Relaxed);

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
    use crate::reorder_buffer::ReorderBuffer;
    use crate::replica::ReplicaConfig;
    use crate::tests;
    use crate::utils::{Ballot, T, T0};
    use crate::wait_handler::WaitHandler;
    use consensus_transport::consensus_transport::{CommitRequest, Dependency, State};
    use consensus_transport::network::NodeInfo;
    use consensus_transport::replica::Replica;
    use monotime::MonoTime;
    use std::collections::HashMap;
    use std::sync::Arc;
    use tokio::sync::Mutex;

    #[tokio::test]
    async fn start_recovery() {
        let event_store = Arc::new(Mutex::new(EventStore::init(None, 1)));

        let conflicting_t0 = MonoTime::new(0, 0);
        event_store
            .lock()
            .await
            .upsert(Event {
                t_zero: T0(conflicting_t0),
                t: T(conflicting_t0),
                state: State::PreAccepted,
                event: Default::default(),
                dependencies: HashMap::new(), // No deps
                ballot: Ballot::default(),
            })
            .await;

        let network = Arc::new(tests::NetworkMock::default());

        let replica = ReplicaConfig {
            _node_info: Arc::new(NodeInfo {
                id: Default::default(),
                serial: 0,
            }),
            network: network.clone(),
            event_store: event_store.clone(),
            stats: Arc::new(Default::default()),
            reorder_buffer: ReorderBuffer::new(event_store.clone()),
            wait_handler: WaitHandler::new(event_store.clone(), network.clone()),
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
            network.get_requests().await.first().unwrap(),
            consensus_transport::network::BroadcastRequest::Recover(_)
        ));
    }
}
