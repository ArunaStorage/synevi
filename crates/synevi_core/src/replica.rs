use crate::node::Node;
use crate::utils::{from_dependency, into_dependency};
use crate::wait_handler::{WaitAction, WaitHandler};
use anyhow::Result;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use synevi_network::consensus_transport::*;
use synevi_network::network::Network;
use synevi_network::replica::Replica;
use synevi_persistence::event::UpsertEvent;
use synevi_persistence::event_store::Store;
use synevi_types::{Ballot, Executor, T, T0};
use tracing::instrument;

pub struct ReplicaConfig<N, E, S>
where
    N: Network + Send + Sync,
    E: Executor + Send + Sync,
    S: Store + Send + Sync,
{
    node: Arc<Node<N, E, S>>,
    wait_handler: Arc<WaitHandler<N, E, S>>,
}

impl<N, E, S> ReplicaConfig<N, E, S> {
    pub fn new(node: Arc<Node<N, E, S>>, wait_handler: Arc<WaitHandler<N, E, S>>) -> Self {
        Self { node, wait_handler }
    }
}

#[async_trait::async_trait]
impl<N, E, S> Replica for ReplicaConfig<N, E, S>
where
    N: Network + Send + Sync,
    E: Executor + Send + Sync,
    S: Store + Send + Sync,
{
    #[instrument(level = "trace", skip(self))]
    async fn pre_accept(
        &self,
        request: PreAcceptRequest,
        _node_serial: u16,
    ) -> Result<PreAcceptResponse> {
        let t0 = T0::try_from(request.timestamp_zero.as_slice())?;

        //println!("PreAccept: {:?} @ {:?}", t0, self.node_info.serial);

        // TODO(perf): Remove the lock here
        // Creates contention on the event store
        if !self
            .node
            .event_store
            .lock()
            .await
            .accept_tx_ballot(&t0, Ballot::default())
        {
            return Ok(PreAcceptResponse {
                nack: true,
                ..Default::default()
            });
        }

        // let waiting_time = self.network.get_waiting_time(node_serial).await;

        // let (sx, rx) = oneshot::channel();

        let (deps, t) = self.node.event_store.lock().await.pre_accept_tx(
            u128::from_be_bytes(request.id.as_slice().try_into()?),
            t0,
            request.event,
        )?;

        // self.reorder_buffer
        //      .send_msg(t0, sx, request.event, waiting_time)
        //      .await?;

        // let (t, deps) = rx.await?;

        Ok(PreAcceptResponse {
            timestamp: t.into(),
            dependencies: deps.to_vec(),
            nack: false,
        })
    }

    #[instrument(level = "trace", skip(self))]
    async fn accept(&self, request: AcceptRequest) -> Result<AcceptResponse> {
        let t_zero = T0::try_from(request.timestamp_zero.as_slice())?;
        //println!("Accept: {:?} @ {:?}", t_zero, self.node_info.serial);

        let t = T::try_from(request.timestamp.as_slice())?;
        let id = u128::from_be_bytes(request.id.as_slice().try_into()?);
        let request_ballot = Ballot::try_from(request.ballot.as_slice())?;

        let dependencies = {
            let mut store = self.node.event_store.lock().await;

            if !store.accept_tx_ballot(&t_zero, request_ballot) {
                return Ok(AcceptResponse {
                    dependencies: Vec::new(),
                    nack: true,
                });
            }
            store.upsert(UpsertEvent {
                id,
                t_zero,
                t,
                state: State::Accepted,
                transaction: Some(request.event),
                dependencies: Some(from_dependency(request.dependencies)?),
                ballot: Some(request_ballot),
                ..Default::default()
            });

            store.get_tx_dependencies(&t, &t_zero)
        };
        Ok(AcceptResponse {
            dependencies: into_dependency(&dependencies),
            nack: false,
        })
    }

    #[instrument(level = "trace", skip(self))]
    async fn commit(&self, request: CommitRequest) -> Result<CommitResponse> {
        let t_zero = T0::try_from(request.timestamp_zero.as_slice())?;
        let id = u128::from_be_bytes(request.id.as_slice().try_into()?);
        //println!("Commit: {:?} @ {:?}", t_zero, self.node_info.serial);
        let t = T::try_from(request.timestamp.as_slice())?;
        let deps = from_dependency(request.dependencies)?;
        let (sx, rx) = tokio::sync::oneshot::channel();
        self.wait_handler
            .send_msg(
                t_zero,
                t,
                deps,
                request.event,
                WaitAction::CommitBefore,
                sx,
                id,
            )
            .await?;
        let _ = rx.await;
        Ok(CommitResponse {})
    }

    #[instrument(level = "trace", skip(self))]
    async fn apply(&self, request: ApplyRequest) -> Result<ApplyResponse> {
        let transaction: Vec<u8> = request.event;

        let id = u128::from_be_bytes(request.id.as_slice().try_into()?);
        let t_zero = T0::try_from(request.timestamp_zero.as_slice())?;
        let t = T::try_from(request.timestamp.as_slice())?;

        let deps = from_dependency(request.dependencies)?;

        let (sx, rx) = tokio::sync::oneshot::channel();

        self.wait_handler
            .send_msg(t_zero, t, deps, transaction, WaitAction::ApplyAfter, sx, id)
            .await?;
        let _ = rx.await;

        Ok(ApplyResponse {})
    }

    #[instrument(level = "trace", skip(self))]
    async fn recover(&self, request: RecoverRequest) -> Result<RecoverResponse> {
        let id = u128::from_be_bytes(request.id.as_slice().try_into()?);
        let t_zero = T0::try_from(request.timestamp_zero.as_slice())?;
        let mut event_store = self.node.event_store.lock().await;

        if let Some(state) = event_store.get_event_state(&t_zero) {
            // If another coordinator has started recovery with a higher ballot
            // Return NACK with the higher ballot number
            let request_ballot = Ballot::try_from(request.ballot.as_slice())?;
            if !event_store.accept_tx_ballot(&t_zero, request_ballot) {
                return Ok(AcceptResponse {
                    dependencies: Vec::new(),
                    nack: true,
                });
            }

            if matches!(state, State::Undefined) {
                event_store.pre_accept_tx(id, t_zero, request.event);
            };
        } else {
            event_store.pre_accept_tx(id, t_zero, request.event);
        }
        let recover_deps = event_store.get_recover_deps(&t_zero)?;

        self.stats.total_recovers.fetch_add(1, Ordering::Relaxed);

        let local_state = event_store
            .get_event_state(&t_zero)
            .ok_or_else(|| anyhow::anyhow!("Event not found"))?;
        Ok(RecoverResponse {
            local_state: local_state.into(),
            wait: into_dependency(&recover_deps.wait),
            superseding: recover_deps.superseding,
            dependencies: into_dependency(&recover_deps.dependencies),
            timestamp: recover_deps.timestamp.into(),
            nack: Ballot::default().into(),
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::node::Stats;
    use crate::reorder_buffer::ReorderBuffer;
    use crate::replica::ReplicaConfig;
    use crate::tests;
    use crate::wait_handler::WaitHandler;
    use bytes::{BufMut, BytesMut};
    use diesel_ulid::DieselUlid;
    use monotime::MonoTime;
    use std::sync::Arc;
    use synevi_network::consensus_transport::{CommitRequest, State};
    use synevi_network::network::NodeInfo;
    use synevi_network::replica::Replica;
    use tokio::sync::Mutex;

    // #[tokio::test]
    // async fn start_recovery() {
    //     let (sdx, _) = tokio::sync::mpsc::channel(100);
    //     let event_store = Arc::new(Mutex::new(EventStore::init(None, 1, sdx).unwrap()));

    //     let conflicting_t0 = MonoTime::new(0, 0);
    //     event_store
    //         .lock()
    //         .await
    //         .upsert(Event {
    //             t_zero: T0(conflicting_t0),
    //             t: T(conflicting_t0),
    //             state: State::PreAccepted,
    //             ..Default::default()
    //         })
    //         .await;

    //     let network = Arc::new(tests::NetworkMock::default());
    //     let node_info = Arc::new(NodeInfo {
    //         id: Default::default(),
    //         serial: 0,
    //     });

    //     let stats = Arc::new(Stats::default());

    //     let wait_handler = WaitHandler::new(
    //         event_store.clone(),
    //         network.clone(),
    //         stats.clone(),
    //         node_info.clone(),
    //     );

    //     let wh_clone = wait_handler.clone();
    //     tokio::spawn(async move {
    //         wh_clone.run().await.unwrap();
    //     });

    //     let replica = ReplicaConfig {
    //         _node_info: node_info.clone(),
    //         event_store: event_store.clone(),
    //         stats: stats.clone(),
    //         _reorder_buffer: ReorderBuffer::new(event_store.clone()),
    //         wait_handler,
    //     };

    //     let t0 = conflicting_t0.next().into_time();

    //     let mut buf = BytesMut::with_capacity(16);
    //     buf.put_u128(conflicting_t0.into());
    //     let request = CommitRequest {
    //         id: DieselUlid::generate().as_byte_array().to_vec(),
    //         event: Vec::new(),
    //         timestamp_zero: t0.into(),
    //         timestamp: t0.into(),
    //         dependencies: buf.freeze().to_vec(),
    //     };
    //     replica.commit(request).await.unwrap();
    //     assert!(matches!(
    //         network.get_requests().await.first().unwrap(),
    //         synevi_network::network::BroadcastRequest::Recover(_)
    //     ));
    // }
}
