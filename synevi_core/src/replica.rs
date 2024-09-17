use crate::node::Node;
use crate::utils::{from_dependency, into_dependency};
use crate::wait_handler::WaitAction;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use synevi_network::configure_transport::{
    GetEventResponse, JoinElectorateResponse, ReadyElectorateResponse,
};
use synevi_network::consensus_transport::{
    AcceptRequest, AcceptResponse, ApplyRequest, ApplyResponse, CommitRequest, CommitResponse,
    PreAcceptRequest, PreAcceptResponse, RecoverRequest, RecoverResponse,
};
use synevi_network::network::Network;
use synevi_network::reconfiguration::Reconfiguration;
use synevi_network::replica::Replica;
use synevi_types::traits::Store;
use synevi_types::types::{Member, TransactionPayload, UpsertEvent};
use synevi_types::{Ballot, Executor, State, T, T0};
use synevi_types::{SyneviError, Transaction};
use tokio::sync::mpsc::Receiver;
use tokio_stream::wrappers::ReceiverStream;
use tracing::{instrument, trace};
use ulid::Ulid;

pub struct ReplicaConfig<N, E, S>
where
    N: Network,
    E: Executor,
    S: Store,
{
    node: Arc<Node<N, E, S>>,
}

impl<N, E, S> ReplicaConfig<N, E, S>
where
    N: Network,
    E: Executor,
    S: Store,
{
    pub fn new(node: Arc<Node<N, E, S>>) -> Self {
        Self { node }
    }
}

#[async_trait::async_trait]
impl<N, E, S> Replica for ReplicaConfig<N, E, S>
where
    N: Network + Send + Sync,
    E: Executor + Send + Sync,
    S: Store + Send + Sync,
{
    #[instrument(level = "trace", skip(self, request))]
    async fn pre_accept(
        &self,
        request: PreAcceptRequest,
        _node_serial: u16,
    ) -> Result<PreAcceptResponse, SyneviError> {
        let request_id = u128::from_be_bytes(request.id.as_slice().try_into()?);
        trace!(?request_id, "Replica: PreAccept");

        let t0 = T0::try_from(request.timestamp_zero.as_slice())?;

        // TODO(perf): Remove the lock here
        // Creates contention on the event store
        if let Some(ballot) = self
            .node
            .event_store
            .accept_tx_ballot(&t0, Ballot::default())
            .await
        {
            if ballot != Ballot::default() {
                return Ok(PreAcceptResponse {
                    nack: true,
                    ..Default::default()
                });
            }
        }

        // let waiting_time = self.network.get_waiting_time(node_serial).await;

        // let (sx, rx) = oneshot::channel();

        let (t, deps) = self
            .node
            .event_store
            .pre_accept_tx(request_id, t0, request.event)
            .await?;

        // self.reorder_buffer
        //      .send_msg(t0, sx, request.event, waiting_time)
        //      .await?;

        // let (t, deps) = rx.await?;

        Ok(PreAcceptResponse {
            timestamp: t.into(),
            dependencies: into_dependency(&deps),
            nack: false,
        })
    }

    #[instrument(level = "trace", skip(self, request))]
    async fn accept(&self, request: AcceptRequest) -> Result<AcceptResponse, SyneviError> {
        let request_id = u128::from_be_bytes(request.id.as_slice().try_into()?);
        trace!(?request_id, "Replica: Accept");

        let t_zero = T0::try_from(request.timestamp_zero.as_slice())?;
        //println!("Accept: {:?} @ {:?}", t_zero, self.node_info.serial);

        let t = T::try_from(request.timestamp.as_slice())?;
        let request_ballot = Ballot::try_from(request.ballot.as_slice())?;

        // TODO/WARNING: This was initially in one mutex lock, but does not look like it needs this
        let dependencies = {
            if let Some(ballot) = self
                .node
                .event_store
                .accept_tx_ballot(&t_zero, request_ballot)
                .await
            {
                if ballot != request_ballot {
                    return Ok(AcceptResponse {
                        dependencies: Vec::new(),
                        nack: true,
                    });
                }
            }

            self.node
                .event_store
                .upsert_tx(UpsertEvent {
                    id: request_id,
                    t_zero,
                    t,
                    state: State::Accepted,
                    transaction: Some(request.event),
                    dependencies: Some(from_dependency(request.dependencies)?),
                    ballot: Some(request_ballot),
                    execution_hash: None,
                })
                .await?;

            self.node.event_store.get_tx_dependencies(&t, &t_zero).await
        };
        Ok(AcceptResponse {
            dependencies: into_dependency(&dependencies),
            nack: false,
        })
    }

    #[instrument(level = "trace", skip(self, request))]
    async fn commit(&self, request: CommitRequest) -> Result<CommitResponse, SyneviError> {
        let request_id = u128::from_be_bytes(request.id.as_slice().try_into()?);
        trace!(?request_id, "Replica: Commit");

        let t_zero = T0::try_from(request.timestamp_zero.as_slice())?;
        let t = T::try_from(request.timestamp.as_slice())?;
        let deps = from_dependency(request.dependencies)?;
        let (sx, rx) = tokio::sync::oneshot::channel();
        self.node
            .get_wait_handler()
            .await?
            .send_msg(
                t_zero,
                t,
                deps,
                request.event,
                WaitAction::CommitBefore,
                sx,
                request_id,
            )
            .await?;
        let _ = rx.await;
        Ok(CommitResponse {})
    }

    #[instrument(level = "trace", skip(self, request))]
    async fn apply(&self, request: ApplyRequest) -> Result<ApplyResponse, SyneviError> {
        let request_id = u128::from_be_bytes(request.id.as_slice().try_into()?);
        trace!(?request_id, "Replica: Apply");

        let transaction = <E as Executor>::Tx::from_bytes(request.event)?;

        let t_zero = T0::try_from(request.timestamp_zero.as_slice())?;
        let t = T::try_from(request.timestamp.as_slice())?;

        let deps = from_dependency(request.dependencies)?;

        let (sx, rx) = tokio::sync::oneshot::channel();

        self.node
            .get_wait_handler()
            .await?
            .send_msg(
                t_zero,
                t,
                deps,
                transaction.as_bytes(),
                WaitAction::ApplyAfter,
                sx,
                request_id,
            )
            .await?;
        let _ = rx.await;

        let _ = self.node.executor.execute(transaction).await;

        Ok(ApplyResponse {})
    }

    #[instrument(level = "trace", skip(self))]
    async fn recover(&self, request: RecoverRequest) -> Result<RecoverResponse, SyneviError> {
        let request_id = u128::from_be_bytes(request.id.as_slice().try_into()?);
        trace!(?request_id, "Replica: Recover");
        let t_zero = T0::try_from(request.timestamp_zero.as_slice())?;

        // TODO/WARNING: This was initially in one Mutex lock
        //let mut event_store = self.node.event_store.lock().await;

        if let Some(state) = self.node.event_store.get_event_state(&t_zero).await {
            // If another coordinator has started recovery with a higher ballot
            // Return NACK with the higher ballot number
            let request_ballot = Ballot::try_from(request.ballot.as_slice())?;
            if let Some(ballot) = self
                .node
                .event_store
                .accept_tx_ballot(&t_zero, request_ballot)
                .await
            {
                if request_ballot != ballot {
                    return Ok(RecoverResponse {
                        nack: ballot.into(),
                        ..Default::default()
                    });
                }
            }

            if matches!(state, State::Undefined) {
                self.node
                    .event_store
                    .pre_accept_tx(request_id, t_zero, request.event)
                    .await?;
            };
        } else {
            self.node
                .event_store
                .pre_accept_tx(request_id, t_zero, request.event)
                .await?;
        }
        let recover_deps = self.node.event_store.get_recover_deps(&t_zero).await?;

        self.node
            .stats
            .total_recovers
            .fetch_add(1, Ordering::Relaxed);

        let local_state = self
            .node
            .event_store
            .get_event_state(&t_zero)
            .await
            .ok_or_else(|| SyneviError::EventNotFound(t_zero.get_inner()))?;
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

#[async_trait::async_trait]
impl<N, E, S> Reconfiguration for ReplicaConfig<N, E, S>
where
    N: Network,
    E: Executor,
    S: Store,
{
    async fn join_electorate(&self) -> Result<JoinElectorateResponse, SyneviError> {
        // TODO:
        // - Start reconfiguration transaction with NewMemberConfig
        let node = self.node.clone();
        let res = node
            .transaction(
                Ulid::new().0,
                TransactionPayload::Internal(Member {
                    id: todo!(),
                    serial: todo!(),
                    host: todo!(),
                }),
            )
            .await?;
        // TODO:
        // - respond with estimated member majority
        Ok(JoinElectorateResponse {
            last_applied: todo!(),
            last_applied_hash: todo!(),
        })
    }

    async fn get_events(&self) -> Receiver<Result<GetEventResponse, SyneviError>> {
        let (sdx, rcv) = tokio::sync::mpsc::channel(100);
        let mut store_rcv = self.node.event_store.get_events_until(T::default()).await;
        while let Some(Ok(event)) = store_rcv.recv().await {
            let response = {
                if let Some(hashes) = event.hashes {
                    Ok(GetEventResponse {
                        id: event.id.to_be_bytes().to_vec(),
                        t_zero: event.t_zero.into(),
                        t: event.t.into(),
                        state: event.state as u32,
                        transaction: event.transaction,
                        dependencies: into_dependency(&event.dependencies),
                        ballot: event.ballot.into(),
                        last_updated: Vec::new(), // TODO
                        previous_hash: hashes.previous_hash.to_vec(),
                        transaction_hash: hashes.transaction_hash.to_vec(),
                        execution_hash: hashes.execution_hash.to_vec(),
                    })
                } else {
                    Err(SyneviError::MissingExecutionHash)
                }
            };
            sdx.send(response).await.unwrap();
        }
        // Stream all events to member
        rcv
    }

    async fn ready_electorate(&self) -> Result<ReadyElectorateResponse, SyneviError> {
        // Start ready electorate transaction with NewMemberUlid
        todo!()
    }
}

#[cfg(test)]
mod tests {}
