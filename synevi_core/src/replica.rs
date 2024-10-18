use crate::coordinator::Coordinator;
use crate::node::Node;
use crate::utils::{from_dependency, into_dependency};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use synevi_network::configure_transport::{
    Config, GetEventRequest, GetEventResponse, JoinElectorateRequest, JoinElectorateResponse,
    ReadyElectorateRequest, ReadyElectorateResponse, ReportElectorateRequest,
    ReportElectorateResponse,
};
use synevi_network::consensus_transport::{
    AcceptRequest, AcceptResponse, ApplyRequest, ApplyResponse, CommitRequest, CommitResponse,
    PreAcceptRequest, PreAcceptResponse, RecoverRequest, RecoverResponse, TryRecoveryRequest,
    TryRecoveryResponse,
};
use synevi_network::network::Network;
use synevi_network::reconfiguration::Reconfiguration;
use synevi_network::replica::Replica;
use synevi_types::traits::Store;
use synevi_types::types::{
    Hashes, InternalExecution, TransactionPayload, UpsertEvent,
};
use synevi_types::{Ballot, Executor, State, T, T0};
use synevi_types::SyneviError;
use tokio::sync::mpsc::Receiver;
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
        let t0 = T0::try_from(request.timestamp_zero.as_slice())?;

        if !self.node.is_ready() {
            return Ok(PreAcceptResponse::default());
        }

        let request_id = u128::from_be_bytes(request.id.as_slice().try_into()?);

        trace!(?request_id, "Replica: PreAccept");

        // TODO(performance): Remove the lock here
        // Creates contention on the event store
        if let Some(ballot) = self
            .node
            .event_store
            .accept_tx_ballot(&t0, Ballot::default())
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
            .pre_accept_tx(request_id, t0, request.event)?;

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
        if !self.node.is_ready() {
            return Ok(AcceptResponse::default());
        }
        let t_zero = T0::try_from(request.timestamp_zero.as_slice())?;
        let request_id = u128::from_be_bytes(request.id.as_slice().try_into()?);
        let t = T::try_from(request.timestamp.as_slice())?;
        let request_ballot = Ballot::try_from(request.ballot.as_slice())?;

        trace!(?request_id, "Replica: Accept");

        let dependencies = {
            if let Some(ballot) = self
                .node
                .event_store
                .accept_tx_ballot(&t_zero, request_ballot)
            {
                if ballot != request_ballot {
                    return Ok(AcceptResponse {
                        dependencies: Vec::new(),
                        nack: true,
                    });
                }
            }

            self.node.event_store.upsert_tx(UpsertEvent {
                id: request_id,
                t_zero,
                t,
                state: State::Accepted,
                transaction: Some(request.event),
                dependencies: Some(from_dependency(request.dependencies)?),
                ballot: Some(request_ballot),
                hashes: None,
            })?;

            self.node.event_store.get_tx_dependencies(&t, &t_zero)
        };

        Ok(AcceptResponse {
            dependencies: into_dependency(&dependencies),
            nack: false,
        })
    }

    #[instrument(level = "trace", skip(self, request))]
    async fn commit(&self, request: CommitRequest) -> Result<CommitResponse, SyneviError> {
        let t_zero = T0::try_from(request.timestamp_zero.as_slice())?;
        let t = T::try_from(request.timestamp.as_slice())?;
        let request_id = u128::from_be_bytes(request.id.as_slice().try_into()?);

        trace!(?request_id, "Replica: Commit");

        let deps = from_dependency(request.dependencies)?;

        self.node
            .commit(UpsertEvent {
                id: request_id,
                t_zero,
                t,
                state: State::Committed,
                transaction: Some(request.event),
                dependencies: Some(deps),
                ballot: None,
                hashes: None,
            })
            .await?;
        Ok(CommitResponse {})
    }

    #[instrument(level = "trace", skip(self, request))]
    async fn apply(&self, request: ApplyRequest) -> Result<ApplyResponse, SyneviError> {
        let t_zero = T0::try_from(request.timestamp_zero.as_slice())?;

        println!(
            "Received apply request for event: {:?} @ {:?}",
            t_zero,
            self.node.get_serial()
        );
        let t = T::try_from(request.timestamp.as_slice())?;
        let request_id = u128::from_be_bytes(request.id.as_slice().try_into()?);
        trace!(?request_id, "Replica: Apply");

        let deps = from_dependency(request.dependencies.clone())?;

        let _ = self.node
            .apply(
                UpsertEvent {
                    id: request_id,
                    t_zero,
                    t,
                    state: State::Applied,
                    transaction: Some(request.event),
                    dependencies: Some(deps),
                    ballot: None,
                    hashes: None,
                },
                Some(Hashes {
                    transaction_hash: request
                        .transaction_hash
                        .try_into()
                        .map_err(|_e| SyneviError::MissingTransactionHash)?,
                    execution_hash: request
                        .execution_hash
                        .try_into()
                        .map_err(|_e| SyneviError::MissingExecutionHash)?,
                    ..Default::default()
                }),
            )
            .await?;

        Ok(ApplyResponse {})
        // TODO: Refactor in execute function
        //
        // let transaction: TransactionPayload<<E as Executor>::Tx> =
        //     TransactionPayload::from_bytes(request.event.clone())?;
        //let result = match transaction {
        //    TransactionPayload::None => {
        //        return Err(SyneviError::TransactionNotFound);
        //    }
        //    TransactionPayload::External(tx) => self
        //        .node
        //        .executor
        //        .execute(tx)
        //        .await
        //        .map(|e| ExecutorResult::<<E as Executor>::Tx>::External(e)),
        //    TransactionPayload::Internal(request) => {
        //        // TODO: Build special execution
        //        let result = match &request {
        //            InternalExecution::JoinElectorate {
        //                id,
        //                serial,
        //                new_node_host,
        //            } => {
        //                if id != &self.node.get_ulid() {
        //                    let res = self
        //                        .node
        //                        .add_member(*id, *serial, new_node_host.clone(), false)
        //                        .await;
        //                    self.node
        //                        .network
        //                        .report_config(new_node_host.clone())
        //                        .await?;
        //                    res
        //                } else {
        //                    Ok(())
        //                }
        //            }
        //            InternalExecution::ReadyElectorate { id, serial } => {
        //                if id != &self.node.get_ulid() {
        //                    self.node.ready_member(*id, *serial).await
        //                } else {
        //                    Ok(())
        //                }
        //            }
        //        };
        //        match result {
        //            Ok(_) => Ok(ExecutorResult::Internal(Ok(request.clone()))),
        //            Err(err) => Ok(ExecutorResult::Internal(Err(err))),
        //        }
        //    }
        //};

        //let mut hasher = Sha3_256::new();
        //postcard::to_io(&result, &mut hasher)?;
        //let hash = hasher.finalize();
        //let hashes = self
        //    .node
        //    .event_store
        //    .get_and_update_hash(t_zero, hash.into())?;
        //if request.transaction_hash != hashes.transaction_hash {
        //    println!(
        //        "{} | Mismatched transaction hash in Tx {:?}",
        //        self.node.get_serial(),
        //        t_zero
        //    );
        //    Err(SyneviError::MismatchedHashes)
        //} else if request.execution_hash != hashes.execution_hash {
        //    println!(
        //        "{} | Mismatched execution hash in Tx {:?} | expected: {:?} != {:?}",
        //        self.node.get_serial(),
        //        t_zero,
        //        request.execution_hash,
        //        hashes.execution_hash
        //    );

        //    Err(SyneviError::MismatchedHashes)
        //} else {
        //Ok(ApplyResponse {})
        //}
    }

    #[instrument(level = "trace", skip(self))]
    async fn recover(&self, request: RecoverRequest) -> Result<RecoverResponse, SyneviError> {
        if !self.node.is_ready() {
            return Ok(RecoverResponse::default());
        }
        let request_id = u128::from_be_bytes(request.id.as_slice().try_into()?);
        trace!(?request_id, "Replica: Recover");
        let t_zero = T0::try_from(request.timestamp_zero.as_slice())?;

        // TODO/WARNING: This was initially in one Mutex lock
        //let mut event_store = self.node.event_store.lock().await;

        if let Some(state) = self.node.event_store.get_event_state(&t_zero) {
            // If another coordinator has started recovery with a higher ballot
            // Return NACK with the higher ballot number
            let request_ballot = Ballot::try_from(request.ballot.as_slice())?;
            if let Some(ballot) = self
                .node
                .event_store
                .accept_tx_ballot(&t_zero, request_ballot)
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
                    .pre_accept_tx(request_id, t_zero, request.event)?;
            };
        } else {
            self.node
                .event_store
                .pre_accept_tx(request_id, t_zero, request.event)?;
        }
        let recover_deps = self.node.event_store.get_recover_deps(&t_zero)?;

        self.node
            .stats
            .total_recovers
            .fetch_add(1, Ordering::Relaxed);

        let local_state = self
            .node
            .event_store
            .get_event_state(&t_zero)
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

    #[instrument(level = "trace", skip(self))]
    async fn try_recover(
        &self,
        request: TryRecoveryRequest,
    ) -> Result<TryRecoveryResponse, SyneviError> {
        let t0 = T0::try_from(request.timestamp_zero.as_slice())?;

        if !self.node.is_ready() {
            if let Some(recover_event) = self
                .node
                .event_store
                .recover_event(&t0, self.node.get_serial())?
            {
                tokio::spawn(Coordinator::recover(self.node.clone(), recover_event));
                return Ok(TryRecoveryResponse { accepted: true });
            }
        }

        // This ensures that this t0 will not get a fast path in the future
        self.node.event_store.inc_time_with_guard(t0)?;
        Ok(TryRecoveryResponse { accepted: false })
    }
}

#[async_trait::async_trait]
impl<N, E, S> Reconfiguration for ReplicaConfig<N, E, S>
where
    N: Network,
    E: Executor,
    S: Store,
{
    async fn join_electorate(
        &self,
        request: JoinElectorateRequest,
    ) -> Result<JoinElectorateResponse, SyneviError> {
        if !self.node.is_ready() {
            return Ok(JoinElectorateResponse::default());
        }
        let Some(Config {
            node_id,
            node_serial,
            host,
            ..
        }) = request.config
        else {
            return Err(SyneviError::TonicStatusError(
                tonic::Status::invalid_argument("No config provided"),
            ));
        };

        let node = self.node.clone();
        let member_count = self.node.network.get_members().await.len() as u32;
        let self_event = Ulid::new();
        println!("Before joining transaction");
        let _res = node
            .internal_transaction(
                self_event.0,
                TransactionPayload::Internal(InternalExecution::JoinElectorate {
                    id: Ulid::from_bytes(node_id.as_slice().try_into()?),
                    serial: node_serial.try_into()?,
                    new_node_host: host,
                }),
            )
            .await?;
        println!("After joining transaction");
        Ok(JoinElectorateResponse { member_count })
    }

    async fn get_events(
        &self,
        request: GetEventRequest,
    ) -> Result<Receiver<Result<GetEventResponse, SyneviError>>, SyneviError> {
        if !self.node.is_ready() {
            return Err(SyneviError::NotReady);
        }
        let (sdx, rcv) = tokio::sync::mpsc::channel(200);
        let last_applied = T::try_from(request.last_applied.as_slice())?;
        let mut store_rcv = self.node.event_store.get_events_after(last_applied)?;
        tokio::spawn(async move {
            while let Some(Ok(event)) = store_rcv.recv().await {
                let response = {
                    if let Some(hashes) = event.hashes {
                        Ok(GetEventResponse {
                            id: event.id.to_be_bytes().to_vec(),
                            t_zero: event.t_zero.into(),
                            t: event.t.into(),
                            state: event.state.into(),
                            transaction: event.transaction,
                            dependencies: into_dependency(&event.dependencies),
                            ballot: event.ballot.into(),
                            last_updated: Vec::new(), // TODO
                            previous_hash: hashes.previous_hash.to_vec(),
                            transaction_hash: hashes.transaction_hash.to_vec(),
                            execution_hash: hashes.execution_hash.to_vec(),
                        })
                    } else {
                        Ok(GetEventResponse {
                            id: event.id.to_be_bytes().to_vec(),
                            t_zero: event.t_zero.into(),
                            t: event.t.into(),
                            state: event.state.into(),
                            transaction: event.transaction,
                            dependencies: into_dependency(&event.dependencies),
                            ballot: event.ballot.into(),
                            last_updated: Vec::new(), // TODO
                            previous_hash: [0; 32].to_vec(),
                            transaction_hash: [0; 32].to_vec(),
                            execution_hash: [0; 32].to_vec(),
                        })
                    }
                };
                sdx.send(response).await.unwrap();
            }
        });
        println!("Returning streaming receiver");
        // Stream all events to member
        Ok(rcv)
    }

    // Existing Node
    async fn ready_electorate(
        &self,
        request: ReadyElectorateRequest,
    ) -> Result<ReadyElectorateResponse, SyneviError> {
        if !self.node.is_ready() {
            return Ok(ReadyElectorateResponse::default());
        }
        // Start ready electorate transaction with NewMemberUlid
        let ReadyElectorateRequest {
            node_id,
            node_serial,
        } = request;
        let node = self.node.clone();
        //dbg!("Before ready transaction");
        node.internal_transaction(
            Ulid::new().0,
            TransactionPayload::Internal(InternalExecution::ReadyElectorate {
                id: Ulid::from_bytes(node_id.as_slice().try_into()?),
                serial: node_serial.try_into()?,
            }),
        )
        .await?;
        //dbg!("After ready transaction");
        Ok(ReadyElectorateResponse {})
    }

    // TODO: Move trait to Joining Node -> Rename to receive_config, Ready checks
    async fn report_electorate(
        &self,
        request: ReportElectorateRequest,
    ) -> Result<ReportElectorateResponse, SyneviError> {
        if self.node.is_ready() {
            return Ok(ReportElectorateResponse::default());
        }
        for member in request.configs {
            self.node
                .add_member(
                    Ulid::from_bytes(member.node_id.as_slice().try_into()?),
                    member.node_serial as u16,
                    member.host,
                    member.ready,
                )
                .await?;
        }
        self.node
            .network
            .get_node_status()
            .members_responded
            .fetch_add(1, Ordering::Relaxed);
        Ok(ReportElectorateResponse {})
    }
}

impl<N, E, S> Clone for ReplicaConfig<N, E, S>
where
    N: Network,
    E: Executor,
    S: Store,
{
    fn clone(&self) -> Self {
        Self {
            node: self.node.clone(),
        }
    }
}

#[cfg(test)]
mod tests {}
