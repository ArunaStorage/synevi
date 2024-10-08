use crate::node::Node;
use crate::utils::{from_dependency, into_dependency};
use crate::wait_handler::WaitAction;
use sha3::{Digest, Sha3_256};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use synevi_network::configure_transport::{
    Config, GetEventRequest, GetEventResponse, JoinElectorateRequest, JoinElectorateResponse,
    ReadyElectorateRequest, ReadyElectorateResponse, ReportLastAppliedRequest,
    ReportLastAppliedResponse,
};
use synevi_network::consensus_transport::{
    AcceptRequest, AcceptResponse, ApplyRequest, ApplyResponse, CommitRequest, CommitResponse,
    PreAcceptRequest, PreAcceptResponse, RecoverRequest, RecoverResponse,
};
use synevi_network::network::Network;
use synevi_network::reconfiguration::{BufferedMessage, Reconfiguration, Report};
use synevi_network::replica::Replica;
use synevi_types::traits::Store;
use synevi_types::types::{ExecutorResult, InternalExecution, TransactionPayload, UpsertEvent};
use synevi_types::{Ballot, Executor, State, T, T0};
use synevi_types::{SyneviError, Transaction};
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::sync::Mutex;
use tracing::{instrument, trace};
use ulid::Ulid;

pub struct ReplicaConfig<N, E, S>
where
    N: Network,
    E: Executor,
    S: Store,
{
    node: Arc<Node<N, E, S>>,
    buffer: Arc<Mutex<BTreeMap<(T0, State), BufferedMessage>>>,
    notifier: Sender<Report>,
    ready: Arc<AtomicBool>,
    configuring: Arc<AtomicBool>,
}

impl<N, E, S> ReplicaConfig<N, E, S>
where
    N: Network,
    E: Executor,
    S: Store,
{
    pub fn new(node: Arc<Node<N, E, S>>, ready: Arc<AtomicBool>) -> (Self, Receiver<Report>) {
        let (notifier, receiver) = channel(10);
        (
            Self {
                node,
                buffer: Arc::new(Mutex::new(BTreeMap::default())),
                notifier,
                ready,
                configuring: Arc::new(AtomicBool::new(false)),
            },
            receiver,
        )
    }

    pub async fn send_buffered(
        &self,
    ) -> Result<Receiver<Option<(T0, State, BufferedMessage)>>, SyneviError> {
        let (sdx, rcv) = channel(100);
        let inner = self.buffer.clone();
        let serial = self.node.info.serial;
        let node = self.node.clone();
        let configure_lock = self.configuring.clone();
        tokio::spawn(async move {
            configure_lock.store(true, Ordering::SeqCst);
            loop {
                let event = inner.lock().await.pop_first();
                if let Some(((t0, state), event)) = event {
                    sdx.send(Some((t0, state, event))).await.map_err(|_| {
                        SyneviError::SendError(
                            "Channel for receiving buffered messages closed".to_string(),
                        )
                    })?;
                } else {
                    //if serial == 6 {
                    //    dbg!("BUFFER_CLOSED");
                        node.set_ready();
                    //}
                    sdx.send(None).await.map_err(|_| {
                        SyneviError::SendError(
                            "Channel for receiving buffered messages closed".to_string(),
                        )
                    })?;
                    break;
                }
            }
            Ok::<(), SyneviError>(())
        });
        Ok(rcv)
    }

    pub async fn dump_buffer(&self) -> () {
        let buffer = self.buffer.lock().await.clone();
        for msg in buffer {
            match msg.1 {
                BufferedMessage::Commit(req) => {
                    let t0 = T0::try_from(req.timestamp_zero.as_slice()).unwrap();
                    let id = u128::from_be_bytes(req.id.try_into().unwrap());
                    dbg!("CommitBuffer", id, msg.0, t0);
                }
                BufferedMessage::Apply(req) => {
                    let t0 = T0::try_from(req.timestamp_zero.as_slice()).unwrap();
                    let id = u128::from_be_bytes(req.id.try_into().unwrap());
                    dbg!("ApplyBuffer", id, msg.0, t0);
                }
            }
        }
        //panic!("Dumped buffer");
    }
}

#[async_trait::async_trait]
impl<N, E, S> Replica for ReplicaConfig<N, E, S>
where
    N: Network + Send + Sync,
    E: Executor + Send + Sync,
    S: Store + Send + Sync,
{
    fn is_ready(&self) -> bool {
        self.ready.load(Ordering::SeqCst)
    }
    #[instrument(level = "trace", skip(self, request))]
    async fn pre_accept(
        &self,
        request: PreAcceptRequest,
        _node_serial: u16,
        ready: bool,
    ) -> Result<PreAcceptResponse, SyneviError> {
        // TODO: REMOVE AFTER DEBUGGING
        let t0 = T0::try_from(request.timestamp_zero.as_slice())?;
        let request_id = u128::from_be_bytes(request.id.as_slice().try_into()?);

        if !ready {
            // self.node
            //     .event_store
            //     .upsert_tx(UpsertEvent {
            //         id: request_id,
            //         t_zero: t0,
            //         t: T::try_from(request.timestamp_zero.as_slice())?,
            //         state: State::PreAccepted,
            //         transaction: Some(request.event),
            //         ..Default::default()
            //     })
            //     .await?;
            return Ok(PreAcceptResponse::default());
        }

        trace!(?request_id, "Replica: PreAccept");

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
    async fn accept(
        &self,
        request: AcceptRequest,
        ready: bool,
    ) -> Result<AcceptResponse, SyneviError> {
        // TODO: REMOVE AFTER DEBUGGING
        let t_zero = T0::try_from(request.timestamp_zero.as_slice())?;
        let request_id = u128::from_be_bytes(request.id.as_slice().try_into()?);
        let t = T::try_from(request.timestamp.as_slice())?;
        let request_ballot = Ballot::try_from(request.ballot.as_slice())?;

        if !ready {
            // self.node
            //     .event_store
            //     .upsert_tx(UpsertEvent {
            //         id: request_id,
            //         t_zero,
            //         t,
            //         state: State::Accepted,
            //         transaction: Some(request.event),
            //         dependencies: Some(from_dependency(request.dependencies)?),
            //         ballot: Some(request_ballot),
            //         execution_hash: None,
            //     })
            //     .await?;
            return Ok(AcceptResponse::default());
        }
        trace!(?request_id, "Replica: Accept");

        //println!("Accept: {:?} @ {:?}", t_zero, self.node_info.serial);

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
    async fn commit(
        &self,
        request: CommitRequest,
        ready: bool,
    ) -> Result<CommitResponse, SyneviError> {
        let t_zero = T0::try_from(request.timestamp_zero.as_slice())?;
        let t = T::try_from(request.timestamp.as_slice())?;
        let request_id = u128::from_be_bytes(request.id.as_slice().try_into()?);
        if !self.configuring.load(Ordering::SeqCst) && !ready {
            //dbg!("Got buffer message", &request_id, &t_zero, &t);
            self.buffer
                .lock()
                .await
                .insert((t_zero, State::Commited), BufferedMessage::Commit(request));
            return Ok(CommitResponse {});
        }

        trace!(?request_id, "Replica: Commit");

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
    async fn apply(
        &self,
        request: ApplyRequest,
        ready: bool,
    ) -> Result<ApplyResponse, SyneviError> {
        let t_zero = T0::try_from(request.timestamp_zero.as_slice())?;
        let t = T::try_from(request.timestamp.as_slice())?;
        let request_id = u128::from_be_bytes(request.id.as_slice().try_into()?);
//         if self.node.info.serial == 6 {
//             println!(
//                 "APPLYING
// ID:     {:?}
// T0:     {:?}
// T:       {:?}
// ",
//                 request_id, t_zero, t
//             );
//         }
        if !self.configuring.load(Ordering::SeqCst) && !ready {
            //dbg!("Got buffer message", &request_id, &t_zero, &t);
            self.buffer
                .lock()
                .await
                .insert((t_zero, State::Applied), BufferedMessage::Apply(request));
            return Ok(ApplyResponse {});
        }
        trace!(?request_id, "Replica: Apply");

        let transaction: TransactionPayload<<E as Executor>::Tx> =
            TransactionPayload::from_bytes(request.event)?;
        //let transaction = <E as Executor>::Tx::from_bytes(request.event)?;

        let deps = from_dependency(request.dependencies)?;
        let (sx, rx) = tokio::sync::oneshot::channel();

        self.node
            .get_wait_handler()
            .await?
            .send_msg(
                t_zero,
                t,
                deps.clone(),
                transaction.as_bytes(),
                WaitAction::ApplyAfter,
                sx,
                request_id,
            )
            .await?;

        rx.await
            .map_err(|_| SyneviError::ReceiveError("Wait receiver closed".to_string()))?;

        let result = match transaction {
            TransactionPayload::None => {
                return Err(SyneviError::TransactionNotFound);
            }
            TransactionPayload::External(tx) => {
                self.node.executor.execute(tx).await
            }
            TransactionPayload::Internal(request) => {
                // TODO: Build special execution
                let result = match &request {
                    InternalExecution::JoinElectorate { id, serial, host } => {
                        if id != &self.node.info.id {
                            let res = self
                                .node
                                .add_member(*id, *serial, host.clone(), false)
                                .await;
                            let (t, hash) = self.node.event_store.last_applied_hash().await?;
                            self.node
                                .network
                                .report_config(t, hash, host.clone())
                                .await?;
                            res
                        } else {
                            Ok(())
                        }
                    }
                    InternalExecution::ReadyElectorate { id, serial } => {
                        if id != &self.node.info.id {
                            self.node.ready_member(*id, *serial).await
                        } else {
                            // TODO: Maybe set self.node.is_ready = true here instead of inside
                            // reconfiguration loop
                            Ok(())
                        }
                    }
                };
                match result {
                    Ok(_) => Ok(ExecutorResult::Internal(Ok(request.clone()))),
                    Err(err) => Ok(ExecutorResult::Internal(Err(err))),
                }
            }
        };

        let mut hasher = Sha3_256::new();
        postcard::to_io(&result, &mut hasher)?;
        let hash = hasher.finalize();
        let hashes = self
            .node
            .event_store
            .get_and_update_hash(t_zero, hash.into())
            .await?;
//        {
//            Ok(hashes) => hashes,
//            Err(err) => {}};
//                dbg!("HASHES_ERR", err);
//                println!(
//                    "
//NODE:   {:?},
//ID:     {:?},
//T0:     {:?},
//T:       {:?},
//",
//                    self.node.info.serial, request_id, t_zero, t
//                );
//                panic!("hash error");
//            }
//        };
        if request.transaction_hash != hashes.transaction_hash
            //|| request.execution_hash != hashes.execution_hash
        {}
//             println!(
//                 "MISMATCHED HASHES
// NODE:       {:?}
// ID:         {:?}
// T0:         {:?}
// deps:       {:?}
// 
// TRANS       
// EXPECTED    {:?}
// GOT         {:?}
// 
// EXEC        
// EXPECTED    {:?}
// GOT         {:?}
// ",
//                 self.node.info.serial,
//                 request_id,
//                 t_zero,
//                 deps,
//                 request.transaction_hash,
//                 hashes.transaction_hash,
//                 request.execution_hash,
//                 hashes.execution_hash
//             );
//             //panic!("Mismatched hashes")
//         }

        Ok(ApplyResponse {})
    }

    #[instrument(level = "trace", skip(self))]
    async fn recover(
        &self,
        request: RecoverRequest,
        ready: bool,
    ) -> Result<RecoverResponse, SyneviError> {
        if !ready {
            return Ok(RecoverResponse::default());
        }
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
    async fn join_electorate(
        &self,
        request: JoinElectorateRequest,
    ) -> Result<JoinElectorateResponse, SyneviError> {
        if !self.ready.load(Ordering::SeqCst) {
            return Ok(JoinElectorateResponse::default());
        }
        let Some(Config {
            node_id,
            node_serial,
            host,
        }) = request.config
        else {
            return Err(SyneviError::TonicStatusError(
                tonic::Status::invalid_argument("No config provided"),
            ));
        };

        let node = self.node.clone();
        let majority = self.node.network.get_member_len().await;
        let self_event = Ulid::new();
        let _res = node
            .transaction(
                self_event.0,
                TransactionPayload::Internal(InternalExecution::JoinElectorate {
                    id: Ulid::from_bytes(node_id.as_slice().try_into()?),
                    serial: node_serial.try_into()?,
                    host,
                }),
            )
            .await?;
        match _res {
            ExecutorResult::External(_) => {}
            ExecutorResult::Internal(_internal_execution) => {}
        }

        Ok(JoinElectorateResponse {
            majority,
            self_event: self_event.to_bytes().to_vec(),
        })
    }

    async fn get_events(
        &self,
        request: GetEventRequest,
    ) -> Receiver<Result<GetEventResponse, SyneviError>> {
        if !self.ready.load(Ordering::SeqCst) {
            todo!()
        }
        let (sdx, rcv) = tokio::sync::mpsc::channel(200);
        let t = match request.last_applied.as_slice().try_into() {
            Ok(t) => t,
            Err(err) => {
                sdx.send(Err(err)).await.unwrap();
                return rcv;
            }
        };
        let event_id = match request.self_event.as_slice().try_into() {
            Ok(id) => u128::from_be_bytes(id),
            Err(err) => {
                sdx.send(Err(SyneviError::InvalidConversionSlice(err)))
                    .await
                    .unwrap();
                return rcv;
            }
        };
        let mut store_rcv = self.node.event_store.get_events_after(t, event_id).await;
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
                        //dbg!("MissingExecutionHash");
                        // TODO: Upsert execution hash flow

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
        // Stream all events to member
        rcv
    }

    // Existing Node
    async fn ready_electorate(
        &self,
        request: ReadyElectorateRequest,
    ) -> Result<ReadyElectorateResponse, SyneviError> {
        if !self.ready.load(Ordering::SeqCst) {
            return Ok(ReadyElectorateResponse::default());
        }
        // Start ready electorate transaction with NewMemberUlid
        let ReadyElectorateRequest {
            node_id,
            node_serial,
        } = request;
        let node = self.node.clone();
        //dbg!("Before ready transaction");
        node.transaction(
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
    async fn report_last_applied(
        &self,
        request: ReportLastAppliedRequest,
    ) -> Result<ReportLastAppliedResponse, SyneviError> {
        if self.ready.load(Ordering::SeqCst) {
            return Ok(ReportLastAppliedResponse::default());
        }
        let Some(Config {
            node_serial,
            node_id,
            host,
        }) = request.config
        else {
            return Err(SyneviError::InvalidConversionRequest(
                "Invalid config".to_string(),
            ));
        };
        let report = Report {
            node_id: Ulid::from_bytes(node_id.try_into().map_err(|_| {
                SyneviError::InvalidConversionFromBytes("Invalid Ulid conversion".to_string())
            })?),
            node_serial: node_serial.try_into()?,
            node_host: host,
            last_applied: request.last_applied.as_slice().try_into()?,
            last_applied_hash: request.last_applied_hash.try_into().map_err(|_| {
                SyneviError::InvalidConversionFromBytes("Invalid hash conversion".to_string())
            })?,
        };
        //dbg!(&report);
        self.notifier.send(report).await.map_err(|_| {
            SyneviError::SendError("Sender for reporting last applied closed".to_string())
        })?;
        Ok(ReportLastAppliedResponse {})
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
            buffer: self.buffer.clone(),
            notifier: self.notifier.clone(),
            ready: self.ready.clone(),
            configuring: self.configuring.clone(),
        }
    }
}

#[cfg(test)]
mod tests {}
