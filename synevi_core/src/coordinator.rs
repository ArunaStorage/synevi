use crate::node::Node;
use crate::utils::{from_dependency, into_dependency};
use ahash::RandomState;
use serde::Serialize;
use sha3::{Digest, Sha3_256};
use std::collections::HashSet;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use synevi_network::consensus_transport::{
    AcceptRequest, AcceptResponse, ApplyRequest, CommitRequest, PreAcceptRequest,
    PreAcceptResponse, RecoverRequest, RecoverResponse,
};
use synevi_network::network::{BroadcastRequest, Network, NetworkInterface};
use synevi_network::utils::IntoInner;
use synevi_types::traits::Store;
use synevi_types::types::{
    ExecutorResult, Hashes, InternalExecution, InternalSyneviResult, RecoverEvent, RecoveryState,
    TransactionPayload,
};
use synevi_types::{Ballot, Executor, State, SyneviError, Transaction, T, T0};
use tracing::{instrument, trace};

type RecoveryInternalSyneviResult<E> =
    Result<RecoveryState<ExecutorResult<<E as Executor>::Tx>>, SyneviError>;

pub struct Coordinator<N, E, S>
where
    N: Network,
    E: Executor,
    S: Store,
{
    pub node: Arc<Node<N, E, S>>,
    pub transaction: TransactionStateMachine<E::Tx>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TransactionStateMachine<Tx: Transaction> {
    pub id: u128,
    pub state: State,
    pub transaction: TransactionPayload<Tx>,
    pub t_zero: T0,
    pub t: T,
    pub dependencies: HashSet<T0, RandomState>,
    pub ballot: Ballot,
}

impl<Tx> TransactionStateMachine<Tx>
where
    Tx: Transaction + Serialize,
{
    fn get_transaction_bytes(&self) -> Vec<u8> {
        self.transaction.as_bytes()
    }
}

impl<N, E, S> Coordinator<N, E, S>
where
    N: Network,
    E: Executor,
    S: Store,
{
    #[instrument(level = "trace", skip(node, transaction))]
    pub async fn new(
        node: Arc<Node<N, E, S>>,
        transaction: TransactionPayload<E::Tx>,
        id: u128,
    ) -> Self {
        trace!(?id, "Coordinator: New");
        let t0 = node.event_store.init_t_zero(node.get_serial());
        Coordinator {
            node,
            transaction: TransactionStateMachine {
                id,
                state: State::PreAccepted,
                transaction,
                t_zero: t0,
                t: T(*t0),
                dependencies: HashSet::default(),
                ballot: Ballot::default(),
            },
        }
    }

    #[instrument(level = "trace", skip(self))]
    pub async fn run(&mut self) -> InternalSyneviResult<E> {
        self.pre_accept().await
    }

    #[instrument(level = "trace", skip(self))]
    async fn pre_accept(&mut self) -> InternalSyneviResult<E> {
        trace!(id = ?self.transaction.id, "Coordinator: Preaccept");

        self.node
            .stats
            .total_requests
            .fetch_add(1, Ordering::Relaxed);

        let last_applied = {
            let (t, _) = self.node.event_store.last_applied();
            t.into()
        };

        // Create the PreAccepted msg
        let pre_accepted_request = PreAcceptRequest {
            id: self.transaction.id.to_be_bytes().into(),
            event: self.transaction.get_transaction_bytes(),
            timestamp_zero: (*self.transaction.t_zero).into(),
            last_applied,
        };

        let network_interface = self.node.network.get_interface().await;

        let pre_accepted_responses = network_interface
            .broadcast(BroadcastRequest::PreAccept(
                pre_accepted_request,
                self.node.get_serial(),
            ))
            .await?;

        let pa_responses = pre_accepted_responses
            .into_iter()
            .map(|res| res.into_inner())
            .collect::<Result<Vec<_>, SyneviError>>()?;

        if pa_responses
            .iter()
            .any(|PreAcceptResponse { nack, .. }| *nack)
        {
            return Err(SyneviError::CompetingCoordinator);
        }

        self.pre_accept_consensus(&pa_responses).await?;

        self.accept().await
    }

    #[instrument(level = "trace", skip(self))]
    async fn pre_accept_consensus(
        &mut self,
        responses: &[PreAcceptResponse],
    ) -> Result<(), SyneviError> {
        // Collect deps by t_zero and only keep the max t
        let (_, last_applied_t0) = self.node.event_store.last_applied();
        if last_applied_t0 != T0::default() {
            self.transaction.dependencies.insert(last_applied_t0);
        }
        for response in responses {
            let t_response = T::try_from(response.timestamp.as_slice())?;
            if t_response > self.transaction.t {
                self.transaction.t = t_response;
            }
            self.transaction
                .dependencies
                .extend(from_dependency(response.dependencies.clone())?);
        }

        // Upsert store
        self.node
            .event_store
            .upsert_tx((&self.transaction).into())?;

        Ok(())
    }

    #[instrument(level = "trace", skip(self))]
    async fn accept(&mut self) -> InternalSyneviResult<E> {
        trace!(id = ?self.transaction.id, "Coordinator: Accept");

        // Safeguard: T0 <= T
        assert!(*self.transaction.t_zero <= *self.transaction.t);

        if *self.transaction.t_zero != *self.transaction.t {
            self.node
                .stats
                .total_accepts
                .fetch_add(1, Ordering::Relaxed);
            let last_applied = {
                let (t, _) = self.node.event_store.last_applied();
                t.into()
            };
            let accepted_request = AcceptRequest {
                id: self.transaction.id.to_be_bytes().into(),
                ballot: self.transaction.ballot.into(),
                event: self.transaction.get_transaction_bytes(),
                timestamp_zero: (*self.transaction.t_zero).into(),
                timestamp: (*self.transaction.t).into(),
                dependencies: into_dependency(&self.transaction.dependencies),
                last_applied,
            };

            let network_interface = self.node.network.get_interface().await;
            let accepted_responses = network_interface
                .broadcast(BroadcastRequest::Accept(accepted_request))
                .await?;

            let pa_responses = accepted_responses
                .into_iter()
                .map(|res| res.into_inner())
                .collect::<Result<Vec<_>, SyneviError>>()?;

            if pa_responses.iter().any(|AcceptResponse { nack, .. }| *nack) {
                return Err(SyneviError::CompetingCoordinator);
            }

            self.accept_consensus(&pa_responses).await?;
        }
        self.commit().await
    }

    #[instrument(level = "trace", skip(self))]
    async fn accept_consensus(&mut self, responses: &[AcceptResponse]) -> Result<(), SyneviError> {
        // A little bit redundant, but I think the alternative to create a common behavior between responses may be even worse
        // Handle returned dependencies
        for response in responses {
            for dep in from_dependency(response.dependencies.clone())?.iter() {
                if !self.transaction.dependencies.contains(dep) {
                    self.transaction.dependencies.insert(*dep);
                }
            }
        }

        // Mut state and update entry
        self.transaction.state = State::Accepted;
        self.node
            .event_store
            .upsert_tx((&self.transaction).into())?;
        Ok(())
    }

    #[instrument(level = "trace", skip(self))]
    async fn commit(&mut self) -> InternalSyneviResult<E> {
        trace!(id = ?self.transaction.id, "Coordinator: Commit");

        let committed_request = CommitRequest {
            id: self.transaction.id.to_be_bytes().into(),
            event: self.transaction.get_transaction_bytes(),
            timestamp_zero: (*self.transaction.t_zero).into(),
            timestamp: (*self.transaction.t).into(),
            dependencies: into_dependency(&self.transaction.dependencies),
        };

        let network_interface = self.node.network.get_interface().await;
        let (committed_result, broadcast_result) = tokio::join!(
            self.commit_consensus(),
            network_interface.broadcast(BroadcastRequest::Commit(committed_request))
        );

        committed_result.unwrap();
        broadcast_result.unwrap(); // TODO: Recovery

        self.apply().await
    }

    #[instrument(level = "trace", skip(self))]
    async fn commit_consensus(&mut self) -> Result<(), SyneviError> {
        self.transaction.state = State::Commited;
        self.node.commit((&self.transaction).into()).await?;
        Ok(())
    }

    #[instrument(level = "trace", skip(self))]
    async fn apply(&mut self) -> InternalSyneviResult<E> {
        trace!(id = ?self.transaction.id, "Coordinator: Apply");
        println!("Coordinator: Apply");

        let (synevi_result, hashes) = self.execute_consensus().await?;

        println!("Coordinator: Apply after execute");

        let applied_request = ApplyRequest {
            id: self.transaction.id.to_be_bytes().into(),
            event: self.transaction.get_transaction_bytes(),
            timestamp: (*self.transaction.t).into(),
            timestamp_zero: (*self.transaction.t_zero).into(),
            dependencies: into_dependency(&self.transaction.dependencies),
            execution_hash: hashes.execution_hash.to_vec(),
            transaction_hash: hashes.transaction_hash.to_vec(),
        };

        println!("Coordinator: Apply before broadcast");

        let network_interface = self.node.network.get_interface().await;
        network_interface
            .broadcast(BroadcastRequest::Apply(applied_request))
            .await?; // TODO: This should not be awaited, but can be used to compare hashes

        println!("Coordinator: Apply after broadcast");

        synevi_result
    }

    #[instrument(level = "trace", skip(self))]
    async fn execute_consensus(
        &mut self,
    ) -> Result<(InternalSyneviResult<E>, Hashes), SyneviError> {
        self.transaction.state = State::Applied;

        self.node.apply((&self.transaction).into()).await?;

        let result = match &self.transaction.transaction {
            TransactionPayload::None => Err(SyneviError::TransactionNotFound),
            TransactionPayload::External(tx) => self
                .node
                .executor
                .execute(tx.clone())
                .await
                .map(|e| ExecutorResult::External(e)),
            TransactionPayload::Internal(request) => {
                let result = match request {
                    InternalExecution::JoinElectorate {
                        id,
                        serial,
                        new_node_host,
                    } => {
                        let res = self
                            .node
                            .add_member(*id, *serial, new_node_host.clone(), false)
                            .await;
                        self.node
                            .network
                            .report_config(new_node_host.to_string())
                            .await?;
                        res
                    }
                    InternalExecution::ReadyElectorate { id, serial } => {
                        self.node.ready_member(*id, *serial).await
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
            .get_and_update_hash(self.transaction.t_zero, hash.into())?;
        Ok((result, hashes))
    }

    #[instrument(level = "trace", skip(node))]
    pub async fn recover(
        node: Arc<Node<N, E, S>>,
        recover_event: RecoverEvent,
    ) -> InternalSyneviResult<E> {
        loop {
            let node = node.clone();

            let network_interface = node.network.get_interface().await;
            let recover_responses = network_interface
                .broadcast(BroadcastRequest::Recover(RecoverRequest {
                    id: recover_event.id.to_be_bytes().to_vec(),
                    ballot: recover_event.ballot.into(),
                    event: recover_event.transaction.clone(),
                    timestamp_zero: recover_event.t_zero.into(),
                }))
                .await?;

            let mut recover_coordinator = Coordinator::<N, E, S> {
                node,
                transaction: TransactionStateMachine {
                    transaction: TransactionPayload::from_bytes(recover_event.transaction.clone())?,
                    t_zero: recover_event.t_zero,
                    t: recover_event.t,
                    ballot: recover_event.ballot,
                    state: recover_event.state,
                    id: recover_event.id,
                    dependencies: recover_event.dependencies.clone(),
                },
            };

            let recover_result = recover_coordinator
                .recover_consensus(
                    recover_responses
                        .into_iter()
                        .map(|res| res.into_inner())
                        .collect::<Result<Vec<_>, SyneviError>>()?,
                )
                .await;
            if let Err(err) = &recover_result {
                dbg!(&err);
            }
            match recover_result? {
                RecoveryState::Recovered(result) => return Ok(result),
                RecoveryState::RestartRecovery => {
                    continue;
                }
                RecoveryState::CompetingCoordinator => {
                    return Err(SyneviError::CompetingCoordinator)
                }
            }
        }
    }

    #[instrument(level = "trace", skip(self))]
    async fn recover_consensus(
        &mut self,
        mut responses: Vec<RecoverResponse>,
    ) -> RecoveryInternalSyneviResult<E> {
        // Keep track of values to replace
        let mut highest_ballot: Option<Ballot> = None;
        let mut superseding = false;
        let mut waiting: HashSet<T0> = HashSet::new();

        let mut fast_path_counter = 0usize;
        let mut fast_path_deps = HashSet::default();

        for response in responses.iter_mut() {
            let response_ballot = Ballot::try_from(response.nack.clone().as_slice())?;
            if response_ballot > Ballot::default() || highest_ballot.is_some() {
                match highest_ballot.as_mut() {
                    None => {
                        highest_ballot = Some(response_ballot);
                    }
                    Some(b) if &response_ballot > b => {
                        *b = response_ballot;
                    }
                    _ => {}
                }
                continue;
            }

            let replica_t = T::try_from(response.timestamp.clone().as_slice())?;

            if response.superseding {
                superseding = true;
            }
            waiting.extend(from_dependency(response.wait.clone())?);

            // Update state
            let replica_state = State::from(response.local_state() as i32);

            match replica_state {
                State::PreAccepted if self.transaction.state <= State::PreAccepted => {
                    if replica_t > self.transaction.t {
                        // Slow path
                        self.transaction.t = replica_t;
                        self.transaction
                            .dependencies
                            .extend(from_dependency(response.dependencies.clone())?);
                    } else {
                        // Would be fast path
                        fast_path_counter += 1;
                        fast_path_deps.extend(from_dependency(response.dependencies.clone())?);
                    }
                    self.transaction.state = State::PreAccepted;
                }
                State::Accepted if self.transaction.state < State::Accepted => {
                    self.transaction.t = replica_t;
                    self.transaction.state = State::Accepted;
                    self.transaction.dependencies = from_dependency(response.dependencies.clone())?;
                }
                State::Accepted
                    if self.transaction.state == State::Accepted
                        && replica_t > self.transaction.t =>
                {
                    self.transaction.t = replica_t;
                    self.transaction.dependencies = from_dependency(response.dependencies.clone())?;
                }
                any_state if any_state > self.transaction.state => {
                    self.transaction.state = any_state;
                    self.transaction.t = replica_t;
                    if self.transaction.state >= State::Accepted {
                        self.transaction.dependencies =
                            from_dependency(response.dependencies.clone())?;
                    }
                }
                _ => {}
            }
        }

        if fast_path_counter > (responses.len() / 2) {
            // Enforce the fast path -> Slow path was minority
            self.transaction.t = T(*self.transaction.t_zero);
            self.transaction.dependencies = fast_path_deps;
        }

        if let Some(ballot) = highest_ballot {
            self.node
                .event_store
                .accept_tx_ballot(&self.transaction.t_zero, ballot);
            return Ok(RecoveryState::CompetingCoordinator);
        }

        // Wait for deps
        Ok(match self.transaction.state {
            State::Applied => RecoveryState::Recovered(self.apply().await?),
            State::Commited => RecoveryState::Recovered(self.commit().await?),
            State::Accepted => RecoveryState::Recovered(self.accept().await?),

            State::PreAccepted => {
                if superseding {
                    RecoveryState::Recovered(self.accept().await?)
                } else if !waiting.is_empty() {
                    // We will wait anyway if RestartRecovery is returned
                    return Ok(RecoveryState::RestartRecovery);
                } else {
                    self.transaction.t = T(*self.transaction.t_zero);
                    RecoveryState::Recovered(self.accept().await?)
                }
            }
            _ => {
                tracing::warn!("Recovery state not matched");
                return Err(SyneviError::UndefinedRecovery);
            }
        })
    }
}

#[cfg(test)]
pub mod tests {
    use super::Coordinator;
    use crate::node::Node;
    use crate::tests::DummyExecutor;
    use crate::tests::NetworkMock;
    use std::sync::atomic::Ordering;
    use synevi_network::consensus_transport::PreAcceptRequest;
    use synevi_network::network::Network;
    use synevi_network::network::{BroadcastRequest, NetworkInterface};
    use synevi_network::utils::IntoInner;
    use synevi_persistence::mem_store::MemStore;
    use synevi_types::traits::Store;
    use synevi_types::SyneviError;
    use synevi_types::{Executor, State, Transaction};
    use ulid::Ulid;

    #[derive(Default, Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
    #[allow(dead_code)]
    struct TestTx;
    impl Transaction for TestTx {
        type TxOk = Vec<u8>;
        type TxErr = ();

        fn as_bytes(&self) -> Vec<u8> {
            Vec::new()
        }
        fn from_bytes(_bytes: Vec<u8>) -> Result<Self, SyneviError> {
            Ok(Self)
        }
    }

    impl<N, E, S> Coordinator<N, E, S>
    where
        N: Network,
        E: Executor,
        S: Store,
    {
        pub async fn failing_pre_accept(&mut self) -> Result<(), SyneviError> {
            self.node
                .stats
                .total_requests
                .fetch_add(1, Ordering::Relaxed);

            let last_applied = {
                let (t, _) = self.node.event_store.last_applied();
                t.into()
            };

            // Create the PreAccepted msg
            let pre_accepted_request = PreAcceptRequest {
                id: self.transaction.id.to_be_bytes().into(),
                event: self.transaction.get_transaction_bytes(),
                timestamp_zero: (*self.transaction.t_zero).into(),
                last_applied,
            };

            let network_interface = self.node.network.get_interface().await;
            let pre_accepted_responses = network_interface
                .broadcast(BroadcastRequest::PreAccept(
                    pre_accepted_request,
                    self.node.get_serial(),
                ))
                .await?;

            let pa_responses = pre_accepted_responses
                .into_iter()
                .map(|res| res.into_inner())
                .collect::<Result<Vec<_>, SyneviError>>()?;

            self.pre_accept_consensus(&pa_responses).await?;
            Ok(())
        }
    }

    #[tokio::test]
    async fn init_test() {
        let node = Node::<NetworkMock, DummyExecutor, MemStore>::new_with_network_and_executor(
            Ulid::new(),
            0,
            NetworkMock::default(),
            DummyExecutor,
        )
        .await
        .unwrap();

        let coordinator = Coordinator::new(
            node,
            synevi_types::types::TransactionPayload::External(b"foo".to_vec()),
            0,
        )
        .await;

        assert_eq!(coordinator.transaction.state, State::PreAccepted);
        assert_eq!(*coordinator.transaction.t_zero, *coordinator.transaction.t);
        assert_eq!(coordinator.transaction.t_zero.0.get_node(), 0);
        assert_eq!(coordinator.transaction.t_zero.0.get_seq(), 1);
        assert!(coordinator.transaction.dependencies.is_empty());
    }
}
