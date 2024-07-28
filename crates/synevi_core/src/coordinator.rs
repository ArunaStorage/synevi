use crate::error::ConsensusError;
use crate::node::{Node, Stats};
use crate::utils::{from_dependency, into_dependency};
use crate::wait_handler::{WaitAction, WaitHandler};
use ahash::RandomState;
use anyhow::Result;
use std::collections::HashSet;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use synevi_network::consensus_transport::{
    AcceptRequest, AcceptResponse, ApplyRequest, CommitRequest, PreAcceptRequest,
    PreAcceptResponse, RecoverRequest, RecoverResponse,
};
use synevi_network::network::{BroadcastRequest, Network, NetworkInterface, NodeInfo};
use synevi_network::utils::IntoInner;
use synevi_persistence::event_store::Store;
use synevi_types::types::RecoverEvent;
use synevi_types::{Ballot, Executor, State, Transaction, T, T0};
use tokio::sync::Mutex;
use tracing::instrument;

pub struct Coordinator<Tx, N, E, S>
where
    Tx: Transaction,
    N: Network,
    E: Executor,
    S: Store,
{
    pub node: Arc<Node<N, E, S>>,
    pub network_interface: Arc<N::Ni>,
    pub transaction: TransactionStateMachine<Tx>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TransactionStateMachine<Tx: Transaction> {
    pub id: u128,
    pub state: State,
    pub transaction: Option<Tx>,
    pub t_zero: T0,
    pub t: T,
    pub dependencies: HashSet<T0, RandomState>,
    pub ballot: Ballot,
}

impl<Tx> TransactionStateMachine<Tx>
where
    Tx: Transaction,
{
    fn get_transaction_bytes(&self) -> Vec<u8> {
        self.transaction
            .as_ref()
            .map_or_else(Vec::new, |tx| tx.as_bytes())
    }
}

impl<Tx, N, E, S> Coordinator<Tx, N, E, S>
where
    Tx: Transaction,
    N: Network,
    E: Executor<Tx = Tx>,
    S: Store,
{
    #[instrument(level = "trace", skip(node, transaction))]
    pub async fn new(node: Arc<Node<N, E, S>>, transaction: Tx, id: u128) -> Self {
        let t0 = node.event_store.lock().await.init_t_zero(node.info.serial);
        let network_interface = node.network.get_interface().await;
        Coordinator {
            node,
            network_interface,
            transaction: TransactionStateMachine {
                id,
                state: State::Undefined,
                transaction: Some(transaction),
                t_zero: t0,
                t: T(*t0),
                ..Default::default()
            },
        }
    }

    #[instrument(level = "trace", skip(self))]
    pub async fn run(&mut self) -> Result<Tx::ExecutionResult> {
        match self.pre_accept().await {
            Ok(result) => Ok(result),
            Err(_e) => todo!(), // Handle error / recover
        }
    }

    #[instrument(level = "trace", skip(self))]
    pub async fn pre_accept(&mut self) -> Result<Tx::ExecutionResult, ConsensusError> {
        self.node
            .stats
            .total_requests
            .fetch_add(1, Ordering::Relaxed);

        // Create the PreAccepted msg
        let pre_accepted_request = PreAcceptRequest {
            id: self.transaction.id.to_be_bytes().into(),
            event: self.transaction.get_transaction_bytes(),
            timestamp_zero: (*self.transaction.t_zero).into(),
        };

        let pre_accepted_responses = self
            .network_interface
            .broadcast(BroadcastRequest::PreAccept(
                pre_accepted_request,
                self.node.info.serial,
            ))
            .await?;

        let pa_responses = pre_accepted_responses
            .into_iter()
            .map(|res| res.into_inner())
            .collect::<Result<Vec<_>>>()?;

        if pa_responses
            .iter()
            .any(|PreAcceptResponse { nack, .. }| *nack)
        {
            return Err(ConsensusError::CompetingCoordinator);
        }

        self.pre_accept_consensus(&pa_responses).await?;

        self.accept().await
    }

    #[instrument(level = "trace", skip(self))]
    async fn pre_accept_consensus(&mut self, responses: &[PreAcceptResponse]) -> Result<()> {
        // Collect deps by t_zero and only keep the max t
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
            .lock()
            .await
            .upsert_tx((&self.transaction).into());

        Ok(())
    }

    #[instrument(level = "trace", skip(self))]
    pub async fn accept(&mut self) -> Result<Tx::ExecutionResult, ConsensusError> {
        // Safeguard: T0 <= T
        assert!(*self.transaction.t_zero <= *self.transaction.t);

        if *self.transaction.t_zero != *self.transaction.t {
            self.node
                .stats
                .total_accepts
                .fetch_add(1, Ordering::Relaxed);
            let accepted_request = AcceptRequest {
                id: self.transaction.id.to_be_bytes().into(),
                ballot: self.transaction.ballot.into(),
                event: self.transaction.get_transaction_bytes(),
                timestamp_zero: (*self.transaction.t_zero).into(),
                timestamp: (*self.transaction.t).into(),
                dependencies: into_dependency(&self.transaction.dependencies),
            };
            let accepted_responses = self
                .network_interface
                .broadcast(BroadcastRequest::Accept(accepted_request))
                .await?;

            let pa_responses = accepted_responses
                .into_iter()
                .map(|res| res.into_inner())
                .collect::<Result<Vec<_>>>()?;

            if pa_responses.iter().any(|AcceptResponse { nack, .. }| *nack) {
                return Err(ConsensusError::CompetingCoordinator);
            }

            self.accept_consensus(&pa_responses).await?;
        }
        self.commit().await
    }

    #[instrument(level = "trace", skip(self))]
    async fn accept_consensus(&mut self, responses: &[AcceptResponse]) -> Result<()> {
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
            .lock()
            .await
            .upsert_tx((&self.transaction).into());
        Ok(())
    }

    #[instrument(level = "trace", skip(self))]
    pub async fn commit(&mut self) -> Result<Tx::ExecutionResult, ConsensusError> {
        let committed_request = CommitRequest {
            id: self.transaction.id.to_be_bytes().into(),
            event: self.transaction.get_transaction_bytes(),
            timestamp_zero: (*self.transaction.t_zero).into(),
            timestamp: (*self.transaction.t).into(),
            dependencies: into_dependency(&self.transaction.dependencies),
        };
        let network_interface_clone = self.network_interface.clone();

        let (committed_result, broadcast_result) = tokio::join!(
            self.commit_consensus(),
            network_interface_clone.broadcast(BroadcastRequest::Commit(committed_request))
        );

        committed_result.unwrap();
        broadcast_result.unwrap(); // TODO Recovery

        self.apply().await
    }

    #[instrument(level = "trace", skip(self))]
    async fn commit_consensus(&mut self) -> Result<()> {
        self.transaction.state = State::Commited;

        let (sx, rx) = tokio::sync::oneshot::channel();
        self.node
            .wait_handler
            .read()
            .await
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("Missing wait handler"))?
            .send_msg(
                self.transaction.t_zero,
                self.transaction.t,
                self.transaction.dependencies.clone(),
                self.transaction.get_transaction_bytes(),
                WaitAction::CommitBefore,
                sx,
                self.transaction.id,
            )
            .await?;
        let _ = rx.await;

        Ok(())
    }

    #[instrument(level = "trace", skip(self))]
    pub async fn apply(&mut self) -> Result<Tx::ExecutionResult, ConsensusError> {
        eprintln!("START APPLY {}", self.transaction.id);
        let result = self.execute_consensus().await?;

        let applied_request = ApplyRequest {
            id: self.transaction.id.to_be_bytes().into(),
            event: self.transaction.get_transaction_bytes(),
            timestamp: (*self.transaction.t).into(),
            timestamp_zero: (*self.transaction.t_zero).into(),
            dependencies: into_dependency(&self.transaction.dependencies),
            //result: vec![], // Theoretically not needed right?
        };

        self.network_interface
            .broadcast(BroadcastRequest::Apply(applied_request))
            .await?; // This should not be awaited

        Ok(result)
    }

    #[instrument(level = "trace", skip(self))]
    async fn execute_consensus(&mut self) -> Result<Tx::ExecutionResult> {
        self.transaction.state = State::Applied;
        let (sx, _) = tokio::sync::oneshot::channel();
        self.node
            .get_wait_handler()
            .await?
            .send_msg(
                self.transaction.t_zero,
                self.transaction.t,
                self.transaction.dependencies.clone(),
                self.transaction.get_transaction_bytes(),
                WaitAction::ApplyAfter,
                sx,
                self.transaction.id,
            )
            .await?;

        let transaction = self
            .transaction
            .transaction
            .take()
            .ok_or_else(|| anyhow::anyhow!("Transaction not found in coordinator"))?;

        self.node.executor.execute(transaction)
    }

    #[instrument(level = "trace", skip(node))]
    pub async fn recover(node: Arc<Node<N, E, S>>, t0_recover: T0) -> Result<Tx::ExecutionResult> {
        let recover_event = node.event_store.lock().await.recover_event(&t0_recover)?;
        let network_interface = node.network.get_interface().await;

        let recover_responses = network_interface
            .broadcast(BroadcastRequest::Recover(RecoverRequest {
                id: recover_event.id.to_be_bytes().to_vec(),
                ballot: recover_event.ballot.into(),
                event: recover_event.transaction.clone(),
                timestamp_zero: t0_recover.into(),
            }))
            .await?;

        let mut recover_coordinator = Coordinator::<Tx, N, E, S> {
            node,
            network_interface,
            transaction: TransactionStateMachine {
                transaction: Some(Tx::from_bytes(recover_event.transaction)?),
                t_zero: recover_event.t_zero,
                t: recover_event.t,
                ballot: recover_event.ballot,
                state: recover_event.state,
                id: recover_event.id,
                dependencies: recover_event.dependencies,
            },
        };

        recover_coordinator
            .recover_consensus(
                recover_responses
                    .into_iter()
                    .map(|res| res.into_inner())
                    .collect::<Result<Vec<_>>>()?,
            )
            .await
    }

    #[instrument(level = "trace", skip(self))]
    async fn recover_consensus(
        &mut self,
        responses: Vec<RecoverResponse>,
    ) -> Result<Tx::ExecutionResult> {
        // Query the newest state
        // let event = event_store.lock().await.get_or_insert(t0).await;
        // let previous_state = event.state;

        // Keep track of values to replace
        let mut highest_ballot: Option<Ballot> = None;
        let mut superseding = false;
        let mut waiting: HashSet<T0> = HashSet::new();

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
                        self.transaction.t = replica_t;
                    }
                    self.transaction.state = State::PreAccepted;
                    self.transaction
                        .dependencies
                        .extend(from_dependency(response.dependencies.clone())?);
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

        if highest_ballot.is_some() {
            if previous_state != State::Applied {
                return Ok(CoordinatorIterator::Recovering);
            }
            return Ok(CoordinatorIterator::Applied);
        }
        if let Some(event) = event_store.lock().await.get_event(t0).await {
            if event.ballot > ballot {
                return Ok(CoordinatorIterator::Recovering);
            }
        };

        // Wait for deps

        Ok(match self.transaction.state {
            State::Applied => self.apply().await?,
            State::Commited => self.commit().await?,
            State::Accepted => self.accept().await?,

            State::PreAccepted => {
                if superseding {
                    self.pre_accept().await?
                } else if !waiting.is_empty() {
                    // We will wait anyway if RestartRecovery is returned
                    return Ok(CoordinatorIterator::RestartRecovery);
                } else {
                    state_machine.t = T(*state_machine.t_zero);
                    self.pre_accept().await?
                }
            }
            _ => {
                tracing::warn!("Recovery state not matched");
                CoordinatorIterator::Recovering
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::Coordinator;
    use crate::{
        coordinator::{Initialized, TransactionStateMachine},
        event_store::{Event, EventStore},
        node::Stats,
        tests::NetworkMock,
        utils::{from_dependency, Ballot, Transaction, T, T0},
        wait_handler::WaitHandler,
    };
    use anyhow::Result;
    use bytes::BufMut;
    use diesel_ulid::DieselUlid;
    use monotime::MonoTime;
    use std::{collections::HashSet, sync::Arc, vec};
    use synevi_network::{
        consensus_transport::{PreAcceptResponse, State},
        network::NodeInfo,
    };
    use tokio::sync::mpsc::channel;
    use tokio::sync::Mutex;

    #[derive(Default, Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
    struct TestTx;
    impl Transaction for TestTx {
        type ExecutionResult = ();
        fn as_bytes(&self) -> Vec<u8> {
            Vec::new()
        }
        fn from_bytes(_bytes: Vec<u8>) -> Result<Self> {
            Ok(Self)
        }
        fn execute(&self) -> Result<Self::ExecutionResult> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn init_test() {
        let (sdx, _rcv) = channel(100);
        let event_store = Arc::new(Mutex::new(EventStore::init(None, 1, sdx).unwrap()));

        let network = Arc::new(NetworkMock::default());
        let coordinator = Coordinator::<Initialized, TestTx>::new(
            Arc::new(NodeInfo {
                id: DieselUlid::generate(),
                serial: 0,
            }),
            event_store.clone(),
            network.clone(),
            TestTx::from_bytes(b"test".to_vec()).unwrap(),
            Arc::new(Default::default()),
            WaitHandler::new(event_store, network, Default::default(), Default::default()),
            0,
        )
        .await;
        assert_eq!(coordinator.transaction.state, State::PreAccepted);
        assert_eq!(*coordinator.transaction.t_zero, *coordinator.transaction.t);
        assert_eq!(coordinator.transaction.t_zero.0.get_node(), 0);
        assert_eq!(coordinator.transaction.t_zero.0.get_seq(), 1);
        assert!(coordinator.transaction.dependencies.is_empty());
    }

    #[tokio::test]
    async fn pre_accepted_fast_path_test() {
        let (sdx, _) = channel(100);
        let event_store = Arc::new(Mutex::new(EventStore::init(None, 1, sdx).unwrap()));

        let id = u128::from_be_bytes(DieselUlid::generate().as_byte_array());

        let state_machine = TransactionStateMachine::<TestTx> {
            id,
            state: State::PreAccepted,
            transaction: TestTx,
            t_zero: T0(MonoTime::new_with_time(10u128, 0, 0)),
            t: T(MonoTime::new_with_time(10u128, 0, 0)),
            dependencies: HashSet::default(),
            ballot: Ballot::default(),
            tx_result: None,
        };

        let network = Arc::new(NetworkMock::default());

        let node_info = Arc::new(NodeInfo {
            id: DieselUlid::generate(),
            serial: 0,
        });

        let mut coordinator = Coordinator::<Initialized, TestTx> {
            node: node_info.clone(),
            network_interface: network.clone(),
            event_store: event_store.clone(),
            transaction: state_machine.clone(),
            stats: Arc::new(Default::default()),
            wait_handler: WaitHandler::new(
                event_store.clone(),
                network,
                Arc::new(Stats::default()),
                node_info,
            ),
            phantom: Default::default(),
        };

        let pre_accepted_ok = vec![
            PreAcceptResponse {
                timestamp: MonoTime::new_with_time(10u128, 0, 0).into(),
                dependencies: Vec::new(),
                nack: false,
            };
            3
        ];

        coordinator
            .pre_accept_consensus(&pre_accepted_ok)
            .await
            .unwrap();
        assert_eq!(event_store.lock().await.events.len(), 1);
        assert_eq!(event_store.lock().await.mappings.len(), 1);
        assert_eq!(event_store.lock().await.last_applied, T::default());
        assert_eq!(
            event_store.lock().await.events.iter().next().unwrap().1,
            &Event {
                id,
                state: State::PreAccepted,
                event: Vec::new(),
                t_zero: T0(MonoTime::new_with_time(10u128, 0, 0)),
                t: T(MonoTime::new_with_time(10u128, 0, 0)),
                dependencies: HashSet::default(),
                ballot: Ballot::default(),
                last_updated: 0,
                previous_hash: None,
            }
        );

        // FastPath with dependencies

        let mut deps = Vec::with_capacity(16);
        deps.put_u128(MonoTime::new_with_time(1u128, 0, 0).into());

        let mut deps_2 = deps.clone();
        deps_2.put_u128(MonoTime::new_with_time(3u128, 0, 0).into());

        let mut deps_3 = deps.clone();
        deps_3.put_u128(MonoTime::new_with_time(2u128, 0, 0).into());

        let pre_accepted_ok = vec![
            PreAcceptResponse {
                timestamp: MonoTime::new_with_time(10u128, 0, 0).into(),
                dependencies: deps,
                nack: false,
            },
            PreAcceptResponse {
                timestamp: MonoTime::new_with_time(10u128, 0, 0).into(),
                dependencies: deps_2,
                nack: false,
            },
            PreAcceptResponse {
                timestamp: MonoTime::new_with_time(10u128, 0, 0).into(),
                dependencies: deps_3,
                nack: false,
            },
        ];

        coordinator
            .pre_accept_consensus(&pre_accepted_ok)
            .await
            .unwrap();

        let state_machine = TransactionStateMachine {
            id,
            state: State::PreAccepted,
            transaction: TestTx,
            t_zero: T0(MonoTime::new_with_time(10u128, 0, 0)),
            t: T(MonoTime::new_with_time(10u128, 0, 0)),
            dependencies: HashSet::from_iter(
                [
                    T0(MonoTime::new_with_time(1u128, 0, 0)),
                    T0(MonoTime::new_with_time(2u128, 0, 0)),
                    T0(MonoTime::new_with_time(3u128, 0, 0)),
                ]
                .iter()
                .cloned(),
            ),
            ballot: Ballot::default(),
            tx_result: None,
        };

        assert_eq!(state_machine, coordinator.transaction);
        assert_eq!(event_store.lock().await.events.len(), 1);
        assert_eq!(event_store.lock().await.mappings.len(), 1);
        assert_eq!(event_store.lock().await.last_applied, T::default());
        assert_eq!(
            event_store.lock().await.events.iter().next().unwrap().1,
            &Event {
                id,
                state: State::PreAccepted,
                event: Vec::new(),
                t_zero: T0(MonoTime::new_with_time(10u128, 0, 0)),
                t: T(MonoTime::new_with_time(10u128, 0, 0)),
                dependencies: HashSet::from_iter(
                    [
                        T0(MonoTime::new_with_time(1u128, 0, 0)),
                        T0(MonoTime::new_with_time(2u128, 0, 0)),
                        T0(MonoTime::new_with_time(3u128, 0, 0)),
                    ]
                    .iter()
                    .cloned()
                ),
                ..Default::default()
            }
        );
    }

    #[tokio::test]
    async fn pre_accepted_slow_path_test() {
        let (sdx, _rcv) = channel(100);
        let event_store = Arc::new(Mutex::new(EventStore::init(None, 1, sdx).unwrap()));

        let id = u128::from_be_bytes(DieselUlid::generate().as_byte_array());

        let state_machine = TransactionStateMachine {
            id,
            state: State::PreAccepted,
            transaction: TestTx,
            t_zero: T0(MonoTime::new_with_time(10u128, 0, 0)),
            t: T(MonoTime::new_with_time(10u128, 0, 0)),
            dependencies: HashSet::default(),
            ballot: Ballot::default(),
            tx_result: None,
        };

        let node_info = Arc::new(NodeInfo {
            id: DieselUlid::generate(),
            serial: 0,
        });

        let network = Arc::new(NetworkMock::default());
        let mut coordinator = Coordinator {
            node: node_info.clone(),
            network_interface: network.clone(),
            event_store: event_store.clone(),
            transaction: state_machine.clone(),
            stats: Arc::new(Default::default()),
            wait_handler: WaitHandler::new(
                event_store.clone(),
                network,
                Arc::new(Stats::default()),
                node_info,
            ),
            phantom: Default::default(),
        };

        let mut deps = Vec::with_capacity(16);
        deps.put_u128(MonoTime::new_with_time(11u128, 0, 0).into());

        let pre_accepted_ok = vec![
            PreAcceptResponse {
                timestamp: MonoTime::new_with_time(12u128, 0, 1).into(),
                dependencies: deps.clone(),
                nack: false,
            },
            PreAcceptResponse {
                timestamp: MonoTime::new_with_time(10u128, 0, 0).into(),
                dependencies: Vec::new(),
                nack: false,
            },
            PreAcceptResponse {
                timestamp: MonoTime::new_with_time(10u128, 0, 0).into(),
                dependencies: Vec::new(),
                nack: false,
            },
        ];

        coordinator
            .pre_accept_consensus(&pre_accepted_ok)
            .await
            .unwrap();
        assert_eq!(event_store.lock().await.events.len(), 1);
        assert_eq!(event_store.lock().await.mappings.len(), 1);
        assert_eq!(event_store.lock().await.last_applied, T::default());
        assert_eq!(
            event_store.lock().await.events.iter().next().unwrap().1,
            &Event {
                id,
                state: State::PreAccepted,
                event: Vec::new(),
                t_zero: T0(MonoTime::new_with_time(10u128, 0, 0)),
                t: T(MonoTime::new_with_time(12u128, 0, 1)),
                dependencies: from_dependency(deps).unwrap(),
                ..Default::default()
            }
        );
    }
}
