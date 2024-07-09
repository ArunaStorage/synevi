use crate::error::ConsensusError;
use crate::event_store::EventStore;
use crate::node::Stats;
use crate::utils::{from_dependency, into_dependency, Ballot, T, T0};
use crate::wait_handler::{WaitAction, WaitHandler};
use ahash::RandomState;
use anyhow::Result;
use bytes::Bytes;
use consensus_transport::consensus_transport::{
    AcceptRequest, AcceptResponse, ApplyRequest, CommitRequest, PreAcceptRequest,
    PreAcceptResponse, RecoverRequest, RecoverResponse, State,
};
use consensus_transport::network::{BroadcastRequest, NetworkInterface, NodeInfo};
use consensus_transport::utils::IntoInner;
use std::collections::HashSet;
use std::marker::PhantomData;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tracing::instrument;

/// An iterator that goes through the different states of the coordinator

pub enum CoordinatorIterator {
    Initialized(Option<Coordinator<Initialized>>),
    PreAccepted(Option<Coordinator<PreAccepted>>),
    Accepted(Option<Coordinator<Accepted>>),
    Committed(Option<Coordinator<Committed>>),
    Applied,
    Recovering,
    RestartRecovery,
}

impl CoordinatorIterator {
    pub async fn new(
        node: Arc<NodeInfo>,
        event_store: Arc<Mutex<EventStore>>,
        network_interface: Arc<dyn NetworkInterface>,
        transaction: Vec<u8>,
        stats: Arc<Stats>,
        wait_handler: Arc<WaitHandler>,
    ) -> Self {
        CoordinatorIterator::Initialized(Some(
            Coordinator::<Initialized>::new(
                node,
                event_store,
                network_interface,
                transaction,
                stats,
                wait_handler,
            )
            .await,
        ))
    }

    pub async fn next(&mut self) -> Result<Option<()>> {
        match self {
            CoordinatorIterator::Initialized(coordinator) => {
                if let Some(c) = coordinator.take() {
                    let pre_accepted = c.pre_accept().await;
                    match pre_accepted {
                        Ok(c) => {
                            *self = CoordinatorIterator::PreAccepted(Some(c));
                            Ok(Some(()))
                        }
                        Err(ConsensusError::CompetingCoordinator) => {
                            *self = CoordinatorIterator::Recovering;
                            Ok(None)
                        }
                        Err(e) => Err(e.into()),
                    }
                } else {
                    Ok(None)
                }
            }
            CoordinatorIterator::PreAccepted(coordinator) => {
                if let Some(c) = coordinator.take() {
                    let accepted = c.accept().await;
                    match accepted {
                        Ok(c) => {
                            *self = CoordinatorIterator::Accepted(Some(c));
                            Ok(Some(()))
                        }
                        Err(ConsensusError::CompetingCoordinator) => {
                            *self = CoordinatorIterator::Recovering;
                            Ok(None)
                        }
                        Err(e) => Err(e.into()),
                    }
                } else {
                    Ok(None)
                }
            }
            CoordinatorIterator::Accepted(coordinator) => {
                if let Some(c) = coordinator.take() {
                    *self = CoordinatorIterator::Committed(Some(c.commit().await?));
                    Ok(Some(()))
                } else {
                    Ok(None)
                }
            }
            CoordinatorIterator::Committed(coordinator) => {
                if let Some(c) = coordinator.take() {
                    c.apply().await?;
                    *self = CoordinatorIterator::Applied;
                    Ok(Some(()))
                } else {
                    Ok(None)
                }
            }
            _ => Ok(None),
        }
    }

    pub async fn recover(
        node: Arc<NodeInfo>,
        event_store: Arc<Mutex<EventStore>>,
        network_interface: Arc<dyn NetworkInterface>,
        t0_recover: T0,
        stats: Arc<Stats>,
        wait_handler: Arc<WaitHandler>,
    ) -> Result<()> {
        let mut backoff_counter: u8 = 0;
        while backoff_counter <= MAX_RETRIES {
            let mut coordinator_iter = Coordinator::<Recover>::recover(
                node.clone(),
                event_store.clone(),
                network_interface.clone(),
                t0_recover,
                stats.clone(),
                wait_handler.clone(),
            )
            .await?;
            if let CoordinatorIterator::RestartRecovery = coordinator_iter {
                backoff_counter += 1;
                tokio::time::sleep(Duration::from_millis(1000)).await;
                continue;
            }
            stats.total_recovers.fetch_add(1, Ordering::Relaxed);
            while coordinator_iter.next().await?.is_some() {}
            break;
        }
        Ok(())
    }
}

const MAX_RETRIES: u8 = 5;

pub struct Initialized;
pub struct PreAccepted;
pub struct Accepted;
pub struct Committed;
pub struct Applied;
pub struct Recover;

pub struct Coordinator<X> {
    pub node: Arc<NodeInfo>,
    pub network_interface: Arc<dyn NetworkInterface>,
    pub event_store: Arc<Mutex<EventStore>>,
    pub transaction: TransactionStateMachine,
    pub phantom: PhantomData<X>,
    pub stats: Arc<Stats>,
    pub wait_handler: Arc<WaitHandler>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TransactionStateMachine {
    pub state: State,
    pub transaction: Vec<u8>,
    pub t_zero: T0,
    pub t: T,
    pub dependencies: HashSet<T0, RandomState>,
    pub ballot: Ballot,
}

impl<X> Coordinator<X> {
    #[instrument(level = "trace")]
    pub async fn new(
        node: Arc<NodeInfo>,
        event_store: Arc<Mutex<EventStore>>,
        network_interface: Arc<dyn NetworkInterface>,
        transaction: Vec<u8>,
        stats: Arc<Stats>,
        wait_handler: Arc<WaitHandler>,
    ) -> Coordinator<Initialized> {
        // Create struct
        let transaction = event_store
            .lock()
            .await
            .init_transaction(transaction, node.serial)
            .await;
        Coordinator::<Initialized> {
            node,
            network_interface,
            event_store,
            transaction,
            phantom: PhantomData,
            stats,
            wait_handler,
        }
    }
}

const RECOVER_TIMEOUT: u64 = 10000;

impl Coordinator<Recover> {
    #[instrument(level = "trace")]
    pub async fn recover(
        node: Arc<NodeInfo>,
        event_store: Arc<Mutex<EventStore>>,
        network_interface: Arc<dyn NetworkInterface>,
        t0_recover: T0,
        stats: Arc<Stats>,
        wait_handler: Arc<WaitHandler>,
    ) -> Result<CoordinatorIterator> {
        panic!();
        let mut event_store_lock = event_store.lock().await;
        let event = event_store_lock.get_or_insert(t0_recover).await;
        if matches!(event.state, State::Undefined) {
            tracing::debug!("Node {} with undefined recovery", node.serial);
            return Ok(CoordinatorIterator::Recovering);
        }

        // Just for sanity purposes
        assert_eq!(event.t_zero, t0_recover);

        let ballot = Ballot(event.ballot.next_with_node(node.serial).into_time());
        event_store_lock.update_ballot(&t0_recover, ballot);
        drop(event_store_lock);

        let recover_responses = network_interface
            .broadcast(BroadcastRequest::Recover(RecoverRequest {
                ballot: ballot.into(),
                event: event.event,
                timestamp_zero: t0_recover.into(),
            }))
            .await?;

        Self::recover_consensus(
            node,
            event_store,
            network_interface,
            recover_responses
                .into_iter()
                .map(|res| res.into_inner())
                .collect::<Result<Vec<_>>>()?,
            t0_recover,
            stats,
            ballot,
            wait_handler,
        )
        .await
    }

    #[instrument(level = "trace")]
    async fn recover_consensus(
        node: Arc<NodeInfo>,
        event_store: Arc<Mutex<EventStore>>,
        network_interface: Arc<dyn NetworkInterface>,
        mut responses: Vec<RecoverResponse>,
        t0: T0,
        stats: Arc<Stats>,
        ballot: Ballot,
        wait_handler: Arc<WaitHandler>,
    ) -> Result<CoordinatorIterator> {
        // Query the newest state
        let event = event_store.lock().await.get_or_insert(t0).await;
        let previous_state = event.state;

        let mut state_machine = TransactionStateMachine {
            transaction: event.event,
            t_zero: event.t_zero,
            t: event.t,
            ballot,
            state: previous_state,
            ..Default::default()
        };

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
            let replica_state = response.local_state();

            match replica_state {
                State::PreAccepted if state_machine.state <= State::PreAccepted => {
                    if replica_t > state_machine.t {
                        state_machine.t = replica_t;
                    }
                    state_machine.state = State::PreAccepted;
                    state_machine
                        .dependencies
                        .extend(from_dependency(response.dependencies.clone())?);
                }
                State::Accepted if state_machine.state < State::Accepted => {
                    state_machine.t = replica_t;
                    state_machine.state = State::Accepted;
                    state_machine.dependencies = from_dependency(response.dependencies.clone())?;
                }
                State::Accepted
                    if state_machine.state == State::Accepted && replica_t > state_machine.t =>
                {
                    state_machine.t = replica_t;
                    state_machine.dependencies = from_dependency(response.dependencies.clone())?;
                }
                any_state if any_state > state_machine.state => {
                    state_machine.state = any_state;
                    state_machine.t = replica_t;
                    if state_machine.state >= State::Accepted {
                        state_machine.dependencies =
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

        Ok(match state_machine.state {
            State::Applied => CoordinatorIterator::Committed(Some(Coordinator::<Committed> {
                node,
                network_interface,
                event_store,
                transaction: state_machine,
                stats,
                wait_handler,
                phantom: Default::default(),
            })),
            State::Commited => CoordinatorIterator::Accepted(Some(Coordinator::<Accepted> {
                node,
                network_interface,
                event_store,
                transaction: state_machine,
                stats,
                wait_handler,
                phantom: Default::default(),
            })),
            State::Accepted => CoordinatorIterator::PreAccepted(Some(Coordinator::<PreAccepted> {
                node,
                network_interface,
                event_store,
                transaction: state_machine,
                stats,
                wait_handler,
                phantom: Default::default(),
            })),

            State::PreAccepted => {
                if superseding {
                    CoordinatorIterator::PreAccepted(Some(Coordinator::<PreAccepted> {
                        node,
                        network_interface,
                        event_store,
                        transaction: state_machine,
                        stats,
                        wait_handler,
                        phantom: Default::default(),
                    }))
                } else if !waiting.is_empty() {
                    todo!();
                    //let mut rx = event.state.subscribe();
                    // timeout(
                    //     Duration::from_millis(RECOVER_TIMEOUT),
                    //     rx.wait_for(|(s, _)| *s > previous_state),
                    // )
                    // .await??;
                    return Ok(CoordinatorIterator::RestartRecovery);
                } else {
                    state_machine.t = T(*state_machine.t_zero);
                    CoordinatorIterator::PreAccepted(Some(Coordinator::<PreAccepted> {
                        node,
                        network_interface,
                        event_store,
                        transaction: state_machine,
                        stats,
                        wait_handler,
                        phantom: Default::default(),
                    }))
                }
            }
            _ => {
                tracing::warn!(?state_machine, "Recovery state not matched");
                CoordinatorIterator::Recovering
            }
        })
    }
}

impl Coordinator<Initialized> {
    #[instrument(level = "trace", skip(self))]
    pub async fn pre_accept(mut self) -> Result<Coordinator<PreAccepted>, ConsensusError> {
        self.stats.total_requests.fetch_add(1, Ordering::Relaxed);

        // Create the PreAccepted msg
        let pre_accepted_request = PreAcceptRequest {
            event: self.transaction.transaction.clone(),
            timestamp_zero: (*self.transaction.t_zero).into(),
        };

        let pre_accepted_responses = self
            .network_interface
            .broadcast(BroadcastRequest::PreAccept(
                pre_accepted_request,
                self.node.serial,
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

        Ok(Coordinator::<PreAccepted> {
            node: self.node,
            network_interface: self.network_interface,
            event_store: self.event_store,
            transaction: self.transaction,
            stats: self.stats,
            wait_handler: self.wait_handler,
            phantom: PhantomData,
        })
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
        self.event_store
            .lock()
            .await
            .upsert((&self.transaction).into())
            .await;

        Ok(())
    }
}

impl Coordinator<PreAccepted> {
    #[instrument(level = "trace", skip(self))]
    pub async fn accept(mut self) -> Result<Coordinator<Accepted>, ConsensusError> {
        // Safeguard: T0 <= T
        assert!(*self.transaction.t_zero <= *self.transaction.t);

        if *self.transaction.t_zero != *self.transaction.t {
            self.stats.total_accepts.fetch_add(1, Ordering::Relaxed);
            let accepted_request = AcceptRequest {
                ballot: self.transaction.ballot.into(),
                event: self.transaction.transaction.clone().into(),
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

        Ok(Coordinator::<Accepted> {
            node: self.node,
            network_interface: self.network_interface,
            event_store: self.event_store,
            transaction: self.transaction,
            stats: self.stats,
            wait_handler: self.wait_handler,
            phantom: PhantomData,
        })
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
        self.event_store
            .lock()
            .await
            .upsert((&self.transaction).into())
            .await;

        Ok(())
    }
}

impl Coordinator<Accepted> {
    #[instrument(level = "trace", skip(self))]
    pub async fn commit(mut self) -> Result<Coordinator<Committed>> {
        let committed_request = CommitRequest {
            event: self.transaction.transaction.clone(),
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

        Ok(Coordinator::<Committed> {
            node: self.node,
            network_interface: self.network_interface,
            event_store: self.event_store,
            transaction: self.transaction,
            stats: self.stats,
            wait_handler: self.wait_handler,
            phantom: PhantomData,
        })
    }

    #[instrument(level = "trace", skip(self))]
    async fn commit_consensus(&mut self) -> Result<()> {
        self.transaction.state = State::Commited;

        let (sx, rx) = tokio::sync::oneshot::channel();
        self.wait_handler
            .send_msg(
                self.transaction.t_zero,
                self.transaction.t,
                self.transaction.dependencies.clone(),
                self.transaction.transaction.clone(),
                WaitAction::CommitBefore,
                sx,
            )
            .await?;
        let _ = rx.await;

        Ok(())
    }
}

impl Coordinator<Committed> {
    #[instrument(level = "trace", skip(self))]
    pub async fn apply(mut self) -> Result<Coordinator<Applied>> {
        self.execute_consensus().await?;

        let applied_request = ApplyRequest {
            event: self.transaction.transaction.clone(),
            timestamp: (*self.transaction.t).into(),
            timestamp_zero: (*self.transaction.t_zero).into(),
            dependencies: into_dependency(&self.transaction.dependencies),
            //result: vec![], // Theoretically not needed right?
        };

        self.network_interface
            .broadcast(BroadcastRequest::Apply(applied_request))
            .await?; // This should not be awaited

        Ok(Coordinator::<Applied> {
            node: self.node,
            network_interface: self.network_interface,
            event_store: self.event_store,
            transaction: self.transaction,
            stats: self.stats,
            wait_handler: self.wait_handler,
            phantom: PhantomData,
        })
    }

    #[instrument(level = "trace", skip(self))]
    async fn execute_consensus(&mut self) -> Result<()> {
        // TODO: Apply in backend
        self.transaction.state = State::Applied;
        let (sx, _) = tokio::sync::oneshot::channel();
        self.wait_handler
            .send_msg(
                self.transaction.t_zero,
                self.transaction.t,
                self.transaction.dependencies.clone(),
                self.transaction.transaction.clone(),
                WaitAction::ApplyAfter,
                sx,
            )
            .await?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::Coordinator;
    use crate::{
        coordinator::{Initialized, TransactionStateMachine},
        event_store::{Event, EventStore},
        tests::NetworkMock,
        utils::{from_dependency, Ballot, T, T0},
        wait_handler::WaitHandler,
    };
    use bytes::{BufMut, Bytes};
    use consensus_transport::{
        consensus_transport::{PreAcceptResponse, State},
        network::NodeInfo,
    };
    use diesel_ulid::DieselUlid;
    use monotime::MonoTime;
    use std::{
        collections::HashSet,
        sync::Arc,
        vec,
    };
    use tokio::sync::Mutex;

    #[tokio::test]
    async fn init_test() {
        let event_store = Arc::new(Mutex::new(EventStore::init(None, 1)));

        let network = Arc::new(NetworkMock::default());
        let coordinator = Coordinator::<Initialized>::new(
            Arc::new(NodeInfo {
                id: DieselUlid::generate(),
                serial: 0,
            }),
            event_store.clone(),
            network.clone(),
            Vec::from("test"),
            Arc::new(Default::default()),
            WaitHandler::new(event_store, network),
        )
        .await;
        assert_eq!(coordinator.transaction.state, State::PreAccepted);
        assert_eq!(coordinator.transaction.transaction, Bytes::from("test"));
        assert_eq!(*coordinator.transaction.t_zero, *coordinator.transaction.t);
        assert_eq!(coordinator.transaction.t_zero.0.get_node(), 0);
        assert_eq!(coordinator.transaction.t_zero.0.get_seq(), 1);
        assert!(coordinator.transaction.dependencies.is_empty());
    }

    #[tokio::test]
    async fn pre_accepted_fast_path_test() {
        let event_store = Arc::new(Mutex::new(EventStore::init(None, 1)));

        let state_machine = TransactionStateMachine {
            state: State::PreAccepted,
            transaction: Vec::new(),
            t_zero: T0(MonoTime::new_with_time(10u128, 0, 0)),
            t: T(MonoTime::new_with_time(10u128, 0, 0)),
            dependencies: HashSet::default(),
            ballot: Ballot::default(),
        };

        let network = Arc::new(NetworkMock::default());
        let mut coordinator = Coordinator::<Initialized> {
            node: Arc::new(NodeInfo {
                id: DieselUlid::generate(),
                serial: 0,
            }),
            network_interface: network.clone(),
            event_store: event_store.clone(),
            transaction: state_machine.clone(),
            stats: Arc::new(Default::default()),
            wait_handler: WaitHandler::new(event_store.clone(), network),
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
        assert_eq!(coordinator.transaction, state_machine);
        assert_eq!(event_store.lock().await.events.len(), 1);
        assert_eq!(event_store.lock().await.mappings.len(), 1);
        assert_eq!(event_store.lock().await.last_applied, T::default());
        assert_eq!(
            event_store.lock().await.events.iter().next().unwrap().1,
            &Event {
                state: State::PreAccepted,
                event: Vec::new(),
                t_zero: T0(MonoTime::new_with_time(10u128, 0, 0)),
                t: T(MonoTime::new_with_time(10u128, 0, 0)),
                dependencies: HashSet::default(),
                ballot: Ballot::default(),
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
            state: State::PreAccepted,
            transaction: Vec::new(),
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
        };

        assert_eq!(coordinator.transaction, state_machine);
        assert_eq!(event_store.lock().await.events.len(), 1);
        assert_eq!(event_store.lock().await.mappings.len(), 1);
        assert_eq!(event_store.lock().await.last_applied, T::default());
        assert_eq!(
            event_store.lock().await.events.iter().next().unwrap().1,
            &Event {
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
                ballot: Ballot::default(),
            }
        );
    }

    #[tokio::test]
    async fn pre_accepted_slow_path_test() {
        let event_store = Arc::new(Mutex::new(EventStore::init(None, 1)));

        let state_machine = TransactionStateMachine {
            state: State::PreAccepted,
            transaction: Vec::new(),
            t_zero: T0(MonoTime::new_with_time(10u128, 0, 0)),
            t: T(MonoTime::new_with_time(10u128, 0, 0)),
            dependencies: HashSet::default(),
            ballot: Ballot::default(),
        };

        let network = Arc::new(NetworkMock::default());
        let mut coordinator = Coordinator {
            node: Arc::new(NodeInfo {
                id: DieselUlid::generate(),
                serial: 0,
            }),
            network_interface: network.clone(),
            event_store: event_store.clone(),
            transaction: state_machine.clone(),
            stats: Arc::new(Default::default()),
            wait_handler: WaitHandler::new(event_store.clone(), network),
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
                state: State::PreAccepted,
                event: Vec::new(),
                t_zero: T0(MonoTime::new_with_time(10u128, 0, 0)),
                t: T(MonoTime::new_with_time(12u128, 0, 1)),
                dependencies: from_dependency(deps).unwrap(),
                ballot: Ballot::default(),
            }
        );
    }
}
