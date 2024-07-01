use crate::event_store::EventStore;
use crate::node::Stats;
use crate::utils::{await_dependencies, from_dependency, into_dependency, T, T0};
use anyhow::Result;
use bytes::Bytes;
use consensus_transport::consensus_transport::{
    AcceptRequest, AcceptResponse, ApplyRequest, CommitRequest, Dependency, PreAcceptRequest,
    PreAcceptResponse, RecoverRequest, RecoverResponse, State,
};
use consensus_transport::network::{BroadcastRequest, NetworkInterface, NodeInfo};
use consensus_transport::utils::IntoInner;
use monotime::MonoTime;
use std::collections::{BTreeMap, HashMap};
use std::marker::PhantomData;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::time::timeout;
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
        transaction: Bytes,
        stats: Arc<Stats>,
    ) -> Self {
        CoordinatorIterator::Initialized(Some(
            Coordinator::<Initialized>::new(
                node,
                event_store,
                network_interface,
                transaction,
                stats,
            )
            .await,
        ))
    }

    pub async fn next(&mut self) -> Result<Option<()>> {
        match self {
            CoordinatorIterator::Initialized(coordinator) => {
                if let Some(c) = coordinator.take() {
                    *self = CoordinatorIterator::PreAccepted(Some(c.pre_accept().await?));
                    Ok(Some(()))
                } else {
                    Ok(None)
                }
            }
            CoordinatorIterator::PreAccepted(coordinator) => {
                if let Some(c) = coordinator.take() {
                    *self = CoordinatorIterator::Accepted(Some(c.accept().await?));
                    Ok(Some(()))
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
    ) -> Result<()> {
        println!("{node:?}");
        let mut backoff_counter: u8 = 0;
        while backoff_counter <= MAX_RETRIES {
            let mut coordinator_iter = Coordinator::<Recover>::recover(
                node.clone(),
                event_store.clone(),
                network_interface.clone(),
                t0_recover,
                stats.clone(),
            )
            .await?;
            if let CoordinatorIterator::RestartRecovery = coordinator_iter {
                backoff_counter += 1;
                continue;
            }
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
}

#[derive(Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord)]
pub struct TransactionStateMachine {
    pub state: State,
    pub transaction: Bytes,
    pub t_zero: T0,
    pub t: T,
    pub dependencies: BTreeMap<T, T0>, // T -> T0
    pub ballot: u32,
}

impl<X> Coordinator<X> {
    #[instrument(level = "trace")]
    pub async fn new(
        node: Arc<NodeInfo>,
        event_store: Arc<Mutex<EventStore>>,
        network_interface: Arc<dyn NetworkInterface>,
        transaction: Bytes,
        stats: Arc<Stats>,
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
    ) -> Result<CoordinatorIterator> {
        let mut event_store_lock = event_store.lock().await;
        let event = event_store_lock.get_or_insert(t0_recover).await;
        let mut rx = event.state.subscribe();
        timeout(
            Duration::from_millis(RECOVER_TIMEOUT),
            rx.wait_for(|(s, _)| *s != State::Undefined),
        )
        .await??;

        // Just for sanity purposes
        assert_eq!(event.t_zero, t0_recover);

        let ballot = event.ballot + 1;
        event_store_lock.update_ballot(&t0_recover, ballot);
        drop(event_store_lock);
        
        println!("Send rcv from node: {}", node.serial);
        let recover_responses = network_interface
            .broadcast(BroadcastRequest::Recover(RecoverRequest {
                ballot,
                event: event.event.to_vec(),
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
    ) -> Result<CoordinatorIterator> {
        
        
        // Query the newest state
        let event = event_store.lock().await.get_or_insert(t0).await;
        let previous_state = event.state.borrow().0;

        let mut state_machine = TransactionStateMachine {
            transaction: event.event,
            t_zero: event.t_zero,
            ballot: event.ballot,
            ..Default::default()
        };

        // Keep track of values to replace
        let mut highest_ballot: Option<u32> = None;
        let mut superseding = false;
        let mut waiting: Vec<Dependency> = Vec::new();

        for response in responses.iter_mut() {
            if response.nack > 0 || highest_ballot.is_some() {
                match highest_ballot.as_mut() {
                    None => {
                        highest_ballot = Some(response.nack);
                    }
                    Some(b) if &response.nack > b => {
                        *b = response.nack;
                    }
                    _ => {}
                }
                continue;
            }

            let replica_t = T(MonoTime::try_from(response.timestamp.as_slice())?);

            if !response.superseding.is_empty() {
                superseding = true;
            }
            waiting.extend(std::mem::take(&mut response.wait));

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
        

        if let Some(highest_b) = highest_ballot {
            println!("Updating ballot with : {}", highest_b);
            event_store.lock().await.update_ballot(&t0, highest_b);
            if previous_state != State::Applied {
                println!("Waiting for timeout ...");
                let mut rx = event.state.subscribe();
                timeout(
                    Duration::from_millis(RECOVER_TIMEOUT),
                    rx.wait_for(|(s, _)| *s > previous_state),
                )
                .await??;
                return Ok(CoordinatorIterator::Recovering);
            }
            return Ok(CoordinatorIterator::Applied);
        }

        // Wait for deps
        println!("Reached state {:?}; Node: {}", state_machine.state, node.serial);

        Ok(match state_machine.state {
            State::Applied => CoordinatorIterator::Committed(Some(Coordinator::<Committed> {
                node,
                network_interface,
                event_store,
                transaction: state_machine,
                stats,
                phantom: Default::default(),
            })),
            State::Commited => CoordinatorIterator::Accepted(Some(Coordinator::<Accepted> {
                node,
                network_interface,
                event_store,
                transaction: state_machine,
                stats,
                phantom: Default::default(),
            })),
            State::Accepted => CoordinatorIterator::PreAccepted(Some(Coordinator::<PreAccepted> {
                node,
                network_interface,
                event_store,
                transaction: state_machine,
                stats,
                phantom: Default::default(),
            })),

            State::PreAccepted => {
                if superseding {
                    println!("Starting preaccept");
                    CoordinatorIterator::PreAccepted(Some(Coordinator::<PreAccepted> {
                        node,
                        network_interface,
                        event_store,
                        transaction: state_machine,
                        stats,
                        phantom: Default::default(),
                    }))
                } else if !waiting.is_empty() {
                    let mut rx = event.state.subscribe();
                    timeout(
                        Duration::from_millis(RECOVER_TIMEOUT),
                        rx.wait_for(|(s, _)| *s > previous_state),
                    )
                    .await??;
                    return Ok(CoordinatorIterator::RestartRecovery);
                } else {
                    state_machine.t = T(*state_machine.t_zero);
                    CoordinatorIterator::PreAccepted(Some(Coordinator::<PreAccepted> {
                        node,
                        network_interface,
                        event_store,
                        transaction: state_machine,
                        stats,
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
    pub async fn pre_accept(mut self) -> Result<Coordinator<PreAccepted>> {
        self.stats.total_requests.fetch_add(1, Ordering::Relaxed);

        // Create the PreAccepted msg
        let pre_accepted_request = PreAcceptRequest {
            event: self.transaction.transaction.to_vec(),
            timestamp_zero: (*self.transaction.t_zero).into(),
        };

        let pre_accepted_responses = self
            .network_interface
            .broadcast(BroadcastRequest::PreAccept(pre_accepted_request))
            .await?;

        self.pre_accept_consensus(
            &pre_accepted_responses
                .into_iter()
                .map(|res| res.into_inner())
                .collect::<Result<Vec<_>>>()?,
        )
        .await?;

        Ok(Coordinator::<PreAccepted> {
            node: self.node,
            network_interface: self.network_interface,
            event_store: self.event_store,
            transaction: self.transaction,
            stats: self.stats,
            phantom: PhantomData,
        })
    }

    #[instrument(level = "trace", skip(self))]
    async fn pre_accept_consensus(&mut self, responses: &[PreAcceptResponse]) -> Result<()> {
        // Collect deps by t_zero and only keep the max t
        let mut dependencies_inverted = HashMap::new(); // TZero -> MaxT
        for response in responses {
            let t_response = T(MonoTime::try_from(response.timestamp.as_slice())?);
            if t_response > self.transaction.t {
                self.transaction.t = t_response;
            }
            for dep in response.dependencies.iter() {
                let t = T(MonoTime::try_from(dep.timestamp.as_slice())?);
                let t_zero = T0(MonoTime::try_from(dep.timestamp_zero.as_slice())?);
                if t_zero != self.transaction.t_zero {
                    let entry = dependencies_inverted.entry(t_zero).or_insert(t);
                    if t > *entry {
                        *entry = t;
                    }
                }
            }
        }
        // Invert map to BTreeMap with t -> t_zero
        self.transaction.dependencies = dependencies_inverted
            .iter()
            .map(|(t_zero, t)| (*t, *t_zero))
            .collect();

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
    pub async fn accept(mut self) -> Result<Coordinator<Accepted>> {
        // Safeguard: T0 <= T
        assert!(*self.transaction.t_zero <= *self.transaction.t);

        if *self.transaction.t_zero != *self.transaction.t {
            self.stats.total_accepts.fetch_add(1, Ordering::Relaxed);
            let accepted_request = AcceptRequest {
                ballot: self.transaction.ballot,
                event: self.transaction.transaction.clone().into(),
                timestamp_zero: (*self.transaction.t_zero).into(),
                timestamp: (*self.transaction.t).into(),
                dependencies: into_dependency(self.transaction.dependencies.clone()),
            };
            let accepted_responses = self
                .network_interface
                .broadcast(BroadcastRequest::Accept(accepted_request))
                .await?;

            self.accept_consensus(
                &accepted_responses
                    .into_iter()
                    .map(|res| res.into_inner())
                    .collect::<Result<Vec<_>>>()?,
            )
            .await?;
        }

        Ok(Coordinator::<Accepted> {
            node: self.node,
            network_interface: self.network_interface,
            event_store: self.event_store,
            transaction: self.transaction,
            stats: self.stats,
            phantom: PhantomData,
        })
    }

    #[instrument(level = "trace", skip(self))]
    async fn accept_consensus(&mut self, responses: &[AcceptResponse]) -> Result<()> {
        // A little bit redundant, but I think the alternative to create a common behavior between responses may be even worse
        // Handle returned dependencies
        let mut dependencies_inverted = HashMap::new(); // TZero -> MaxT
        for response in responses {
            for dep in response.dependencies.iter() {
                let t = T(MonoTime::try_from(dep.timestamp.as_slice())?);
                let t_zero = T0(MonoTime::try_from(dep.timestamp_zero.as_slice())?);
                if t_zero != self.transaction.t_zero {
                    let entry = dependencies_inverted.entry(t_zero).or_insert(t);
                    if t > *entry {
                        *entry = t;
                    }
                }
            }
        }
        // Invert map to BTreeMap with t -> t_zero
        self.transaction.dependencies = dependencies_inverted
            .iter()
            .map(|(t_zero, t)| (*t, *t_zero))
            .collect();

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
            event: self.transaction.transaction.to_vec(),
            timestamp_zero: (*self.transaction.t_zero).into(),
            timestamp: (*self.transaction.t).into(),
            dependencies: into_dependency(self.transaction.dependencies.clone()),
        };
        let network_interface_clone = self.network_interface.clone();

        let (committed_result, broadcast_result) = tokio::join!(
            self.commit_consensus(),
            network_interface_clone.broadcast(BroadcastRequest::Commit(committed_request))
        );

        committed_result?; // TODO Recovery
        broadcast_result?; // TODO Recovery

        Ok(Coordinator::<Committed> {
            node: self.node,
            network_interface: self.network_interface,
            event_store: self.event_store,
            transaction: self.transaction,
            stats: self.stats,
            phantom: PhantomData,
        })
    }

    #[instrument(level = "trace", skip(self))]
    async fn commit_consensus(&mut self) -> Result<()> {
        self.transaction.state = State::Commited;
        self.event_store
            .lock()
            .await
            .upsert((&self.transaction).into())
            .await;
        Box::pin(await_dependencies(
            self.node.clone(),
            self.event_store.clone(),
            &self.transaction.dependencies,
            self.network_interface.clone(),
            self.transaction.t,
            self.stats.clone(),
        ))
        .await?;
        Ok(())
    }
}

impl Coordinator<Committed> {
    #[instrument(level = "trace", skip(self))]
    pub async fn apply(mut self) -> Result<Coordinator<Applied>> {
        self.execute_consensus().await?;

        let applied_request = ApplyRequest {
            event: self.transaction.transaction.to_vec(),
            timestamp: (*self.transaction.t).into(),
            timestamp_zero: (*self.transaction.t_zero).into(),
            dependencies: into_dependency(self.transaction.dependencies.clone()),
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
            phantom: PhantomData,
        })
    }

    #[instrument(level = "trace", skip(self))]
    async fn execute_consensus(&mut self) -> Result<()> {
        self.transaction.state = State::Applied;
        self.event_store
            .lock()
            .await
            .upsert((&self.transaction).into())
            .await;

        // TODO: Apply in backend
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
        utils::{T, T0},
    };
    use bytes::Bytes;
    use consensus_transport::{
        consensus_transport::{Dependency, PreAcceptResponse, State},
        network::NodeInfo,
    };
    use diesel_ulid::DieselUlid;
    use monotime::MonoTime;
    use std::{collections::BTreeMap, sync::Arc, vec};
    use tokio::sync::Mutex;

    #[tokio::test]
    async fn init_test() {
        let event_store = Arc::new(Mutex::new(EventStore::init()));
        let coordinator = Coordinator::<Initialized>::new(
            Arc::new(NodeInfo {
                id: DieselUlid::generate(),
                serial: 0,
            }),
            event_store,
            Arc::new(NetworkMock {}),
            Bytes::from("test"),
            Arc::new(Default::default()),
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
        let event_store = Arc::new(Mutex::new(EventStore::init()));

        let state_machine = TransactionStateMachine {
            state: State::PreAccepted,
            transaction: Bytes::new(),
            t_zero: T0(MonoTime::new_with_time(10u128, 0, 0)),
            t: T(MonoTime::new_with_time(10u128, 0, 0)),
            dependencies: BTreeMap::default(),
            ballot: 0,
        };
        let mut coordinator = Coordinator::<Initialized> {
            node: Arc::new(NodeInfo {
                id: DieselUlid::generate(),
                serial: 0,
            }),
            network_interface: Arc::new(NetworkMock {}),
            event_store: event_store.clone(),
            transaction: state_machine.clone(),
            stats: Arc::new(Default::default()),
            phantom: Default::default(),
        };

        let pre_accepted_ok = vec![
            PreAcceptResponse {
                timestamp: MonoTime::new_with_time(10u128, 0, 0).into(),
                dependencies: vec![],
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
                state: tokio::sync::watch::Sender::new((
                    State::PreAccepted,
                    T(MonoTime::new_with_time(10u128, 0, 0))
                )),
                event: Bytes::new(),
                t_zero: T0(MonoTime::new_with_time(10u128, 0, 0)),
                t: T(MonoTime::new_with_time(10u128, 0, 0)),
                dependencies: BTreeMap::default(),
                ballot: 0,
            }
        );

        // FastPath with dependencies

        let pre_accepted_ok = vec![
            PreAcceptResponse {
                timestamp: MonoTime::new_with_time(10u128, 0, 0).into(),
                dependencies: vec![Dependency {
                    timestamp_zero: MonoTime::new_with_time(1u128, 0, 0).into(),
                    timestamp: MonoTime::new_with_time(1u128, 0, 0).into(),
                }],
            },
            PreAcceptResponse {
                timestamp: MonoTime::new_with_time(10u128, 0, 0).into(),
                dependencies: vec![
                    Dependency {
                        timestamp_zero: MonoTime::new_with_time(1u128, 0, 0).into(),
                        timestamp: MonoTime::new_with_time(1u128, 0, 0).into(),
                    },
                    Dependency {
                        timestamp_zero: MonoTime::new_with_time(3u128, 0, 0).into(),
                        timestamp: MonoTime::new_with_time(3u128, 0, 0).into(),
                    },
                ],
            },
            PreAcceptResponse {
                timestamp: MonoTime::new_with_time(10u128, 0, 0).into(),
                dependencies: vec![
                    Dependency {
                        timestamp_zero: MonoTime::new_with_time(1u128, 0, 0).into(),
                        timestamp: MonoTime::new_with_time(1u128, 0, 0).into(),
                    },
                    Dependency {
                        timestamp_zero: MonoTime::new_with_time(2u128, 0, 0).into(),
                        timestamp: MonoTime::new_with_time(2u128, 0, 0).into(),
                    },
                ],
            },
        ];

        coordinator
            .pre_accept_consensus(&pre_accepted_ok)
            .await
            .unwrap();

        let state_machine = TransactionStateMachine {
            state: State::PreAccepted,
            transaction: Bytes::new(),
            t_zero: T0(MonoTime::new_with_time(10u128, 0, 0)),
            t: T(MonoTime::new_with_time(10u128, 0, 0)),
            dependencies: BTreeMap::from_iter(
                [
                    (
                        T(MonoTime::new_with_time(1u128, 0, 0)),
                        T0(MonoTime::new_with_time(1u128, 0, 0)),
                    ),
                    (
                        T(MonoTime::new_with_time(2u128, 0, 0)),
                        T0(MonoTime::new_with_time(2u128, 0, 0)),
                    ),
                    (
                        T(MonoTime::new_with_time(3u128, 0, 0)),
                        T0(MonoTime::new_with_time(3u128, 0, 0)),
                    ),
                ]
                .iter()
                .cloned(),
            ),
            ballot: 0,
        };

        assert_eq!(coordinator.transaction, state_machine);
        assert_eq!(event_store.lock().await.events.len(), 1);
        assert_eq!(event_store.lock().await.mappings.len(), 1);
        assert_eq!(event_store.lock().await.last_applied, T::default());
        assert_eq!(
            event_store.lock().await.events.iter().next().unwrap().1,
            &Event {
                state: tokio::sync::watch::Sender::new((
                    State::PreAccepted,
                    T(MonoTime::new_with_time(10u128, 0, 0))
                )),
                event: Bytes::new(),
                t_zero: T0(MonoTime::new_with_time(10u128, 0, 0)),
                t: T(MonoTime::new_with_time(10u128, 0, 0)),
                dependencies: BTreeMap::from_iter(
                    [
                        (
                            T(MonoTime::new_with_time(1u128, 0, 0)),
                            T0(MonoTime::new_with_time(1u128, 0, 0))
                        ),
                        (
                            T(MonoTime::new_with_time(2u128, 0, 0)),
                            T0(MonoTime::new_with_time(2u128, 0, 0))
                        ),
                        (
                            T(MonoTime::new_with_time(3u128, 0, 0)),
                            T0(MonoTime::new_with_time(3u128, 0, 0))
                        )
                    ]
                    .iter()
                    .cloned()
                ),
                ballot: 0,
            }
        );
    }

    #[tokio::test]
    async fn pre_accepted_slow_path_test() {
        let event_store = Arc::new(Mutex::new(EventStore::init()));

        let state_machine = TransactionStateMachine {
            state: State::PreAccepted,
            transaction: Bytes::new(),
            t_zero: T0(MonoTime::new_with_time(10u128, 0, 0)),
            t: T(MonoTime::new_with_time(10u128, 0, 0)),
            dependencies: BTreeMap::default(),
            ballot: 0,
        };
        let mut coordinator = Coordinator {
            node: Arc::new(NodeInfo {
                id: DieselUlid::generate(),
                serial: 0,
            }),
            network_interface: Arc::new(NetworkMock {}),
            event_store: event_store.clone(),
            transaction: state_machine.clone(),
            stats: Arc::new(Default::default()),
            phantom: Default::default(),
        };

        let pre_accepted_ok = vec![
            PreAcceptResponse {
                timestamp: MonoTime::new_with_time(12u128, 0, 1).into(),
                dependencies: vec![Dependency {
                    timestamp_zero: T(MonoTime::new_with_time(11u128, 0, 1)).into(),
                    timestamp: T0(MonoTime::new_with_time(13u128, 0, 1)).into(),
                }],
            },
            PreAcceptResponse {
                timestamp: MonoTime::new_with_time(10u128, 0, 0).into(),
                dependencies: vec![],
            },
            PreAcceptResponse {
                timestamp: MonoTime::new_with_time(10u128, 0, 0).into(),
                dependencies: vec![],
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
                state: tokio::sync::watch::Sender::new((
                    State::PreAccepted,
                    T(MonoTime::new_with_time(12u128, 0, 1))
                )),
                event: Bytes::new(),
                t_zero: T0(MonoTime::new_with_time(10u128, 0, 0)),
                t: T(MonoTime::new_with_time(12u128, 0, 1)),
                dependencies: BTreeMap::from_iter([(
                    T(MonoTime::new_with_time(13u128, 0, 1)),
                    T0(MonoTime::new_with_time(11u128, 0, 1))
                ),]),
                ballot: 0,
            }
        );
    }
}
