use crate::event_store::EventStore;
use crate::utils::{from_dependency, into_dependency, T, T0};
use anyhow::Result;
use bytes::Bytes;
use consensus_transport::consensus_transport::{
    AcceptRequest, AcceptResponse, ApplyRequest, CommitRequest, CommitResponse, Dependency,
    PreAcceptRequest, PreAcceptResponse, RecoverRequest, RecoverResponse, State,
};
use consensus_transport::network::{BroadcastRequest, NetworkInterface, NodeInfo};
use consensus_transport::utils::IntoInner;
use monotime::MonoTime;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time;
use tokio::sync::Mutex;
use tracing::instrument;

pub struct Coordinator {
    pub node: Arc<NodeInfo>,
    pub network_interface: Arc<dyn NetworkInterface>,
    pub event_store: Arc<Mutex<EventStore>>,
    pub transaction: TransactionStateMachine,
}

trait StateMachine {
    async fn init(&mut self, transaction: Bytes);
    async fn pre_accept(&mut self, responses: &[PreAcceptResponse]) -> Result<()>;
    async fn accept(&mut self, responses: &[AcceptResponse]) -> Result<()>;
    async fn commit(&mut self) -> Result<()>;
    async fn execute(&mut self, responses: &[CommitResponse]) -> Result<()>;
    async fn recover(&mut self, responses: &[RecoverResponse]) -> Result<RecoverState>;
}

#[derive(Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord)]
pub struct TransactionStateMachine {
    pub state: State,
    pub transaction: Bytes,
    pub t_zero: T0,
    pub t: T,
    pub dependencies: BTreeMap<T, T0>, // Consists of t and t_zero
}

impl StateMachine for Coordinator {
    #[instrument(level = "trace", skip(self))]
    async fn init(&mut self, transaction: Bytes) {
        // Create struct
        self.transaction = self
            .event_store
            .lock()
            .await
            .init_transaction(transaction, self.node.serial)
            .await;
    }

    #[instrument(level = "trace", skip(self))]
    async fn pre_accept(&mut self, responses: &[PreAcceptResponse]) -> Result<()> {
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

    #[instrument(level = "trace", skip(self))]
    async fn accept(&mut self, responses: &[AcceptResponse]) -> Result<()> {
        // A little bit redundant but I think the alternative to create a common behavior between responses may be even worse
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

    #[instrument(level = "trace", skip(self))]
    async fn commit(&mut self) -> Result<()> {
        self.transaction.state = State::Commited;
        self.event_store
            .lock()
            .await
            .upsert((&self.transaction).into())
            .await;

        let mut handles = self
            .event_store
            .lock()
            .await
            .create_wait_handles(self.transaction.dependencies.clone(), self.transaction.t)
            .await?;

        let initial_len = handles.0.len();
        let mut counter = 0;
        while let Some(x) = handles.0.join_next().await {
            if let Err(e) = x.unwrap() {
                let store = &self.event_store.lock().await.events;
                //
                let store_filtered = store
                    .iter()
                    .map(|(k, v)| (k, *v.state.borrow()))
                    .collect::<Vec<_>>();

                let not_in_vec: Vec<_> = self
                    .transaction
                    .dependencies
                    .iter()
                    .filter(|(_, b)| store.get(b).is_none())
                    .collect();

                let smallest_t = store
                    .iter()
                    .filter(|(_, b)| b.state.borrow().0 <= State::Commited)
                    .min_by(|(_, event1), (_, event2)| event1.t.cmp(&event2.t))
                    .unwrap()
                    .1;

                let filtered_smallest_t: Vec<_> = smallest_t
                    .dependencies
                    .iter()
                    .filter(|(a, b)| {
                        store.get(b).unwrap().state.borrow().0 != State::Applied
                            && *a < &smallest_t.t
                    })
                    .collect();

                println!("PANIC COORD");
                println!(
                    "T0: {:?}, T: {:?}, NOT IN EV: {:?}, {:?} / {}",
                    self.transaction.t_zero, self.transaction.t, not_in_vec, counter, initial_len
                );
                println!("Error: {:?}", e);
                println!("Dependencies: {:?}", self.transaction.dependencies);
                println!("Store: {:?}", store_filtered);
                println!("Hanger: {:?}", smallest_t);
                println!("Hanger_not_applied: {:?}", filtered_smallest_t);

                let mut handles = self
                    .event_store
                    .lock()
                    .await
                    .create_wait_handles(smallest_t.dependencies.clone(), smallest_t.t)
                    .await?;

                while let Some(x) = handles.0.join_next().await {
                    if let Err(e) = x.unwrap() {
                        println!("Smallest T hangs on: {:?}", e);
                    }
                }
                println!("No reason to wait!");
                panic!()
            }
            counter += 1;
            // TODO: Recovery when timeout
        }
        Ok(())
    }

    #[instrument(level = "trace", skip(self))]
    async fn execute(&mut self, _responses: &[CommitResponse]) -> Result<()> {
        // TODO: Read commit responses and calc client response

        self.transaction.state = State::Applied;
        self.event_store
            .lock()
            .await
            .upsert((&self.transaction).into())
            .await;

        Ok(())
    }

    #[instrument(level = "trace", skip(self))]
    async fn recover(&mut self, responses: &[RecoverResponse]) -> Result<RecoverState> {
        let mut highest_state = self.transaction.state;
        let mut highest_t = self.transaction.t;
        let mut superseding = Vec::new();
        let mut waiting = Vec::new();
        for res in responses {
            superseding.extend(res.superseding.clone());
            waiting.extend(res.wait.clone());
            // Check if we can continue state machine
            let local_state = res.local_state();
            if local_state > highest_state && local_state != State::Recover {
                // Overwrite the highest state
                highest_state = local_state;

                // Set new state
                self.transaction.state = highest_state;
                // Set accepted/committed/applied t
                self.transaction.t = T(MonoTime::try_from(res.timestamp.as_slice())?);

                // Set dependencies
                let mut deps = res.wait.clone();
                deps.extend(res.superseding.clone());
                self.transaction.dependencies = from_dependency(deps)?;
            }

            // Propose highest t if we cannot continue state machine
            let local_timestamp = T(MonoTime::try_from(res.timestamp.as_slice())?);
            if local_timestamp > highest_t {
                highest_t = local_timestamp;
            }
        }
        
        Ok(match highest_state {
            State::Accepted => {
                self.transaction.t = highest_t;
                RecoverState::Accepted
            }
            State::Commited => {RecoverState::Committed}
            State::Applied => {RecoverState::Applied}
            _ => {
                if !superseding.is_empty() {
                    self.transaction.t = highest_t;
                   RecoverState::HighestT 
                } else if !waiting.is_empty() {
                    RecoverState::Retry
                } else {
                    RecoverState::ProposeT0
                }
            }
        })
    }
}

#[derive(Debug)]
enum RecoverState {
    Applied,
    Committed,
    Accepted,
    HighestT,
    Retry,
    ProposeT0,
}

impl Coordinator {
    #[instrument(level = "trace")]
    pub fn new(
        node: Arc<NodeInfo>,
        network_interface: Arc<dyn NetworkInterface>,
        event_store: Arc<Mutex<EventStore>>,
    ) -> Self {
        Coordinator {
            node,
            network_interface,
            event_store,
            transaction: Default::default(),
        }
    }

    #[instrument(level = "trace", skip(self))]
    pub async fn transaction(&mut self, transaction: Bytes) -> Result<()> {
        //
        //  INIT
        //

        // Create a new transaction state
        // Question: When should this be persisted?
        self.init(transaction).await;

        //
        //  PRE ACCEPT STATE
        //
        let network_interface_clone = self.network_interface.clone();

        let _start = time::Instant::now();

        // Create the PreAccept msg
        let pre_accept_request = PreAcceptRequest {
            node: self.node.id.to_string(),
            timestamp_zero: (*self.transaction.t_zero).into(),
            event: self.transaction.transaction.clone().into(),
        };

        // Broadcast message

        let pre_accept_responses = self
            .network_interface
            .broadcast(BroadcastRequest::PreAccept(pre_accept_request))
            .await?;

        // println!(
        //     "PA: T0: {:?}, T: {:?}, C: {:?}, Time: {:?}",
        //     self.transaction.t_zero,
        //     self.transaction.t,
        //     self.transaction.transaction,
        //     start.elapsed()
        // );

        // Collect responses
        self.pre_accept(
            &pre_accept_responses
                .into_iter()
                .map(|res| res.into_inner())
                .collect::<Result<Vec<_>>>()?,
        )
        .await?;

        let commit_responses = if *self.transaction.t_zero == *self.transaction.t {
            //
            //   FAST PATH
            //

            // println!(
            //     "FP: T0: {:?}, T: {:?}, C: {:?}, Time: {:?}, deps: {:?}",
            //     self.transaction.t_zero,
            //     self.transaction.t,
            //     self.transaction.transaction,
            //     start.elapsed(),
            //     self.transaction.dependencies
            // );
            // Commit
            let commit_request = CommitRequest {
                node: self.node.id.to_string(),
                event: self.transaction.transaction.clone().into(),
                timestamp_zero: (*self.transaction.t_zero).into(),
                timestamp: (*self.transaction.t).into(),
                dependencies: into_dependency(self.transaction.dependencies.clone()),
            };

            let (commit_result, broadcast_result) = tokio::join!(
                self.commit(),
                network_interface_clone.broadcast(BroadcastRequest::Commit(commit_request))
            );

            // println!(
            //     "C FP: T0: {:?}, T: {:?}",
            //     self.transaction.t_zero, self.transaction.t,
            // );

            commit_result?;
            broadcast_result?
        } else {
            //
            //  SLOW PATH
            //

            // Accept
            // println!(
            //     "A SP: T0: {:?}, T: {:?}, C: {:?}, Time: {:?}",
            //     self.transaction.t_zero,
            //     self.transaction.t,
            //     self.transaction.transaction,
            //     start.elapsed()
            // );

            let accept_request = AcceptRequest {
                node: self.node.id.to_string(),
                event: self.transaction.transaction.clone().into(),
                timestamp_zero: (*self.transaction.t_zero).into(),
                timestamp: (*self.transaction.t).into(),
                dependencies: into_dependency(self.transaction.dependencies.clone()),
            };
            let accept_responses = self
                .network_interface
                .broadcast(BroadcastRequest::Accept(accept_request))
                .await?;

            self.accept(
                &accept_responses
                    .into_iter()
                    .map(|res| res.into_inner())
                    .collect::<Result<Vec<_>>>()?,
            )
            .await?;

            // println!(
            //     "A -> C SP: T0: {:?}, T: {:?}, C: {:?}, Time: {:?}, deps: {:?}",
            //     self.transaction.t_zero,
            //     self.transaction.t,
            //     self.transaction.transaction,
            //     start.elapsed(),
            //     self.transaction.dependencies
            // );

            // Commit
            let commit_request = CommitRequest {
                node: self.node.id.to_string(),
                event: self.transaction.transaction.clone().into(),
                timestamp_zero: (*self.transaction.t_zero).into(),
                timestamp: (*self.transaction.t).into(),
                dependencies: into_dependency(self.transaction.dependencies.clone()),
            };

            let (commit_result, broadcast_result) = tokio::join!(
                self.commit(),
                network_interface_clone.broadcast(BroadcastRequest::Commit(commit_request))
            );
            // println!(
            //     "C SP: T0: {:?}, T: {:?}",
            //     self.transaction.t_zero, self.transaction.t,
            // );

            commit_result?;
            broadcast_result?
        };

        //
        // Execution
        //
        // println!(
        //     "E: T0: {:?}, T: {:?}, C: {:?}, Time: {:?}",
        //     self.transaction.t_zero,
        //     self.transaction.t,
        //     self.transaction.transaction,
        //     start.elapsed()
        // );

        self.execute(
            &commit_responses
                .into_iter()
                .map(|res| -> Result<CommitResponse> { res.into_inner() })
                .collect::<Result<Vec<CommitResponse>>>()?,
        )
        .await?;

        let apply_request = ApplyRequest {
            node: self.node.id.to_string(),
            event: self.transaction.transaction.to_vec(),
            timestamp: (*self.transaction.t).into(),
            timestamp_zero: (*self.transaction.t_zero).into(),
            dependencies: into_dependency(self.transaction.dependencies.clone()),
            result: vec![], // Theoretically not needed right?
        };

        // println!(
        //     "E Broadcast: T0: {:?}, T: {:?}",
        //     self.transaction.t_zero, self.transaction.t,
        // );

        self.network_interface
            .broadcast(BroadcastRequest::Apply(apply_request))
            .await?; // This should not be awaited

        Ok(())
    }
    
    async fn continue_from_applied() -> Result<()> {
        todo!()
    }
    async fn continue_from_committed() -> Result<()> {
        todo!()
    }
    async fn continue_from_accepted() -> Result<()> {
        todo!()
    }

    #[instrument(level = "trace", skip(self))]
    async fn recovery(&mut self, t0: T0, transaction: Bytes) -> Result<()> {
        let recover_request = RecoverRequest {
            node: "".to_string(),
            event: transaction.to_vec(),
            timestamp_zero: t0.into(),
        };
        let responses = self
            .network_interface
            .broadcast(BroadcastRequest::Recover(recover_request))
            .await?;
        
        let path_forward = self.recover(&responses
                .into_iter()
                .map(|res| -> Result<RecoverResponse> { res.into_inner() })
                .collect::<Result<Vec<RecoverResponse>>>()?,).await?;
        
        match path_forward {
            RecoverState::Applied => {
                todo!("Continue apply path")
            }
            RecoverState::Committed => {
                todo!("Continue commit path")
            }
            RecoverState::Accepted => {
                todo!("Continue accept path")
            }
            RecoverState::HighestT => {
                todo!("Propose highest t")
            }
            RecoverState::Retry => {
                todo!("Wait and retry")
            }
            RecoverState::ProposeT0 => {
                todo!("Propose t0")
            }
        }
        
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::Coordinator;
    use crate::{
        coordinator::{StateMachine, TransactionStateMachine},
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
    use tracing::event;

    #[tokio::test]
    async fn init_test() {
        let event_store = Arc::new(Mutex::new(EventStore::init()));
        let mut coordinator = Coordinator::new(
            Arc::new(NodeInfo {
                id: DieselUlid::generate(),
                serial: 0,
            }),
            Arc::new(NetworkMock {}),
            event_store,
        );
        coordinator.init(Bytes::from("test")).await;

        assert_eq!(coordinator.transaction.state, State::PreAccepted);
        assert_eq!(coordinator.transaction.transaction, Bytes::from("test"));
        assert_eq!(*coordinator.transaction.t_zero, *coordinator.transaction.t);
        assert_eq!(coordinator.transaction.t_zero.0.get_node(), 0);
        assert_eq!(coordinator.transaction.t_zero.0.get_seq(), 1);
        assert!(coordinator.transaction.dependencies.is_empty());
    }

    #[tokio::test]
    async fn pre_accept_fast_path_test() {
        let event_store = Arc::new(Mutex::new(EventStore::init()));

        let state_machine = TransactionStateMachine {
            state: State::PreAccepted,
            transaction: Bytes::new(),
            t_zero: T0(MonoTime::new_with_time(10u128, 0, 0)),
            t: T(MonoTime::new_with_time(10u128, 0, 0)),
            dependencies: BTreeMap::default(),
        };
        let mut coordinator = Coordinator {
            node: Arc::new(NodeInfo {
                id: DieselUlid::generate(),
                serial: 0,
            }),
            network_interface: Arc::new(NetworkMock {}),
            event_store: event_store.clone(),
            transaction: state_machine.clone(),
        };

        let preaccept_ok = vec![
            PreAcceptResponse {
                node: "a".to_string(),
                timestamp_zero: MonoTime::new_with_time(10u128, 0, 0).into(),
                timestamp: MonoTime::new_with_time(10u128, 0, 0).into(),
                dependencies: vec![],
            };
            3
        ];

        coordinator.pre_accept(&preaccept_ok).await.unwrap();
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
            }
        );

        // FastPath with dependencies

        let preaccept_ok = vec![
            PreAcceptResponse {
                node: "a".to_string(),
                timestamp_zero: MonoTime::new_with_time(10u128, 0, 0).into(),
                timestamp: MonoTime::new_with_time(10u128, 0, 0).into(),
                dependencies: vec![Dependency {
                    timestamp_zero: MonoTime::new_with_time(1u128, 0, 0).into(),
                    timestamp: MonoTime::new_with_time(1u128, 0, 0).into(),
                }],
            },
            PreAcceptResponse {
                node: "a".to_string(),
                timestamp_zero: MonoTime::new_with_time(10u128, 0, 0).into(),
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
                node: "a".to_string(),
                timestamp_zero: MonoTime::new_with_time(10u128, 0, 0).into(),
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

        coordinator.pre_accept(&preaccept_ok).await.unwrap();

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
            }
        );
    }

    #[tokio::test]
    async fn pre_accept_slow_path_test() {
        let event_store = Arc::new(Mutex::new(EventStore::init()));

        let state_machine = TransactionStateMachine {
            state: State::PreAccepted,
            transaction: Bytes::new(),
            t_zero: T0(MonoTime::new_with_time(10u128, 0, 0)),
            t: T(MonoTime::new_with_time(10u128, 0, 0)),
            dependencies: BTreeMap::default(),
        };
        let mut coordinator = Coordinator {
            node: Arc::new(NodeInfo {
                id: DieselUlid::generate(),
                serial: 0,
            }),
            network_interface: Arc::new(NetworkMock {}),
            event_store: event_store.clone(),
            transaction: state_machine.clone(),
        };

        let preaccept_ok = vec![
            PreAcceptResponse {
                node: "a".to_string(),
                timestamp_zero: MonoTime::new_with_time(10u128, 0, 0).into(),
                timestamp: MonoTime::new_with_time(12u128, 0, 1).into(),
                dependencies: vec![Dependency {
                    timestamp_zero: T(MonoTime::new_with_time(11u128, 0, 1)).into(),
                    timestamp: T0(MonoTime::new_with_time(13u128, 0, 1)).into(),
                }],
            },
            PreAcceptResponse {
                node: "b".to_string(),
                timestamp_zero: MonoTime::new_with_time(10u128, 0, 0).into(),
                timestamp: MonoTime::new_with_time(10u128, 0, 0).into(),
                dependencies: vec![],
            },
            PreAcceptResponse {
                node: "b".to_string(),
                timestamp_zero: MonoTime::new_with_time(10u128, 0, 0).into(),
                timestamp: MonoTime::new_with_time(10u128, 0, 0).into(),
                dependencies: vec![],
            },
        ];

        coordinator.pre_accept(&preaccept_ok).await.unwrap();
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
            }
        );
    }
}
