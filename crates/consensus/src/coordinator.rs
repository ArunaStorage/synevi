use crate::event_store::EventStore;
use crate::node::NodeInfo;
use crate::utils::{into_dependency, T, T0};
use anyhow::Result;
use bytes::Bytes;
use consensus_transport::consensus_transport::{
    AcceptRequest, AcceptResponse, ApplyRequest, CommitRequest, CommitResponse, PreAcceptRequest,
    PreAcceptResponse, State,
};
use consensus_transport::network::{BroadcastRequest, NetworkInterface};
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
}

#[derive(Clone, Debug, Default)]
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
        // Generate timestamp
        let t_zero = MonoTime::new(0, self.node.serial);

        // Create struct
        self.transaction = TransactionStateMachine {
            state: State::PreAccepted,
            transaction,
            t_zero: T0(t_zero),
            t: T(t_zero),
            dependencies: BTreeMap::new(),
        };
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
            if x.unwrap().is_err() {
                let store = &self.event_store.lock().await.events;
                //
                let store = store
                    .iter()
                    .filter(|(_, v)| v.state.borrow().0 != State::Applied)
                    .map(|(k, v)| (k, *v.state.borrow()))
                    .collect::<Vec<_>>();
                println!(
                    "PANIC COORD: T0: {:?}, T: {:?} deps: {:?}, store: {:?} | {:?} / {}",
                    self.transaction.t_zero,
                    self.transaction.t,
                    handles.1,
                    store,
                    counter,
                    initial_len
                );
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
            .broadcast(BroadcastRequest::PreAccept(pre_accept_request), true)
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
                network_interface_clone.broadcast(BroadcastRequest::Commit(commit_request), true)
            );

            // println!(
            //     "C FP: T0: {:?}, T: {:?}, C: {:?}, Time: {:?}",
            //     self.transaction.t_zero,
            //     self.transaction.t,
            //     self.transaction.transaction,
            //     start.elapsed()
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
                .broadcast(BroadcastRequest::Accept(accept_request), true)
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
                network_interface_clone.broadcast(BroadcastRequest::Commit(commit_request), true)
            );
            // println!(
            //     "C SP: T0: {:?}, T: {:?}, C: {:?}, Time: {:?}",
            //     self.transaction.t_zero,
            //     self.transaction.t,
            //     self.transaction.transaction,
            //     start.elapsed()
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
        //     "E Broadcast: T0: {:?}, T: {:?}, C: {:?}, Time: {:?}",
        //     self.transaction.t_zero,
        //     self.transaction.t,
        //     self.transaction.transaction,
        //     start.elapsed()
        // );

        self.network_interface
            .broadcast(BroadcastRequest::Apply(apply_request), false)
            .await?; // This should not be awaited

        Ok(())
    }

    #[instrument(level = "trace")]
    async fn recover() {
        todo!("Implement recovery protocol and call when failing")
    }
}
