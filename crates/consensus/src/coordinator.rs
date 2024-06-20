use crate::event_store::EventStore;
use crate::node::{Member, NodeInfo};
use crate::utils::{into_dependency, IntoInner};
use anyhow::Result;
use bytes::Bytes;
use consensus_transport::consensus_transport::consensus_transport_client::ConsensusTransportClient;
use consensus_transport::consensus_transport::{
    AcceptRequest, AcceptResponse, ApplyRequest, ApplyResponse, CommitRequest, CommitResponse,
    PreAcceptRequest, PreAcceptResponse, State,
};
use monotime::MonoTime;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time;
use tokio::sync::Mutex;
use tokio::task::JoinSet;
use tracing::instrument;

pub struct Coordinator {
    pub node: Arc<NodeInfo>,
    pub members: Vec<Arc<Member>>,
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
    pub t_zero: MonoTime,
    pub t: MonoTime,
    pub dependencies: BTreeMap<MonoTime, MonoTime>, // Consists of t and t_zero
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
            t_zero,
            t: t_zero,
            dependencies: BTreeMap::new(),
        };
    }

    #[instrument(level = "trace", skip(self))]
    async fn pre_accept(&mut self, responses: &[PreAcceptResponse]) -> Result<()> {
        // Collect deps by t_zero and only keep the max t
        let mut dependencies_inverted = HashMap::new(); // TZero -> MaxT
        for response in responses {
            let t_response = MonoTime::try_from(response.timestamp.as_slice())?;
            if t_response > self.transaction.t {
                self.transaction.t = t_response;
            }
            for dep in response.dependencies.iter() {
                let t = MonoTime::try_from(dep.timestamp.as_slice())?;
                let t_zero = MonoTime::try_from(dep.timestamp_zero.as_slice())?;
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
                let t = MonoTime::try_from(dep.timestamp.as_slice())?;
                let t_zero = MonoTime::try_from(dep.timestamp_zero.as_slice())?;
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
            .create_wait_handles(
                self.transaction.dependencies.clone(),
                self.transaction.t_zero,
            )
            .await?;

        while let Some(x) = handles.join_next().await {
            x??
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

#[derive(Debug)]
enum ConsensusRequest {
    PreAccept(PreAcceptRequest),
    Accept(AcceptRequest),
    Commit(CommitRequest),
    Apply(ApplyRequest),
    // TODO: Recover
}

#[derive(Debug)]
pub(crate) enum ConsensusResponse {
    PreAccept(PreAcceptResponse),
    Accept(AcceptResponse),
    Commit(CommitResponse),
    Apply(ApplyResponse),
    // TODO: Recover
}

impl Coordinator {
    #[instrument(level = "trace")]
    pub fn new(
        node: Arc<NodeInfo>,
        members: Vec<Arc<Member>>,
        event_store: Arc<Mutex<EventStore>>,
    ) -> Self {
        Coordinator {
            node,
            members,
            event_store,
            transaction: Default::default(),
        }
    }
    #[instrument(level = "trace", skip(self))]
    pub async fn add_members(&self, members: Vec<Member>) -> Result<()> {
        todo!("Add members to self")
    }

    #[instrument(level = "trace", skip(self))]
    pub async fn transaction(&mut self, transaction: Bytes) -> Result<()> {
        //
        //  INIT
        //

        // We need a fixed size of members for each transaction
        let members = self.members.clone();

        // Create a new transaction state
        self.init(transaction).await;

        //
        //  PRE ACCEPT STATE
        //

        let _start = time::Instant::now();

        // Create the PreAccept msg
        let pre_accept_request = PreAcceptRequest {
            node: self.node.id.to_string(),
            timestamp_zero: self.transaction.t_zero.into(),
            event: self.transaction.transaction.clone().into(),
        };

        // Broadcast message
        let pre_accept_responses = Coordinator::broadcast(
            &members,
            ConsensusRequest::PreAccept(pre_accept_request),
            true,
        )
        .await?;

        // Collect responses
        self.pre_accept(
            &pre_accept_responses
                .into_iter()
                .map(|res| res.into_inner())
                .collect::<Result<Vec<_>>>()?,
        )
        .await?;

        let commit_responses = if self.transaction.t_zero == self.transaction.t {
            //
            //   FAST PATH
            //

            // Commit
            let commit_request = CommitRequest {
                node: self.node.id.to_string(),
                event: self.transaction.transaction.clone().into(),
                timestamp_zero: self.transaction.t_zero.into(),
                timestamp: self.transaction.t.into(),
                dependencies: into_dependency(self.transaction.dependencies.clone()),
            };

            let (commit_result, broadcast_result) = tokio::join!(
                self.commit(),
                Coordinator::broadcast(&members, ConsensusRequest::Commit(commit_request), true)
            );

            commit_result?;
            broadcast_result?
        } else {
            //
            //  SLOW PATH
            //

            // Accept
            let accept_request = AcceptRequest {
                node: self.node.id.to_string(),
                event: self.transaction.transaction.clone().into(),
                timestamp_zero: self.transaction.t_zero.into(),
                timestamp: self.transaction.t.into(),
                dependencies: into_dependency(self.transaction.dependencies.clone()),
            };
            let accept_responses =
                Coordinator::broadcast(&members, ConsensusRequest::Accept(accept_request), true)
                    .await?;

            self.accept(
                &accept_responses
                    .into_iter()
                    .map(|res| res.into_inner())
                    .collect::<Result<Vec<_>>>()?,
            )
            .await?;

            // Commit
            let commit_request = CommitRequest {
                node: self.node.id.to_string(),
                event: self.transaction.transaction.clone().into(),
                timestamp_zero: self.transaction.t_zero.into(),
                timestamp: self.transaction.t.into(),
                dependencies: into_dependency(self.transaction.dependencies.clone()),
            };

            let (commit_result, broadcast_result) = tokio::join!(
                self.commit(),
                Coordinator::broadcast(&members, ConsensusRequest::Commit(commit_request), true)
            );
            commit_result?;
            broadcast_result?
        };

        //
        // Execution
        //
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
            timestamp: self.transaction.t.into(),
            timestamp_zero: self.transaction.t_zero.into(),
            dependencies: into_dependency(self.transaction.dependencies.clone()),
            result: vec![], // Theoretically not needed right?
        };

        Coordinator::broadcast(&members, ConsensusRequest::Apply(apply_request), false).await?; // This should not be awaited

        Ok(())
    }

    #[instrument(level = "trace")]
    async fn recover() {
        todo!("Implement recovery protocol and call when failing")
    }

    #[instrument(level = "trace")]
    async fn broadcast(
        members: &[Arc<Member>],
        request: ConsensusRequest,
        await_majority: bool,
    ) -> Result<Vec<ConsensusResponse>> {
        //dbg!("[broadcast]: Start");
        let mut responses: JoinSet<Result<ConsensusResponse>> = JoinSet::new();
        let mut result = Vec::new();

        // Send PreAccept request to every known member
        // Call match only once ...

        //dbg!("[broadcast]: Create clients & requests");
        match &request {
            ConsensusRequest::PreAccept(req) => {
                // ... and then iterate over every member ...
                for replica in members {
                    let channel = replica.channel.clone();
                    let request = req.clone();
                    // ... and send a request to member
                    responses.spawn(async move {
                        let mut client = ConsensusTransportClient::new(channel);
                        Ok(ConsensusResponse::PreAccept(
                            client.pre_accept(request).await?.into_inner(),
                        ))
                    });
                }
            }
            ConsensusRequest::Accept(req) => {
                for replica in members {
                    let channel = replica.channel.clone();
                    let request = req.clone();
                    responses.spawn(async move {
                        let mut client = ConsensusTransportClient::new(channel);
                        Ok(ConsensusResponse::Accept(
                            client.accept(request).await?.into_inner(),
                        ))
                    });
                }
            }
            ConsensusRequest::Commit(req) => {
                for replica in members {
                    let channel = replica.channel.clone();
                    let request = req.clone();
                    responses.spawn(async move {
                        let mut client = ConsensusTransportClient::new(channel);
                        Ok(ConsensusResponse::Commit(
                            client.commit(request).await?.into_inner(),
                        ))
                    });
                }
            }
            ConsensusRequest::Apply(req) => {
                for replica in members {
                    let channel = replica.channel.clone();
                    let request = req.clone();
                    responses.spawn(async move {
                        let mut client = ConsensusTransportClient::new(channel);
                        Ok(ConsensusResponse::Apply(
                            client.apply(request).await?.into_inner(),
                        ))
                    });
                }
            }
        }

        let majority = (members.len() / 2) + 1;
        let mut counter = 0_usize;

        // Poll majority
        if await_majority {
            while let Some(response) = responses.join_next().await {
                result.push(response??);
                counter += 1;
                if counter >= majority {
                    break;
                }
            }
        } else {
            tokio::spawn(async move { while let Some(_) = responses.join_next().await {} });
        }
        Ok(result)
    }
}
