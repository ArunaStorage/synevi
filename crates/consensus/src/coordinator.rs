use std::collections::HashMap;
use std::sync::Arc;

use anyhow::Result;
use bytes::Bytes;
use diesel_ulid::DieselUlid;
use tokio::task::JoinSet;
use tonic::transport::Channel;

use consensus_transport::consensus_transport::{
    AcceptRequest, AcceptResponse, ApplyRequest, ApplyResponse, CommitRequest, CommitResponse,
    Dependency, PreAcceptRequest, PreAcceptResponse, State,
};
use consensus_transport::consensus_transport::consensus_transport_client::ConsensusTransportClient;

use crate::event_store::EventStore;
use crate::utils::{into_dependency, IntoInner};

pub static MAX_RETRIES: u64 = 10;

pub struct Coordinator {
    pub node: Arc<String>,
    pub members: Vec<Arc<Member>>,
    pub event_store: Arc<EventStore>,
    pub transaction: TransactionStateMachine,
}
trait StateMachine {
    async fn init(&mut self, transaction: Bytes);
    async fn pre_accept(&mut self, response: PreAcceptResponse) -> Result<()>;
    async fn accept(&mut self, response: AcceptResponse) -> Result<()>;
    async fn commit(&mut self) -> Result<()>;
    async fn execute(&mut self, responses: Vec<CommitResponse>) -> Result<()>;
    fn handle_dependencies(&mut self, dependencies: Vec<Dependency>) -> Result<()>;
}

#[derive(Clone, Debug)]
pub struct Member {
    pub info: MemberInfo,
    pub channel: Channel,
}

#[derive(Clone, Debug)]
pub struct MemberInfo {
    pub host: String,
    pub node: String,
}
#[derive(Clone, Debug, Default)]
pub struct TransactionStateMachine {
    pub state: State,
    pub transaction: Bytes,
    pub t_zero: DieselUlid,
    pub t: DieselUlid,
    pub dependencies: HashMap<DieselUlid, DieselUlid>, // Consists of t and t_zero
}

impl StateMachine for Coordinator {
    async fn init(&mut self, transaction: Bytes) {
        // Generate timestamp
        let t_zero = DieselUlid::generate();

        // Create struct
        self.transaction = TransactionStateMachine {
            state: State::PreAccepted,
            transaction,
            t_zero,
            t: t_zero,
            dependencies: HashMap::new(),
        };
    }
    async fn pre_accept(&mut self, response: PreAcceptResponse) -> Result<()> {
        //dbg!("[PRE_ACCEPT]: Transaction pre accept", &self.transaction);
        let t_response = DieselUlid::try_from(response.timestamp.as_slice())?;

        //dbg!("[PRE_ACCEPT]: t_response", &t_response);
        if self.transaction.t.timestamp() < t_response.timestamp() {

            // Store old t to access map
            let old_t = self.transaction.t;
            // replace t in state machine
            self.transaction.t = t_response;

            //dbg!("[PRE_ACCEPT]: slow path", &self.transaction);

            // These deps stem from individually collected replicas and get unified here
            // that in return will be sent via Accept{} so that every participating replica
            // knows of ALL (unified) dependencies
            self.handle_dependencies(response.dependencies)?;

            //dbg!("[PRE_ACCEPT]: slow path deps", &self.transaction.dependencies);

            // Update transaction, reorder map with updated t and insert dependencies
            self.event_store
                .upsert(old_t, &self.transaction).await;

            //dbg!("[PRE_ACCEPT]: updated event store");
        } else {

            //dbg!("[PRE_ACCEPT]: Fast path");
            // Update transaction with new dependencies
            self.handle_dependencies(response.dependencies)?;

            //dbg!("[PRE_ACCEPT]: Fast path deps", &self.transaction.dependencies);
            self.event_store
                .upsert(self.transaction.t, &self.transaction).await;

            //dbg!("[PRE_ACCEPT]: updated event store");
        }

        Ok(())
    }

    async fn accept(&mut self, response: AcceptResponse) -> Result<()> {
        // Handle returned dependencies
        self.handle_dependencies(response.dependencies)?;

        // Mut state and update entry
        self.transaction.state = State::Accepted;
        self.event_store
            .upsert(self.transaction.t, &self.transaction).await;

        Ok(())
    }
    async fn commit(&mut self) -> Result<()> {
        self.transaction.state = State::Commited;
        self.event_store
            .upsert(self.transaction.t, &self.transaction).await;

        self.event_store
            .wait_for_dependencies(&mut self.transaction)
            .await?;
        Ok(())
    }

    async fn execute(&mut self, _responses: Vec<CommitResponse>) -> Result<()> {
        // TODO: Read commit responses and calc client response

        self.transaction.state = State::Applied;
        self.event_store.persist(self.transaction.clone()).await;

        Ok(())
    }
    fn handle_dependencies(&mut self, dependencies: Vec<Dependency>) -> Result<()> {
        for Dependency {
            timestamp,
            timestamp_zero,
        } in dependencies
        {
            self.transaction.dependencies.insert(
                DieselUlid::try_from(timestamp.as_slice())?,
                DieselUlid::try_from(timestamp_zero.as_slice())?,
            );
        }
        Ok(())
    }
}

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
    pub fn new(node: Arc<String>, members: Vec<Arc<Member>>, event_store: Arc<EventStore>) -> Self {

        Coordinator {
            node,
            members,
            event_store,
            transaction: Default::default(),
        }
    }
    pub async fn add_members(&self, members: Vec<Member>) -> Result<()> {
        todo!("Add members to self")
    }
    pub async fn transaction(&mut self, transaction: Bytes) -> Result<()> {
        //
        //  INIT
        //
        //dbg!("[INIT]");

        // We need a fixed size of members for each transaction
        let members = self.members.clone();

        // Create a new transaction state
        self.init(transaction).await;

        //
        //  PRE ACCEPT STATE
        //
        //dbg!("[PRE_ACCEPT]");

        // Create the PreAccept msg
        let pre_accept_request = PreAcceptRequest {
            node: self.node.to_string(),
            timestamp_zero: self.transaction.t_zero.as_byte_array().into(),
            event: self.transaction.transaction.clone().into(),
        };

        //dbg!("[PRE_ACCEPT]: broadcast");
        // Broadcast message
        let pre_accept_responses =
            Coordinator::broadcast(&members, ConsensusRequest::PreAccept(pre_accept_request))
                .await?;


        //dbg!("[PRE_ACCEPT]: broadcast responses");
        // Collect responses
        for response in pre_accept_responses {
            self.pre_accept(response.into_inner()?).await?;
        }
        //dbg!("[PRE_ACCEPT]:", &self.transaction);

        let commit_responses = if self.transaction.t_zero == self.transaction.t {
            //
            //   FAST PATH
            //
            dbg!("[FAST_PATH]");

            // Commit
            let commit_request = CommitRequest {
                node: self.node.to_string(),
                event: self.transaction.transaction.clone().into(),
                timestamp_zero: self.transaction.t_zero.as_byte_array().into(),
                timestamp: self.transaction.t.as_byte_array().into(),
                dependencies: into_dependency(self.transaction.dependencies.clone()),
            };

            //dbg!("[FAST_PATH]: Commit broadcast");
            let (commit_result, broadcast_result) = tokio::join!(
                self.commit(),
                Coordinator::broadcast(&members, ConsensusRequest::Commit(commit_request))
            );

            //dbg!("[FAST_PATH]: Commit result");
            commit_result?;
            broadcast_result?
        } else {
            //
            //  SLOW PATH
            //
            dbg!("[SLOW_PATH]");

            // Accept
            let accept_request = AcceptRequest {
                node: self.node.to_string(),
                event: self.transaction.transaction.clone().into(),
                timestamp_zero: self.transaction.t_zero.as_byte_array().into(),
                timestamp: self.transaction.t.as_byte_array().into(),
                dependencies: into_dependency(self.transaction.dependencies.clone()),
            };
            let accept_responses =
                Coordinator::broadcast(&members, ConsensusRequest::Accept(accept_request)).await?;
            for response in accept_responses {
                self.accept(response.into_inner()?).await?;
            }

            // Commit
            let commit_request = CommitRequest {
                node: self.node.to_string(),
                event: self.transaction.transaction.clone().into(),
                timestamp_zero: self.transaction.t_zero.as_byte_array().into(),
                timestamp: self.transaction.t.as_byte_array().into(),
                dependencies: into_dependency(self.transaction.dependencies.clone()),
            };

            let (commit_result, broadcast_result) = tokio::join!(
                self.commit(),
                Coordinator::broadcast(&members, ConsensusRequest::Commit(commit_request))
            );
            commit_result?;
            broadcast_result?
        };

        //
        // Execution
        //
        self.execute(
            commit_responses
                .into_iter()
                .map(|res| -> Result<CommitResponse> { res.into_inner() })
                .collect::<Result<Vec<CommitResponse>>>()?,
        )
        .await?;

        let apply_request = ApplyRequest {
            node: self.node.to_string(),
            event: self.transaction.transaction.to_vec(),
            timestamp: self.transaction.t.as_byte_array().into(),
            timestamp_zero: self.transaction.t_zero.as_byte_array().into(),
            dependencies: into_dependency(self.transaction.dependencies.clone()),
            result: vec![], // Theoretically not needed right?
        };
        Coordinator::broadcast(&members, ConsensusRequest::Apply(apply_request)).await?; // This should not be awaited

        Ok(())
    }

    async fn recover() {
        todo!("Implement recovery protocol and call when failing")
    }

    async fn broadcast(
        members: &[Arc<Member>],
        request: ConsensusRequest,
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

        //dbg!("[broadcast]: Calc majority");
        // await JoinSet
        let majority = (members.len() / 2) + 1;
        let mut counter = 0_usize;

        // Poll majority

        //dbg!("[broadcast]: Poll majority");
        while let Some(response) = responses.join_next().await {
            // dbg!("[broadcast]: Response: {:?}", &response);
            result.push(response??);
            counter += 1;
            // dbg!(&majority);
            // dbg!(&counter);
            if counter >= majority {
                // dbg!("break");
                break;
            }
        }
        Ok(result)
    }
}
