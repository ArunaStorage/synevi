use crate::event_store::EventStore;
use crate::utils::{into_dependency, IntoInner};
use anyhow::Result;
use bytes::Bytes;
use consensus_transport::consensus_transport::consensus_transport_client::ConsensusTransportClient;
use consensus_transport::consensus_transport::{
    AcceptRequest, AcceptResponse, ApplyRequest, ApplyResponse, CommitRequest, CommitResponse,
    Dependency, PreAcceptRequest, PreAcceptResponse, State,
};
use diesel_ulid::DieselUlid;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::task::JoinSet;
use tonic::transport::Channel;

pub static MAX_RETRIES: u64 = 10;

pub struct Coordinator {
    pub node: String,
    pub members: Vec<Member>,
    pub event_store: Arc<Mutex<EventStore>>,
    pub transaction: Transaction,
}

#[derive(Clone, Debug)]
pub struct Member {
    pub host: String,
    pub node: String,
    pub channel: Channel,
}

#[derive(Clone, Debug)]
pub struct Transaction {
    pub state: State,
    pub transaction: Bytes,
    pub t_zero: DieselUlid,
    pub t: DieselUlid,
    pub dependencies: HashSet<DieselUlid>,
}

impl Coordinator {
    async fn init(&mut self, transaction: Bytes) {
        // Generate timestamp
        let t_zero = DieselUlid::generate();

        // Create struct
        self.transaction = Transaction {
            state: State::PreAccepted,
            transaction,
            t_zero,
            t: t_zero,
            dependencies: HashSet::new(),
        };
    }
    async fn pre_accept(&mut self, response: PreAcceptResponse) -> Result<()> {
        let t_response = DieselUlid::try_from(response.timestamp.as_slice())?;
        if self.transaction.t.timestamp() < t_response.timestamp() {
            self.transaction.t = t_response;
        }
        // These deps stem from individually collected replicas and get unified here
        // that in return will be sent via Accept{} so that every participating replica
        // knows of ALL (unified) dependencies
        self.handle_dependencies(response.dependencies).await?;

        // upsert into event store with updated deps and reordered t
        self.event_store
            .lock()
            .await
            .update(t_response, &self.transaction)?;
        Ok(())
    }

    async fn accept(&mut self, response: AcceptResponse) -> Result<()> {
        // Handle returned dependencies
        self.handle_dependencies(response.dependencies).await?;

        // Mut state and update entry
        self.transaction.state = State::Accepted;
        self.event_store
            .lock()
            .await
            .update(self.transaction.t, &self.transaction)?;

        Ok(())
    }
    async fn commit(&mut self) -> Result<()> {
        self.transaction.state = State::Commited;
        self.event_store
            .lock()
            .await
            .update_state(&self.transaction.t, State::Commited)?;
        let mut wait = true;
        let mut counter: u64 = 0;
        while wait {
            let dependencies = self.transaction.dependencies.clone();
            for ulid in dependencies {
                match self.event_store.lock().await.search(&ulid) {
                    Some(event) => {
                        if matches!(event.state, State::Applied) {
                            self.transaction.dependencies.remove(&ulid);
                        }
                    }
                    None => todo!(),
                }
            }
            if self.transaction.dependencies.is_empty() {
                wait = false;
            } else {
                counter += 1;
                tokio::time::sleep(Duration::from_millis(counter.pow(2))).await;
                if counter == MAX_RETRIES {
                    todo!("Start recovery")
                } else {
                    continue;
                }
            }
        }
        Ok(())
    }

    async fn execute(&mut self, _responses: Vec<CommitResponse>) -> Result<()> {
        // TODO: Read commit responses and calc client response

        self.event_store
            .lock()
            .await
            .update_state(&self.transaction.t, State::Applied)?; // This should probably be `t` here
        self.transaction.state = State::Applied;

        Ok(())
    }
    async fn handle_dependencies(&mut self, dependencies: Vec<Dependency>) -> Result<()> {
        for Dependency { timestamp } in dependencies {
            self.transaction
                .dependencies
                .insert(DieselUlid::try_from(timestamp.as_slice())?);
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

pub(crate) enum ConsensusResponse {
    PreAccept(PreAcceptResponse),
    Accept(AcceptResponse),
    Commit(CommitResponse),
    Apply(ApplyResponse),
    // TODO: Recover
}

impl Coordinator {
    pub async fn add_members(&self, members: Vec<Member>) -> Result<()> {
        todo!("Add members to self")
    }
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

        // Create the PreAccept msg
        let pre_accept_request = PreAcceptRequest {
            node: self.node.clone(),
            timestamp_zero: self.transaction.t_zero.as_byte_array().into(),
            event: self.transaction.transaction.clone().into(),
        };

        // Broadcast message
        let pre_accept_responses =
            Coordinator::broadcast(&members, ConsensusRequest::PreAccept(pre_accept_request))
                .await?;

        // Collect responses
        for response in pre_accept_responses {
            self.pre_accept(response.into_inner()?).await?;
        }

        let commit_responses = if self.transaction.t_zero == self.transaction.t {
            //
            //   FAST PATH
            //

            // Commit
            let commit_request = CommitRequest {
                node: self.node.clone(),
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
        } else {
            //
            //  SLOW PATH
            //

            // Accept
            let accept_request = AcceptRequest {
                node: self.node.clone(),
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
                node: self.node.clone(),
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
            node: self.node.clone(),
            event: self.transaction.transaction.to_vec(),
            timestamp: self.transaction.t.as_byte_array().into(),
            dependencies: into_dependency(self.transaction.dependencies.clone()),
            result: vec![], // This is not needed if we just store updated deps?
        };
        Coordinator::broadcast(&members, ConsensusRequest::Apply(apply_request)).await?;

        Ok(())
    }

    async fn recover() {
        todo!("Implement recovery protocol and call when failing")
    }

    async fn broadcast(
        members: &[Member],
        request: ConsensusRequest,
    ) -> Result<Vec<ConsensusResponse>> {
        let mut responses: JoinSet<Result<ConsensusResponse>> = JoinSet::new();
        let mut result = Vec::new();

        // Send PreAccept request to every known member
        // Call match only once ...
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

        // await JoinSet
        let majority = (members.len() / 2) + 1;
        let mut counter = 0_usize;

        // Poll majority
        while let Some(response) = responses.join_next().await {
            result.push(response??);
            counter += 1;
            if counter >= majority {
                break;
            }
        }
        Ok(result)
    }
}
