use crate::event_store::{Event, EventStore};
use crate::utils::{into_dependency, IntoInner};
use anyhow::{anyhow, Result};
use bytes::Bytes;
use consensus_transport::consensus_transport::consensus_transport_client::ConsensusTransportClient;
use consensus_transport::consensus_transport::{
    AcceptRequest, AcceptResponse, ApplyRequest, ApplyResponse, CommitRequest, CommitResponse,
    Dependency, PreAcceptRequest, PreAcceptResponse, State,
};
use diesel_ulid::DieselUlid;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::task::JoinSet;
use tonic::transport::Channel;

pub struct Coordinator {
    pub node: String,
    pub members: Vec<Member>,
    pub event_store: Arc<Mutex<EventStore>>,
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
    pub dependencies: HashMap<DieselUlid, Bytes>,
}

impl Transaction {
    async fn init(transaction: Bytes, event_store: &Mutex<EventStore>) -> Self {
        // Generate timestamp
        let t_zero = DieselUlid::generate();

        // Insert into event store
        event_store.lock().await.insert(
            t_zero,
            Event {
                state: State::PreAccepted,
                event: transaction.clone(),
            },
        );

        // Create struct
        Transaction {
            state: State::PreAccepted,
            transaction,
            t_zero,
            t: t_zero,
            dependencies: HashMap::new(),
        }
    }
    async fn pre_accept(&mut self, response: PreAcceptResponse, event_store: &Mutex<EventStore>) -> Result<()> {
        let t_response = DieselUlid::try_from(response.timestamp.as_slice())?;
        if self.t.timestamp() < t_response.timestamp() {
            self.t = t_response;
            self.handle_dependencies(response.dependencies, State::PreAccepted, event_store).await?;
        }
        Ok(())
    }

    async fn accept(&mut self, response: AcceptResponse, event_store: &Mutex<EventStore>) -> Result<()> {
        self.handle_dependencies(response.dependencies, State::Accepted, event_store).await?;
        Ok(())
    }
    async fn commit(&mut self, event_store: &Mutex<EventStore>) -> Result<()> {
        event_store
            .lock()
            .await
            .update_state(&self.t_zero, State::Commited)?;
        self.state = State::Commited;
        Ok(())
    }

    async fn execute(
        &mut self,
        responses: Vec<CommitResponse>,
        event_store: &Mutex<EventStore>,
    ) -> Result<()> {
        for response in responses {
            for Dependency{timestamp, event} in response.results {
                self.dependencies.insert( DieselUlid::try_from(timestamp.as_slice())?, event.clone().into());
            }
        }
        for ulid in self.dependencies.keys() {
            event_store.lock().await.persist(ulid)?
        }
        Ok(())
    }
    async fn handle_dependencies(&mut self, dependencies: Vec<Dependency>, state: State, event_store: &Mutex<EventStore>) -> Result<()> {
        for Dependency { timestamp, event } in dependencies {
            self.dependencies
                .insert(DieselUlid::try_from(timestamp.as_slice())?, event.clone().into());
            event_store.lock().await.insert(DieselUlid::try_from(timestamp.as_slice())?, Event{ state, event: event.into() });
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
    async fn add_members(&self, members: Vec<Member>) -> Result<()> {
        todo!("Add members to self")
    }
    pub async fn transaction(&self, transaction: Bytes) -> Result<()> {
        //
        //  INIT
        //

        // We need a fixed size of members for each transaction
        let members = self.members.clone();

        // Create a new transaction state
        let mut transaction = Transaction::init(transaction, &self.event_store).await;

        //
        //  PRE ACCEPT STATE
        //

        // Create the PreAccept msg
        let pre_accept_request = PreAcceptRequest {
            node: self.node.clone(),
            timestamp_zero: transaction.t_zero.as_byte_array().into(),
            event: transaction.transaction.clone().into(),
        };

        // Broadcast message
        let pre_accept_responses =
            Coordinator::broadcast(&members, ConsensusRequest::PreAccept(pre_accept_request))
                .await?;

        // Collect responses
        for response in pre_accept_responses {
            transaction.pre_accept(response.into_inner()?, &self.event_store).await?;
        }

        let mut commit_responses = if transaction.t_zero == transaction.t {
            //
            //   FAST PATH
            //

            // Commit
            transaction.commit(&self.event_store).await?;
            let commit_request = CommitRequest {
                node: self.node.clone(),
                timestamp_zero: transaction.t_zero.as_byte_array().into(),
                timestamp: transaction.t.as_byte_array().into(),
                dependencies: into_dependency(transaction.dependencies.clone()),
            };
            Coordinator::broadcast(&members, ConsensusRequest::Commit(commit_request)).await?
        } else {
            //
            //  SLOW PATH
            //

            // Accept
            let accept_request = AcceptRequest {
                node: self.node.clone(),
                timestamp_zero: transaction.t_zero.as_byte_array().into(),
                timestamp: transaction.t.as_byte_array().into(),
                dependencies: into_dependency(transaction.dependencies.clone()),
            };
            let accept_responses =
                Coordinator::broadcast(&members, ConsensusRequest::Accept(accept_request)).await?;
            for response in accept_responses {
                transaction.accept(response.into_inner()?, &self.event_store).await?;
            }

            // Commit
            transaction.commit(&self.event_store).await?;
            let commit_request = CommitRequest {
                node: self.node.clone(),
                timestamp_zero: transaction.t_zero.as_byte_array().into(),
                timestamp: transaction.t.as_byte_array().into(),
                dependencies: into_dependency(transaction.dependencies.clone()),
            };
            Coordinator::broadcast(&members, ConsensusRequest::Commit(commit_request)).await?
        };

        //
        // Execution
        //
        transaction
            .execute(
                commit_responses
                    .into_iter()
                    .map(|res| -> Result<CommitResponse> { res.into_inner() })
                    .collect::<Result<Vec<CommitResponse>>>()?,
                &self.event_store,
            )
            .await?;
        
        let apply_request = ApplyRequest{
            node: self.node.clone(),
            event: transaction.transaction.to_vec(),
            timestamp: transaction.t.as_byte_array().into(),
            dependencies: into_dependency(transaction.dependencies),
            result: vec![], // This is not needed if we just store updated deps?
        };
        Coordinator::broadcast(&members, ConsensusRequest::Apply(apply_request)).await?;
            
        // TODO:
        // - recovery
        
        Ok(())
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
