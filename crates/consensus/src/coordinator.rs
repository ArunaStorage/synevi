use std::sync::Arc;
use anyhow::{anyhow, Result};
use bytes::Bytes;
use tonic::transport::Channel;
use consensus_transport::consensus_transport::{AcceptRequest, AcceptResponse, ApplyRequest, ApplyResponse, CommitRequest, CommitResponse, Dependency, PreAcceptRequest, PreAcceptResponse, State};
use consensus_transport::consensus_transport::consensus_transport_client::ConsensusTransportClient;
use diesel_ulid::DieselUlid;
use tokio::task::JoinSet;
use tonic::Response;

#[derive(Clone, Debug)]
pub struct Coordinator {
    pub node: String,
    pub members: Vec<Arc<Member>>,
}

#[derive(Clone, Debug)]
pub struct Member {
    pub host: String,
    pub node: String,
    pub channel: Channel
}

#[derive(Clone, Debug)]
pub struct Transaction {
    pub state: State,
    pub transaction: Bytes,
    pub t_zero: DieselUlid,
    pub t: DieselUlid,
    pub dependencies: Vec<Dependency>,
}

impl Transaction {
    pub fn init(transaction: Bytes) -> Self {
        let t_zero = DieselUlid::generate();
        Transaction {
            state: State::PreAccepted,
            transaction,
            t_zero,
            t: t_zero.clone(),
            dependencies: Vec::new(),
        }

    }
    pub fn pre_accept(&mut self, t: Vec<u8>) -> Result<()> {
        let new_ulid = DieselUlid::try_from(t.as_slice())?;
        if self.t.timestamp() < new_ulid.timestamp() {
            self.t = new_ulid;
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

enum ConsensusResponse {
    PreAccept(PreAcceptResponse),
    Accept(AcceptResponse),
    Commit(CommitResponse),
    Apply(ApplyResponse),
}

impl Coordinator {
    async fn add_members(&self, members: Vec<Member>) -> Result<()> {
        todo!("Add members to self")
    }
    pub async fn transaction(&self, transaction: Bytes) -> Result<()> {
        /*
            INIT
        */

        // We need a fixed size of members for each transaction
        let members = self.members.clone();

        // Create a new transaction state
        let mut transaction = Transaction::init(transaction);

        /*
            PRE ACCEPT
         */
        
        // Create the PreAccept msg
        let pre_accept_request = PreAcceptRequest {
            node: self.node.clone(),
            timestamp_zero: transaction.t_zero.as_byte_array().into(),
            event: transaction.transaction.clone().into(),
        };

        let mut pre_accept_responses =  JoinSet::new() ;

        // Send PreAccept request to every known member
        for replica in &members {
            let channel = replica.channel.clone();
            let request = pre_accept_request.clone();
            pre_accept_responses.spawn(async move {
                let mut client = ConsensusTransportClient::new(channel);
                client.pre_accept(request).await
            });
        }

        // await JoinSet
        let majority = (members.len()/2) + 1;
        let mut counter = 0_usize;

        // Poll majority
        while let Some(response) = pre_accept_responses.join_next().await {
            let response = response??.into_inner();
            transaction.pre_accept(response.timestamp)?;

            counter += 1;
            if counter >= majority {
                break;
            }
        }
        
        
        if transaction.t_zero == transaction.t {
            /*
                FAST PATH 
            */
            for 
            
        } else {
            /*
                SLOW PATH
            */
        }
        // - execution
        // - recovery


        Err(anyhow!("Not yet implemented"))
    }

    async fn execute() -> Result<()> {
        todo!("Impl execute")
    }
    async fn apply() -> Result<()> {
       todo!("Apply all persistent changes when rebooting")
    }
    
    async fn broadcast(members: &[Member], request: ConsensusRequest) -> Result<Vec<ConsensusResponse>> {
        let mut responses =  JoinSet::new() ;
        let mut result = Vec::new();

        // Send PreAccept request to every known member
        for replica in members {
            let channel = replica.channel.clone();
            match &request {
                ConsensusRequest::PreAccept(req) => {
                    responses.spawn(async move {
                        let mut client = ConsensusTransportClient::new(channel);
                        client.pre_accept(req.clone()).await.try_into()?
                    });        
                }
                ConsensusRequest::Accept(req) => {
                    responses.spawn(async move {
                        let mut client = ConsensusTransportClient::new(channel);
                        client.accept(req.clone()).await.try_into()?
                    });
                }
                ConsensusRequest::Commit(req) => {
                    responses.spawn(async move {
                        let mut client = ConsensusTransportClient::new(channel);
                        client.commit(req.clone()).await.try_into()?
                    });
                }
                ConsensusRequest::Apply(req) => {
                    responses.spawn(async move {
                        let mut client = ConsensusTransportClient::new(channel);
                        client.apply(req.clone()).await.try_into()?
                    });
                }
            };
            
        }

        // await JoinSet
        let majority = (members.len()/2) + 1;
        let mut counter = 0_usize;

        // Poll majority
        while let Some(response) = responses.join_next().await {
            result.push(response?);
            
            counter += 1;
            if counter >= majority {
                break;
            }
        }
        Ok(result)
    }
}

impl TryFrom<Result<PreAcceptResponse>> for ConsensusResponse {
    type Error = anyhow::Error;
    fn try_from(value: Result<PreAcceptResponse>) -> Result<Self> {
        Ok(ConsensusResponse::PreAccept(value?))
    }
    
}
impl TryFrom<Result<AcceptResponse>> for ConsensusResponse {
    type Error = anyhow::Error;
    fn try_from(value: Result<AcceptResponse>) -> Result<Self> {
        Ok(ConsensusResponse::Accept(value?))
    }

}
impl TryFrom<Result<CommitResponse>> for ConsensusResponse {
    type Error = anyhow::Error;
    fn try_from(value: Result<CommitResponse>) -> Result<Self> {
        Ok(ConsensusResponse::Commit(value?))
    }

}
impl TryFrom<Result<ApplyResponse>> for ConsensusResponse {
    type Error = anyhow::Error;
    fn try_from(value: Result<ApplyResponse>) -> Result<Self> {
        Ok(ConsensusResponse::Apply(value?))
    }

}