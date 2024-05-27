use std::sync::Arc;
use anyhow::{anyhow, Result};
use bytes::Bytes;
use tonic::transport::Channel;
use consensus_transport::consensus_transport::{CommitRequest, Dependency, PreAcceptRequest, State};
use consensus_transport::consensus_transport::consensus_transport_client::ConsensusTransportClient;
use diesel_ulid::DieselUlid;
use tokio::task::JoinSet;

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
    pub fn pre_accept(&mut self, t: Bytes) -> Result<()> {
        Ok(())
    }
}

pub enum Path {
    Fast,
    Slow,
}

impl Coordinator {
    async fn add_members(&self, members: Vec<Member>) -> Result<()> {
        todo!("Add members to self")
    }
    pub async fn transaction(&self, transaction: Bytes) -> Result<()> {
        
        // We need a fixed size of members for each transaction
        let members = self.members.clone();
       
        // Create a new transaction state
        let mut transaction = Transaction::init(transaction);
        
        // Create the PreAccept msg
        let pre_accept_request = PreAcceptRequest {
            node: self.node.clone(),
            timestamp_zero: transaction.t_zero.as_byte_array().into(),
            event: transaction.transaction.into(),
        };
        
        let mut pre_accept_responses =  JoinSet::new() ;
        
        // Send PreAccept request to every known member
        for replica in &members {
            let mut client = ConsensusTransportClient::new(replica.channel.clone());
            let response = client.pre_accept(pre_accept_request.clone());
            pre_accept_responses.spawn(response);
        }
        
        // await JoinSet
       
        // Implement any/contains/find manually, because you cannot return Results ...
        let mut path = Path::Fast;
        for resp in pre_accept_responses.iter() {
            let time_slice = <&[u8; 16]>::try_from(resp.timestamp.as_slice())?;
            if DieselUlid::from(time_slice) != transaction.t_zero {
                continue
            } else {
                path = Path::Slow;
                break;
            }
        }

        // TODO: 
        match path {
            Path::Fast => {
                let commit_request = CommitRequest {
                    node: self.node.clone(),
                    timestamp_zero: transaction.t_zero.as_byte_array().into(),
                    timestamp: transaction.t.as_byte_array().into(),
                    dependencies: vec![],
                };
                for replica in &members {
                    let mut client = ConsensusTransportClient::new(replica.channel.clone());
                    let response = client.commit(commit_request.clone()).await?.into_inner();
                }
                // - fast path
            }
            Path::Slow => {
                // - slow path
            }
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
}