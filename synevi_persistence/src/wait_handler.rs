use std::collections::HashMap;
use ahash::RandomState;
use synevi_types::T0;
use tokio::{sync::oneshot, time::Instant};

pub struct Waiter {
    waited_since: Instant,
    dependency_states: u64,
    sender: Vec<oneshot::Sender<()>>,
}

pub struct WaitHandler {
    waiters: HashMap<T0, Waiter, RandomState>,
}


impl WaitHandler {

    pub fn run() {

        loop {}
    }
}

// Tx1 = dep[Tx0]

// -> Tx0 commit 
//  -> for each waiter: is tx0 in deps?
//  -> if yes! -> is t(tx0) > t(tx1) 
//        -> y -> do nothing 
//        -> n -> increase dep_state +1 
//           -> if dep_state == dep.len() -> send signal to waiter 
//  
//
//loop {
//  if waiter.waited_since > 10s -> Find inital tx everyone is waiting for -> 
// 
//}

