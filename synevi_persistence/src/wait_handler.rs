

// Tx1 = dep[Tx0]

// -> Tx0 commit 
//  -> for each waiter: is tx0 in deps?
//  -> if yes! -> is t(tx0) > t(tx1) 
//        -> y -> do nothing 
//        -> n -> increase dep_state +1 
//           -> if dep_state == dep.len() -> send signal to waiter 
//  



pub struct Waiter {
    waited_since: Instant,
    dependency_states: u64,
    sender: Vec<oneshot::Sender<()>>,
}

//loop {
//  if waiter.waited_since > 10s -> Find inital tx everyone is waiting for -> 
// 
//}




pub struct WaitHandler {
    waiters: HashMap<T0, Waiter>,
}



impl WaitHandler {

    pub fn run() {

        for 
    }
}