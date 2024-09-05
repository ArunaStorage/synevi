# Reconfiguration Protocol

Each node has an epoch u32.

# Initialisation (0 Nodes)

The first node starts with `epoch = 0`. It adds itself to the config (address etc.). And initializes with serial 0;

# Join cluster as new node

### Onboarding Phase 1
0. Accept all consensus requests and put them to a buffer.

1. Ask all node for last applied + hash + current configuration
2. Ask all other nodes if max last applied + hash is correct ?
3. If yes OK else ask another node 1.
4. Download from any node that responded OK all data from your last_applied to the last_applied from 1. (Check if your last applied is correct ?!)

If your node was existing:

1. No consensus requests received --> Check if you are 

Existing nodes can check if they are already a member of the current epoch and/or their state is correct (yet outdated).
- If state is correct: Recover the latest states and go ahead
    - You are still member of an epoch and will receive Consensus requests. -> Go to buffer
    - The first Apply Consensus you will receive is the last that needs to be synced out of consensus.
    - Sync !
- If state is incorrect: Create a new epoch that replaces the old node with a new one

### Onboarding Phase 2

Fetch all applied of node-x based on my last applied + last applied hash;

Node sends a `FetchConfiguration` request to a running node (possibly the initial one). I receives the current member configuration and the current epoch e.

Reponse: Current config from the node (possible outdated)

Node sends a `JoinCluster` request to all current members with e == e+1 and its own configuration and optionally its last applied t0. 

Response:
Majority(Old) -> OK (Last applied / Hash)
              -> Replica start


NoMajority because someone else already got majority for this epoch.


# Re-join cluster as existing node


