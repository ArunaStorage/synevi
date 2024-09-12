# Reconfiguration Protocol

Each node has an epoch u32.

# Initialisation (0 Nodes)

The first node starts with `epoch = 0`. It adds itself to the config (address etc.). And initializes with serial 0;

# Join cluster as new node

TODO: Electorate Networkinterface pre-accept
Electorate @ pre-accept = Set of all members from current epoch
If pre-accept returns a new epoch... Fetch configuration.
Accept to all members of old epoch and all members of new epoch

In our impl:
When the coordinator receives info about a new epoch in PA responses.
-> Fetch config of epoch from appropriate node
Await fetch && PA to all (old + new)

1. Update Config request (UpdateConfigRequest) on node N1
2. Wait for the current epoch to be ready ? (ReadyEpoch = e)
3. If node was part of e-1

--------------------------------

### Onboarding revision 2

ArunaCtx {
    1. New node starts NewNodeCtx
    2. New node requests config + all_applieds from ExistingNodeCtx
    3. Existing node 1 sends special transaction (NewConfig + Epoch) to other nodes with new config
}

NewNodeCtx {
    1. Init ReplicaBuffer and collect Commits/Applies
    2. Apply all_applieds
    3. Apply/Commit ReplicaBuffer
    4. Send JoinElectorate && Init FullReplica
}

ExistingNodeCtx {
    1. Receive UpdateConfig
    2. Send all applieds
    4. Update Config
    5. Send NewConfig to all
}

ElectorateCtx {
    1. Rcv & Apply NewConfig + Epoch
    2. Broadcast to updated config
    3. Keep old electorate until RcvJoinElectorate
}

OldElectorateCtx {
    1. if new epoch start ArunaCtx ?
}



--------------------------------

### Onboarding Phase 1

- New node gets created -> Send Join request to an existing node
- Wait for the current epoch (e) to be ready
    - Responds with current config (e)
- Existing node creates a transaction (to the old electorate) with the new epoch and asynchronously starts the reconfiguration protocol.
    - Replicas notice the new epoch and initialize the reconfiguration protocol after fetching from node

X |      |      | X
New epoch +1 (ts)

- Reconfiguration protocol: 

- Wait for the current epoch to be ready
- If node was part of e-1 (previous epoch) 
    - Wait until the ReadyElectorate is true
    - Send join electorate to all new nodes
- If node is a "new" one (Is part of e+1 but not e)
    - ReadyElectorate = false
- else: ReadyElectorate = true

- Increase epoch e = e+1
- 








-------
## Old ideas
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


