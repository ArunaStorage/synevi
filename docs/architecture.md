# Synevi architecture documentation

Synevi has multiple modules. The core modules are [consensus](../crates/consensus/) and [consensus_transport](../crates/consensus_transport/). While the transport crate defines the messages and services for data exchange between the coordinator and the replicas the consensus crate contains the main logic for consensus.

## Consensus algorithm

The consensus algorithm is based on Apache Cassandras [Accord](https://cwiki.apache.org/confluence/download/attachments/188744725/Accord.pdf?version=1&modificationDate=1630847737000&api=v2) consensus algorithm which is a leaderless concurrency optimized variant of EPaxos.

This algorithm consists of four distinct phases:

1. PreAccept:

2. Accept (optional):

3. Commit & ~~Read~~: 

4. Apply:

TBC