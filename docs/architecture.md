# Synevi architecture documentation

Synevi has multiple modules. The core modules are [consensus](../crates/consensus/) and [consensus_transport](../crates/consensus_transport/). While the transport crate defines the messages and services for data exchange between the coordinator and the replicas the consensus crate contains the main logic for consensus.

## Consensus algorithm

The consensus algorithm is based on Apache Cassandras [Accord](https://cwiki.apache.org/confluence/download/attachments/188744725/Accord.pdf?version=1&modificationDate=1630847737000&api=v2) consensus algorithm which is a leaderless concurrency optimized variant of EPaxos.

This original algorithm consists of four distinct phases:

```mermaid
sequenceDiagram
    participant C as Coordinator
    participant R as Replica
    note over C,R: Consensus
    C ->> R: PreAccept
    alt ok: t0 > t
        R ->> C: PreAcceptOK
    else not ok: t0 < t
        R ->> C: PreAcceptNotOK
        C ->> R: Accept
        R ->> C: AcceptOK
    end
    note over C,R: Execution
    par Commit
        C ->> R: Commit
        R ->> C: CommitOK
    and Read
        C -->> R: Read
        R -->> C: ReadOK
    end
        C ->> R: Apply
        C ->> C: Execute;
        R ->> R: Execute
```

Modified execution phase in Synevi:

```mermaid
sequenceDiagram
    participant C as Coordinator
    participant R as Replica
    note over C,R: Execution
    C ->> R: Commit (to all)
    R ->> C: CommitOK (wait for majority)
    C ->> R: Apply (async)
    C ->> C: Execute;
    R ->> R: Execute
```

1. PreAccept [PA]:

Send PreAccept (t0) to majority (all)

    Disaster cases:
    1. Crash before anyone notices request: -> Forget transaction
    2. Crash before majority notices request -> R-Recover(X, t0)
    3. Crash after majority notices request -> R-Recover(X, t0)

2. Accept (optional) [A]:

3. Commit [C]:

4. Applied

5. Recovery:

- Disaster PA:
    - 1 Forget transaction
    - 2 Recovery(X):
    ```
        if any !pre-accepted
            Continue state machine: Retry highest state machine step  
        else 
            Wait / Superceding 
    ```


TBC