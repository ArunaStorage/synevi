# Recovery

Transactions are recovered after a specific time has passed.

If the node has data about the transaction (is either the coordinator or has witnessed a previous request).
Send recover request to all replicas. This has the function as an explicit pre-accept request and all replicas answer with their current state of the transaction (Tx). 

## Cases

- No other has witnessed Tx -> Recover functions as Pre-Accept, continue with Accept 
- If others have only witnessed a Pre-Accept: 
    - Do superseding transactions exist ? -> Continue with Accept
    - Are there "waiting" transactions that should execute before ? -> Wait and restart recovery
    - Otherwise continue with Accept
    - For Accept: Enforce the fast-path if the original request got a majority (original T0 as T) otherwise use max(T)
- Accepted / Commited / Applied -> Continue with last known state

## Synevi specific implementation details

Unlike the original Accord implementation, synevi only collects T0s of dependencies.
This was done to mitigate the effects of larger transaction bodys, which would otherwise result in drastically increased message sizes.

One downside of this is that nodes can witness the existence of transactions via dependencies without the actual transaction body.
If it is necessary to recover that transaction to satisfy the dependencies, this creates a problem of "unknown recoveries".

To fix this: The waiting node can request a recovery from other nodes. These nodes will respond with a status information if they have witnessed the Tx and can recover it. As long as one node has the ability to recover the Tx and has witnessed the request the Tx will be recovered. 

If a majority responds with an unable to recover status and the rest do not respond, the dependency can be removed:

The reason for this is that because the original transaction has not yet received a majority, it cannot have taken the fast path and must be recovered using a slow path. This also implies that a node that responds with a false state cannot allow the transaction to take the fast path. 