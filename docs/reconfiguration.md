# Reconfiguration Protocol

Each node has an epoch u32.

# Initialisation (0 Nodes)

The first node starts with `epoch = 0`. It adds itself to the config (address etc.).

# Join cluster as new node

Node sends a `FetchConfiguration` request to a running node (possibly the initial one). I receives the current member configuration and the current epoch e. Node sends a `JoinCluster` request to all current members with e = e+1 and its own configuration and optionally its last applied t0 

Response: 


# Re-join cluster as existing node


