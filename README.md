# Synevi
Synevi (greek: συνέβει - "it happened") is leaderless, strict serializable, embeddable event store for event sourcing. 

It is based on Apache Cassandras [Accord](https://cwiki.apache.org/confluence/download/attachments/188744725/Accord.pdf?version=1&modificationDate=1630847737000&api=v2) consensus algorithm which is a leaderless concurrency optimized variant of EPaxos.

## Features

- **Leaderless**: In contrast to many other eventstores *synevi* does not rely on a single elected leader, every node can act as a coordinator in parallel
- **Embedabble**: Synevi is designed to be directly embedded into your server application, transforming it into a fast and secure distributed system without additional network and maintenance overhead
- **Event Sourcing**: Built with event sourcing in mind to distribute your application state as a chain of events enabling advanced auditing, vetting and replay functionalities
- ...

## Contributing


## License

Licensed under either of

    Apache License, Version 2.0 (LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0)
    MIT license (LICENSE-MIT or http://opensource.org/licenses/MIT)

at your option.

