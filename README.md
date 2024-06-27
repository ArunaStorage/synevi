# Synevi
Synevi (greek: συνέβει - "it happened") is a leaderless, strict serializable, embeddable event store for event sourcing. 

It is based on Apache Cassandras [Accord](https://cwiki.apache.org/confluence/download/attachments/188744725/Accord.pdf?version=1&modificationDate=1630847737000&api=v2) consensus algorithm which is a leaderless concurrency optimized variant of EPaxos.

For a more detailed explanation of the underlying architecture please visit our [architecture documentation](./docs/architecture.md).

## Features

- **Leaderless**: In contrast to many other eventstores *synevi* does not rely on a single elected leader, every node can act as a coordinator in parallel
- **Embedabble**: Synevi is designed to be directly embedded into your server application, transforming it into a fast and secure distributed system without additional network and maintenance overhead
- **Event Sourcing**: Built with event sourcing in mind to distribute your application state as a chain of events enabling advanced auditing, vetting and replay functionalities
- ...

## Usage

### Embedded

TBD

### As standalone Application

TBD

## Feedback & Contributions

If you have any ideas, suggestions, or issues, please don't hesitate to open an issue and/or PR. Contributions to this project are always welcome ! We appreciate your help in making this project better. Please have a look at our [Contributor Guidelines](./CONTRIBUTING.md) for more information.


## License

Licensed under either of

 * Apache License, Version 2.0
   ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
 * MIT license
   ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)

at your option. Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in Synevi by you, as defined in the Apache-2.0 license, shall be dual licensed as above, without any additional terms or conditions. 

