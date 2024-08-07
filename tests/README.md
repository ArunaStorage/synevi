# Testing Guidelines

Synevi is tested in multiple ways. [./consensus_e2e.rs](consensus_e2e.rs) contains direct e2e tests that creates 5 GRPC nodes and tests the consensus with multiple conditions.

A comprehensive test is performed via the [maelstrom](https://github.com/jepsen-io/maelstrom) workbench.

## Running regular tests

After checking out the repo, just run `cargo test` to run all non-maelstrom unit and integration tests. 

## Running maelstrom tests

Synevis consistency can be tested with the maelstrom `lin-kv` workload. 


**Prerequisites**: A working maelstrom installation see: [installation guidelines](https://github.com/jepsen-io/maelstrom/blob/main/doc/01-getting-ready/index.md)


Build the `lin_kv` example:

```bash
cargo build --bin maelstrom_lin_kv --release
```

Run the maelstrom workload:

```bash
./maelstrom test -w lin-kv --bin ./target/release/maelstrom_lin_kv --concurrency 4n --node-count 3 --rate 100
```

If everything went well maelstrom will show some statistics and report:

```
Everything looks good! ヽ(‘ー`)ノ
```

You can get a more sophisticated report including visualizations by running `./maelstrom serve` and opening `http:/0.0.0.0:8080` in a browser.