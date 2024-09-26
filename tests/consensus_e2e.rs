#[cfg(test)]
mod tests {
    use rand::Rng;
    use std::collections::BTreeMap;
    use std::net::SocketAddr;
    use std::str::FromStr;
    use std::sync::Arc;
    use synevi::{State, Store, T, T0};
    use synevi_core::node::Node;
    use synevi_core::tests::DummyExecutor;
    use synevi_network::network::GrpcNetwork;
    use synevi_persistence::database::PersistentStore;
    use synevi_persistence::mem_store::MemStore;
    use tokio::fs;
    use tokio::runtime::Builder;
    use ulid::Ulid;

    #[tokio::test(flavor = "multi_thread")]
    async fn parallel_execution() {
        let node_names: Vec<_> = (0..5).map(|_| Ulid::new()).collect();
        let mut nodes: Vec<Arc<Node<GrpcNetwork, DummyExecutor, MemStore>>> = vec![];

        for (i, m) in node_names.iter().enumerate() {
            let socket_addr = SocketAddr::from_str(&format!("0.0.0.0:{}", 10000 + i)).unwrap();
            let network = synevi_network::network::GrpcNetwork::new(
                socket_addr,
                format!("http://localhost:{}", 10000 + i),
                *m,
                i as u16,
            );
            let node = Node::new_with_network_and_executor(*m, i as u16, network, DummyExecutor)
                .await
                .unwrap();
            nodes.push(node);
        }
        for (i, name) in node_names.iter().enumerate() {
            for (i2, node) in nodes.iter_mut().enumerate() {
                if i != i2 {
                    node.add_member(
                        *name,
                        i as u16,
                        format!("http://localhost:{}", 10000 + i),
                        true,
                    )
                    .await
                    .unwrap();
                }
            }
        }

        let coordinator = nodes.pop().unwrap();

        let mut joinset = tokio::task::JoinSet::new();

        for i in 0..10000 {
            let coordinator = coordinator.clone();
            joinset.spawn(async move {
                coordinator
                    .transaction(
                        i,
                        synevi::TransactionPayload::External(Vec::from("This is a transaction")),
                    )
                    .await
            });
        }
        while let Some(res) = joinset.join_next().await {
            match res.unwrap().unwrap() {
                synevi::ExecutorResult::External(res) => {
                    res.unwrap();
                }
                synevi::ExecutorResult::Internal(res) => {
                    res.unwrap();
                }
            };
        }

        let (total, accepts, recovers) = coordinator.get_stats();
        println!(
            "Fast: {:?}, Slow: {:?} Paths / {:?} Total / {:?} Recovers",
            total - accepts,
            accepts,
            total,
            recovers
        );

        //assert_eq!(recovers, 0);

        let coordinator_store: BTreeMap<T0, (T, Option<[u8; 32]>)> = coordinator
            .event_store
            .get_event_store()
            .await
            .into_values()
            .map(|e| (e.t_zero, (e.t, e.get_latest_hash())))
            .collect();

        assert!(coordinator
            .event_store
            .get_event_store()
            .await
            .iter()
            .all(|(_, e)| e.state == State::Applied));

        let mut got_mismatch = false;
        for node in nodes {
            let node_store: BTreeMap<T0, (T, Option<[u8; 32]>)> = node
                .event_store
                .get_event_store()
                .await
                .into_values()
                .map(|e| (e.t_zero, (e.t, e.get_latest_hash())))
                .collect();
            assert!(
                node.event_store
                    .get_event_store()
                    .await
                    .iter()
                    .all(|(_, e)| e.state == State::Applied),
                "Not all applied @ {:?}",
                node.get_info()
            );
            assert_eq!(coordinator_store.len(), node_store.len());
            if coordinator_store != node_store {
                println!("Node: {:?}", node.get_info());
                let mut node_store_iter = node_store.iter();
                for (k, v) in coordinator_store.iter() {
                    if let Some(next) = node_store_iter.next() {
                        if next != (k, v) {
                            println!("Diff: Got {:?}, Expected: {:?}", next, (k, v));
                            println!("Nanos: {:?} | {:?}", next.1 .0.get_nanos(), v.0.get_nanos());
                        }
                    }
                }
                got_mismatch = true;
            }

            assert!(!got_mismatch);
        }
    }

    #[test]
    fn contention_execution() {
        let runtime = Builder::new_multi_thread().enable_all().build().unwrap();

        let handle = runtime.handle().clone();
        handle.block_on(async move {
            let node_names: Vec<_> = (0..5).map(|_| Ulid::new()).collect();
            let mut nodes: Vec<Arc<Node<GrpcNetwork, DummyExecutor, MemStore>>> = vec![];

            for (i, m) in node_names.iter().enumerate() {
                let socket_addr = SocketAddr::from_str(&format!("0.0.0.0:{}", 11000 + i)).unwrap();
                let network = synevi_network::network::GrpcNetwork::new(
                    socket_addr,
                    format!("http://localhost:{}", 10000 + i),
                    *m,
                    i as u16,
                );
                let node =
                    Node::new_with_network_and_executor(*m, i as u16, network, DummyExecutor)
                        .await
                        .unwrap();
                nodes.push(node);
            }
            for (i, name) in node_names.iter().enumerate() {
                for (i2, node) in nodes.iter_mut().enumerate() {
                    if i != i2 {
                        node.add_member(
                            *name,
                            i as u16,
                            format!("http://localhost:{}", 11000 + i),
                            true,
                        )
                        .await
                        .unwrap();
                    }
                }
            }

            let coordinator1 = nodes.pop().unwrap();
            let coordinator2 = nodes.pop().unwrap();
            let coordinator3 = nodes.pop().unwrap();
            let coordinator4 = nodes.pop().unwrap();
            let coordinator5 = nodes.pop().unwrap();

            let mut joinset = tokio::task::JoinSet::new();

            let start = std::time::Instant::now();

            for _ in 0..10000 {
                let coordinator1 = coordinator1.clone();
                let coordinator2 = coordinator2.clone();
                let coordinator3 = coordinator3.clone();
                let coordinator4 = coordinator4.clone();
                let coordinator5 = coordinator5.clone();
                joinset.spawn(async move {
                    coordinator1
                        .transaction(
                            u128::from_be_bytes(Ulid::new().to_bytes()),
                            synevi::TransactionPayload::External(Vec::from("C1")),
                        )
                        .await
                });
                joinset.spawn(async move {
                    coordinator2
                        .transaction(
                            u128::from_be_bytes(Ulid::new().to_bytes()),
                            synevi::TransactionPayload::External(Vec::from("C2")),
                        )
                        .await
                });
                joinset.spawn(async move {
                    coordinator3
                        .transaction(
                            u128::from_be_bytes(Ulid::new().to_bytes()),
                            synevi::TransactionPayload::External(Vec::from("C3")),
                        )
                        .await
                });
                joinset.spawn(async move {
                    coordinator4
                        .transaction(
                            u128::from_be_bytes(Ulid::new().to_bytes()),
                            synevi::TransactionPayload::External(Vec::from("C4")),
                        )
                        .await
                });
                joinset.spawn(async move {
                    coordinator5
                        .transaction(
                            u128::from_be_bytes(Ulid::new().to_bytes()),
                            synevi::TransactionPayload::External(Vec::from("C5")),
                        )
                        .await
                });
            }
            while let Some(res) = joinset.join_next().await {
                match res.unwrap().unwrap() {
                    synevi::ExecutorResult::External(res) => {
                        res.unwrap();
                    }
                    synevi::ExecutorResult::Internal(res) => {
                        res.unwrap();
                    }
                };
            }

            println!("Time: {:?}", start.elapsed());

            let (total, accepts, recovers) = coordinator1.get_stats();
            println!(
                "C1: Fast: {:?}, Slow: {:?} Paths / {:?} Total / {:?} Recovers",
                total - accepts,
                accepts,
                total,
                recovers
            );
            let (total, accepts, recovers) = coordinator2.get_stats();
            println!(
                "C2: Fast: {:?}, Slow: {:?} Paths / {:?} Total / {:?} Recovers",
                total - accepts,
                accepts,
                total,
                recovers
            );

            let (total, accepts, recovers) = coordinator3.get_stats();
            println!(
                "C3: Fast: {:?}, Slow: {:?} Paths / {:?} Total / {:?} Recovers",
                total - accepts,
                accepts,
                total,
                recovers
            );

            let (total, accepts, recovers) = coordinator4.get_stats();
            println!(
                "C4: Fast: {:?}, Slow: {:?} Paths / {:?} Total / {:?} Recovers",
                total - accepts,
                accepts,
                total,
                recovers
            );

            let (total, accepts, recovers) = coordinator5.get_stats();
            println!(
                "C5: Fast: {:?}, Slow: {:?} Paths / {:?} Total / {:?} Recovers",
                total - accepts,
                accepts,
                total,
                recovers
            );

            assert_eq!(recovers, 0);

            let coordinator_store: BTreeMap<T0, (T, Option<[u8; 32]>)> = coordinator1
                .event_store
                .get_event_store()
                .await
                .into_values()
                .map(|e| (e.t_zero, (e.t, e.get_latest_hash())))
                .collect();

            nodes.push(coordinator2);
            nodes.push(coordinator3);
            nodes.push(coordinator4);
            nodes.push(coordinator5);

            let mut got_mismatch = false;
            for node in nodes {
                let node_store: BTreeMap<T0, (T, Option<[u8; 32]>)> = node
                    .event_store
                    .get_event_store()
                    .await
                    .into_values()
                    .map(|e| (e.t_zero, (e.t, e.get_latest_hash())))
                    .collect();
                assert!(node
                    .event_store
                    .get_event_store()
                    .await
                    .iter()
                    .all(|(_, e)| e.state == State::Applied));
                assert_eq!(coordinator_store.len(), node_store.len());
                if coordinator_store != node_store {
                    println!("Node: {:?}", node.get_info());
                    let mut node_store_iter = node_store.iter();
                    for (k, v) in coordinator_store.iter() {
                        if let Some(next) = node_store_iter.next() {
                            if next != (k, v) {
                                println!("Diff: Got {:?}, Expected: {:?}", next, (k, v));
                                println!(
                                    "Nanos: {:?} | {:?}",
                                    next.1 .0.get_nanos(),
                                    v.0.get_nanos()
                                );
                            }
                        }
                    }
                    got_mismatch = true;
                }

                assert!(!got_mismatch);
            }
        });
        runtime.shutdown_background();
    }

    #[test]
    fn consecutive_execution() {
        let runtime = Builder::new_multi_thread().enable_all().build().unwrap();

        let handle = runtime.handle().clone();
        handle.block_on(async move {
            let mut node_names: Vec<_> = (0..5).map(|_| Ulid::new()).collect();
            let mut nodes: Vec<Arc<Node<GrpcNetwork, DummyExecutor, MemStore>>> = vec![];

            for (i, m) in node_names.iter().enumerate() {
                let _path = format!("../tests/database/{}_test_db", i);
                let socket_addr = SocketAddr::from_str(&format!("0.0.0.0:{}", 12000 + i)).unwrap();
                let network = synevi_network::network::GrpcNetwork::new(
                    socket_addr,
                    format!("http://localhost:{}", 10000 + i),
                    *m,
                    i as u16,
                );
                let node =
                    Node::new_with_network_and_executor(*m, i as u16, network, DummyExecutor)
                        .await
                        .unwrap();
                nodes.push(node);
            }
            let coordinator = nodes.pop().unwrap();
            let _ = node_names.pop(); // Do not connect to your self

            for (i, name) in node_names.iter().enumerate() {
                coordinator
                    .clone()
                    .add_member(
                        *name,
                        i as u16,
                        format!("http://localhost:{}", 12000 + i),
                        true,
                    )
                    .await
                    .unwrap();
            }

            for i in 0..1000 {
                match coordinator
                    .clone()
                    .transaction(
                        i,
                        synevi::TransactionPayload::External(Vec::from("This is a transaction")),
                    )
                    .await
                    .unwrap()
                {
                    synevi::ExecutorResult::Internal(res) => {
                        res.unwrap();
                    }
                    synevi::ExecutorResult::External(res) => {
                        res.unwrap();
                    }
                };
            }

            runtime.shutdown_background();
        });
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn reconfiguration() {
        let node_names: Vec<_> = (0..5).map(|_| Ulid::new()).collect();
        let mut nodes: Vec<Arc<Node<GrpcNetwork, DummyExecutor, PersistentStore>>> = vec![];

        for (i, m) in node_names.iter().enumerate() {
            let socket_addr = SocketAddr::from_str(&format!("0.0.0.0:{}", 13000 + i)).unwrap();
            if i == 0 {
                let network = synevi_network::network::GrpcNetwork::new(
                    socket_addr,
                    format!("http://0.0.0.0:{}", 13000 + i),
                    *m,
                    i as u16,
                );

                // Copy & create db
                let test_path = format!("/dev/shm/{m}/");
                fs::create_dir(&test_path).await.unwrap();
                dbg!(&test_path);
                let store = PersistentStore::new(test_path, i as u16).unwrap();
                let node = Node::new(*m, i as u16, network, DummyExecutor, store)
                    .await
                    .unwrap();

                nodes.push(node);
            } else {
                let network = synevi_network::network::GrpcNetwork::new(
                    socket_addr,
                    format!("http://0.0.0.0:{}", 13000 + i),
                    *m,
                    i as u16,
                );

                // Copy & create db
                let test_path = format!("/dev/shm/{m}/");
                fs::create_dir(&test_path).await.unwrap();
                dbg!(&test_path);
                let store = PersistentStore::new(test_path, i as u16).unwrap();
                let node = Node::new_with_member(
                    *m,
                    i as u16,
                    network,
                    DummyExecutor,
                    store,
                    "http://0.0.0.0:13000".to_string(),
                )
                .await
                .unwrap();
                nodes.push(node);
            }
        }
        dbg!("Finished setup");

        let coordinator = nodes.pop().unwrap();

        let mut joinset = tokio::task::JoinSet::new();

        let random_number = rand::thread_rng().gen_range(0..99);
        dbg!(&random_number);
        for i in 0..100 {
            if i == random_number {
                let id = Ulid::new();
                let network = synevi_network::network::GrpcNetwork::new(
                    SocketAddr::from_str("0.0.0.0:13006").unwrap(),
                    "http://0.0.0.0:13006".to_string(),
                    id,
                    6,
                );

                // Copy & create db
                let test_path = format!("/dev/shm/{id}/");
                fs::create_dir(&test_path).await.unwrap();
                dbg!(&test_path);
                let store = PersistentStore::new(test_path, 6).unwrap();
                let node = Node::new_with_member(
                    id,
                    6,
                    network,
                    DummyExecutor,
                    store,
                    "http://0.0.0.0:13000".to_string(),
                )
                .await
                .unwrap();
                nodes.push(node);
                dbg!("Finished second config");
            } else {
                let coordinator = coordinator.clone();
                joinset.spawn(async move {
                    coordinator
                        .transaction(
                            i,
                            synevi::TransactionPayload::External(Vec::from(
                                "This is a transaction",
                            )),
                        )
                        .await
                });
            }
        }
        while let Some(res) = joinset.join_next().await {
            match res.unwrap().unwrap() {
                synevi::ExecutorResult::External(res) => {
                    res.unwrap();
                }
                synevi::ExecutorResult::Internal(res) => {
                    res.unwrap();
                }
            };
        }

        let (total, accepts, recovers) = coordinator.get_stats();
        println!(
            "Fast: {:?}, Slow: {:?} Paths / {:?} Total / {:?} Recovers",
            total - accepts,
            accepts,
            total,
            recovers
        );

        //assert_eq!(recovers, 0);

        let coordinator_store: BTreeMap<T0, (T, Option<[u8; 32]>)> = coordinator
            .event_store
            .get_event_store()
            .await
            .into_values()
            .map(|e| (e.t_zero, (e.t, e.get_latest_hash())))
            .collect();

        assert!(coordinator
            .event_store
            .get_event_store()
            .await
            .iter()
            .all(|(_, e)| e.state == State::Applied));

        let mut got_mismatch = false;
        for node in nodes {
            let node_store: BTreeMap<T0, (T, Option<[u8; 32]>)> = node
                .event_store
                .get_event_store()
                .await
                .into_values()
                .map(|e| (e.t_zero, (e.t, e.get_latest_hash())))
                .collect();
            assert!(
                node.event_store
                    .get_event_store()
                    .await
                    .iter()
                    .all(|(_, e)| e.state == State::Applied),
                "Not all applied @ {:?}",
                node.get_info()
            );
            assert_eq!(coordinator_store.len(), node_store.len());
            if coordinator_store != node_store {
                println!("Node: {:?}", node.get_info());
                let mut node_store_iter = node_store.iter();
                for (k, v) in coordinator_store.iter() {
                    if let Some(next) = node_store_iter.next() {
                        if next != (k, v) {
                            println!("Diff: Got {:?}, Expected: {:?}", next, (k, v));
                            println!("Nanos: {:?} | {:?}", next.1 .0.get_nanos(), v.0.get_nanos());
                        }
                    }
                }
                got_mismatch = true;
            }

            assert!(!got_mismatch);
        }
    }
}
