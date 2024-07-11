#[cfg(test)]
mod tests {
    use diesel_ulid::DieselUlid;
    use std::collections::BTreeMap;
    use std::net::SocketAddr;
    use std::str::FromStr;
    use std::sync::Arc;
    use synevi_consensus::node::Node;
    use synevi_consensus::utils::{T, T0};
    use synevi_network::consensus_transport::State;
    use tokio::runtime::Builder;

    #[tokio::test(flavor = "multi_thread")]
    async fn parallel_execution() {
        let node_names: Vec<_> = (0..5).map(|_| DieselUlid::generate()).collect();
        let mut nodes: Vec<Node> = vec![];

        for (i, m) in node_names.iter().enumerate() {
            let _path = format!("../tests/database/{}_test_db", i);
            let socket_addr = SocketAddr::from_str(&format!("0.0.0.0:{}", 10000 + i)).unwrap();
            let network = Arc::new(synevi_network::network::NetworkConfig::new(socket_addr));
            let node = Node::new_with_parameters(*m, i as u16, network, None)
                .await
                .unwrap();
            nodes.push(node);
        }
        for (i, name) in node_names.iter().enumerate() {
            for (i2, node) in nodes.iter_mut().enumerate() {
                if i != i2 {
                    node.add_member(*name, i as u16, format!("http://localhost:{}", 10000 + i))
                        .await
                        .unwrap();
                }
            }
        }

        let coordinator = nodes.pop().unwrap();
        let arc_coordinator = Arc::new(coordinator);

        let mut joinset = tokio::task::JoinSet::new();

        for _ in 0..10000 {
            let coordinator = arc_coordinator.clone();
            joinset.spawn(async move {
                coordinator
                    .transaction(Vec::from("This is a transaction"))
                    .await
            });
        }
        while let Some(res) = joinset.join_next().await {
            res.unwrap().unwrap();
        }

        let (total, accepts, recovers) = arc_coordinator.get_stats();
        println!(
            "Fast: {:?}, Slow: {:?} Paths / {:?} Total / {:?} Recovers",
            total - accepts,
            accepts,
            total,
            recovers
        );

        //assert_eq!(recovers, 0);

        let coordinator_store: BTreeMap<T0, (T, Option<[u8; 32]>)> = arc_coordinator
            .get_event_store()
            .lock()
            .await
            .events
            .clone()
            .into_values()
            .map(|e| (e.t_zero, (e.t, e.previous_hash)))
            .collect();

        assert!(arc_coordinator
            .get_event_store()
            .lock()
            .await
            .events
            .clone()
            .iter()
            .all(|(_, e)| e.state == State::Applied));

        let mut got_mismatch = false;
        for node in nodes {
            let node_store: BTreeMap<T0, (T, Option<[u8; 32]>)> = node
                .get_event_store()
                .lock()
                .await
                .events
                .clone()
                .into_values()
                .map(|e| (e.t_zero, (e.t, e.previous_hash)))
                .collect();
            assert!(node
                .get_event_store()
                .lock()
                .await
                .events
                .clone()
                .iter()
                .all(|(_, e)| e.state == State::Applied));
            assert_eq!(coordinator_store.len(), node_store.len());
            if coordinator_store != node_store {
                println!("Node:         {:?}", node.get_info());
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
            let node_names: Vec<_> = (0..5).map(|_| DieselUlid::generate()).collect();
            let mut nodes: Vec<Node> = vec![];

            for (i, m) in node_names.iter().enumerate() {
                let socket_addr = SocketAddr::from_str(&format!("0.0.0.0:{}", 11000 + i)).unwrap();
                let network = Arc::new(synevi_network::network::NetworkConfig::new(socket_addr));
                let node = Node::new_with_parameters(*m, i as u16, network, None)
                    .await
                    .unwrap();
                nodes.push(node);
            }
            for (i, name) in node_names.iter().enumerate() {
                for (i2, node) in nodes.iter_mut().enumerate() {
                    if i != i2 {
                        node.add_member(*name, i as u16, format!("http://localhost:{}", 11000 + i))
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

            let arc_coordinator1 = Arc::new(coordinator1);
            let arc_coordinator2 = Arc::new(coordinator2);
            let arc_coordinator3 = Arc::new(coordinator3);
            let arc_coordinator4 = Arc::new(coordinator4);
            let arc_coordinator5 = Arc::new(coordinator5);

            let start = std::time::Instant::now();

            for _ in 0..10000 {
                let coordinator1 = arc_coordinator1.clone();
                let coordinator2 = arc_coordinator2.clone();
                let coordinator3 = arc_coordinator3.clone();
                let coordinator4 = arc_coordinator4.clone();
                let coordinator5 = arc_coordinator5.clone();
                joinset.spawn(async move { coordinator1.transaction(Vec::from("C1")).await });
                joinset.spawn(async move { coordinator2.transaction(Vec::from("C2")).await });
                joinset.spawn(async move { coordinator3.transaction(Vec::from("C3")).await });
                joinset.spawn(async move { coordinator4.transaction(Vec::from("C4")).await });
                joinset.spawn(async move { coordinator5.transaction(Vec::from("C5")).await });
            }
            while let Some(res) = joinset.join_next().await {
                res.unwrap().unwrap();
            }

            println!("Time: {:?}", start.elapsed());

            let (total, accepts, recovers) = arc_coordinator1.get_stats();
            println!(
                "C1: Fast: {:?}, Slow: {:?} Paths / {:?} Total / {:?} Recovers",
                total - accepts,
                accepts,
                total,
                recovers
            );
            let (total, accepts, recovers) = arc_coordinator2.get_stats();
            println!(
                "C2: Fast: {:?}, Slow: {:?} Paths / {:?} Total / {:?} Recovers",
                total - accepts,
                accepts,
                total,
                recovers
            );

            let (total, accepts, recovers) = arc_coordinator3.get_stats();
            println!(
                "C3: Fast: {:?}, Slow: {:?} Paths / {:?} Total / {:?} Recovers",
                total - accepts,
                accepts,
                total,
                recovers
            );

            let (total, accepts, recovers) = arc_coordinator4.get_stats();
            println!(
                "C4: Fast: {:?}, Slow: {:?} Paths / {:?} Total / {:?} Recovers",
                total - accepts,
                accepts,
                total,
                recovers
            );

            let (total, accepts, recovers) = arc_coordinator5.get_stats();
            println!(
                "C5: Fast: {:?}, Slow: {:?} Paths / {:?} Total / {:?} Recovers",
                total - accepts,
                accepts,
                total,
                recovers
            );

            assert_eq!(recovers, 0);

            let coordinator_store: BTreeMap<T0, (T, Option<[u8; 32]>)> = arc_coordinator1
                .get_event_store()
                .lock()
                .await
                .events
                .clone()
                .into_values()
                .map(|e| (e.t_zero, (e.t, e.previous_hash)))
                .collect();

            nodes.push(Arc::<Node>::into_inner(arc_coordinator2).unwrap());
            nodes.push(Arc::<Node>::into_inner(arc_coordinator3).unwrap());
            nodes.push(Arc::<Node>::into_inner(arc_coordinator4).unwrap());
            nodes.push(Arc::<Node>::into_inner(arc_coordinator5).unwrap());

            let mut got_mismatch = false;
            for node in nodes {
                let node_store: BTreeMap<T0, (T, Option<[u8; 32]>)> = node
                    .get_event_store()
                    .lock()
                    .await
                    .events
                    .clone()
                    .into_values()
                    .map(|e| (e.t_zero, (e.t, e.previous_hash)))
                    .collect();
                assert!(node
                    .get_event_store()
                    .lock()
                    .await
                    .events
                    .clone()
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
            let mut node_names: Vec<_> = (0..5).map(|_| DieselUlid::generate()).collect();
            let mut nodes: Vec<Node> = vec![];
            for (i, m) in node_names.iter().enumerate() {
                let socket_addr = SocketAddr::from_str(&format!("0.0.0.0:{}", 12000 + i)).unwrap();
                let network = Arc::new(synevi_network::network::NetworkConfig::new(socket_addr));
                let node = Node::new_with_parameters(*m, i as u16, network, None)
                    .await
                    .unwrap();
                nodes.push(node);
            }
            let mut coordinator = nodes.pop().unwrap();
            let _ = node_names.pop(); // Do not connect to your self

            for (i, name) in node_names.iter().enumerate() {
                coordinator
                    .add_member(*name, i as u16, format!("http://localhost:{}", 12000 + i))
                    .await
                    .unwrap();
            }

            for _ in 0..1000 {
                coordinator
                    .transaction(Vec::from("This is a transaction"))
                    .await
                    .unwrap();
            }

            runtime.shutdown_background();
        });
    }
}
