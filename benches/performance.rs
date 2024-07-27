use criterion::{criterion_group, criterion_main, Criterion};
use diesel_ulid::DieselUlid;
use std::{net::SocketAddr, str::FromStr, sync::Arc, time::Duration};
use synevi_core::node::Node;
use tokio::runtime;

async fn prepare() -> (Vec<Arc<Node>>, Vec<u8>) {
    let node_names: Vec<_> = (0..5).map(|_| DieselUlid::generate()).collect();
    let mut nodes: Vec<Node> = vec![];
    let mut receivers = vec![];

    for (i, m) in node_names.iter().enumerate() {
        let socket_addr = SocketAddr::from_str(&format!("0.0.0.0:{}", 10000 + i)).unwrap();
        let network = Arc::new(synevi_network::network::NetworkConfig::new(socket_addr));
        //let path = format!("../tests/database/{}_test_db", i);
        let (sender, receiver) = tokio::sync::mpsc::channel(100);
        let node = Node::new_with_parameters(*m, i as u16, network, None, sender)
            .await
            .unwrap();
        nodes.push(node);
        receivers.push(receiver);
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
    let payload = vec![u8::MAX; 2_000_000];
    (nodes.into_iter().map(Arc::new).collect(), payload.clone())
}

async fn parallel_execution(coordinator: Arc<Node>) {
    let mut joinset = tokio::task::JoinSet::new();

    for i in 0..1000 {
        let coordinator = coordinator.clone();
        joinset.spawn(async move {
            coordinator
                .transaction(i, Vec::from("This is a transaction"))
                .await
        });
    }
    while let Some(res) = joinset.join_next().await {
        res.unwrap().unwrap();
    }
}

async fn contention_execution(coordinators: Vec<Arc<Node>>) {
    let mut joinset = tokio::task::JoinSet::new();

    for i in 0..200 {
        for coordinator in coordinators.iter() {
            let coordinator = coordinator.clone();
            joinset.spawn(async move {
                coordinator
                    .transaction(i, Vec::from("This is a transaction"))
                    .await
            });
        }
    }
    while let Some(res) = joinset.join_next().await {
        res.unwrap().unwrap();
    }
}

async fn _bigger_payloads_execution(coordinator: Arc<Node>, payload: Vec<u8>) {
    let mut joinset = tokio::task::JoinSet::new();

    for i in 0..10 {
        let coordinator = coordinator.clone();
        let payload = payload.clone();
        joinset.spawn(async move { coordinator.transaction(i, payload).await });
    }
    while let Some(res) = joinset.join_next().await {
        res.unwrap().unwrap();
    }
}

pub fn criterion_benchmark(c: &mut Criterion) {
    let runtime = runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();

    let (nodes, _payload) = runtime.block_on(async { prepare().await });
    c.bench_function("parallel", |b| {
        b.to_async(&runtime)
            .iter(|| parallel_execution(nodes.first().unwrap().clone()))
    });

    c.bench_function("contention", |b| {
        b.to_async(&runtime)
            .iter(|| contention_execution(nodes.clone()))
    });

    //c.bench_function("bigger_payloads", |b| {
    //    b.to_async(&runtime)
    //        .iter(|| bigger_payloads_execution(nodes.first().unwrap().clone(), payload.clone()))
    //});
}

criterion_group! {
  name = benches;
  config = Criterion::default().measurement_time(Duration::from_secs(15));
  targets = criterion_benchmark
}
//criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
