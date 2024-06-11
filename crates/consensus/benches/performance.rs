use std::{net::SocketAddr, str::FromStr, sync::Arc};

use bytes::Bytes;
use consensus::node::{MemberInfo, Node};
use criterion::{criterion_group, criterion_main, Criterion};
use tokio::runtime;


async fn parallel_execution() {
    let node_names = ["first", "second", "third", "fourth", "fifth"];
    let mut nodes: Vec<Node> = vec![];
    for (i, m) in node_names.iter().enumerate() {
        let socket_addr = SocketAddr::from_str(&format!("0.0.0.0:{}", 10000 + i)).unwrap();
        let node = Node::new_with_parameters(m.to_string(), socket_addr).await;
        nodes.push(node);
    }
    let mut coordinator = nodes.pop().unwrap();
    for (i, name) in node_names.iter().enumerate() {
        coordinator
            .add_member(MemberInfo {
                node: name.to_string(),
                host: format!("http://localhost:{}", 10000 + i),
            })
            .await
            .unwrap();
    }

    let mut joinset = tokio::task::JoinSet::new();

    let arc_coordinator = Arc::new(coordinator);

    for _ in 0..10 {
        let coordinator = arc_coordinator.clone();
        joinset.spawn(async move {
            coordinator
                .transaction(Bytes::from("This is a transaction"))
                .await
        });
    }
    while let Some(_res) = joinset.join_next().await {}
}


pub fn criterion_benchmark(c: &mut Criterion) {

    let runtime = runtime::Builder::new_multi_thread().enable_all().build().unwrap();
    c.bench_function("parallel", |b| b.to_async(&runtime).iter(|| parallel_execution())); 
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);