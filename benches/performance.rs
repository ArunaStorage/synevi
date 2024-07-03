use bytes::Bytes;
use consensus::node::Node;
use criterion::{criterion_group, criterion_main, Criterion};
use diesel_ulid::DieselUlid;
use std::{net::SocketAddr, str::FromStr, sync::Arc, time::Duration};
use tokio::{runtime, sync::Mutex};

async fn prepare() -> Arc<Node> {
    let mut node_names: Vec<_> = (0..5).map(|_| DieselUlid::generate()).collect();
    let mut nodes: Vec<Node> = vec![];

    for (i, m) in node_names.iter().enumerate() {
        let socket_addr = SocketAddr::from_str(&format!("0.0.0.0:{}", 10000 + i)).unwrap();
        let network = Arc::new(Mutex::new(
            consensus_transport::network::NetworkConfig::new(socket_addr),
        ));
        let node = Node::new_with_parameters(*m, i as u16, network, None)
            .await
            .unwrap();
        nodes.push(node);
    }
    let mut coordinator = nodes.pop().unwrap();
    let _ = node_names.pop(); // Do not connect to your self
    for (i, name) in node_names.iter().enumerate() {
        coordinator
            .add_member(*name, i as u16, format!("http://localhost:{}", 10000 + i))
            .await
            .unwrap();
    }

    Arc::new(coordinator)
}
async fn parallel_execution(coordinator: Arc<Node>) {
    let mut joinset = tokio::task::JoinSet::new();

    for _ in 0..1000 {
        let coordinator = coordinator.clone();
        joinset.spawn(async move {
            coordinator
                .transaction(Bytes::from("This is a transaction"))
                .await
        });
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

    let coordinator = runtime.block_on(async { prepare().await });
    c.bench_function("parallel", |b| {
        b.to_async(&runtime)
            .iter(|| parallel_execution(coordinator.clone()))
    });
}

criterion_group! {
  name = benches;
  config = Criterion::default().measurement_time(Duration::from_secs(15));
  targets = criterion_benchmark
}
//criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
