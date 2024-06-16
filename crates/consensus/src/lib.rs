mod coordinator;
mod event_store;
pub mod node;
mod replica;
mod utils;

#[cfg(test)]
mod tests {
    use crate::node::Node;
    use bytes::Bytes;
    use diesel_ulid::DieselUlid;
    use std::net::SocketAddr;
    use std::str::FromStr;
    use std::sync::Arc;

    #[tokio::test]
    async fn parallel_execution() {
        let mut node_names: Vec<_> = (0..5).map(|_| DieselUlid::generate()).collect();
        let mut nodes: Vec<Node> = vec![];
        for (i, m) in node_names.iter().enumerate() {
            let socket_addr = SocketAddr::from_str(&format!("0.0.0.0:{}", 10000 + i)).unwrap();
            let node = Node::new_with_parameters(*m, i as u16, socket_addr).await;
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

        let mut joinset = tokio::task::JoinSet::new();

        let arc_coordinator = Arc::new(coordinator);

        for _ in 0..1000 {
            let coordinator = arc_coordinator.clone();
            //tokio::time::sleep(tokio::time::Duration::from_millis(1)).await;
            joinset.spawn(async move {
                coordinator
                    .transaction(Bytes::from("This is a transaction"))
                    .await
            });
        }
        while let Some(_res) = joinset.join_next().await {}
    }

    #[tokio::test]
    async fn consecutive_execution() {
        let mut node_names: Vec<_> = (0..5).map(|_| DieselUlid::generate()).collect();
        let mut nodes: Vec<Node> = vec![];
        for (i, m) in node_names.iter().enumerate() {
            let socket_addr = SocketAddr::from_str(&format!("0.0.0.0:{}", 11000 + i)).unwrap();
            let node = Node::new_with_parameters(*m, i as u16, socket_addr).await;
            nodes.push(node);
        }
        let mut coordinator = nodes.pop().unwrap();
        let _ = node_names.pop(); // Do not connect to your self

        for (i, name) in node_names.iter().enumerate() {
            coordinator
                .add_member(*name, i as u16, format!("http://localhost:{}", 11000 + i))
                .await
                .unwrap();
        }

        for _ in 0..1000 {
            coordinator
                .transaction(Bytes::from("This is a transaction"))
                .await
                .unwrap();
        }
    }
}
