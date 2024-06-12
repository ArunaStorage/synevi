mod coordinator;
mod event_store;
pub mod node;
mod replica;
mod utils;

#[cfg(test)]
mod tests {
    use crate::node::{Node, MemberInfo};
    use bytes::Bytes;
    use std::net::SocketAddr;
    use std::str::FromStr;
    use std::sync::Arc;

    #[tokio::test]
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
        let node_names = ["first", "second", "third", "fourth", "fifth"];
        let mut nodes: Vec<Node> = vec![];
        for (i, m) in node_names.iter().enumerate() {
            let socket_addr = SocketAddr::from_str(&format!("0.0.0.0:{}", 11000 + i)).unwrap();
            let node = Node::new_with_parameters(m.to_string(), socket_addr).await;
            nodes.push(node);
        }
        let mut coordinator = nodes.pop().unwrap();
        for (i, name) in node_names.iter().enumerate() {
            coordinator
                .add_member(MemberInfo {
                    node: name.to_string(),
                    host: format!("http://localhost:{}", 11000 + i),
                })
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
