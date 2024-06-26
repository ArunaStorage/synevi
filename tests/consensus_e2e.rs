// #[cfg(test)]
// mod tests {
//     use bytes::Bytes;
//     use consensus::node::Node;
//     use diesel_ulid::DieselUlid;
//     use std::net::SocketAddr;
//     use std::str::FromStr;
//     use std::sync::Arc;
//     use tokio::runtime::Builder;

//     #[tokio::test(flavor = "multi_thread")]
//     async fn parallel_execution() {
//         let mut node_names: Vec<_> = (0..5).map(|_| DieselUlid::generate()).collect();
//         let mut nodes: Vec<Node> = vec![];
//         for (i, m) in node_names.iter().enumerate() {
//             let socket_addr = SocketAddr::from_str(&format!("0.0.0.0:{}", 10000 + i)).unwrap();
//             let node = Node::new_with_parameters(*m, i as u16, socket_addr).await;
//             nodes.push(node);
//         }
//         let mut coordinator = nodes.pop().unwrap();
//         let _ = node_names.pop(); // Do not connect to your self
//         for (i, name) in node_names.iter().enumerate() {
//             coordinator
//                 .add_member(*name, i as u16, format!("http://localhost:{}", 10000 + i))
//                 .await
//                 .unwrap();
//         }

//         let mut joinset = tokio::task::JoinSet::new();

//         let arc_coordinator = Arc::new(coordinator);

//         for _ in 0..1000 {
//             let coordinator = arc_coordinator.clone();
//             joinset.spawn(async move {
//                 coordinator
//                     .transaction(Bytes::from("This is a transaction"))
//                     .await
//             });
//         }
//         while let Some(res) = joinset.join_next().await {
//             res.unwrap().unwrap();
//         }
//     }

//     #[test]
//     fn contention_execution() {
//         let runtime = Builder::new_multi_thread().enable_all().build().unwrap();

//         let handle = runtime.handle().clone();
//         handle.block_on(async move {
//             let node_names: Vec<_> = (0..5).map(|_| DieselUlid::generate()).collect();
//             let mut nodes: Vec<Node> = vec![];
//             for (i, m) in node_names.iter().enumerate() {
//                 let socket_addr = SocketAddr::from_str(&format!("0.0.0.0:{}", 10000 + i)).unwrap();
//                 let node = Node::new_with_parameters(*m, i as u16, socket_addr).await;
//                 nodes.push(node);
//             }
//             let coordinator1 = nodes.get_mut(3).unwrap();
//             for (i, name) in node_names.iter().enumerate() {
//                 if i != 3 {
//                     coordinator1
//                         .add_member(*name, i as u16, format!("http://localhost:{}", 10000 + i))
//                         .await
//                         .unwrap();
//                 }
//             }
//             let coordinator2 = nodes.get_mut(4).unwrap();

//             for (i, name) in node_names.iter().enumerate() {
//                 if i != 4 {
//                     coordinator2
//                         .add_member(*name, i as u16, format!("http://localhost:{}", 10000 + i))
//                         .await
//                         .unwrap();
//                 }
//             }

//             let coordinator1 = nodes.pop().unwrap();
//             let coordinator2 = nodes.pop().unwrap();

//             let mut joinset = tokio::task::JoinSet::new();

//             let arc_coordinator1 = Arc::new(coordinator1);
//             let arc_coordinator2 = Arc::new(coordinator2);

//             for _ in 0..1000 {
//                 let coordinator1 = arc_coordinator1.clone();
//                 let coordinator2 = arc_coordinator2.clone();
//                 joinset.spawn(async move { coordinator1.transaction(Bytes::from("C1")).await });
//                 joinset.spawn(async move { coordinator2.transaction(Bytes::from("C2")).await });
//             }
//             while let Some(res) = joinset.join_next().await {
//                 res.unwrap().unwrap();
//             }

//             println!("Done");
//         });
//         runtime.shutdown_background();
//     }

//     #[test]
//     fn consecutive_execution() {
//         let runtime = Builder::new_multi_thread().enable_all().build().unwrap();

//         let handle = runtime.handle().clone();
//         handle.block_on(async move {
//             let mut node_names: Vec<_> = (0..5).map(|_| DieselUlid::generate()).collect();
//             let mut nodes: Vec<Node> = vec![];
//             for (i, m) in node_names.iter().enumerate() {
//                 let socket_addr = SocketAddr::from_str(&format!("0.0.0.0:{}", 11000 + i)).unwrap();
//                 let node = Node::new_with_parameters(*m, i as u16, socket_addr).await;
//                 nodes.push(node);
//             }
//             let mut coordinator = nodes.pop().unwrap();
//             let _ = node_names.pop(); // Do not connect to your self

//             for (i, name) in node_names.iter().enumerate() {
//                 coordinator
//                     .add_member(*name, i as u16, format!("http://localhost:{}", 11000 + i))
//                     .await
//                     .unwrap();
//             }

//             for _ in 0..1000 {
//                 coordinator
//                     .transaction(Bytes::from("This is a transaction"))
//                     .await
//                     .unwrap();
//             }

//             runtime.shutdown_background();
//         });
//     }
// }
