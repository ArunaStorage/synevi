use anyhow::Result;
use std::{
    sync::Arc,
    time::{self, Duration, Instant},
};
use tokio::sync::RwLock;
use tonic::{Request, Response};

use crate::{
    configure_transport::{
        time_service_client::TimeServiceClient, GetTimeRequest, GetTimeResponse,
    },
    network::MemberWithLatency,
};

const LATENCY_INTERVAL: u64 = 10;

pub async fn get_latency(members: Arc<RwLock<Vec<MemberWithLatency>>>) -> Result<()> {
    loop {
        for member in members.read().await.iter() {
            let mut client = TimeServiceClient::new(member.member.channel.clone());
            let time = time::SystemTime::now()
                .duration_since(time::UNIX_EPOCH)
                .unwrap()
                .as_nanos();

            let now = time::Instant::now();
            let Ok(response) = client
                .get_time(Request::new(GetTimeRequest {
                    timestamp: time.to_be_bytes().to_vec(),
                }))
                .await
            else {
                println!(
                    "Failed to get time from member: {:?}",
                    member.member.info.id
                );
                continue;
            };
            let Ok((latency, skew)) = calculate_times(now, response) else {
                continue;
            };
            member
                .latency
                .store(latency, std::sync::atomic::Ordering::Relaxed);
            member
                .skew
                .store(skew, std::sync::atomic::Ordering::Relaxed);
        }
        tokio::time::sleep(Duration::from_secs(LATENCY_INTERVAL)).await;
    }
}

fn calculate_times(
    instant_before: Instant,
    response: Response<GetTimeResponse>,
) -> Result<(u64, i64)> {
    let elapsed = instant_before.elapsed().as_nanos();
    let time_now = time::SystemTime::now()
        .duration_since(time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let response = response.into_inner();
    let remote_time = u128::from_be_bytes(response.local_timestamp.as_slice().try_into()?);
    let diff: i64 =
        ((time_now as i128 - (elapsed / 2) as i128 - remote_time as i128) / 1000).try_into()?;
    let remote_time: u64 = (remote_time / 1000).try_into()?;
    Ok((remote_time, diff))
}
