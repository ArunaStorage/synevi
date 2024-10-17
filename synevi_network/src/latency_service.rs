use bytes::{BufMut, BytesMut};
use std::collections::HashMap;
use std::{
    sync::Arc,
    time::{self, Duration, Instant},
};
use synevi_types::error::SyneviError;
use tokio::sync::RwLock;
use tonic::{Request, Response};
use ulid::Ulid;

use crate::{
    configure_transport::{
        time_service_client::TimeServiceClient, GetTimeRequest, GetTimeResponse,
    },
    network::MemberWithLatency,
};

const LATENCY_INTERVAL: u64 = 10;

pub async fn get_latency(
    members: Arc<RwLock<HashMap<Ulid, Arc<MemberWithLatency>, ahash::RandomState>>>,
) -> Result<(), SyneviError> {
    loop {
        for (_, member) in members.read().await.iter() {
            let mut client = TimeServiceClient::new(member.member.channel.clone());
            let time = time::SystemTime::now()
                .duration_since(time::UNIX_EPOCH)
                .unwrap()
                .as_nanos();

            let now = time::Instant::now();
            let mut buf = BytesMut::with_capacity(16);
            buf.put_u128(time);
            let Ok(response) = client
                .get_time(Request::new(GetTimeRequest {
                    timestamp: buf.freeze().to_vec(),
                }))
                .await
            else {
                tracing::error!("Failed to get time from {:?}", member.member);
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
) -> Result<(u64, i64), SyneviError> {
    let elapsed = instant_before.elapsed().as_nanos();
    let time_now = time::SystemTime::now()
        .duration_since(time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let response = response.into_inner();
    let remote_time = u128::from_be_bytes(response.local_timestamp.as_slice().try_into()?);
    let diff: i64 =
        ((time_now as i128 - (elapsed / 2) as i128 - remote_time as i128) / 1000).try_into()?;
    let elapsed_time: u64 = (elapsed / 1000).try_into()?;
    Ok((elapsed_time, diff))
}
