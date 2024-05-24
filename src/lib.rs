use std::collections::HashMap;
use chrono::{DateTime, Utc};

pub struct Event {}

pub struct PreAcceptMsg {
    event: Event,
    time_zero: DateTime<Utc>,
}
pub struct ReplyMsg {
    time: DateTime<Utc>,
    dependencies: HashMap<DateTime<Utc>, Event>,
}
pub struct CommitMsg {
    event: Event,
    time_zero: DateTime<Utc>,
    time: DateTime<Utc>,
    union: HashMap<DateTime<Utc>, Event>,
}
pub struct ApplyMsg {
    event: Event,
    time: DateTime<Utc>,
    dependencies: HashMap<DateTime<Utc>, Event>,
    result: HashMap<DateTime<Utc>, Event>,
}
pub struct RecoverMsg {}

#[cfg(test)]
mod tests {
}
