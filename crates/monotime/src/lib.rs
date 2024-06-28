use anyhow::bail;
use anyhow::Result;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct MonoTime(u128); // nanos << 48 | seq << 32 | node << 16 | 0*16

pub enum TimeResult {
    Time(MonoTime),
    Drift(MonoTime, u128),
}

impl TimeResult {
    pub fn into_time(self) -> MonoTime {
        match self {
            TimeResult::Time(time) => time,
            TimeResult::Drift(time, _) => time,
        }
    }

    pub fn get_time(&self) -> &MonoTime {
        match self {
            TimeResult::Time(time) => time,
            TimeResult::Drift(time, _) => time,
        }
    }

    pub fn get_drift(&self) -> Option<u128> {
        match self {
            TimeResult::Time(_) => None,
            TimeResult::Drift(_, drift) => Some(*drift),
        }
    }
}

impl MonoTime {
    pub fn new(seq: u16, node: u16) -> Self {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap() // This must fail if the system clock is before the UNIX_EPOCH
            .as_nanos();
        let time_stamp = nanos << 48 | (seq as u128) << 32 | (node as u128) << 16;
        assert!(time_stamp.trailing_zeros() >= 16);
        MonoTime(time_stamp)
    }

    pub fn get_nanos(&self) -> u128 {
        self.0 >> 48
    }

    pub fn get_node(&self) -> u16 {
        (self.0 << 96 >> 112) as u16 // Shift out nanos and seq, then shift back to get node
    }

    pub fn get_seq(&self) -> u16 {
        (self.0 << 80 >> 112) as u16 // Shift out nanos, then shift back to get seq
    }

    pub fn next(self) -> TimeResult {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap() // This must fail if the system clock is before the UNIX_EPOCH
            .as_nanos()
            << 48;

        let seq = (self.get_seq().wrapping_add(1) as u128) << 32; // Shift out the nanos than add 1 to seq
        let node = (self.get_node() as u128) << 16;
        if self.0 > nanos {
            let drift = (self.0 - nanos) >> 48 << 48; // Shift out the seq and node first right than back left
            if drift != 0 {
                // Shift out the seq and node than add 1 to nanos
                // shift back and add new seq and node back
                let next_time = ((self.0 >> 48) + 1) << 48 | seq | node;
                return TimeResult::Drift(MonoTime(next_time), drift);
            }
        }
        TimeResult::Time(MonoTime(nanos | seq | node)) // And nanos and increased seq
    }

    pub fn next_with_node(self, node: u16) -> TimeResult {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap() // This must fail if the system clock is before the UNIX_EPOCH
            .as_nanos()
            << 48;

        let seq = (self.get_seq().wrapping_add(1) as u128) << 32; // Shift out the nanos than add 1 to seq
        let node = (node as u128) << 16;
        if self.0 > nanos {
            let drift = (self.0 - nanos) >> 48 << 48; // Shift out the seq and node first right than back left
            if drift != 0 {
                // Shift out the seq and node than add 1 to nanos
                // shift back and add new seq and node back
                let next_time = ((self.0 >> 48) + 1) << 48 | seq | node;
                return TimeResult::Drift(MonoTime(next_time), drift);
            }
        }
        TimeResult::Time(MonoTime(nanos | seq | node)) // And nanos and increased seq
    }

    // Ensures that the time is greater than self and greater than guard
    pub fn next_with_guard(self, guard: &MonoTime) -> TimeResult {
        let time = self.next().into_time();
        if &time < guard {
            let drift = guard.get_nanos() - time.get_nanos();
            let seq_node = (time.0 << 80 >> 80) + (1 << 32); // Shift out the nanos than add 1 to seq
            let next_time = MonoTime((guard.get_nanos() + 1) << 48 | seq_node);
            TimeResult::Drift(next_time, drift)
        } else {
            TimeResult::Time(time)
        }
    }

    pub fn next_with_guard_and_node(self, guard: &MonoTime, node: u16) -> TimeResult {
        let time = self.next_with_node(node).into_time();
        if &time < guard {
            let drift = guard.get_nanos() - time.get_nanos();
            let seq = (time.get_seq() as u128) << 32;
            let node = (time.get_node() as u128) << 16;
            let next_time = MonoTime((guard.get_nanos() + 1) << 48 | seq | node);
            TimeResult::Drift(next_time, drift)
        } else {
            TimeResult::Time(time)
        }
    }

    #[cfg(feature = "unsafe_time")]
    pub fn new_with_time(nanos: u128, seq: u16, node: u16) -> Self {
        let time_stamp = nanos << 48 | (seq as u128) << 32 | (node as u128) << 16;
        assert!(time_stamp.trailing_zeros() >= 16);
        MonoTime(time_stamp)
    }
}

impl From<MonoTime> for [u8; 16] {
    fn from(val: MonoTime) -> Self {
        let mut bytes = [0u8; 16];
        bytes.copy_from_slice(&val.0.to_be_bytes());
        bytes
    }
}

impl From<MonoTime> for Vec<u8> {
    fn from(val: MonoTime) -> Self {
        val.0.to_be_bytes().to_vec()
    }
}

impl TryFrom<&[u8]> for MonoTime {
    type Error = anyhow::Error;

    fn try_from(value: &[u8]) -> Result<Self> {
        if value.len() != 16 {
            bail!("Invalid length");
        }
        let ts = u128::from_be_bytes(value[0..16].try_into()?);
        Ok(MonoTime(ts))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::convert::TryInto;

    #[test]
    fn test_roundtrip() {
        let time = MonoTime::new(0, 0);
        let bytes: [u8; 16] = time.into();
        let time2: MonoTime = bytes.as_ref().try_into().unwrap();
        assert_eq!(time.get_nanos(), time2.get_nanos());
        assert_eq!(time.get_seq(), time2.get_seq());
        assert_eq!(time.get_node(), time2.get_node());
    }

    #[test]
    fn test_next() {
        let time = MonoTime::new(0, 0);
        let time2 = time.next().into_time();
        assert!(time.get_nanos() <= time2.get_nanos());
        assert_eq!(time.get_seq() + 1, time2.get_seq());
        assert_eq!(time.get_node(), time2.get_node());
    }

    #[test]
    fn test_time_backwards() {
        let time = MonoTime::new(0, 0);
        let time2 = MonoTime(
            (time.get_nanos() + 100000000) << 48
                | (time.get_seq() as u128) << 32
                | (time.get_node() as u128) << 16,
        );
        assert!(time2.next().get_drift().is_some());
    }

    #[test]
    fn test_compare() {
        for _ in 0..100000 {
            let time = MonoTime::new(0, 0);
            let time2 = MonoTime::new(0, 0);
            assert!(time < time2);
        }
        let time = MonoTime::new(0, 0);
        assert_eq!(
            time,
            MonoTime(
                (time.get_nanos()) << 48
                    | (time.get_seq() as u128) << 32
                    | (time.get_node() as u128) << 16
            )
        );
        assert!(
            time < MonoTime(
                (time.get_nanos() + 1) << 48
                    | (time.get_seq() as u128) << 32
                    | (time.get_node() as u128) << 16
            )
        );
        assert!(
            time < MonoTime(
                time.get_nanos() << 48
                    | (time.get_seq() as u128 + 1) << 32
                    | (time.get_node() as u128) << 16
            )
        );
        assert!(
            time < MonoTime(
                time.get_nanos() << 48
                    | (time.get_seq() as u128 + 1) << 32
                    | (time.get_node() as u128 + 1) << 16
            )
        );
    }
}
