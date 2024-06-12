use std::time::{SystemTime, UNIX_EPOCH};
use anyhow::Result;
use anyhow::bail;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct MonoTime {
    pub nanos: u128,
    pub seq: u16,
    pub node: u16,
}

impl MonoTime {
    pub fn new(seq: u16, node: u16) -> Self {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap() // This must fail if the system clock is before the UNIX_EPOCH
            .as_nanos();
        MonoTime { nanos, seq, node }
    }

    pub fn next(self) -> Result<Self> {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap() // This must fail if the system clock is before the UNIX_EPOCH
            .as_nanos();

        if self.nanos > nanos {
            bail!("Time went backwards");
        }
        Ok(MonoTime {
            nanos,
            seq: self.seq + 1,
            node: self.node,
        })
    }
}


impl Into<[u8; 20]> for MonoTime {
    fn into(self) -> [u8; 20] {
        let mut bytes = [0u8; 20];
        bytes[0..16].copy_from_slice(&self.nanos.to_be_bytes());
        bytes[16..18].copy_from_slice(&self.seq.to_be_bytes());
        bytes[18..20].copy_from_slice(&self.node.to_be_bytes());
        bytes
    }
}

impl TryFrom<&[u8]> for MonoTime {
    type Error = anyhow::Error;

    fn try_from(value: &[u8]) -> Result<Self> {
        if value.len() != 20 {
            bail!("Invalid length");
        }
        let nanos = u128::from_be_bytes(value[0..16].try_into()?);
        let seq = u16::from_be_bytes(value[16..18].try_into()?);
        let node = u16::from_be_bytes(value[18..20].try_into()?);
        Ok(MonoTime { nanos, seq, node })
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use std::convert::TryInto;

    #[test]
    fn test_roundtrip() {
        let time = MonoTime::new(0, 0);
        let bytes: [u8; 20] = time.into();
        let time2: MonoTime = bytes.as_ref().try_into().unwrap();
        assert_eq!(time.nanos, time2.nanos);
        assert_eq!(time.seq, time2.seq);
        assert_eq!(time.node, time2.node);
    }

    #[test]
    fn test_next() {
        let time = MonoTime::new(0, 0);
        let time2 = time.next().unwrap();
        assert!(time.nanos <= time2.nanos);
        assert_eq!(time.seq + 1, time2.seq);
        assert_eq!(time.node, time2.node);
    }

    #[test]
    fn test_time_backwards() {
        let time = MonoTime::new(0, 0);
        let time2 = MonoTime {
            nanos: time.nanos + 10000000000,
            seq: time.seq,
            node: time.node,
        };
        assert!(time2.next().is_err());
    }

    #[test]
    fn test_compare() {
        for _ in 0..100000 {
            let time = MonoTime::new(0, 0);
            let time2 = MonoTime::new(0, 0);
            assert!(time < time2);
        }
        let time = MonoTime::new(0, 0);
        assert_eq!(time, MonoTime{ nanos: time.nanos, seq: time.seq, node: time.node });
        assert!(time < MonoTime{ nanos: time.nanos + 1, seq: time.seq, node: time.node });
        assert!(time < MonoTime{ nanos: time.nanos, seq: time.seq + 1, node: time.node });
        assert!(time < MonoTime{ nanos: time.nanos, seq: time.seq, node: time.node + 1});
    }
}