use std::fmt::{Debug, Formatter};
use anyhow::anyhow;
use crate::kv_store::{KVStore, Transaction};

impl Debug for KVStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KVStore")
            .field("store", &self.store)
            .field("transactions", &self.transactions)
            .finish()
    }
}

impl TryFrom<Vec<u8>> for Transaction {
    type Error = anyhow::Error;
    fn try_from(value: Vec<u8>) -> std::result::Result<Self, Self::Error> {
        let (state, payload) = value.split_at(1);
        match state {
            [0u8] => Ok(Transaction::Read {
                key: String::from_utf8(payload.to_vec())?,
            }),
            [1u8] => {
                let (len, key_val) = payload.split_at(8);
                let len = u64::from_be_bytes(<[u8; 8]>::try_from(len)?);
                let (key, val) = key_val.split_at(len.try_into()?);

                Ok(Transaction::Write {
                    key: String::from_utf8(key.to_vec())?,
                    value: String::from_utf8(val.to_vec())?,
                })
            }
            [2u8] => {
                // Get key len and parse key
                let (key_len, key_from_to) = payload.split_at(8);
                let key_len = u64::from_be_bytes(<[u8; 8]>::try_from(key_len)?);
                let (key, from_to) = key_from_to.split_at(key_len.try_into()?);

                // Get from len and parse from
                let (from_len, from_to) = from_to.split_at(8);
                let from_len = u64::from_be_bytes(<[u8; 8]>::try_from(from_len)?);
                let (from, to) = from_to.split_at(from_len.try_into()?);

                Ok(Transaction::Cas {
                    key: String::from_utf8(key.to_vec())?,
                    from: String::from_utf8(from.to_vec())?,
                    to: String::from_utf8(to.to_vec())?,
                })
            }
            _ => Err(anyhow!("Invalid state")),
        }
    }
}

impl From<Transaction> for Vec<u8> {
    fn from(value: Transaction) -> Self {
        let mut result = Vec::new();
        match value {
            Transaction::Read { key } => {
                result.push(0);
                result.extend(key.into_bytes());
            }
            Transaction::Write { key, value } => {
                result.push(1);
                let key = key.into_bytes();
                let len = key.len() as u64;
                result.extend(len.to_be_bytes());
                result.extend(key);
                result.extend(value.into_bytes());
            }
            Transaction::Cas { key, from, to } => {
                result.push(2);
                let key = key.into_bytes();
                let key_len = key.len() as u64;
                result.extend(key_len.to_be_bytes());
                result.extend(key);
                let from = from.into_bytes();
                let from_len = from.len() as u64;
                result.extend(from_len.to_be_bytes());
                result.extend(from);
                result.extend(to.into_bytes());
            }
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use crate::utils::Transaction;

    #[test]
    fn test_conversion() {
        let transaction_read = Transaction::Read {
            key: "1".to_string(),
        };
        let transaction_write = Transaction::Write {
            key: "1".to_string(),
            value: "world".to_string(),
        };
        let transaction_cas = Transaction::Cas {
            key: "1".to_string(),
            from: "2".to_string(),
            to: "3".to_string(),
        };
        let convert_read: Vec<u8> = transaction_read.clone().into();
        let back_converted_read = convert_read.try_into().unwrap();
        assert_eq!(transaction_read, back_converted_read);

        let convert_write: Vec<u8> = transaction_write.clone().into();
        let back_converted_write = convert_write.try_into().unwrap();
        assert_eq!(transaction_write, back_converted_write);

        let convert_cas: Vec<u8> = transaction_cas.clone().into();
        let back_converted_cas = convert_cas.try_into().unwrap();
        assert_eq!(transaction_cas, back_converted_cas);
    }
}
