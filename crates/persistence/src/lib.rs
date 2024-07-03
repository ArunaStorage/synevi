use anyhow::{anyhow, Result};
use bytes::Bytes;
use rocksdb::{DBPath, DBWithThreadMode, MultiThreaded, SingleThreaded, DB};

#[derive(Debug)]
pub struct Database {
    db: DBWithThreadMode<MultiThreaded>,
}
impl Database {
    pub fn new(path: String) -> Result<Self> {
        let db = DBWithThreadMode::open_default(path)?;
        //let db = DB::open_default(path)?;
        Ok(Database { db })
    }
    pub fn persist(&self, key: Bytes, value: Bytes) -> Result<()> {
        self.db.put(key, value).map_err(|e| anyhow!(e.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use crate::Database;
    use bytes::Bytes;

    #[test]
    fn test_db() {
        let db = Database::new("../../tests/database".to_string()).unwrap();
        db.persist(Bytes::from("key"), Bytes::from("value"))
            .unwrap()
    }
}
