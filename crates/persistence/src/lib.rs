use anyhow::{anyhow, Result};
use bytes::Bytes;
use rocksdb::{ColumnFamilyDescriptor, DBWithThreadMode, MultiThreaded, Options};
use tokio::task::spawn_blocking;

#[derive(Debug)]
pub struct Database {
    db: DBWithThreadMode<MultiThreaded>,
}

pub struct Entry {
    pub t0: Bytes,
    pub t: Bytes,
    pub state: i32,
    pub transaction: Bytes,
}
impl Database {
    pub fn new(path: String) -> Result<Self> {
        //let db = DBWithThreadMode::open_default(path)?;

        // Setup column families
        let mut cf_opts = Options::default();
        cf_opts.set_max_write_buffer_number(16);
        let events = ColumnFamilyDescriptor::new("events", cf_opts.clone());
        let states = ColumnFamilyDescriptor::new("states", cf_opts);
        let column_families= vec![events,states];


        // Setup database
        let mut db_opts = Options::default();
        db_opts.create_missing_column_families(true);
        db_opts.create_if_missing(true);

        // Create DB
        let db = DBWithThreadMode::open_cf_descriptors(&db_opts, path, column_families)?;
        Ok(Database { db })

    }
    pub fn persist(&self, key: Bytes, value: Bytes) -> Result<()> {
        self.db.put(key, value).map_err(|e| anyhow!(e.to_string()))
    }

    pub async fn init(&self, event: Entry) -> Result<()> {
        todo!();
        //spawn_blocking(|| -> Result<()> {
        //    let event_cf= self.db.cf_handle("events").ok_or_else(||anyhow!("ColumnFamily for events not found"))?;
        //    let state_cf= self.db.cf_handle("states").ok_or_else(||anyhow!("ColumnFamily for states not found"))?;
        //    self.db.put_cf( &event_cf ,event.t0, event.transaction).map_err(|e| anyhow!(e))?;
        //    self.db.put_cf( &state_cf, event.t0, event.transaction).map_err(|e| anyhow!(e))?;
        //    Ok(())
        //}).await?
    }
    pub async fn update(&self, event: Entry) -> Result<()> {
        todo!();
        //spawn_blocking(|| -> Result<()> {
        //    self.db.put(event.t0, event.transaction).map_err(|e| anyhow!(e))
        //}).await?
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
