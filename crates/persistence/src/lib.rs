use anyhow::{anyhow, Result};
use bytes::Bytes;
use rocksdb::{ColumnFamilyDescriptor, DBWithThreadMode, MultiThreaded, Options};
use std::sync::Arc;
use tokio::task::spawn_blocking;

#[derive(Clone, Debug)]
pub struct Database {
    db: Arc<DBWithThreadMode<MultiThreaded>>,
}

impl Database {
    pub fn new(path: String) -> Result<Database> {
        //let db = DBWithThreadMode::open_default(path)?;

        // Setup column families
        let mut cf_opts = Options::default();
        cf_opts.set_max_write_buffer_number(16);
        let events = ColumnFamilyDescriptor::new("events", cf_opts.clone());
        let states = ColumnFamilyDescriptor::new("states", cf_opts);
        let column_families = vec![events, states];

        // Setup database
        let mut db_opts = Options::default();
        db_opts.create_missing_column_families(true);
        db_opts.create_if_missing(true);

        // Create DB
        let db: Arc<DBWithThreadMode<MultiThreaded>> = Arc::new(
            DBWithThreadMode::open_cf_descriptors(&db_opts, path, column_families)?,
        );

        Ok(Database { db })
    }

    pub async fn init(&self, key: Bytes, transaction: Bytes, state: Bytes) -> Result<()> {
        let db = self.clone();
        spawn_blocking(move || -> Result<()> {
            let event_cf = db
                .db
                .cf_handle("events")
                .ok_or_else(|| anyhow!("ColumnFamily for events not found"))?;
            let state_cf = db
                .db
                .cf_handle("states")
                .ok_or_else(|| anyhow!("ColumnFamily for states not found"))?;
            db.db
                .put_cf(&event_cf, &key, transaction)
                .map_err(|e| anyhow!(e))?;
            db.db
                .put_cf(&state_cf, key, state)
                .map_err(|e| anyhow!(e))?;
            Ok(())
        })
        .await?
    }

    pub async fn update(&self, key: Bytes, state: Bytes) -> Result<()> {
        let db = self.clone();
        spawn_blocking(move || -> Result<()> {
            let state_cf = db
                .db
                .cf_handle("states")
                .ok_or_else(|| anyhow!("ColumnFamily for states not found"))?;
            db.db
                .put_cf(&state_cf, key, state)
                .map_err(|e| anyhow!(e))?;
            Ok(())
        })
        .await?
    }
}

#[cfg(test)]
mod tests {

    #[test]
    fn test_db() {
        // TODO
        //let db = Database::new("../../tests/database".to_string()).unwrap();
        //db.init(Bytes::from("key"), Bytes::from("value"))
        //    .unwrap()
    }
}
