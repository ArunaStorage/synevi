use anyhow::{anyhow, Result};
use bytes::Bytes;
use rocksdb::{ColumnFamilyDescriptor, DBWithThreadMode, IteratorMode, MultiThreaded, Options};
use std::sync::Arc;
use tokio::task::spawn_blocking;

#[derive(Clone, Debug)]
pub struct Database {
    db: Arc<DBWithThreadMode<MultiThreaded>>,
}

#[derive(Clone, Debug)]
pub struct SplitEvent {
    pub key: Bytes,
    pub event: Bytes,
    pub state: Bytes,
}

impl Database {
    pub fn new(path: String) -> Result<Database> {
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

    pub fn read_all(&self) -> Result<Vec<SplitEvent>> {
        let event_cf = self
            .db
            .cf_handle("events")
            .ok_or_else(|| anyhow!("ColumnFamily for events not found"))?;
        let state_cf = self
            .db
            .cf_handle("states")
            .ok_or_else(|| anyhow!("ColumnFamily for states not found"))?;
        let event_iter = self.db.iterator_cf(&event_cf, IteratorMode::Start);
        let mut result = Vec::new();
        for event in event_iter {
            let (key, event) =
                event.map(|(k, v)| (Bytes::from(k.to_vec()), Bytes::from(v.to_vec())))?;
            let state = if let Some(exists) = self.db.get_cf(&state_cf, &key)? {
                Bytes::from(exists)
            } else {
                Bytes::new()
            };
            result.push(SplitEvent { key, event, state });
        }
        Ok(result)
    }

    pub async fn init_object(&self, key: Bytes, transaction: Bytes, state: Bytes) -> Result<()> {
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

    pub async fn update_object(&self, key: Bytes, state: Bytes) -> Result<()> {
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
