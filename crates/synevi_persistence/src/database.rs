use heed::{
    byteorder::BigEndian,
    types::{SerdeBincode, U128},
    Database, Env, EnvOpenOptions,
};
use synevi_types::error::PersistenceError;

use crate::event::Event;

#[derive(Clone, Debug)]
pub struct Storage {
    db: Env,
}

const DB_NAME: &str = "events";

impl Storage {
    pub fn new(path: String) -> Result<Storage, PersistenceError> {
        let db = unsafe { EnvOpenOptions::new().open(path)? };
        Ok(Storage { db })
    }

    pub fn new_with_env(env: Env) -> Storage {
        Storage { db: env }
    }

    pub fn read_all(&self) -> Result<Vec<Event>, PersistenceError> {
        let mut wtxn = self.db.read_txn()?;
        let events_db: Database<U128<BigEndian>, SerdeBincode<Event>> = self
            .db
            .open_database(&mut wtxn, Some(DB_NAME))?
            .ok_or_else(|| PersistenceError::DatabaseNotFound(DB_NAME))?;
        let result = events_db
            .iter(&wtxn)?
            .filter_map(|e| {
                if let Ok((_, event)) = e {
                    Some(event)
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();
        Ok(result)
    }

    pub fn upsert_object(&self, event: Event) -> Result<(), PersistenceError> {
        let mut wtxn = self.db.write_txn()?;
        let events_db: Database<U128<BigEndian>, SerdeBincode<Event>> =
            self.db.create_database(&mut wtxn, Some(DB_NAME))?;
        events_db.put(&mut wtxn, &event.t_zero.get_inner(), &event)?;
        wtxn.commit()?;
        Ok(())
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
