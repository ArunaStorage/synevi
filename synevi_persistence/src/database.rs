use heed::{
    byteorder::BigEndian,
    types::{SerdeBincode, U128},
    Database, Env, EnvOpenOptions,
};
use synevi_types::{error::SyneviError, traits::{Dependencies, Store}, types::Event};

#[derive(Clone, Debug)]
pub struct PersistentStore {
    db: Env,
}

const EVENT_DB_NAME: &str = "events";

impl PersistentStore {
    pub fn new(path: String) -> Result<PersistentStore, SyneviError> {
        let db = unsafe { EnvOpenOptions::new().open(path)? };
        Ok(PersistentStore { db })
    }

    pub fn new_with_env(env: Env) -> PersistentStore {
        PersistentStore { db: env }
    }

    pub fn read_all(&self) -> Result<Vec<Event>, SyneviError> {
        let wtxn = self.db.read_txn()?;
        let events_db: Database<U128<BigEndian>, SerdeBincode<Event>> = self
            .db
            .open_database(&wtxn, Some(EVENT_DB_NAME))?
            .ok_or_else(|| SyneviError::DatabaseNotFound(EVENT_DB_NAME))?;
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

    pub fn upsert_object(&self, event: Event) -> Result<(), SyneviError> {
        let mut wtxn = self.db.write_txn()?;
        let events_db: Database<U128<BigEndian>, SerdeBincode<Event>> =
            self.db.create_database(&mut wtxn, Some(EVENT_DB_NAME))?;
        events_db.put(&mut wtxn, &event.t_zero.get_inner(), &event)?;
        wtxn.commit()?;
        Ok(())
    }
}

impl Store for PersistentStore {
    fn new(node_serial: u16) -> Result<Self, SyneviError> {
        todo!()
    }

    fn init_t_zero(&mut self, node_serial: u16) -> synevi_types::T0 {
        todo!()
    }

    fn pre_accept_tx(
        &mut self,
        id: u128,
        t_zero: synevi_types::T0,
        transaction: Vec<u8>,
    ) -> Result<(synevi_types::T, Dependencies), SyneviError> {
        todo!()
    }

    fn get_tx_dependencies(&self, t: &synevi_types::T, t_zero: &synevi_types::T0) -> Dependencies {
        todo!()
    }

    fn get_recover_deps(&self, t_zero: &synevi_types::T0) -> Result<synevi_types::types::RecoverDependencies, SyneviError> {
        todo!()
    }

    fn recover_event(
        &mut self,
        t_zero_recover: &synevi_types::T0,
        node_serial: u16,
    ) -> Result<synevi_types::types::RecoverEvent, SyneviError> {
        todo!()
    }

    fn accept_tx_ballot(&mut self, t_zero: &synevi_types::T0, ballot: synevi_types::Ballot) -> Option<synevi_types::Ballot> {
        todo!()
    }

    fn upsert_tx(&mut self, upsert_event: synevi_types::types::UpsertEvent) -> Result<Option<synevi_types::types::Hashes>, SyneviError> {
        todo!()
    }

    fn get_event_state(&self, t_zero: &synevi_types::T0) -> Option<synevi_types::State> {
        todo!()
    }

    fn get_event_store(&self) -> std::collections::BTreeMap<synevi_types::T0, Event> {
        todo!()
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
