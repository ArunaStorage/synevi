use anyhow::{anyhow, Result};
use bytes::Bytes;
use consensus_transport::consensus_transport::{Dependency, State};
use diesel_ulid::DieselUlid;
use std::collections::{BTreeMap, HashMap};

pub struct EventStore {
    // TODO:
    // - Both must store t and t0
    //   and key should always be the highest t
    // - Record ballot number for recovery paths
    persisted: HashMap<DieselUlid, Event>,
    temporary: BTreeMap<DieselUlid, Event>,
}

#[derive(Clone, Debug)]
pub struct Event {
    pub state: State,
    pub event: Bytes,
}

impl EventStore {
    pub fn insert(&mut self, t: DieselUlid, event: Event) {
        self.temporary.insert(t, event);
    }
    pub fn persist(&mut self, t: &DieselUlid) -> Result<()> {
        if let Some(Event { event, .. }) = self.temporary.remove(t) {
            self.persisted.insert(
                *t,
                Event {
                    state: State::Applied,
                    event,
                },
            );
            Ok(())
        } else {
            Err(anyhow!("Event not found in temp"))
        }
    }
    pub fn search(&self, t: &DieselUlid) -> Option<Event> {
        match self.temporary.get(t) {
            Some(event) => Some(event.clone()),
            None => self.persisted.get(t).cloned(),
        }
    }
    pub fn update_state(&mut self, t: &DieselUlid, state: State) -> Result<()> {
        if let Some(event) = self.temporary.get_mut(t) {
            event.state = state;
            Ok(())
        } else {
            Err(anyhow!("Event not found"))
        }
    }

    pub fn get_dependencies(&mut self, t_zero: &DieselUlid) -> Vec<Dependency> {
        self.temporary
            .range(t_zero..)
            .map(|(k, v)| Dependency {
                timestamp: k.as_byte_array().into(),
                event: v.event.to_vec(),
                state: v.state.into(),
            })
            .collect()
    }
}
