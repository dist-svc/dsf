use crate::sync::{Arc, Mutex};
use std::time::SystemTime;
use std::{collections::HashMap, convert::TryFrom};

use dsf_core::options::Filters;
use log::{debug, error, trace};

use dsf_core::{prelude::*, wire::Container};
use dsf_rpc::replica::ReplicaInfo;

use super::Core;

#[derive(Clone, Debug, PartialEq)]
pub struct ReplicaInst {
    pub info: ReplicaInfo,
    pub page: Container,
}

impl Core {

    /// Find replicas for a given service
    pub fn find_replicas(&self, service_id: &Id) -> Result<Vec<ReplicaInst>, DsfError> {
        let v = self.replicas.get(service_id);

        Ok(v.map(|v| v.clone()).unwrap_or(vec![]))
    }

    /// Create or update a given replica instance
    pub fn create_or_update_replica(
        &mut self,
        service_id: &Id,
        peer_id: &Id,
        page: &Container,
    ) -> Result<(), DsfError> {
        let replicas = self.replicas.entry(service_id.clone()).or_insert(vec![]);
        let replica = replicas.iter_mut().find(|r| &r.info.peer_id == peer_id);

        match replica {
            Some(r) => *r = ReplicaInst::try_from(page.to_owned())?,
            None => {
                let r = ReplicaInst::try_from(page.to_owned())?;
                replicas.push(r);
            }
        }

        Ok(())
    }

    /// Update a specified replica
    pub fn update_replica<F: Fn(&mut ReplicaInst)>(
        &mut self,
        service_id: &Id,
        peer_id: &Id,
        f: F,
    ) -> Result<(), ()> {
        let replicas = self.replicas.entry(service_id.clone()).or_insert(vec![]);
        let replica = replicas.iter_mut().find(|r| &r.info.peer_id == peer_id);

        if let Some(mut r) = replica {
            f(&mut r);
        }

        Ok(())
    }

    /// Remove a specified replica
    pub fn remove_replica(&mut self, _service_id: &Id, _peer_id: &Id) -> Result<Self, ()> {
        todo!()
    }
}


impl TryFrom<Container> for ReplicaInst {
    type Error = DsfError;

    fn try_from(page: Container) -> Result<Self, Self::Error> {
        // Replica pages are _always_ secondary types
        let peer_id = match page.info()? {
            PageInfo::Secondary(s) => s.peer_id.clone(),
            _ => unimplemented!(),
        };

        let info = ReplicaInfo {
            peer_id,

            version: page.header().index(),
            page_id: page.id(),

            issued: page.public_options_iter().issued().unwrap().into(),
            expiry: page.public_options_iter().expiry().map(|v| v.into()),
            updated: SystemTime::now(),

            active: false,
        };

        Ok(Self { page, info })
    }
}
