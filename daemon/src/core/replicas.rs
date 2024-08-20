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
    pub fn find_replicas(&self, service_id: &Id) -> Result<Vec<ReplicaInfo>, DsfError> {
        let v = match self.replicas.get(service_id) {
            Some(v) => v,
            None => return Ok(vec![]),
        };

        Ok(v.iter().map(|r| r.info.clone()).collect())
    }

    /// Create or update a given replica instance
    pub fn create_or_update_replicas(
        &mut self,
        service_id: &Id,
        replicas: Vec<Container>,
    ) -> Result<Vec<ReplicaInfo>, DsfError> {
        let mut updated = vec![];

        for replica_page in replicas {
            // Parse replica information
            let e = match ReplicaInst::try_from(&replica_page) {
                Ok(v) => v,
                Err(e) => {
                    error!(
                        "Failed to parse replica page {:?}: {e:?}",
                        replica_page.signature()
                    );
                    continue;
                }
            };

            // Find matching replica entry for service and peer
            let svc_replicas = self.replicas.entry(service_id.clone()).or_default();
            let matching_replica = svc_replicas
                .iter_mut()
                .find(|r| &r.info.peer_id == &e.info.peer_id);

            // Add information to update list
            updated.push(e.info.clone());

            // Insert or update the replica entry
            match matching_replica {
                Some(r) => {
                    // TODO(low): compute whether the new data is actually an update and select whether to apply
                    *r = e;
                }
                None => {
                    svc_replicas.push(e);
                }
            }
        }

        Ok(updated)
    }

    /// Update a specified replica
    pub fn update_replica<F: Fn(&mut ReplicaInst)>(
        &mut self,
        service_id: &Id,
        peer_id: &Id,
        f: F,
    ) -> Result<(), ()> {
        let replicas = self.replicas.entry(service_id.clone()).or_default();
        let replica = replicas.iter_mut().find(|r| &r.info.peer_id == peer_id);

        if let Some(r) = replica {
            f(r);
        }

        Ok(())
    }

    /// Remove a specified replica
    pub fn remove_replica(&mut self, _service_id: &Id, _peer_id: &Id) -> Result<Self, ()> {
        todo!("replica remove not yet implemented")
    }
}

impl TryFrom<&Container> for ReplicaInst {
    type Error = DsfError;

    fn try_from(page: &Container) -> Result<Self, Self::Error> {
        // TODO(low): Check replica headers (this is pre-checked in the daemon)
        // Replica pages are _always_ secondary types
        let peer_id = match page.info()? {
            PageInfo::Secondary(s) => s.peer_id.clone(),
            _ => return Err(DsfError::InvalidPageKind),
        };

        let info = ReplicaInfo {
            peer_id,

            version: page.header().index(),
            page_id: page.id(),

            issued: page.public_options_iter().issued().unwrap().into(),
            expiry: page.public_options_iter().expiry().map(|v| v.into()),
            updated: SystemTime::now(),

            active: false,
            authorative: false,
        };

        Ok(Self {
            page: page.clone(),
            info,
        })
    }
}
