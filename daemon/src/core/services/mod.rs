//! Services module maintains the service database
//!
//!

use crate::sync::{Arc, Mutex};
use std::collections::HashMap;
use std::ops::Add;
use std::time::{Duration, SystemTime};

use dsf_core::wire::Container;
use log::{debug, error, info, trace};

use dsf_core::options::Options;
use dsf_core::prelude::*;
use dsf_core::service::Subscriber;

pub use dsf_rpc::service::{ServiceInfo, ServiceState};

use crate::store::Store;

pub mod inst;
pub use inst::ServiceInst;

pub mod data;
pub use data::Data;

/// ServiceManager keeps track of local and remote services
pub struct ServiceManager {
    pub(crate) services: HashMap<Id, ServiceInst>,
    store: Store,
}

impl ServiceManager {
    /// Create a new service manager instance
    pub fn new(store: Store) -> Self {
        let services = HashMap::new();

        let mut s = Self { services, store };

        s.load();

        s
    }

    /// Register a local service for tracking
    pub fn register(
        &mut self,
        service: Service,
        primary_page: &Container,
        state: ServiceState,
        updated: Option<SystemTime>,
    ) -> Result<ServiceInfo, DsfError> {
        let id = service.id();

        // Create a service instance wrapper
        let inst = ServiceInst {
            service,
            state,
            index: self.services.len(),
            last_updated: updated,
            primary_page: Some(primary_page.clone()),
            replica_page: None,
            changed: true,
        };

        let info = inst.info();

        // Write new instance to disk
        #[cfg(feature = "store")]
        self.sync_inst(&inst);

        // Insert into storage
        self.services.insert(id, inst);

        Ok(info)
    }

    /// Determine whether a service is known in the database
    pub fn known(&self, id: &Id) -> bool {
        self.services.contains_key(id)
    }

    /// Fetch service information for a given service id from the manager
    pub fn find(&self, id: &Id) -> Option<ServiceInfo> {
        let service = match self.services.get(id) {
            Some(v) => v,
            None => return None,
        };

        Some(service.info())
    }

    /// Fetch a copy of a stored service
    pub fn find_copy(&self, id: &Id) -> Option<Service> {
        let inst = match self.services.get(id) {
            Some(v) => v,
            None => return None,
        };

        Some(inst.service.clone())
    }

    /// Fetch a service by local index
    pub fn index(&self, index: usize) -> Option<ServiceInfo> {
        self.services
            .iter()
            .find(|(_id, s)| s.index == index)
            .map(|(_id, s)| s.info())
    }

    pub fn index_to_id(&self, index: usize) -> Option<Id> {
        self.services
            .iter()
            .find(|(_id, s)| s.index == index)
            .map(|(id, _s)| id.clone())
    }

    pub fn remove(&mut self, id: &Id) -> Result<Option<ServiceInfo>, DsfError> {
        // Remove from memory
        let service = self.services.remove(id);

        // Remove from database
        if let Some(s) = service {
            let info = s.info();
            // TODO: DEADLOCK?
            #[cfg(feature = "store")]
            let _ = self.store.delete_service(&info);

            return Ok(Some(info));
        };

        return Ok(None);
    }

    /// Fetch data for a given service
    #[cfg(nope)]
    pub fn data(&self, id: &Id, n: usize) -> Result<Vec<DataInfo>, DsfError> {
        let service = match self.find(id) {
            Some(s) => s,
            None => return Err(DsfError::UnknownService),
        };

        let s = service;

        let d = s.get_data(n).map(|d| DataInfo {
            service: id.clone(),
            index: d.version(),
            body: d.body().clone(),
            previous: None,
            signature: d.signature().unwrap(),
        });

        Ok(d.collect())
    }

    /// Update a service instance (if found)
    pub fn with<F, R>(&mut self, id: &Id, mut f: F) -> Option<R>
    where
        F: FnMut(&mut ServiceInst) -> R,
    {
        match self.services.get_mut(id) {
            Some(svc) => {
                let r = (f)(svc);
                svc.changed = true;
                Some(r)
            }
            None => None,
        }
    }

    /// Update a service instance (if found)
    pub fn update_inst<F>(&mut self, id: &Id, mut f: F) -> Option<ServiceInfo>
    where
        F: FnMut(&mut ServiceInst),
    {
        match self.services.get_mut(id) {
            Some(svc) => {
                (f)(svc);
                svc.changed = true;
                Some(svc.info())
            }
            None => None,
        }
    }

    pub fn validate_pages(&mut self, id: &Id, pages: &[Container]) -> Result<(), DsfError> {
        let service_inst = match self.services.get_mut(id) {
            Some(s) => s,
            None => return Err(DsfError::UnknownService),
        };

        for p in pages {
            service_inst.service().validate_page(p)?;
        }

        Ok(())
    }

    /// Fetch a field from a service instance
    pub fn filter<F, R>(&mut self, id: &Id, f: F) -> Option<R>
    where
        F: Fn(&ServiceInst) -> R,
    {
        match self.services.get(id) {
            Some(s) => {
                let svc = s;
                Some((f)(&svc))
            }
            None => None,
        }
    }

    /// Fetch services with a matching state requiring update
    pub fn updates_required(
        &mut self,
        state: ServiceState,
        interval: Duration,
        force: bool,
    ) -> Vec<ServiceInfo> {
        let updates: Vec<_> = self
            .services
            .iter()
            .filter_map(|(_id, svc)| {
                let i = svc;
                match i.update_required(state, interval, force) {
                    true => Some(i.info()),
                    false => None,
                }
            })
            .collect();

        trace!("updates required (state: {:?}): {:?}", state, updates);

        updates
    }

    /// Sync a service instance to disk
    pub(crate) fn sync_inst(&mut self, inst: &ServiceInst) {
        trace!("service sync inst");

        #[cfg(feature = "store")]
        if let Err(e) = self.store.save_service(&inst.info()) {
            error!("Error writing service instance {}: {:?}", inst.id(), e);
        }

        #[cfg(feature = "store")]
        if let Some(p) = &inst.primary_page {
            self.store.save_page(p).unwrap();
        }

        #[cfg(feature = "store")]
        if let Some(p) = &inst.replica_page {
            self.store.save_page(p).unwrap();
        }
    }

    /// Fetch a list of information for known services
    pub fn list(&self) -> Vec<ServiceInfo> {
        self.services.iter().map(|(_k, v)| v.info()).collect()
    }

    /// Fetch the number of known services
    pub fn count(&self) -> usize {
        self.services.len()
    }

    /// Sync the service database to disk
    pub fn sync(&mut self) {
        trace!("services sync");

        #[cfg(feature = "store")]
        for (id, inst) in self.services.iter_mut() {
            // Skip unchanged instances
            if !inst.changed {
                continue;
            }

            if let Err(e) = self.store.save_service(&inst.info()) {
                error!("Error writing service instance {}: {:?}", id, e);
            }

            if let Some(p) = &inst.primary_page {
                self.store.save_page(p).unwrap();
            }

            if let Some(p) = &inst.replica_page {
                self.store.save_page(p).unwrap();
            }

            inst.changed = false;
        }
    }

    /// Load the service database from disk
    pub fn load(&mut self) {
        trace!("services load");

        #[cfg(feature = "store")]
        let service_info = self.store.load_services().unwrap();

        #[cfg(feature = "store")]
        debug!("Loading {} services from database", service_info.len());

        #[cfg(feature = "store")]
        for i in service_info {
            let keys = Keys::new(i.public_key.clone());

            let primary_page = match i.primary_page {
                Some(p) => self.store.load_page(&p, &keys).unwrap().unwrap(),
                None => {
                    trace!("No primary page for service: {:?}", i);
                    continue;
                }
            };

            let replica_page = i
                .replica_page
                .map(|s| self.store.load_page(&s, &keys).unwrap())
                .flatten();

            let mut service = Service::load(&primary_page).unwrap();

            service.set_private_key(i.private_key);
            service.set_secret_key(i.secret_key);

            // URGENT TODO: rehydrate other components here
            // TODO: maybe replicas and subscribers should not be stored against the service inst but in separate core modules?
            let s = ServiceInst {
                service,
                state: i.state,
                index: i.index,

                last_updated: i.last_updated,
                primary_page: Some(primary_page),
                replica_page,

                changed: false,
            };

            self.services.entry(i.id).or_insert(s);
        }
    }
}

impl Drop for ServiceManager {
    fn drop(&mut self) {
        // TODO: sync..?
    }
}
