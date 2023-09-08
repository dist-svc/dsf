use std::time::SystemTime;

use tracing::{debug, info, warn, error};

use dsf_rpc::{ServiceInfo, ServiceState, ServiceFlags};
use dsf_core::{prelude::*, types::ShortId};

use crate::{error::Error, core::store::DataStore, store::object::ObjectIdentifier};
use super::{Core, store::AsyncStore};

#[derive(Debug)]
pub struct ServiceInst {
    /// Base service instance for signing, verifying etc.
    pub service: Service,

    /// Information about the service
    pub info: ServiceInfo,

    /// Most recent primary page
    pub primary_page: Container,

    // Our most recent replica page (if replicated)
    pub replica_page: Option<Container>,
}

impl Core {

    /// Create a new service (hosted on this peer)
    pub async fn service_create(&mut self, service: Service, pages: Vec<Container>) -> Result<ServiceInfo, Error> {
        let id = service.id();

        debug!("Creating service {id}");

        // Fetch primary page
        let primary_page = match pages.iter().find(|p| {
            let h = p.header();
            h.kind().is_page() && !h.flags().contains(Flags::SECONDARY) && p.id() == id
        }) {
            Some(p) => p.clone(),
            None => return Err(Error::NotFound),
        };

        // Build service info
        let info = build_service_info(&service, ServiceState::Created, &primary_page);

        // Add to in-memory storage
        let inst = ServiceInst {
            service,
            info: info.clone(),
            primary_page: primary_page.clone(),
            replica_page: None,
        };
        self.services.insert(id.clone(), inst);


        // Write primary page and updated service info to store
        self.store.object_put(primary_page.to_owned()).await?;
        self.store.service_update(&info).await?;

        // TODO: store other non-primary pages?

        // Return created service info
        Ok(info)
    }
  
    /// Register a service from primary and secondary pages
    pub async fn service_register(&mut self, id: Id, pages: Vec<Container>) -> Result<ServiceInfo, Error> {
        debug!("Registering service {id}");

        // Fetch primary page
        let primary_page = match pages.iter().find(|p| {
            let h = p.header();
            h.kind().is_page() && !h.flags().contains(Flags::SECONDARY) && p.id() == id
        }) {
            Some(p) => p.clone(),
            None => return Err(Error::NotFound),
        };

        // TODO: wtf is this emant to be doing..?
        if primary_page.id() != id {
            debug!("Registering service for matching peer");
        }

        // Create or update service instance
        let info = match self.services.get_mut(&id) {
            Some(inst) => {
                info!("updating existing service to version {}", primary_page.header().index());

                // Apply primary page
                if let Err(e) = inst.service.apply_primary(&primary_page) {
                    error!("Failed to apply primary page: {e:?}");
                    return Err(e.into())
                }
                
                // Update in-memory representation
                inst.primary_page = primary_page.clone();
                inst.info.primary_page = Some(primary_page.signature());
                inst.info.last_updated = Some(SystemTime::now());

                inst.info.clone()
            }
            None => {
                info!("creating new service entry at version {}",
                    primary_page.header().index());

                // Create service from page
                let service = match Service::load(&primary_page) {
                    Ok(s) => s,
                    Err(e) => return Err(e.into()),
                };
                
                let info = build_service_info(&service, ServiceState::Located, &primary_page);

                // Add to in-memory storage
                let inst = ServiceInst {
                    service,
                    info: info.clone(),
                    primary_page: primary_page.clone(),
                    replica_page: None,
                };
                self.services.insert(id.clone(), inst);

                info
            }
        };

        // Write primary page and updated service info to store
        self.store.object_put(primary_page.to_owned()).await?;
        self.store.service_update(&info).await?;

        debug!("Updating replicas");

        // Fetch replica pages
        let replicas: Vec<(Id, &Container)> = pages
        .iter()
        .filter(|p| {
            let h = p.header();
            h.kind().is_page()
                && h.flags().contains(Flags::SECONDARY)
                && h.application_id() == 0
                && h.kind() == PageKind::Replica.into()
        })
        .filter_map(|p| {
            let peer_id = match p.info().map(|i| i.peer_id()) {
                Ok(Some(v)) => v,
                _ => return None,
            };

            Some((peer_id.clone(), p))
        })
        .collect();
    
        debug!("found {} replicas", replicas.len());

        // Update listed replicas
        for (peer_id, page) in &replicas {
            // TODO: handle this error condition properly
            if let Err(e) = self.create_or_update_replica(&id, peer_id, page) {
                error!("Failed to store replica information: {:?}", e);
            }

            // TODO: should we store replica pages or re-load on restart?
        }

        debug!("Service registered: {:?}", info);

        Ok(info)
    }

    /// Helper to load services from the [AsyncStore], called at start
    pub(super) async fn load_services(store: &AsyncStore) -> Result<Vec<ServiceInst>, Error> {
        // Load service info from databases
        let mut service_info = store.service_load().await?;

        let mut services = Vec::with_capacity(service_info.len());

        // Load matching pages and construct service instances
        for info in service_info.drain(..) {
            debug!("Loading service {}", info.id);

            let keys = Keys::new(info.public_key.clone());

            // Fetch primary page for service
            let primary_page = match &info.primary_page {
                Some(sig) => store.object_get(&info.id, sig.into(), &keys).await?,
                None => {
                    warn!("Load service {} failed, no primary page", info.id);
                    continue;
                }
            };
            
            // Load page to create service instance
            let mut service = match Service::load(&primary_page) {
                Ok(v) => v,
                Err(e) => {
                    warn!("Load service {} failed: {e:?}", info.id);
                    continue;
                }
            };

            // Attach keys if available
            service.set_private_key(info.private_key.clone());
            service.set_secret_key(info.secret_key.clone());

            // Fetch latest object to sync last signature
            match store.object_get(&info.id, ObjectIdentifier::Latest, &keys).await {
                Ok(o) => service.set_last(o.header().index() + 1, o.signature()),
                Err(_) => (),
            }

            // TODO: Load replica page if available


            // Add loaded service to list
            services.push(ServiceInst{
                service,
                info,
                primary_page,
                replica_page: None,
            });
        }

        Ok(services)
    }
}

impl ServiceInst {
    pub fn id(&self) -> Id {
        self.service.id()
    }
}

pub(crate) fn build_service_info(service: &Service, state: ServiceState, primary_page: &Container) -> ServiceInfo {
    let id = service.id();
    let short_id = ShortId::from(&id);

    ServiceInfo{
        id,
        short_id,
        index: service.index() as usize,
        kind: service.kind().into(),
        state,
        public_key: service.public_key(),
        private_key: service.private_key(),
        secret_key: service.secret_key(),
        last_updated: Some(SystemTime::now()),
        primary_page: Some(primary_page.signature()),
        replica_page: None,
        subscribers: 0,
        replicas: 0,
        flags: ServiceFlags::empty(),
    }
}
