use std::time::SystemTime;

use tracing::{debug, warn, error};

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
    /// Register a local service for tracking
    pub async fn register_service(
        &mut self,
        service: Service,
        primary_page: &Container,
        state: ServiceState,
        updated: Option<SystemTime>,
    ) -> Result<ServiceInfo, Error> {
        let id = service.id();

        // Parse out service info
        let info = ServiceInfo {
            id: id.clone(),
            short_id: ShortId::from(&id),
            index: primary_page.header().index() as usize,
            kind: service.kind().into(),
            state,
            public_key: service.public_key(),
            private_key: service.private_key(),
            secret_key: service.secret_key(),
            last_updated: updated,
            primary_page: Some(primary_page.signature()),
            replica_page: None,
            subscribers: 0,
            replicas: 0,
            flags: ServiceFlags::empty(),
        };

        // Create a service instance wrapper
        let inst = ServiceInst {
            service,
            info: info.clone(),
            primary_page: primary_page.clone(),
            replica_page: None,
        };

        let info = inst.info.clone();

        // Insert into memory storage
        self.services.insert(id, inst);

        // Write to database
        // TODO: this -could- be handed off to a separate task
        self.store.service_update(&info).await?;

        // Return new service info
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