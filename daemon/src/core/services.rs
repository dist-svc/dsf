use dsf_rpc::ServiceInfo;
use dsf_core::prelude::*;

use crate::{Error, core::store::DataStore, store::object::ObjectIdentifier};
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
    pub fn register_service(
        &mut self,
        service: Service,
        primary_page: &Container,
        state: ServiceState,
        updated: Option<SystemTime>,
    ) -> Result<ServiceInfo, DsfError> {
        let id = service.id();

        // Parse out service info
        let info = ServiceInfo {
            id: id.clone(),
            short_id: ShortId::from(&id),
            kind: service.kind(),
            state,
            public_key: service.public_key(),
            private_key: service.private_key(),
            secret_key: service.secret_key(),
            last_updated: None,
            primary_page: Some(primary_page.signature()),
            replica_page: None,
            subscribers: 0,
            replicas: 0,
            flags: 0,
        };

        // Create a service instance wrapper
        let inst = ServiceInst {
            service,
            info: info.clone(),
            primary_page: Some(primary_page.clone()),
            replica_page: None,
        };

        let info = inst.info();

        // Insert into memory storage
        self.services.insert(id, inst);

        // Write to database
        self.store.service_update(&info).await?;

        // Return new service info
        Ok(info)
    }


    pub(super) async fn load_services(store: &AsyncStore) -> Result<(), Error> {
        // Load service info from databases
        let mut service_info = store.service_load().await?;

        let mut services = Vec::with_capacity(service_info.len());

        // Load matching pages and construct service instances
        for info in service_info.drain(..) {
            debug!("Loading service {}", info.id);

            let keys = Keys::new(info.public_key.clone());

            // Fetch primary page for service
            let primary_page = match info.primary_page {
                Some(sig) => self.store.object_get(&info.id, sig.into(), &keys).await?,
                None => {
                    warn!("Load service {} failed, no primary page", indo.id);
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
            service.set_private_key(info.private_key);
            service.set_secret_key(info.secret_key);

            // TODO: Load replica page if available

            // Fetch latest object to sync last signature
            match self.store.object_get(&info.id, ObjectIdentifier::Latest, &keys).await {
                Ok(o) => service.set_last(o.header().index() + 1, o.signature()),
                Err(_) => (),
            }

            services.push(ServiceInst{
                service,
                info,
                primary_page,
                replica_page: None,
            })
        }

        Ok(services)
    }
}
