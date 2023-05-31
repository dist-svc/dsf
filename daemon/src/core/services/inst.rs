use std::convert::TryFrom;
use std::ops::Add;
use std::time::{Duration, SystemTime};

use dsf_core::types::ShortId;
use dsf_core::wire::Container;
use dsf_rpc::ServiceFlags;
use log::{debug, error, warn};

use diesel::Queryable;
use serde::{Deserialize, Serialize};

use dsf_core::options::{Filters, Options};
use dsf_core::prelude::*;
use dsf_core::service::{Publisher, SecondaryOptions, Subscriber};

use dsf_rpc::service::{ServiceInfo, ServiceState};

#[derive(Debug, Serialize, Deserialize, Queryable)]
pub struct ServiceInst {
    pub(crate) service: Service,

    pub(crate) state: ServiceState,
    pub(crate) index: usize,
    pub(crate) last_updated: Option<SystemTime>,

    /// Most recent primary page
    // TODO: this isn't really optional / should always exist?
    #[serde(skip)]
    pub(crate) primary_page: Option<Container>,

    // Our most recent replica page (if replicated)
    #[serde(skip)]
    pub(crate) replica_page: Option<Container>,

    #[serde(skip)]
    pub(crate) changed: bool,
}

impl ServiceInst {
    pub(crate) fn id(&self) -> Id {
        self.service.id()
    }

    pub(crate) fn service(&mut self) -> &mut Service {
        &mut self.service
    }

    pub(crate) fn info(&self) -> ServiceInfo {
        let service = &self.service;

        let id = service.id();
        let short_id = ShortId::from(&id);

        let mut flags = ServiceFlags::empty();

        if service.is_origin() {
            flags |= ServiceFlags::ORIGIN;
        }
        if service.encrypted() {
            flags |= ServiceFlags::ENCRYPTED;
        }

        ServiceInfo {
            id,
            short_id,
            index: self.index,
            kind: service.kind().into(),
            state: self.state,
            last_updated: self.last_updated,
            primary_page: self.primary_page.as_ref().map(|v| v.signature()),
            replica_page: self.replica_page.as_ref().map(|v| v.signature()),
            public_key: service.public_key(),
            private_key: service.private_key(),
            secret_key: service.secret_key(),
            // TODO: fix replica / subscriber info (split objects?)
            replicas: 0,
            subscribers: 0,
            flags,
        }
    }

    /// Apply an updated service page
    pub(crate) fn apply_update(&mut self, page: &Container) -> Result<bool, DsfError> {
        let changed = self.service.apply_primary(page)?;

        // TODO: mark update required

        Ok(changed)
    }

    pub fn update<F>(&mut self, f: F)
    where
        F: Fn(&mut ServiceInst),
    {
        (f)(self);
        self.changed = true;
    }

    pub(crate) fn update_required(
        &self,
        state: ServiceState,
        update_interval: Duration,
        force: bool,
    ) -> bool {
        // Filter for the specified service state
        if self.state != state {
            return false;
        }

        // Skip checking further if force is set
        if force {
            return true;
        }

        // If we've never updated them, definitely required
        let updated = match self.last_updated {
            Some(u) => u,
            None => return true,
        };

        // Otherwise, only if update time has expired
        if updated.add(update_interval) < SystemTime::now() {
            return true;
        }

        false
    }
}
