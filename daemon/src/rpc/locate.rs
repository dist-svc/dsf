//! Locate operation, looks up a service by ID using the database

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::{SystemTime, Instant};

use dsf_core::wire::Container;
use futures::channel::mpsc;
use futures::prelude::*;

use log::{debug, error, info, warn};
use rpc::ServiceFlags;
use tracing::{instrument, span, Level};

use dsf_core::prelude::*;
use dsf_rpc::{self as rpc, LocateInfo, LocateOptions};

use crate::daemon::{net::NetIf, Dsf};
use crate::error::Error;

use crate::core::peers::Peer;
use crate::core::services::ServiceState;

use super::ops::*;

pub enum LocateState {
    Init,
    Pending,
    Done,
    Error,
}

pub trait ServiceRegistry {
    /// Locate service information using the DHT
    async fn service_locate(&self, options: LocateOptions) -> Result<LocateInfo, DsfError>;
}

impl<T: Engine> ServiceRegistry for T {
    #[instrument(skip(self))]
    async fn service_locate(&self, opts: LocateOptions) -> Result<LocateInfo, DsfError> {
        let t1 = Instant::now();

        info!("Locating service: {:?}", opts);

        // Check for existing / local information
        let local = self.svc_get(opts.id.clone()).await;
        let local = match local {
            Ok(i) => {
                let page = match i.primary_page {
                    Some(sig) => Some(self.object_get(opts.id.clone(), sig).await?),
                    None => None,
                };

                Some(LocateInfo {
                    id: opts.id.clone(),
                    flags: i.flags,
                    updated: false,
                    page_version: i.index as u32,
                    page,
                })
            }
            _ => None,
        };

        // Short-circuit for owned services
        match &local {
            Some(i) if i.flags.contains(ServiceFlags::ORIGIN) => return Ok(i.clone()),
            _ => (),
        }

        // Otherwise, lookup via DHT
        let pages = self.dht_search(opts.id.clone()).await;
        let mut pages = match (pages, &local) {
            (Ok(p), _) => p,
            (_, Some(i)) => {
                warn!("DHT search failed, using local service info");
                return Ok(i.clone());
            }
            (Err(e), _) => {
                error!("DHT search failed: {:?}", e);
                return Err(DsfError::NotFound);
            }
        };

        pages.sort_by_key(|p| p.header().index());
        pages.reverse();

        debug!("Found pages: {:?}", pages);

        // Resolve primary page
        let primary_page = match pages.iter().find(|o| {
            let kind = o.header().kind();
            let flags = o.header().flags();

            kind.is_page() && !flags.contains(Flags::SECONDARY) && !flags.contains(Flags::TERTIARY)
        }) {
            Some(v) => v.clone(),
            None => {
                warn!("no primary page found");
                return Err(DsfError::NotFound);
            }
        };

        debug!("Found primary page: {:?}", primary_page);

        // Add located service to local tracking
        if !opts.no_persist {
            debug!("Adding service {} to store", opts.id);

            let i = self.svc_register(opts.id.clone(), pages.clone()).await?;

            debug!("Stored service: {:?}", i);
        }

        let page_version = primary_page.header().index();
        let mut flags = ServiceFlags::empty();
        if primary_page.header().flags().contains(Flags::ENCRYPTED) {
            flags |= ServiceFlags::ENCRYPTED;
        }


        // Return info
        let info = LocateInfo {
            id: opts.id,
            flags,
            updated: true,
            page_version,
            page: Some(primary_page),
        };

        let elapsed = Instant::now().duration_since(t1);
        info!("Locate complete after {} ms: {:?}", elapsed.as_millis() , info);

        Ok(info)
    }
}
