//! Locate operation, looks up a service by ID using the database

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::SystemTime;

use dsf_core::wire::Container;
use futures::channel::mpsc;
use futures::prelude::*;

use log::{debug, error, info, warn};
use tracing::{span, Level};

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
    async fn service_locate(&self, opts: LocateOptions) -> Result<LocateInfo, DsfError> {
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
                    origin: i.origin,
                    updated: false,
                    page_version: i.index as u16,
                    page,
                })
            }
            _ => None,
        };

        // Short-circuit for owned services
        match &local {
            Some(i) if i.origin => return Ok(i.clone()),
            _ => (),
        }

        // Otherwise, lookup via DHT
        let pages = self.dht_search(opts.id.clone()).await;
        let pages = match (pages, &local) {
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

        debug!("Found pages: {:?}", pages);

        debug!("Adding service {} to store", opts.id);

        // Add located service to local tracking
        let i = self.svc_register(opts.id.clone(), pages.clone()).await?;

        debug!("Stored service: {:?}", i);

        // Map primary page using returned pages or datastore
        let primary_page = match i.primary_page {
            Some(sig) => {
                if let Some(p) = pages.iter().find(|p| sig == p.signature()) {
                    Some(p.clone())
                } else {
                    Some(self.object_get(opts.id.clone(), sig).await?)
                }
            }
            None => None,
        };

        debug!("Found page: {:?}", primary_page);

        // Return info
        let info = LocateInfo {
            id: opts.id,
            origin: i.origin,
            updated: true,
            page_version: i.index as u16,
            // TODO: fetch related page
            page: primary_page,
        };

        Ok(info)
    }
}
