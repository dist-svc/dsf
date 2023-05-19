//! Register operation, used to enrol a service in the database

use std::convert::TryFrom;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::SystemTime;

use dsf_core::service::SecondaryData;
use futures::channel::mpsc;
use futures::prelude::*;
use log::{debug, error, info, trace, warn};
use rpc::ServiceInfo;
use tracing::{span, Level};

use dsf_core::options::Options;
use dsf_core::prelude::*;
use dsf_rpc::{self as rpc, RegisterInfo, RegisterOptions};

use crate::core::peers::Peer;
use crate::core::services::ServiceState;

use super::ops::*;
use crate::daemon::{net::NetIf, Dsf};
use crate::error::Error;

pub enum RegisterState {
    Init,
    Pending,
    Done,
    Error,
}

pub trait ReplicateService {
    /// Replicate a known service, providing a replica for other subscribers
    async fn service_replicate(&self, options: RegisterOptions) -> Result<RegisterInfo, DsfError>;
}

impl<T: Engine> ReplicateService for T {
    async fn service_replicate(&self, options: RegisterOptions) -> Result<RegisterInfo, DsfError> {
        info!("Replicate: {:?}", &options);

        // Resolve service id / index to a service instance
        let svc = self.svc_resolve(options.service).await?;
        let info = self.svc_get(svc.id()).await?;

        // Fetch or generate replica page
        let p = fetch_replica(self, &info).await?;

        // TODO: update service replica status
        let replica_version = Some(p.header().index());

        // Store replica page in DHT
        let peers = match self.dht_put(svc.id(), vec![p]).await {
            Ok(v) => v.len(),
            Err(e) => {
                error!("Failed to store pages for {:#}: {:?}", svc.id(), e);
                0
            }
        };

        // TODO: return registered peer count
        Ok(RegisterInfo {
            page_version: svc.version(),
            replica_version,
            peers,
        })
    }
}

// Fetch or generate replica page for a service
pub(super) async fn fetch_replica<E: Engine>(
    e: &E,
    info: &ServiceInfo,
) -> Result<Container, DsfError> {
    // Fetch existing replica page if available
    if let Some(sig) = &info.replica_page {
        match e.object_get(e.id(), sig.clone()).await {
            Ok(v) if !v.expired() => {
                debug!(
                    "Using existing replica page {:#} for service {:#}",
                    sig, info.id
                );
                return Ok(v);
            }
            Ok(_) => (),
            Err(e) => error!(
                "Failed to fetch replica page {:#} for service {:#}: {:?}",
                sig, info.id, e
            ),
        }
    }

    // Otherwise, create a new replica page
    debug!(
        "Creating new replica page for {:#} (via {:#})",
        info.id,
        e.id()
    );

    let target_id = info.id.clone();
    let page_signature = info.primary_page.clone().unwrap();

    let r = e
        .svc_update(
            e.id(),
            Box::new(move |svc, _state| {
                // Publish replica data object
                let primary_opts = DataOptions {
                    data_kind: DataKind::Replica.into(),
                    body: Some(SecondaryData {
                        sig: page_signature.clone(),
                    }),
                    // TODO: could include peer id here?
                    ..Default::default()
                };

                let (_, p) = svc.publish_data_buff(primary_opts)?;

                // Publish secondary page object, matching replica data
                let opts = SecondaryOptions {
                    page_kind: PageKind::Replica.into(),
                    version: p.header().index(),
                    public_options: &[Options::public_key(svc.public_key())],
                    ..Default::default()
                };

                let (_, c) = svc.publish_secondary_buff(&target_id, opts)?;

                // Return data and secondary page
                Ok(Res::Pages(vec![p.to_owned(), c.to_owned()]))
            }),
        )
        .await;

    // Parse out pages from response
    let objects = match r {
        Ok(Res::Pages(v)) => v,
        Err(e) => {
            error!("Failed to generate replica pages: {:?}", e);
            return Err(e.into());
        }
        _ => unreachable!(),
    };

    let mut replica_page = None;

    // Store newly generated objects
    for o in &objects {
        e.object_put(o.clone()).await?;

        let h = o.header();
        if h.kind().is_page() && h.flags().contains(Flags::SECONDARY) {
            replica_page = Some(o.clone());
        }
    }

    // Return replica page
    Ok(replica_page.unwrap())
}
