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

use super::ops::*;
use crate::{
    core::CoreRes,
    daemon::{net::NetIf, Dsf},
    error::Error,
    rpc::replicate::fetch_replica,
};

pub enum RegisterState {
    Init,
    Pending,
    Done,
    Error,
}

#[allow(async_fn_in_trait)]
pub trait RegisterService {
    /// Register service information using the DHT
    async fn service_register(&self, options: RegisterOptions) -> Result<RegisterInfo, DsfError>;
}

impl<T: Engine> RegisterService for T {
    async fn service_register(&self, options: RegisterOptions) -> Result<RegisterInfo, DsfError> {
        info!("Register: {:?}", &options);

        // Resolve service id / index to a service instance
        let info = self.svc_get(options.service).await?;

        // Locate or generate a primary page for the service
        let primary_page = match fetch_primary(self, &info).await {
            Ok(v) => v,
            Err(e) => {
                error!("Failed to fetch or generate primary page: {:?}", e);
                return Err(e.into());
            }
        };
        let page_version = primary_page.header().index();

        let mut pages = vec![primary_page];
        let mut replica_version = None;

        // Setup replication
        if !options.no_replica {
            debug!("Setting up replica page");

            let p = match fetch_replica(self, &info).await {
                Ok(v) => v,
                Err(e) => {
                    error!("Failed to setup replica for {:#}: {:?}", info.id, e);
                    return Err(e);
                }
            };

            replica_version = Some(p.header().index());
            pages.push(p);
        }

        debug!("Saving pages to DHT: {:?}", pages);

        // Store new page(s) in the DHT
        let peers = match self.dht_put(info.id.clone(), pages).await {
            Ok((v, _i)) => v.len(),
            Err(e) => {
                error!("Failed to store pages for {:#}: {:?}", info.id, e);
                0
            }
        };

        Ok(RegisterInfo {
            page_version,
            replica_version,
            peers,
        })
    }
}

// Fetch or generate primary page for a service
pub(super) async fn fetch_primary<E: Engine>(
    e: &E,
    info: &ServiceInfo,
) -> Result<Container, DsfError> {
    debug!("Fetch primary page for service: {:#}", info.id);

    // Attempt to locate existing primary page
    if let Some(sig) = &info.primary_page {
        match e.object_get((&info.id).into(), sig.clone()).await {
            Ok((_i, v)) if !v.expired() => {
                debug!("Using existing primary page {:#}", sig);
                return Ok(v);
            }
            Ok(_) if info.private_key.is_some() => {
                debug!("Existing primary page has expired");
            }
            Ok(_) => {
                warn!("Cannot register foreign service with expired primary page");
                return Err(DsfError::PageExpired);
            }
            Err(e) => {
                warn!(
                    "Failed to fetch primary page {:#} for service {:#}: {:?}",
                    sig, info.id, e
                );
            }
        }
    }

    // Otherwise, build new primary page
    debug!("Generating new primary page for service {:#}", info.id);

    // Sign primary page
    let r = e
        .svc_update(
            info.id.clone(),
            Box::new(
                |svc, _state| match svc.publish_primary_buff(Default::default()) {
                    Ok((_n, c)) => CoreRes::Pages(vec![c.to_owned()], None),
                    Err(e) => CoreRes::Error(e.into()),
                },
            ),
        )
        .await;

    // Handle errors
    let p = match r {
        Ok(CoreRes::Pages(p, _i)) if p.len() == 1 => p[0].clone(),
        Err(e) => {
            error!("Failed to sign primary page: {:?}", e);
            return Err(e.into());
        }
        _ => {
            error!("Unhandled update response: {:?}", r);
            return Err(DsfError::Unknown);
        }
    };

    // TODO: update service primary_page index

    // Store newly signed page
    e.object_put(p.clone()).await?;

    Ok(p)
}
