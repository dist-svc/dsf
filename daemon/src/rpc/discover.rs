//! Local (broadcast) discovery operation

use std::convert::TryFrom;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::SystemTime;

use futures::channel::mpsc;
use futures::prelude::*;
use tracing::{debug, error, info, instrument, span, trace, warn, Level};

use dsf_core::options::Options;
use dsf_core::prelude::*;
use dsf_rpc::{self as rpc, DiscoverOptions, PeerInfo, ServiceInfo, ServiceState};

use super::ops::*;
use crate::{
    daemon::{net::NetIf, Dsf},
    error::Error,
};

#[allow(async_fn_in_trait)]
pub trait Discover {
    /// Discover a service using local broadcast discovery
    async fn discover(&self, options: DiscoverOptions) -> Result<Vec<ServiceInfo>, DsfError>;
}

impl<T: Engine> Discover for T {
    #[instrument(skip_all)]
    async fn discover(&self, options: DiscoverOptions) -> Result<Vec<ServiceInfo>, DsfError> {
        info!("Discover: {:?}", options);

        // Build discovery request
        let net_req_body = NetRequestBody::Discover(
            options.application_id,
            options.body.clone().unwrap_or_default(),
            options.filters.clone(),
        );

        // Issue discovery request
        let responses = match self.net_bcast(net_req_body).await {
            Ok(v) => v,
            Err(e) => {
                error!("Broadcast request failed: {:?}", e);
                return Err(DsfError::NotFound);
            }
        };

        debug!("Received {} responses", responses.len());

        // Parse discovery results
        let mut services = vec![];
        for (_peer_id, resp) in responses {
            // TODO: add peers to tracking?

            let (id, pages) = match resp.data {
                NetResponseBody::ValuesFound(id, pages) => (id, pages),
                _ => continue,
            };

            // Store matching services
            let info = match self.svc_register(id, pages).await {
                Ok(v) => v,
                Err(e) => {
                    warn!("Failed to register service: {:?}", e);
                    continue;
                }
            };

            services.push(info);
        }

        // Return matching service information
        Ok(services)
    }
}
