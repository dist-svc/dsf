//! Local (broadcast) discovery operation

use std::convert::TryFrom;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::SystemTime;

use futures::channel::mpsc;
use futures::prelude::*;
use log::{debug, error, info, trace, warn};
use tracing::{span, Level};

use dsf_core::options::Options;
use dsf_core::prelude::*;
use dsf_rpc::{self as rpc, DiscoverOptions, ServiceInfo};

use super::ops::*;
use crate::{
    core::peers::Peer,
    core::services::ServiceState,
    daemon::net::NetFuture,
    daemon::{net::NetIf, Dsf},
    error::Error,
};

#[async_trait::async_trait]
pub trait Discover {
    /// Publish data using a known service
    async fn discover(&self, options: DiscoverOptions) -> Result<Vec<ServiceInfo>, DsfError>;
}

#[async_trait::async_trait]
impl<T: Engine> Discover for T {
    async fn discover(&self, options: DiscoverOptions) -> Result<Vec<ServiceInfo>, DsfError> {
        info!("Discover: {:?}", options);

        // Build discovery request
        let net_req_body = NetRequestBody::Discover(
            options.application_id,
            options.body.clone().unwrap_or(vec![]),
            options.filters.clone(),
        );

        // Issue discovery request
        let r = match self.net_bcast(net_req_body).await {
            Ok(v) => v,
            Err(e) => {
                error!("Broadcast request failed: {:?}", e);
                return Err(DsfError::NotFound);
            }
        };

        debug!("Received {} responses", r.len());

        // Parse discovery results

        // Store matching services

        // Return matching service information
        Ok(vec![])
    }
}

pub struct DiscoverOp {
    pub(crate) opts: DiscoverOptions,
    pub(crate) state: DiscoverState,
}

pub enum DiscoverState {
    Init,
    Pending(NetFuture),
    Done,
    Error,
}

pub struct DiscoverFuture {
    rx: mpsc::Receiver<rpc::Response>,
}

unsafe impl Send for DiscoverFuture {}

impl Future for DiscoverFuture {
    type Output = Result<Vec<ServiceInfo>, DsfError>;

    fn poll(mut self: Pin<&mut Self>, ctx: &mut Context<'_>) -> Poll<Self::Output> {
        let resp = match self.rx.poll_next_unpin(ctx) {
            Poll::Ready(Some(r)) => r,
            _ => return Poll::Pending,
        };

        match resp.kind() {
            rpc::ResponseKind::Services(r) => Poll::Ready(Ok(r)),
            rpc::ResponseKind::Error(e) => Poll::Ready(Err(e.into())),
            _ => Poll::Pending,
        }
    }
}
