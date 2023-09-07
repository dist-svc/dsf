//! Push operation, forwards data for a known (and authorised) service.
//! This is provided for brokering / delegation / gateway operation.

use std::collections::HashMap;
use std::convert::TryFrom;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use dsf_core::wire::Container;
use futures::channel::mpsc;
use futures::prelude::*;

use log::{debug, error, info, warn};
use rpc::ServiceInfo;
use tracing::{span, Level};

use dsf_core::prelude::*;

use dsf_core::net;
use dsf_core::service::DataOptions;
use dsf_rpc::{self as rpc, DataInfo, PublishInfo, PushOptions};

use super::ops::*;
use crate::daemon::net::{NetFuture, NetIf};
use crate::daemon::Dsf;
use crate::error::Error;

pub trait PushData {
    /// Push pre-signed data for a known service
    async fn push_data(&self, options: PushOptions) -> Result<PublishInfo, DsfError>;
}

impl<T: Engine> PushData for T {
    async fn push_data(&self, options: PushOptions) -> Result<PublishInfo, DsfError> {
        info!("Push: {:?}", &options);

        // Resolve service id / index to a service instance
        let svc = self.svc_resolve(options.service).await?;
        let _info = self.svc_get(svc.id()).await?;

        // TODO: Parse RPC data to container and validate

        // TODO: Add to local store

        // TODO: Push to subscribers

        todo!("Implement data push")
    }
}

/// Helper function to push published objects to subscribers
pub(super) async fn push_data<E: Engine>(
    e: &E,
    info: &ServiceInfo,
    objects: Vec<Container>,
) -> Result<HashMap<Id, NetResponse>, DsfError> {
    // Fetch service subscribers
    let subs = e.subscribers_get(info.id.clone()).await?;

    // Resolve subscribers to peers for net operation
    let mut peers = Vec::with_capacity(subs.len());
    for s in &subs {
        match e.peer_get(s.clone()).await {
            Ok(p) => peers.push(p),
            Err(e) => {
                warn!("Failed to lookup peer {:#}: {:?}", s, e);
            }
        }
    }

    debug!("Push to {}(/{}) subscribers", peers.len(), subs.len());

    // Issue network request
    let req = net::RequestBody::PushData(info.id.clone(), objects);
    let resp = e.net_req(req, peers).await?;

    // TODO: process subscriber responses

    Ok(resp)
}
