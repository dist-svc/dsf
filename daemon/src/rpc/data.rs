//! Publish operation, publishes data using a known service,
//! distributing updates to any active subscribers

use std::{
    collections::HashMap,
    convert::TryFrom,
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use dsf_core::base::DataBody;
use dsf_core::wire::Container;
use futures::channel::mpsc;
use futures::prelude::*;

use log::{debug, error, info, warn};
use rpc::{DataListOptions, ServiceInfo};
use tracing::{span, Level};

use dsf_core::prelude::*;

use dsf_core::net;
use dsf_core::service::{DataOptions, Publisher};
use dsf_rpc::{self as rpc, DataInfo, PeerInfo, PublishInfo, PublishOptions, PushOptions};

use crate::{core::CoreRes, daemon::Dsf, error::Error};

use super::ops::*;

#[allow(async_fn_in_trait)]
pub trait PublishData {
    /// Publish data using a local service managed by the daemon
    async fn data_publish(&self, options: PublishOptions) -> Result<PublishInfo, DsfError>;

    /// Push pre-signed data for a known service managed externally
    async fn data_push(&self, options: PushOptions) -> Result<PublishInfo, DsfError>;
}

impl<T: Engine> PublishData for T {
    async fn data_publish(&self, options: PublishOptions) -> Result<PublishInfo, DsfError> {
        info!("Publish: {:?}", &options);

        // Resolve service id / index to a service instance
        let info = self.svc_get(options.service).await?;

        // Check we have private keys for signing data objects
        if info.private_key.is_none() {
            return Err(DsfError::NoPrivateKey);
        }

        // Perform publish operation
        let block = publish_data(self, &info, options.data.unwrap_or_default()).await?;

        // Forward to subscribers
        let resps = push_data(self, &info, vec![block.clone()]).await?;

        // TODO: process subscriber responses

        Ok(PublishInfo {
            index: block.header().index(),
            sig: block.signature(),
            subscribers: resps.len(),
        })
    }

    async fn data_push(&self, options: PushOptions) -> Result<PublishInfo, DsfError> {
        info!("Push: {:?}", &options);

        // Resolve service id / index to a service instance
        let _info = self.svc_get(options.service).await?;

        // TODO: Parse RPC data to container and validate

        // TODO: Add to local store

        // TODO: Push to subscribers

        todo!("Implement data push")
    }
}

/// Helper function to publish an abstract data object
pub(super) async fn publish_data<E: Engine, B: DataBody>(
    e: &E,
    info: &ServiceInfo,
    body: B,
) -> Result<Container, DsfError> {
    // Pre-encode body
    let n = body.encode_len().map_err(|_| DsfError::EncodeFailed)?;
    let mut b = vec![0u8; n];
    body.encode(&mut b).map_err(|_| DsfError::EncodeFailed)?;

    debug!("Building data object");

    // Setup publishing options
    let data_options = DataOptions {
        //data_kind: opts.kind.into(),
        body: Some(b),
        ..Default::default()
    };

    // Build and sign data object
    let r = e
        .svc_update(
            info.id.clone(),
            Box::new(
                move |svc, _state| match svc.publish_data_buff(data_options.clone()) {
                    Ok((_n, c)) => CoreRes::Pages(vec![c.to_owned()], None),
                    Err(e) => CoreRes::Error(e),
                },
            ),
        )
        .await;

    // Handle build results
    let block = match r {
        Ok(CoreRes::Pages(p, _)) if p.len() == 1 => p[0].clone(),
        Err(e) => {
            error!("Failed to build data object: {:?}", e);
            return Err(e);
        }
        _ => unimplemented!(),
    };

    debug!("Storing new object: {:#}", block.signature());

    // Add object to storage
    if let Err(e) = e.object_put(block.clone()).await {
        error!("Failed to store object: {:?}", e);
        return Err(e);
    }

    // TODO: Update service last-signed info?

    // Return published block
    Ok(block)
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
