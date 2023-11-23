//! Publish operation, publishes data using a known service,
//! distributing updates to any active subscribers

use std::convert::TryFrom;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use dsf_core::base::DataBody;
use dsf_core::wire::Container;
use futures::channel::mpsc;
use futures::prelude::*;

use log::{debug, error, info, warn};
use rpc::ServiceInfo;
use tracing::{span, Level};

use dsf_core::prelude::*;

use dsf_core::net;
use dsf_core::service::{DataOptions, Publisher};
use dsf_rpc::{self as rpc, DataInfo, PublishInfo, PublishOptions, PeerInfo};

use crate::daemon::net::{NetFuture, NetIf};
use crate::daemon::Dsf;
use crate::error::Error;
use crate::rpc::push::push_data;

use super::ops::*;

pub trait PublishData {
    /// Publish data using a known / local service
    async fn publish(&self, options: PublishOptions) -> Result<PublishInfo, DsfError>;
}

impl<T: Engine> PublishData for T {
    async fn publish(&self, options: PublishOptions) -> Result<PublishInfo, DsfError> {
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
            Box::new(move |svc, _state| {
                let (_n, c) = svc.publish_data_buff(data_options.clone())?;
                Ok(Res::Pages(vec![c.to_owned()], None))
            }),
        )
        .await;

    // Handle build results
    let block = match r {
        Ok(Res::Pages(p, _)) if p.len() == 1 => p[0].clone(),
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
