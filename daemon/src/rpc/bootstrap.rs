//! Bootstrap operation
//! This connects to known peers to bootstrap communication with the network,
//! updates any watched services, and re-establishes subscriptions etc.

use std::collections::HashMap;
use std::time::Duration;

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use rpc::{BootstrapInfo, ConnectInfo};
use tracing::{span, Level};

use log::{debug, error, info, warn};

use futures::channel::mpsc;
use futures::prelude::*;

use kad::prelude::*;

use dsf_core::net;
use dsf_core::prelude::*;

use dsf_rpc::{self as rpc}; //, BootstrapInfo, BootstrapOptions};

use crate::core::peers::{Peer, PeerAddress};
use crate::daemon::{net::NetIf, Dsf};
use crate::error::Error;
use crate::rpc::connect::Connect;

use super::ops::Engine;

#[async_trait::async_trait]
pub trait Bootstrap {
    /// Publish data using a known service
    async fn bootstrap(&self) -> Result<BootstrapInfo, DsfError>;
}

#[async_trait::async_trait]
impl<T: Engine> Bootstrap for T {
    async fn bootstrap(&self) -> Result<BootstrapInfo, DsfError> {
        info!("Bootstrap!");

        // Fetch peer list
        let peers = self.peer_list().await?;
        if peers.len() == 0 {
            warn!("No peers found, skipping bootstrap");
            return Err(DsfError::NoPeersFound);
        }

        // TODO: filter by non-transient peers only
        // (though these should never be written back to the db)
        debug!("Bootstrap via {} peers", peers.len());

        // Issue connect operations to available peers
        // TODO: combine into a single DHT connect op instead of
        // splitting over peers?
        let mut connected = 0;
        for p in &peers {
            match self.dht_connect(p.address(), Some(p.id())).await {
                Ok(_) => connected += 1,
                Err(e) => {
                    warn!("Failed to connect to peer {:?}: {:?}", p, e);
                }
            }
        }

        // TODO: Fetch service list

        // TODO: Update registrations for published services

        // TODO: Update service subscriptions

        warn!("Bootstrap RPC not fully implemented");

        Ok(BootstrapInfo {
            connected,
            registrations: 0,
            subscriptions: 0,
        })
    }
}
