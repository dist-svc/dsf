//! Bootstrap operation
//! This connects to known peers to bootstrap communication with the network,
//! updates any watched services, and re-establishes subscriptions etc.

use std::collections::HashMap;
use std::time::Duration;

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use rpc::{BootstrapInfo, ConnectInfo};
use tracing::{span, Level, instrument};

use log::{debug, error, info, warn};

use futures::channel::mpsc;
use futures::prelude::*;

use kad::prelude::*;

use dsf_core::net;
use dsf_core::prelude::*;

use dsf_rpc::{self as rpc}; //, BootstrapInfo, BootstrapOptions};

use crate::core::peers::{Peer, PeerAddress, PeerFlags};
use crate::daemon::{net::NetIf, Dsf};
use crate::error::Error;
use crate::rpc::connect::Connect;

use super::ops::Engine;

/// [Bootstrap] trait implements startup bootstrapping to connect to the network
pub trait Bootstrap {
    /// Publish data using a known service
    async fn bootstrap(&self) -> Result<BootstrapInfo, DsfError>;
}

impl<T: Engine> Bootstrap for T {
    #[instrument(skip(self))]
    async fn bootstrap(&self) -> Result<BootstrapInfo, DsfError> {
        info!("Bootstrap!");
        let mut connected = 0;

        // Fetch peer list
        let mut peers = self.peer_list().await?;

        // Filter for non-constrained / non-transient peers
        let peers: Vec<_> = peers
            .drain(..)
            .filter(|p| {
                !p.flags.contains(PeerFlags::CONSTRAINED) && !p.flags.contains(PeerFlags::TRANSIENT)
            })
            .collect();

        if peers.len() == 0 {
            warn!("No peers available, skipping peer bootstrap");

            return Ok(BootstrapInfo {
                connected: 0,
                registrations: 0,
                subscriptions: 0,
            })
        }

        debug!("Bootstrap via {} peers", peers.len());

        // Issue hello messages to known peers, used to populate DHT peer listing
        let req = NetRequestBody::Hello;
        let resps = match self.net_req(req, peers.clone()).await {
            Ok(v) => v,
            Err(e) => {
                error!("Failed to issue hello to peers: {:?}", e);
                return Err(DsfError::Unknown);
            }
        };

        debug!("Received {} responses", resps.len());

        // Issue connect operations to available peers
        // (DHT requests required to fill KNodeTable for further DHT ops)
        // TODO: combine into a single DHT connect op instead of
        // splitting over peers?

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

        // TODO: Update service subscriptions and replicas

        // TODO: Re-publish tertiary pages for name services

        warn!("Bootstrap RPC not fully implemented");

        Ok(BootstrapInfo {
            connected,
            registrations: 0,
            subscriptions: 0,
        })
    }
}
