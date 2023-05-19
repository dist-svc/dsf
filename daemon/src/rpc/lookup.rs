//! Lookup operation, locates a peer in the database returning peer info if found

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::SystemTime;

use futures::channel::mpsc;
use futures::prelude::*;

use log::{debug, error, info, warn};
use tracing::{span, Level};

use dsf_core::prelude::*;
use dsf_rpc::{self as rpc, peer::SearchOptions as LookupOptions, PeerInfo};

use crate::daemon::{net::NetIf, Dsf};
use crate::error::Error;

use crate::core::peers::Peer;
use crate::core::services::ServiceState;

use super::ops::*;

pub enum LookupState {
    Init,
    Pending,
    Done,
    Error,
}

pub trait PeerRegistry {
    /// Lookup a peer using the DHT
    async fn peer_lookup(&mut self, options: LookupOptions) -> Result<PeerInfo, DsfError>;
}

impl<T: Engine> PeerRegistry for T {
    async fn peer_lookup(&mut self, options: LookupOptions) -> Result<PeerInfo, DsfError> {
        debug!("Performing peer lookup by ID: {}", options.id);

        // TODO: Check local storage for existing peer info

        // Lookup via DHT
        let peer = match self.dht_locate(options.id.clone()).await {
            Ok(p) => p,
            Err(e) => {
                error!("DHT lookup failed: {:?}", e);
                return Err(e.into());
            }
        };

        debug!("Located peer: {:?}", peer);

        // TODO: what if we explicitly updated the local peer and store here rather than implicitly through the DHT?

        Ok(peer.info)
    }
}
