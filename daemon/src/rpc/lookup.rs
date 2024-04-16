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
use dsf_rpc::{self as rpc, peer::SearchOptions as LookupOptions, PeerInfo, ServiceState};

use crate::daemon::{net::NetIf, Dsf};
use crate::error::Error;

use super::ops::*;

pub enum LookupState {
    Init,
    Pending,
    Done,
    Error,
}

#[allow(async_fn_in_trait)]
pub trait PeerRegistry {
    /// Lookup a peer using the DHT
    async fn peer_lookup(&mut self, options: LookupOptions) -> Result<PeerInfo, DsfError>;
}

impl<T: Engine> PeerRegistry for T {
    async fn peer_lookup(&mut self, options: LookupOptions) -> Result<PeerInfo, DsfError> {
        debug!("Performing peer lookup by ID: {}", options.id);

        // TODO: Check local storage for existing peer info

        // Lookup via DHT
        let (peer, _info) = match self.dht_locate(options.id.clone()).await {
            Ok(p) => p,
            Err(e) => {
                error!("DHT lookup failed: {:?}", e);
                return Err(e);
            }
        };

        debug!("Located peer: {:?}", peer);

        // TODO: what if we explicitly updated the local peer and store here rather than implicitly through the DHT?

        Ok(peer)
    }
}
