//! Connect operation, sets up a connection with the provided peer

use std::time::Duration;

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use tracing::{span, Level};

use log::{debug, error, info, warn};

use futures::channel::mpsc;
use futures::prelude::*;

use kad::common::Entry;
use kad::prelude::*;

use dsf_core::net;
use dsf_core::prelude::*;

use dsf_rpc::{self as rpc, ConnectInfo, ConnectOptions};

use super::ops::Engine;
use crate::core::peers::{Peer, PeerAddress, PeerFlags};
use crate::daemon::{net::NetIf, Dsf};
use crate::error::Error;
use crate::rpc::ops::{OpKind, Res};
use crate::rpc::register::fetch_primary;

#[async_trait::async_trait]
pub trait Connect {
    /// Publish data using a known service
    async fn connect(&self, options: ConnectOptions) -> Result<ConnectInfo, DsfError>;
}

#[async_trait::async_trait]
impl<T: Engine> Connect for T {
    async fn connect(&self, options: ConnectOptions) -> Result<ConnectInfo, DsfError> {
        info!("Connect: {:?}", options);

        // Fetch primary page for our peer service
        let primary_page = match self.exec(OpKind::Primary).await {
            Ok(Res::Pages(p)) if p.len() == 1 => p[0].clone(),
            Err(e) => {
                error!("Failed to fetch primary page: {:?}", e);
                return Err(DsfError::NotFound);
            }
            _ => unimplemented!(),
        };

        debug!("Starting DHT connect");

        // Issue connect request to provided address
        let peers = match self.dht_connect(options.address.into(), None).await {
            Ok(v) => v,
            Err(e) => {
                error!(
                    "Failed to establish connection with {}: {:?}",
                    options.address, e
                );
                return Err(DsfError::NotFound);
            }
        };

        // Query DHT for nearest peers
        debug!("Located {} peers", peers.len());

        let req = NetRequestBody::Hello;
        let resps = match self.net_req(req, peers).await {
            Ok(v) => v,
            Err(e) => {
                error!("Failed to connect to peers: {:?}", e);
                return Err(DsfError::Unknown);
            }
        };

        debug!("Received {} responses", resps.len());

        // TODO: should we manually store peer information here instead of handling implicitly?

        // Publish primary peer page to DHT
        // TODO: should we manually push to all located peers or leave to DHT..?
        let peers = match self.dht_put(self.id(), vec![primary_page]).await {
            Ok(v) => v,
            Err(e) => {
                error!("Failed to store peer page in DHT: {:?}", e);
                return Err(DsfError::NotFound);
            }
        };

        // Return connection info
        // TODO: should be first / connected peer id, not ours
        Ok(ConnectInfo {
            id: self.id(),
            peers: peers.len(),
        })
    }
}
