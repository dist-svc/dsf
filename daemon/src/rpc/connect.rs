//! Connect operation, sets up a connection with the provided peer

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use tracing::{debug, error, info, instrument, span, warn, Level};

use futures::channel::mpsc;
use futures::prelude::*;

use kad::common::Entry;
use kad::prelude::*;

use dsf_core::net;
use dsf_core::prelude::*;
use dsf_rpc::{self as rpc, ConnectInfo, ConnectOptions, PeerAddress, PeerFlags, PeerInfo};

use crate::{
    core::CoreRes,
    daemon::{net::NetIf, Dsf},
    error::Error,
    rpc::{
        ops::{Engine, OpKind},
        register::fetch_primary,
    },
};

#[allow(async_fn_in_trait)]
pub trait Connect {
    /// Connect to a peer via IP or URL
    async fn connect(&self, options: ConnectOptions) -> Result<ConnectInfo, DsfError>;
}

impl<T: Engine> Connect for T {
    #[instrument(skip(self))]
    async fn connect(&self, options: ConnectOptions) -> Result<ConnectInfo, DsfError> {
        info!("Connect: {:?}", options);

        // Fetch primary page for our peer service
        let primary_page = match self.exec(OpKind::Primary).await {
            CoreRes::Pages(p, _) if p.len() == 1 => p[0].clone(),
            CoreRes::Error(e) => {
                error!("Failed to fetch primary page: {:?}", e);
                return Err(DsfError::NotFound);
            }
            _ => unimplemented!(),
        };

        // TODO: issue a Hello to the new peer to check the connection / retrieve keys and flags for DHT support

        debug!("Starting DHT connect");

        // Issue DHT connect request to provided address
        let (peers, _info) = match self.dht_connect(options.address.into(), None).await {
            Ok(v) => v,
            Err(e) => {
                error!(
                    "Failed to establish connection with {}: {:?}",
                    options.address, e
                );
                return Err(DsfError::NotFound);
            }
        };
        debug!("Located {} peers", peers.len());

        // Establish comms with located peers
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
        let (peers, _info) = match self.dht_put(self.id(), vec![primary_page]).await {
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
