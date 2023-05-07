//! Bootstrap operation
//! This connects to known peers to bootstrap communication with the network,
//! updates any watched services, and re-establishes subscriptions etc.

use std::time::Duration;

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

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
use crate::error::Error as DsfError;
use crate::rpc::connect::Connect;

use super::ops::Engine;

// TODO: move to RPC
pub struct BootstrapInfo;

#[async_trait::async_trait]
pub trait Bootstrap {
    /// Publish data using a known service
    async fn bootstrap(&self) -> Result<BootstrapInfo, DsfError>;
}

#[async_trait::async_trait]
impl<T: Engine> Bootstrap for T {
    async fn bootstrap(&self) -> Result<BootstrapInfo, DsfError> {
        // Fetch peer list

        // Connect to available peers

        // Fetch service list

        // Update service registrations

        // Update service subscriptions

        todo!("Implement bootstrap operation")
    }
}
