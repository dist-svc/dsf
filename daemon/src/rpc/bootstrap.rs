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

use super::connect::ConnectFuture;
use super::ops::{RpcKind, RpcOperation};

pub enum BootstrapState {
    Init,
    Connecting(Vec<ConnectFuture>),
    Done,
    Error,
}

pub struct BootstrapInfo;

pub struct BootstrapOp {
    pub(crate) opts: (),
    pub(crate) state: BootstrapState,
}

pub struct BootstrapFuture {
    rx: mpsc::Receiver<rpc::Response>,
}

impl Future for BootstrapFuture {
    type Output = Result<BootstrapInfo, DsfError>;

    fn poll(mut self: Pin<&mut Self>, ctx: &mut Context<'_>) -> Poll<Self::Output> {
        let resp = match self.rx.poll_next_unpin(ctx) {
            Poll::Ready(Some(r)) => r,
            _ => return Poll::Pending,
        };

        match resp.kind() {
            rpc::ResponseKind::None => Poll::Ready(Ok(BootstrapInfo)),
            rpc::ResponseKind::Error(e) => Poll::Ready(Err(e.into())),
            _ => {
                error!("Unexpected response: {:?}", resp);
                Poll::Ready(Err(DsfError::Unknown))
            }
        }
    }
}

impl<Net> Dsf<Net>
where
    Dsf<Net>: NetIf<Interface = Net>,
{
    /// Perform bootstrapping operation.
    /// This connects to known peers to bootstrap communication with the network,
    /// updates any watched services, and re-establishes subscriptions etc.
    pub fn bootstrap(&mut self) -> Result<BootstrapFuture, DsfError> {
        let req_id = rand::random();

        let (tx, rx) = mpsc::channel(1);

        // Create bootstrap object
        let op = RpcOperation {
            req_id,
            kind: RpcKind::bootstrap(()),
            done: tx,
        };

        // Add to tracking
        debug!("Adding RPC op {} to tracking", req_id);
        self.rpc_ops.insert(req_id, op);

        Ok(BootstrapFuture { rx })
    }

    pub fn poll_rpc_bootstrap(
        &mut self,
        req_id: u64,
        bootstrap_op: &mut BootstrapOp,
        ctx: &mut Context,
        mut done: mpsc::Sender<rpc::Response>,
    ) -> Result<bool, DsfError> {
        let BootstrapOp { opts, state } = bootstrap_op;

        match state {
            BootstrapState::Init => {
                // Fetch available peers
                let peers = self.peers().list();

                // TODO: filter by already connected etc.

                if peers.len() == 0 {
                    info!("Skipping boostrap, no peers");
                    *state = BootstrapState::Done;
                    return Ok(false);
                }

                info!("Bootstrap start ({} peers)", peers.len());
                let timeout = Duration::from_millis(200).into();

                let connects: Vec<_> = peers
                    .iter()
                    .map(|(id, p)| {
                        self.connect(dsf_rpc::ConnectOptions {
                            address: p.address().into(),
                            id: Some(id.clone()),
                            timeout,
                        })
                        .unwrap()
                    })
                    .collect();

                *state = BootstrapState::Connecting(connects);

                Ok(false)
            }
            BootstrapState::Connecting(connects) => {
                // Continue bootstrapping if we're done connecting
                // TODO: update services, re-connect subs
                if connects.len() == 0 {
                    *state = BootstrapState::Done;
                    return Ok(false);
                }

                // TODO: check for connect timeouts?
                // Or don't worry because the individual calls should timeout?

                // Poll on connect states and remove connected nodes
                *connects = connects
                    .drain(..)
                    .filter_map(|mut c| match c.poll_unpin(ctx) {
                        Poll::Ready(Ok(v)) => {
                            info!("Connect {} ({:?}) done", v.id, opts);

                            Some(c)
                        }
                        Poll::Ready(Err(e)) => {
                            warn!("Connect {:?} error: {:?}", opts, e);

                            Some(c)
                        }
                        _ => None,
                    })
                    .collect();

                Ok(false)
            }
            BootstrapState::Done => {
                let resp = rpc::Response::new(req_id, rpc::ResponseKind::None);

                done.try_send(resp).unwrap();

                Ok(true)
            }
            BootstrapState::Error => {
                let resp = rpc::Response::new(
                    req_id,
                    rpc::ResponseKind::Error(dsf_core::error::Error::Unknown),
                );

                done.try_send(resp).unwrap();

                Ok(true)
            }
        }
    }
}
