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
    Pending(kad::dht::LocateFuture<Id, Peer>),
    Done,
    Error,
}

pub struct LookupOp {
    pub(crate) opts: LookupOptions,
    pub(crate) state: LookupState,
}

pub struct LookupFuture {
    rx: mpsc::Receiver<rpc::Response>,
}

impl Future for LookupFuture {
    type Output = Result<PeerInfo, DsfError>;

    fn poll(mut self: Pin<&mut Self>, ctx: &mut Context<'_>) -> Poll<Self::Output> {
        let resp = match self.rx.poll_next_unpin(ctx) {
            Poll::Ready(Some(r)) => r,
            _ => return Poll::Pending,
        };

        match resp.kind() {
            rpc::ResponseKind::Peer(r) => Poll::Ready(Ok(r)),
            rpc::ResponseKind::Error(e) => Poll::Ready(Err(e.into())),
            _ => Poll::Pending,
        }
    }
}

impl<Net> Dsf<Net>
where
    Dsf<Net>: NetIf<Interface = Net>,
{
    /// Look-up a peer via the database
    pub fn lookup2(&mut self, options: LookupOptions) -> Result<LookupFuture, Error> {
        let req_id = rand::random();

        let (tx, rx) = mpsc::channel(1);

        // TODO: catch self-lookup and return

        // Create connect object
        let op = RpcOperation {
            req_id,
            kind: RpcKind::lookup(options),
            done: tx,
        };

        // Add to tracking
        debug!("Adding RPC op {} to tracking", req_id);
        self.rpc_ops.insert(req_id, op);

        Ok(LookupFuture { rx })
    }

    pub fn poll_rpc_lookup(
        &mut self,
        req_id: u64,
        create_op: &mut LookupOp,
        ctx: &mut Context,
        mut done: mpsc::Sender<rpc::Response>,
    ) -> Result<bool, DsfError> {
        let LookupOp { opts, state } = create_op;

        match state {
            LookupState::Init => {
                // Initiate lookup via DHT
                let (lookup, _req_id) = match self.dht_mut().locate(opts.id.clone()) {
                    Ok(r) => r,
                    Err(e) => {
                        error!("DHT store error: {:?}", e);
                        return Err(DsfError::Unknown);
                    }
                };

                *state = LookupState::Pending(lookup);
                Ok(false)
            }
            LookupState::Pending(lookup) => {
                match lookup.poll_unpin(ctx) {
                    Poll::Ready(Ok(v)) => {
                        debug!("DHT lookup complete! {:?}", v);

                        // TODO: Register or update peer

                        // Return info
                        let resp = rpc::Response::new(
                            req_id,
                            rpc::ResponseKind::Peer(v.info().info().clone()),
                        );
                        done.try_send(resp).unwrap();

                        *state = LookupState::Done;

                        Ok(false)
                    }
                    Poll::Ready(Err(e)) => {
                        error!("DHT lookup error: {:?}", e);

                        let resp = rpc::Response::new(
                            req_id,
                            rpc::ResponseKind::Error(dsf_core::error::Error::Unknown),
                        );

                        done.try_send(resp).unwrap();

                        *state = LookupState::Error;

                        Ok(false)
                    }
                    Poll::Pending => Ok(false),
                }
            }
            _ => Ok(true),
        }
    }
}

#[async_trait::async_trait]
pub trait PeerRegistry {
    /// Lookup a peer
    async fn peer_lookup(&mut self, options: LookupOptions) -> Result<PeerInfo, DsfError>;
}

#[async_trait::async_trait]
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
