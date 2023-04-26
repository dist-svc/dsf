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

use super::ops::{RpcKind, RpcOperation};
use crate::core::peers::{Peer, PeerAddress, PeerFlags};
use crate::daemon::{net::NetIf, Dsf};
use crate::error::Error as DsfError;

pub enum ConnectState {
    Init,
    Pending(kad::dht::ConnectFuture<Id, Peer>),
    Registering(Vec<Entry<Id, Peer>>, kad::dht::StoreFuture<Id, Peer>),
    Done,
    Error,
}

pub struct ConnectOp {
    pub(crate) opts: ConnectOptions,
    pub(crate) state: ConnectState,
}

pub struct ConnectFuture {
    rx: mpsc::Receiver<rpc::Response>,
}

impl Future for ConnectFuture {
    type Output = Result<ConnectInfo, DsfError>;

    fn poll(mut self: Pin<&mut Self>, ctx: &mut Context<'_>) -> Poll<Self::Output> {
        let resp = match self.rx.poll_next_unpin(ctx) {
            Poll::Ready(Some(r)) => r,
            _ => return Poll::Pending,
        };

        match resp.kind() {
            rpc::ResponseKind::Connected(r) => Poll::Ready(Ok(r)),
            rpc::ResponseKind::Error(e) => Poll::Ready(Err(e.into())),
            _ => Poll::Pending,
        }
    }
}

impl<Net> Dsf<Net>
where
    Dsf<Net>: NetIf<Interface = Net>,
{
    pub fn connect(&mut self, options: ConnectOptions) -> Result<ConnectFuture, DsfError> {
        let req_id = rand::random();

        let (tx, rx) = mpsc::channel(1);

        // Create connect object
        let op = RpcOperation {
            req_id,
            kind: RpcKind::connect(options),
            done: tx,
        };

        // Add to tracking
        debug!("Adding RPC op {} to tracking", req_id);
        self.rpc_ops.insert(req_id, op);

        Ok(ConnectFuture { rx })
    }

    pub fn poll_rpc_connect(
        &mut self,
        req_id: u64,
        connect_op: &mut ConnectOp,
        ctx: &mut Context,
        mut done: mpsc::Sender<rpc::Response>,
    ) -> Result<bool, DsfError> {
        let ConnectOp { opts, state } = connect_op;

        match state {
            ConnectState::Init => {
                // TODO: Check we're not connecting to ourself
                //        if self.bind_address() == options.address {
                //            warn!("[DSF ({:?})] Cannot connect to self", self.id);
                //        }

                // Generate DHT connect operation
                let (connect, req_id, dht_req) = match self.dht_mut().connect_start() {
                    Ok(v) => v,
                    Err(e) => {
                        error!("Error starting DHT connect: {:?}", e);
                        return Err(DsfError::Unknown);
                    }
                };

                debug!(
                    "DHT connect start to: {:?} (id: {:?})",
                    opts.address, req_id
                );

                // Set request flags for initial connection
                let flags = Flags::ADDRESS_REQUEST | Flags::PUB_KEY_REQUEST;

                // Convert into DSF message
                let net_req_body = self.dht_to_net_request(dht_req);
                let mut net_req = NetRequest::new(self.id(), req_id, net_req_body, flags);

                // Attach public key for TOFU
                net_req.common.public_key = Some(self.service().public_key());

                // Send message
                // This bypasses DSF state tracking as it is managed by the DHT
                // TODO: this precludes _retries_ and state tracking... find a better solution
                self.net_send(
                    &[(opts.address.clone().into(), None)],
                    NetMessage::Request(net_req),
                )
                .unwrap();

                *state = ConnectState::Pending(connect);
                ctx.waker().clone().wake();

                Ok(false)
            }
            ConnectState::Pending(connect) => {
                match connect.poll_unpin(ctx) {
                    Poll::Ready(Ok(v)) => {
                        debug!("DHT connect complete! {:?}", v);

                        // Update newly found peers
                        for p in &v {
                            let i = p.info();

                            self.peers().find_or_create(
                                i.id(),
                                PeerAddress::Implicit(i.address()),
                                i.pub_key(),
                                PeerFlags::empty(),
                            );
                        }

                        // Publish peer page to closest N peers

                        let mut buff = vec![0u8; 1024];
                        let (n, mut primary_page) = self.primary(&mut buff).unwrap();

                        let our_id = self.id();
                        let (store, _req_id) = match self.dht_mut().store_peers(
                            our_id,
                            vec![primary_page.to_owned()],
                            v.iter(),
                        ) {
                            Ok(r) => r,
                            Err(e) => {
                                error!("DHT store error: {:?}", e);
                                return Err(DsfError::Unknown);
                            }
                        };

                        *state = ConnectState::Registering(v, store);
                        ctx.waker().clone().wake();
                    }
                    Poll::Ready(Err(e)) => {
                        warn!("DHT connect error: {:?}", e);

                        let resp = rpc::Response::new(
                            req_id,
                            rpc::ResponseKind::Error(dsf_core::error::Error::Unknown),
                        );

                        let _ = done.try_send(resp);

                        *state = ConnectState::Error;
                        ctx.waker().clone().wake();
                    }
                    _ => (),
                }
                Ok(false)
            }
            ConnectState::Registering(v, store) => {
                match store.poll_unpin(ctx) {
                    Poll::Ready(Ok(_s)) => {
                        // Build connect info
                        // TODO: matching on addresses here fails in -oh so many- ways...
                        // lookup by hostname etc., peer addressing needs major work
                        let p = v
                            .iter()
                            .find(|p| p.info().address() == opts.address.into())
                            .unwrap();
                        let i = ConnectInfo {
                            id: p.info().id(),
                            peers: v.len(),
                        };

                        let resp = rpc::Response::new(req_id, rpc::ResponseKind::Connected(i));

                        let _ = done.try_send(resp);

                        *state = ConnectState::Done;
                        ctx.waker().clone().wake();

                        Ok(true)
                    }
                    Poll::Ready(Err(e)) => {
                        error!("DHT store error: {:?}", e);

                        let resp = rpc::Response::new(
                            req_id,
                            rpc::ResponseKind::Error(dsf_core::error::Error::Unknown),
                        );

                        done.try_send(resp).unwrap();

                        *state = ConnectState::Error;
                        ctx.waker().clone().wake();

                        Ok(true)
                    }
                    _ => Ok(false),
                }
            }
            ConnectState::Done => Ok(true),
            ConnectState::Error => Ok(true),
        }
    }
}
