//! Publish operation, publishes data using a known service, 
//! distributing updates to any active subscribers

use std::convert::TryFrom;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use dsf_core::wire::Container;
use futures::channel::mpsc;
use futures::prelude::*;

use log::{debug, error, info};
use tracing::{span, Level};

use dsf_core::prelude::*;

use dsf_core::net;
use dsf_core::service::{DataOptions, Publisher};
use dsf_rpc::{self as rpc, DataInfo, PublishInfo, PublishOptions};

use crate::core::peers::Peer;
use crate::daemon::net::{NetFuture, NetIf};
use crate::daemon::Dsf;
use crate::error::Error;

use super::ops::*;

pub enum PublishState {
    Init,
    Pending(NetFuture),
    Done,
    Error(Error),
}

pub struct PublishOp {
    pub(crate) opts: PublishOptions,
    pub(crate) state: PublishState,
}

pub struct PublishFuture {
    rx: mpsc::Receiver<rpc::Response>,
}

impl Future for PublishFuture {
    type Output = Result<PublishInfo, DsfError>;

    fn poll(mut self: Pin<&mut Self>, ctx: &mut Context<'_>) -> Poll<Self::Output> {
        let resp = match self.rx.poll_next_unpin(ctx) {
            Poll::Ready(Some(r)) => r,
            _ => return Poll::Pending,
        };

        match resp.kind() {
            rpc::ResponseKind::Published(r) => Poll::Ready(Ok(r)),
            rpc::ResponseKind::Error(e) => Poll::Ready(Err(e.into())),
            _ => Poll::Pending,
        }
    }
}

impl<Net> Dsf<Net>
where
    Dsf<Net>: NetIf<Interface = Net>,
{
    /// Publish a locally known service
    pub async fn publish(&mut self, options: PublishOptions) -> Result<PublishFuture, Error> {
        let req_id = rand::random();
        let (tx, rx) = mpsc::channel(1);

        // Create connect object
        let op = RpcOperation {
            req_id,
            kind: RpcKind::publish(options),
            done: tx,
        };

        // Add to tracking
        debug!("Adding RPC op {} to tracking", req_id);
        self.rpc_ops.insert(req_id, op);

        Ok(PublishFuture { rx })
    }

    pub fn poll_rpc_publish(
        &mut self,
        req_id: u64,
        register_op: &mut PublishOp,
        ctx: &mut Context,
        mut done: mpsc::Sender<rpc::Response>,
    ) -> Result<bool, DsfError> {
        let PublishOp { opts, state } = register_op;

        let span = span!(Level::DEBUG, "publish");
        let _enter = span.enter();

        match state {
            PublishState::Init => {
                debug!("Starting publish operation");

                // Resolve ID from ID or Index options
                let id = match self.resolve_identifier(&opts.service) {
                    Ok(id) => id,
                    Err(_e) => {
                        error!("no matching service for");
                        return Err(DsfError::UnknownService);
                    }
                };

                let services = self.services();
                // Fetch the known service from the service list
                let service_info = match services.find(&id) {
                    Some(s) => s,
                    None => {
                        // Only known services can be registered
                        error!("unknown service (id: {})", id);
                        *state = PublishState::Error(Error::UnknownService);
                        return Err(DsfError::UnknownService);
                    }
                };

                // Fetch the private key for signing service pages
                let _private_key = match service_info.private_key {
                    Some(s) => s,
                    None => {
                        // Only known services can be registered
                        error!("no service private key (id: {})", id);
                        *state = PublishState::Error(Error::NoPrivateKey);
                        return Err(DsfError::NoPrivateKey);
                    }
                };

                // Setup publishing object
                let body = match &opts.data {
                    Some(d) => &d[..],
                    None => &[],
                };
                let data_options = DataOptions {
                    data_kind: opts.kind.into(),
                    body: Some(body),
                    ..Default::default()
                };

                let mut page: Option<Container> = None;

                services.update_inst(&id, |s| {
                    let mut buff = vec![0u8; 1024];
                    let opts = data_options.clone();

                    info!("Generating data page");
                    let (_n, c) = s.service.publish_data(opts, &mut buff).unwrap();
                    page = Some(c.to_owned());
                });

                let page = page.unwrap();

                let _info = PublishInfo {
                    index: page.header().index(),
                };

                debug!("Storing data page");

                // Store new service data
                let data_info = DataInfo::try_from(&page).unwrap();
                match self.data().store_data(&data_info, &page) {
                    Ok(_) => (),
                    Err(e) => {
                        error!("Error storing service data: {:?}", e);
                        *state = PublishState::Error(e);
                        return Err(DsfError::Unknown);
                    }
                }

                // Generate requests
                // Generate push data message
                let req = net::Request::new(
                    self.id(),
                    rand::random(),
                    net::RequestBody::PushData(id.clone(), vec![page]),
                    Flags::default(),
                );

                // Fetch subscribers for the specified service
                let subscribers = match self.subscribers().find_peers(&id) {
                    Ok(v) => v,
                    Err(e) => {
                        error!("Error finding peers for matching subscribers");
                        *state = PublishState::Error(e);
                        return Err(DsfError::Unknown);
                    }
                };

                // Resolve subscriber IDs to peer instances
                let peers: Vec<_> = subscribers
                    .iter()
                    .filter_map(|peer_id| self.peers().find(peer_id))
                    .collect();

                // Issue request to peers
                let op = self.net_op(peers, req);

                *state = PublishState::Pending(op);
                Ok(false)
            }
            PublishState::Pending(op) => {
                // Poll on network operation completion
                match op.poll_unpin(ctx) {
                    Poll::Ready(_d) => {
                        debug!("Publish requests complete");

                        // TODO: fix this to be real publish info (use _d)
                        let i = PublishInfo { index: 0 };

                        // TODO: send completion
                        let resp = rpc::Response::new(req_id, rpc::ResponseKind::Published(i));

                        done.try_send(resp).unwrap();
                    }
                    _ => (),
                };

                Ok(false)
            }
            _ => Ok(true),
        }
    }
}
