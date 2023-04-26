
use std::convert::TryFrom;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::SystemTime;

use futures::channel::mpsc;
use futures::prelude::*;
use log::{debug, error, info, trace, warn};
use tracing::{span, Level};

use dsf_core::options::Options;
use dsf_core::prelude::*;
use dsf_rpc::{self as rpc, ServiceInfo, DiscoverOptions};

use crate::{
    core::peers::Peer,
    core::services::ServiceState,
    daemon::net::NetFuture,
    daemon::{net::NetIf, Dsf},
    error::Error as DsfError,
};
use super::ops::*;


pub struct DiscoverOp {
    pub(crate) opts: DiscoverOptions,
    pub(crate) state: DiscoverState,
}

pub enum DiscoverState {
    Init,
    Pending(NetFuture),
    Done,
    Error,
}

pub struct DiscoverFuture {
    rx: mpsc::Receiver<rpc::Response>,
}

unsafe impl Send for DiscoverFuture {}

impl Future for DiscoverFuture {
    type Output = Result<Vec<ServiceInfo>, DsfError>;

    fn poll(mut self: Pin<&mut Self>, ctx: &mut Context<'_>) -> Poll<Self::Output> {
        let resp = match self.rx.poll_next_unpin(ctx) {
            Poll::Ready(Some(r)) => r,
            _ => return Poll::Pending,
        };

        match resp.kind() {
            rpc::ResponseKind::Services(r) => Poll::Ready(Ok(r)),
            rpc::ResponseKind::Error(e) => Poll::Ready(Err(e.into())),
            _ => Poll::Pending,
        }
    }
}

impl<Net> Dsf<Net>
where
    Dsf<Net>: NetIf<Interface = Net>,
{
    /// Discover local services
    pub fn discover(&mut self, options: DiscoverOptions) -> Result<DiscoverFuture, DsfError> {
        let req_id = rand::random();

        let (tx, rx) = mpsc::channel(1);

        // Create connect object
        let op = RpcOperation {
            req_id,
            kind: RpcKind::discover(options),
            done: tx,
        };

        // Add to tracking
        debug!("Adding RPC op {} (discover) to tracking", req_id);
        self.rpc_ops.insert(req_id, op);

        Ok(DiscoverFuture { rx })
    }


    pub fn poll_rpc_discover(
        &mut self,
        req_id: u64,
        discover_op: &mut DiscoverOp,
        ctx: &mut Context,
        mut done: mpsc::Sender<rpc::Response>,
    ) -> Result<bool, DsfError> {
        let span = span!(Level::DEBUG, "discover");
        let _enter = span.enter();

        let DiscoverOp { opts, state } = discover_op;

        match state {
            DiscoverState::Init => {
                info!("Discover: {:?}", opts);

                // Set request flags for initial connection
                let flags = Flags::ADDRESS_REQUEST | Flags::PUB_KEY_REQUEST;

                // Build discovery request
                let net_req_id = rand::random();
                let net_req_body = NetRequestBody::Discover(
                    opts.body.clone().unwrap_or(vec![]),
                    opts.filters.clone(),
                );
                let req = NetRequest::new(self.id(), net_req_id, net_req_body, flags);

                // Broadcast discovery request
                // TODO: this mechanism doesn't work because responses _may_ be unwrapped
                // service pages... need to figure out how to handle this properly
                let b = self.net_broacast(req);

                *state = DiscoverState::Pending(b);
                ctx.waker().clone().wake();

                Ok(false)
            },
            DiscoverState::Pending(b) => {
                match b.poll_unpin(ctx) {
                    Poll::Ready(v) => {
                        debug!("Broadcast discovery complete! {:?}", v);

                        // TODO: update services in daemon etc.?

                        // TODO: Build service listing
                        let i = vec![];

                        // Send RPC response
                        let resp = rpc::Response::new(req_id, rpc::ResponseKind::Services(i));
                        let _ = done.try_send(resp);

                        // Update state for cleanup
                        *state = DiscoverState::Done;
                        ctx.waker().clone().wake();

                        Ok(false)
                    },
                    _ => Ok(false),
                }
            },
            DiscoverState::Done => Ok(true),
            DiscoverState::Error => Ok(true),
        }
    }
}
