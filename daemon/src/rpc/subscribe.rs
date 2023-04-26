//! Subscribe operation, used to subscribe to a known service, 
//! optionally creating a service replica

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::SystemTime;

use futures::channel::mpsc;
use futures::future::join_all;
use futures::prelude::*;

use log::{debug, error, info, trace, warn};
use rpc::QosPriority;
use tracing::{span, Level};

use dsf_core::error::Error as CoreError;
use dsf_core::net;
use dsf_core::prelude::*;
use dsf_rpc::{self as rpc, SubscribeOptions, SubscriptionInfo, SubscriptionKind};

use crate::core::peers::Peer;
use crate::core::services::ServiceState;
use crate::daemon::net::{NetFuture, NetIf};
use crate::daemon::Dsf;
use crate::error::Error;

use super::ops::*;

pub enum SubscribeState {
    Init,
    Searching(kad::dht::SearchFuture<Data>),
    Locating(Vec<kad::dht::LocateFuture<Id, Peer>>),
    Pending(NetFuture),
    Error(Error),
    Done,
}

pub struct SubscribeOp {
    pub(crate) opts: SubscribeOptions,
    pub(crate) state: SubscribeState,
}

pub struct SubscribeFuture {
    rx: mpsc::Receiver<rpc::Response>,
}

unsafe impl Send for SubscribeFuture {}

impl Future for SubscribeFuture {
    type Output = Result<Vec<SubscriptionInfo>, DsfError>;

    fn poll(mut self: Pin<&mut Self>, ctx: &mut Context<'_>) -> Poll<Self::Output> {
        let resp = match self.rx.poll_next_unpin(ctx) {
            Poll::Ready(Some(r)) => r,
            _ => return Poll::Pending,
        };

        match resp.kind() {
            rpc::ResponseKind::Subscribed(r) => Poll::Ready(Ok(r)),
            rpc::ResponseKind::Error(e) => Poll::Ready(Err(e.into())),
            _ => Poll::Pending,
        }
    }
}

impl<Net> Dsf<Net>
where
    Dsf<Net>: NetIf<Interface = Net>,
{
    // Subscribe to data from a given service
    pub fn subscribe(&mut self, options: SubscribeOptions) -> Result<SubscribeFuture, Error> {
        let span = span!(Level::DEBUG, "subscribe");
        let _enter = span.enter();

        let req_id = rand::random();

        let (tx, rx) = mpsc::channel(1);

        // Create connect object
        let op = RpcOperation {
            req_id,
            kind: RpcKind::subscribe(options),
            done: tx,
        };

        // Add to tracking
        debug!("Adding RPC op {} (subscribe) to tracking", req_id);
        self.rpc_ops.insert(req_id, op);

        Ok(SubscribeFuture { rx })
    }

    pub fn poll_rpc_subscribe(
        &mut self,
        req_id: u64,
        subscribe_op: &mut SubscribeOp,
        ctx: &mut Context,
        mut done: mpsc::Sender<rpc::Response>,
    ) -> Result<bool, DsfError> {
        let SubscribeOp { opts, state } = subscribe_op;

        let span = span!(Level::DEBUG, "subscribe");
        let _enter = span.enter();

        // Resolve service identifier
        let id = match self.resolve_identifier(&opts.service) {
            Ok(id) => id,
            Err(_) => {
                error!("No matching service for {}", opts.service.index.unwrap());
                return Err(DsfError::UnknownService);
            }
        };

        match state {
            SubscribeState::Init => {
                // Fetch the service from the service list
                let _service_info = match self.services().find(&id) {
                    Some(s) => s,
                    None => {
                        // Only known services can be registered
                        error!("unknown service (id: {})", id);
                        return Err(DsfError::UnknownService);
                    }
                };

                if self.peers().seen_count() == 0 {
                    warn!("No active peers, subscription only effects local node");

                    let resp = rpc::Response::new(req_id, rpc::ResponseKind::Subscribed(vec![]));

                    done.try_send(resp).unwrap();

                    *state = SubscribeState::Done;
                    return Ok(true);
                }

                // Search for viable replicas in the database
                let (query, _) = match self.dht_mut().search(id) {
                    Ok(v) => v,
                    Err(e) => {
                        error!("Error starting DHT search: {:?}", e);
                        return Err(DsfError::Unknown);
                    }
                };

                *state = SubscribeState::Searching(query);
                Ok(false)
            }
            SubscribeState::Searching(search) => {
                match search.poll_unpin(ctx) {
                    Poll::Ready(Ok(v)) => {
                        debug!("DHT search complete! {} pages found", v.len());
                        trace!("{:?}", v);

                        // Update located replicas
                        // TODO: perhaps this should happen in the rx handle?
                        for p in &v {
                            // TODO: check other page fields here (id etc.)
                            if let PageInfo::Secondary(s) = &p.info()? {
                                self.replicas().create_or_update(&id, &s.peer_id, p);
                            }
                        }

                        // Fetch peers for viable replicas
                        let replicas = self.replicas().find(&id);
                        let replica_peers: Vec<_> =
                            replicas.iter().map(|r| r.info.peer_id.clone()).collect();

                        // TODO: Skip lookup if no known peers

                        // Lookup (unknown) peers
                        let our_id = self.id();
                        let lookups: Vec<_> = replica_peers
                            .iter()
                            .filter(|peer_id| *peer_id != &our_id)
                            .filter_map(|peer_id| match self.dht_mut().locate(peer_id.clone()) {
                                Ok(v) => Some(v.0),
                                Err(e) => {
                                    error!("Error starting DHT locate for {:?}: {:?}", peer_id, e);
                                    None
                                }
                            })
                            .collect();

                        *state = SubscribeState::Locating(lookups);
                        Ok(false)
                    }
                    Poll::Ready(Err(e)) => {
                        error!("DHT search error: {:?}", e);

                        // TODO: propagate error
                        *state = SubscribeState::Error(Error::Unknown);
                        Ok(false)
                    }
                    _ => Ok(false),
                }
            }
            SubscribeState::Locating(lookups) => {
                // Poll for completion of pending DHT lookups
                let completed: Vec<_> = lookups
                    .iter_mut()
                    .enumerate()
                    .filter_map(|(i, l)| match l.poll_unpin(ctx) {
                        Poll::Ready(v) => Some((i, v)),
                        _ => None,
                    })
                    .collect();

                // Remove completed lookups
                for (i, _r) in &completed {
                    lookups.remove(*i);
                }

                if lookups.len() == 0 {
                    debug!("Subscribe peer lookup complete");

                    // Create subscription request
                    let req = net::Request::new(
                        self.id(),
                        rand::random(),
                        net::RequestBody::Subscribe(id.clone()),
                        Flags::default(),
                    );

                    // Generate peer list for subscription request
                    let peers: Vec<_> = completed
                        .iter()
                        .filter_map(|(_i, r)| match r {
                            Ok(e) => Some(e.info().clone()),
                            _ => None,
                        })
                        .collect();

                    debug!("Issuing subscribe request to {} peers", peers.len());

                    // Issue net operation
                    let op = self.net_op(peers, req);

                    *state = SubscribeState::Pending(op);
                }

                Ok(false)
            }
            SubscribeState::Pending(op) => {
                // Poll on network operation completion
                match op.poll_unpin(ctx) {
                    Poll::Ready(d) => {
                        debug!("Subscribe requests complete");

                        // Generate subscription info
                        let subs: Vec<_> = d
                            .iter()
                            .filter_map(|(peer_id, resp)| match &resp.data {
                                net::ResponseBody::Status(s) if *s == net::Status::Ok => {
                                    Some(SubscriptionInfo {
                                        service_id: id.clone(),
                                        kind: SubscriptionKind::Peer(peer_id.clone()),
                                        updated: Some(SystemTime::now()),
                                        expiry: None,
                                        qos: QosPriority::None,
                                    })
                                }
                                net::ResponseBody::ValuesFound(service_id, _pages) => {
                                    Some(SubscriptionInfo {
                                        service_id: service_id.clone(),
                                        kind: SubscriptionKind::Peer(peer_id.clone()),
                                        updated: Some(SystemTime::now()),
                                        expiry: None,
                                        qos: QosPriority::None,
                                    })
                                }
                                _ => None,
                            })
                            .collect();

                        debug!("Found {} subscribers", subs.len());

                        // TODO: send completion
                        let resp = rpc::Response::new(req_id, rpc::ResponseKind::Subscribed(subs));

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
