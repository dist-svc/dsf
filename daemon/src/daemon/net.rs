use std::collections::HashMap;
use std::convert::TryFrom;
use std::future::Future;
use std::net::{IpAddr, SocketAddr};
use std::ops::Add;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Instant;
use std::time::{Duration, SystemTime};

use kad::common::message;
use log::{debug, error, info, trace, warn};

use bytes::Bytes;

use tokio::time::timeout;

use futures::channel::mpsc;
use futures::prelude::*;
use futures::stream::StreamExt;

use tracing::{span, Level};

use dsf_core::net::{self, Status};
use dsf_core::prelude::*;
use dsf_core::types::ShortId;
use dsf_core::types::{
    address::{IPV4_BROADCAST, IPV6_BROADCAST},
    kinds::Kind,
};
use dsf_core::wire::Container;
use dsf_rpc::{
    DataInfo, LocateOptions, PeerAddress, PeerFlags, PeerInfo, PeerState, QosPriority,
    RegisterOptions, ServiceFlags, ServiceIdentifier, ServiceState, SubscribeOptions,
    SubscriptionInfo, SubscriptionKind,
};

use crate::daemon::Dsf;
use crate::error::Error as DaemonError;

use crate::rpc::locate::ServiceRegistry;
use crate::rpc::register::RegisterService;
use crate::rpc::subscribe::PubSub;
use crate::rpc::Engine;
use crate::store::object::ObjectIdentifier;

/// Network interface abstraction, allows [`Dsf`] instance to be generic over interfaces
#[allow(async_fn_in_trait)]
pub trait NetIf {
    /// Interface for sending
    type Interface;

    /// Send a message to the specified targets
    async fn net_send(
        &mut self,
        targets: &[(Address, Option<Id>)],
        msg: NetMessage,
    ) -> Result<(), DaemonError>;
}

pub type NetSink = mpsc::Sender<(Address, Option<Id>, NetMessage)>;

pub type ByteSink = mpsc::Sender<(Address, Vec<u8>)>;

/// Network implementation for abstract message channel (encode/decode externally, primarily used for testing)
impl NetIf for Dsf<NetSink> {
    type Interface = NetSink;

    async fn net_send(
        &mut self,
        targets: &[(Address, Option<Id>)],
        msg: NetMessage,
    ) -> Result<(), DaemonError> {
        // Fan out message to each target
        for t in targets {
            if let Err(e) = self.net_sink.try_send((t.0, t.1.clone(), msg.clone())) {
                error!("Failed to send message to sink: {:?}", e);
            }
        }
        Ok(())
    }
}

/// Network implementation for encoded message channel (encode/decode internally, used with actual networking)
impl NetIf for Dsf<ByteSink> {
    type Interface = ByteSink;

    async fn net_send(
        &mut self,
        targets: &[(Address, Option<Id>)],
        msg: NetMessage,
    ) -> Result<(), DaemonError> {
        // Encode message

        // TODO: this _should_ probably depend on message types / flags
        // (and only use asymmetric mode on request..?)
        let encoded = match targets.len() {
            0 => return Ok(()),
            // If we have one target, use symmetric or asymmetric mode as suits
            1 => {
                let t = &targets[0];
                self.net_encode(t.1.as_ref(), msg).await?
            }
            // If we have multiple targets, use asymmetric encoding to share objects
            // (note this improves performance but drops p2p message privacy)
            _ => self.net_encode(None, msg).await?,
        };

        // Fan-out encoded message to each target
        for t in targets {
            if let Err(e) = self.net_sink.try_send((t.0, encoded.to_vec())) {
                error!("Failed to send message to sink: {:?}", e);
            }
        }

        Ok(())
    }
}

/// Network operation for management by network module
pub struct NetOp {
    /// Network request (required for retries)
    req: net::Request,

    /// Pending network requests by peer ID
    reqs: HashMap<Id, mpsc::Receiver<net::Response>>,

    /// Received responses by peer ID
    resps: HashMap<Id, net::Response>,

    /// Completion channel
    done: mpsc::Sender<HashMap<Id, net::Response>>,

    /// Operation timeout / maximum duration
    timeout: Duration,

    /// Operation start timestamp
    ts: Instant,

    /// Broadcast flag
    broadcast: bool,
}

impl Future for NetOp {
    type Output = Result<(), ()>;

    fn poll(mut self: Pin<&mut Self>, ctx: &mut Context<'_>) -> Poll<Self::Output> {
        // Poll incoming response channels
        let resps: Vec<_> = self
            .reqs
            .iter_mut()
            .filter_map(|(id, r)| match r.poll_next_unpin(ctx) {
                Poll::Ready(Some(r)) => Some((id.clone(), r)),
                _ => None,
            })
            .collect();

        // Update received responses and remove resolved requests
        for (id, resp) in resps {
            self.reqs.remove(&id);
            self.resps.insert(id, resp);
        }

        // Check for completion (no pending requests)
        let done = if self.reqs.len() == 0 && !self.broadcast {
            debug!("Net operation {} complete", self.req.id);
            true

        // Check for timeouts
        // TODO: propagate this timeout value from global config? network latency should _never_ be this high,
        // but the time for a peer to compute the response may also be non-zero
        } else if Instant::now().saturating_duration_since(self.ts) > self.timeout {
            debug!("Net operation {} timeout", self.req.id);
            true
        } else {
            false
        };

        // Issue completion and resolve when done
        if done {
            // TODO: gracefully drop request channels when closed
            // probably this is the case statement in the network handler

            let resps = self.resps.clone();
            if let Err(e) = self.done.try_send(resps) {
                trace!(
                    "Error sending net done (rx channel may have been dropped): {:?}",
                    e
                );
            }
            return Poll::Ready(Ok(()));
        }

        // Always arm waker because we can't really do anything else
        // TODO: investigate embedding wakers / propagating via DSF wake/poll
        let w = ctx.waker().clone();
        w.wake();

        Poll::Pending
    }
}

pub struct NetFuture {
    rx: mpsc::Receiver<HashMap<Id, net::Response>>,
}

impl Future for NetFuture {
    type Output = HashMap<Id, net::Response>;

    fn poll(mut self: Pin<&mut Self>, ctx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.rx.poll_next_unpin(ctx) {
            Poll::Ready(Some(v)) => Poll::Ready(v),
            _ => Poll::Pending,
        }
    }
}

/// Generic network helper for [`Dsf`] implementation
impl<Net> Dsf<Net>
where
    Dsf<Net>: NetIf<Interface = Net>,
{
    pub async fn handle_net(&mut self, msg: crate::io::NetMessage) -> Result<(), DaemonError> {
        // Decode message
        let (container, _n) = Container::from(&msg.data);
        let header = container.header();
        let id: Id = container.id().into();

        let mut data = msg.data.to_vec();

        debug!("RX: {:?}", container);

        // DELEGATION: Handle unwrapped objects for constrained / delegated services
        if !header.kind().is_message() {
            let resp = match crate::net::handle_net_raw(self.exec(), self.core.clone(), &id, container.to_owned()).await {
               Ok(v) => v,
               Err(e) => {
                    error!("handle_net_raw error: {e:?}");
                    return Ok(())
               }
            };
        }

        // Parse out message object
        // TODO: pass secret keys for encode / decode here
        let (message, _n) = match net::Message::parse(&mut data, self) {
            Ok(v) => v,
            Err(e) => {
                error!("Error decoding base message: {:?}", e);
                return Ok(());
            }
        };
        
        debug!("Handling message: {message:?}");

        // TODO: handle crypto mode errors
        // (eg. message encoded with SYMMETRIC but no pubkey for derivation)

        // Upgrade to symmetric mode on incoming symmetric message
        // TODO: there needs to be another transition for this in p2p comms
        let from = message.from();
        if message.flags().contains(Flags::SYMMETRIC_MODE) {
            // TODO: compute and cache symmetric keys here?

            let _ = self
                .core
                .peer_update(
                    &message.from(),
                    Box::new(move |p| {
                        if !p.flags.contains(PeerFlags::SYMMETRIC_ENABLED) {
                            warn!("Enabling symmetric message crypto for peer: {from}");
                            p.flags |= PeerFlags::SYMMETRIC_ENABLED;
                        }
                    }),
                )
                .await;
        }

        trace!("Net message: {:?}", message);

        // Route responses as required internally
        match message {
            NetMessage::Response(resp) => {
                self.handle_net_resp(msg.address, resp).await?;
            }
            NetMessage::Request(req) => {
                let (tx, mut rx) = mpsc::channel(1);

                self.handle_net_req(msg.address, req, tx).await?;

                let a = msg.address.into();

                // TODO: handle errors
                let _ = self.net_send(&[(a, None)], NetMessage::Response(r)).await;

                // Spawn a task to forward completed response to outgoing messages
                // TODO: this _should_ use the response channel in the io::NetMessage, however,
                // we need to re-encode the message prior to doing this which requires the daemon...
                tokio::task::spawn(async move {
                    if let Some(r) = rx.next().await {
                        debug!("Net response: {r:?}");
                        let _ = o.send((a, None, NetMessage::Response(r))).await;
                    }
                });
            }
        };

        Ok(())
    }

    pub async fn net_encode(
        &mut self,
        id: Option<&Id>,
        mut msg: net::Message,
    ) -> Result<Bytes, DaemonError> {
        // Encode response
        let buff = vec![0u8; 10 * 1024];

        // Fetch cached keys if available, otherwise use service keys
        let (enc_key, sym) = match id {
            Some(id) => {
                // Fetch peer information
                // TODO: un async-ify this?
                let p_id = id.clone();
                let core = self.core.clone();
                let p = core.peer_get(p_id).await;

                match (self.key_cache.get(id), p) {
                    (Some(k), Ok(p)) => (k.clone(), p.flags.contains(PeerFlags::SYMMETRIC_ENABLED)),
                    // TODO: disabled to avoid attempting to encode using peer private key when symmetric mode is disabled
                    // Not _entirely_ sure why this needed to be there at all...
                    // Some((Some(k), _)) => (k, false),
                    _ => (self.service().keys(), false),
                }
            }
            None => (self.service().keys(), false),
        };

        // Set symmetric flag if enabled
        if sym {
            *msg.flags_mut() |= Flags::SYMMETRIC_MODE;
        }

        // Temporary patch to always include public key in messages...
        // this should only be required for constrained services that may
        // not have capacity for caching peer keys etc.
        if !sym {
            msg.set_public_key(self.pub_key());
        }

        trace!("Encoding message: {:?}", msg);
        trace!("Keys: {:?}", enc_key);

        let c = match &msg {
            net::Message::Request(req) => self.service().encode_request(req, &enc_key, buff)?,
            net::Message::Response(resp) => self.service().encode_response(resp, &enc_key, buff)?,
        };

        Ok(Bytes::from(c.raw().to_vec()))
    }

    /// Create a network operation for the given request
    ///
    /// This sends the provided request to the listed peers with retries and timeouts.
    pub async fn net_op(&mut self, peers: Vec<PeerInfo>, req: net::Request) -> NetFuture {
        let req_id = req.id;

        // Setup response channels
        let mut reqs = HashMap::with_capacity(peers.len());
        let mut targets = Vec::with_capacity(peers.len());

        // Add individual requests to tracking
        for p in peers {
            // Create response channel
            let (tx, rx) = mpsc::channel(1);
            self.net_requests
                .insert(((*p.address()).clone(), req_id), tx);

            // Add to operation object
            reqs.insert(p.id.clone(), rx);

            // Add as target for sending
            targets.push(((*p.address()).clone(), Some(p.id.clone())));

            // Update send counter
            let p_id = p.id.clone();
            let mut core = self.core.clone();
            tokio::task::spawn(async move {
                let _ = core.peer_update(&p_id, Box::new(|p| p.sent += 1)).await;
            });
        }

        // Create net operation
        let (tx, rx) = mpsc::channel(1);
        let op = NetOp {
            req: req.clone(),
            reqs,
            resps: HashMap::new(),
            done: tx,
            ts: Instant::now(),
            timeout: self.config.net_timeout,
            broadcast: false,
        };

        // Register operation
        self.net_ops.insert(req_id, op);

        // Issue request
        if let Err(e) = self.net_send(&targets, net::Message::Request(req)).await {
            error!("FATAL network send error: {:?}", e);
        }

        // Return future
        NetFuture { rx }
    }

    pub async fn net_broadcast(&mut self, req: net::Request) -> NetFuture {
        let req_id = req.id;

        // Create net operation
        let (tx, rx) = mpsc::channel(1);
        let op = NetOp {
            req: req.clone(),
            reqs: HashMap::new(),
            resps: HashMap::new(),
            done: tx,
            timeout: self.config.net_timeout,
            ts: Instant::now(),
            broadcast: true,
        };

        // Register operation
        self.net_ops.insert(req_id, op);

        // Issue request
        // TODO: select broadcast address' based on availabile interfaces?
        let targets = [(IPV4_BROADCAST.into(), None), (IPV6_BROADCAST.into(), None)];
        if let Err(e) = self.net_send(&targets, net::Message::Request(req)).await {
            error!("FATAL network send error: {:?}", e);
        }

        // Return future
        NetFuture { rx }
    }

    pub(crate) fn poll_net_ops(&mut self, ctx: &mut Context<'_>) {
        // Poll on all pending net operations
        let completed: Vec<_> = self
            .net_ops
            .iter_mut()
            .filter_map(|(req_id, op)| match op.poll_unpin(ctx) {
                Poll::Ready(_) => Some(*req_id),
                _ => None,
            })
            .collect();

        // Remove completed operations
        for req_id in completed {
            self.net_ops.remove(&req_id);
        }
    }

    /// Handle a received response message and generate an (optional) response
    pub async fn handle_net_resp(
        &mut self,
        addr: SocketAddr,
        resp: net::Response,
    ) -> Result<(), DaemonError> {
        let from = resp.from.clone();
        let req_id = resp.id;

        debug!("response: {:?}", resp);

        // Generic net message processing here
        let _peer = match crate::net::handle_base(self.exec(), &from, &addr.into(), &resp.common)
            .await
        {
            Some(p) => p,
            None => return Ok(()),
        };

        // Look for matching point-to-point requests
        if let Some(mut a) = self.net_requests.remove(&(addr.into(), req_id)) {
            trace!("Found pending request for id {} address: {}", req_id, addr);
            if let Err(e) = a.try_send(resp) {
                error!(
                    "Error forwarding message for id {} from {}: {:?}",
                    req_id, addr, e
                );
            }

            return Ok(());
        };

        // Look for matching broadcast requests
        if let Some(a) = self.net_ops.get_mut(&req_id) {
            if a.broadcast {
                trace!("Found pending broadcast request for id {}", req_id);
                a.resps.insert(from, resp);

                return Ok(());
            }

            return Ok(());
        }

        error!("Received response id {} with no pending request", req_id);

        // TODO: what is required here for three-way-ack support?
        // (useful when establishing symmetric peer encryption)

        Ok(())
    }

    /// Handle a received request message and generate a response
    pub async fn handle_net_req<T: Sink<net::Response> + Clone + Send + Unpin + 'static>(
        &mut self,
        addr: SocketAddr,
        req: net::Request,
        mut tx: T,
    ) -> Result<(), DaemonError> {
        let own_id = self.id();

        let span = span!(Level::DEBUG, "id", "{}", own_id);
        let _enter = span.enter();

        let req_id = req.id;
        let flags = req.flags.clone();
        let our_pub_key = self.service().public_key();
        let from = req.from.clone();

        trace!(
            "handling request (from: {:?} / {})\n {:?}",
            from,
            addr,
            &req
        );

        // Common message handing, fetches and updates peer information
        let peer = match crate::net::handle_base(self.exec(), &from, &addr.into(), &req.common)
            .await
        {
            Some(p) => p,
            None => {
                debug!("Dropped message from {from}: {req:?}");
                return Ok(())
            },
        };

        // Handle specific DHT messages
        let resp = if let Some(dht_req) = Self::net_to_dht_request(&req.data) {
            let dht_resp = self.handle_dht_req(from.clone(), peer, dht_req)?;
            let net_resp = Self::dht_to_net_response(dht_resp);

            Some(net::Response::new(
                own_id,
                req_id,
                net_resp,
                Flags::default(),
            ))

        // Handle DSF requests
        } else {
            let dsf_resp = crate::net::handle_dsf_req(self.exec(), self.core.clone(), peer, req.data.clone(), req.flags.clone()).await?;
            Some(net::Response::new(
                own_id,
                req_id,
                dsf_resp,
                Flags::default(),
            ))
        };

        // Skip processing if we've already got a response
        let mut resp = match resp {
            Some(r) => r,
            None => return Ok(()),
        };

        // Generic response processing here
        // TODO: this should probably be in the dsf tx path rather than here?

        if flags.contains(Flags::PUB_KEY_REQUEST) {
            resp.common.public_key = Some(our_pub_key);
        }

        // Update peer info
        let _ = self
            .core
            .peer_update(&from, Box::new(|p| p.sent += 1))
            .await;

        trace!("returning response (to: {:?})\n {:?}", from, &resp);

        if let Err(_e) = tx.send(resp).await {
            error!("Error forwarding net response: {:?}", ());
        }

        Ok(())
    }
}

impl<Net> KeySource for Dsf<Net>
where
    Dsf<Net>: NetIf<Interface = Net>,
{
    fn keys(&self, id: &Id) -> Option<Keys> {
        // Check key cache first
        if let Some(k) = self.key_cache.get(id) {
            return Some(k.clone());
        }

        // Fallback to core
        if let Some(k) = self.core.keys(id) {
            // TODO: Update local cache?
            return Some(k.clone());
        }

        None
    }
}
