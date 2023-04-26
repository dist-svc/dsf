use std::collections::HashMap;
use std::convert::TryFrom;
use std::future::Future;
use std::net::{IpAddr, SocketAddr};
use std::ops::Add;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Instant;
use std::time::{Duration, SystemTime};

use dsf_core::types::address::{IPV4_BROADCAST, IPV6_BROADCAST};
use dsf_rpc::{LocateOptions, QosPriority, RegisterOptions, ServiceIdentifier, SubscribeOptions};
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
use dsf_core::wire::Container;

use crate::daemon::Dsf;
use crate::error::Error as DaemonError;

use crate::core::data::DataInfo;
use crate::core::peers::{Peer, PeerAddress, PeerFlags, PeerState};

/// Network interface abstraction, allows [`Dsf`] instance to be generic over interfaces
pub trait NetIf {
    /// Interface for sending
    type Interface;

    /// Send a message to the specified targets
    fn net_send(
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

    fn net_send(
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

    fn net_send(
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
                self.net_encode(t.1.as_ref(), msg)?
            }
            // If we have multiple targets, use asymmetric encoding to share objects
            // (note this improves performance but drops p2p message privacy)
            _ => self.net_encode(None, msg)?,
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
        } else if Instant::now().saturating_duration_since(self.ts) > Duration::from_secs(3) {
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
    pub async fn handle_net_raw(&mut self, msg: crate::io::NetMessage) -> Result<(), DaemonError> {
        // Decode message
        let decoded = match self.net_decode(&msg.data).await {
            Ok(v) => v,
            Err(e) => {
                warn!("error decoding message from {:?}: {:?}", msg.address, e);
                return Ok(());
            }
        };

        trace!("Net message: {:?}", decoded);

        // Route responses as required internally
        match decoded {
            NetMessage::Response(resp) => {
                self.handle_net_resp(msg.address, resp)?;
            }
            NetMessage::Request(req) => {
                let (tx, mut rx) = mpsc::channel(1);

                self.handle_net_req(msg.address, req, tx).await?;

                let a = msg.address.into();
                let mut o = self.net_resp_tx.clone();

                // Spawn a task to forward completed response to outgoing messages
                // TODO: this _should_ use the response channel in the io::NetMessage, however,
                // we need to re-encode the message prior to doing this which requires the daemon...
                tokio::task::spawn(async move {
                    if let Some(r) = rx.next().await {
                        let _ = o.send((a, None, NetMessage::Response(r))).await;
                    }
                });
            }
        };

        Ok(())
    }

    pub async fn net_decode(&mut self, data: &[u8]) -> Result<net::Message, DaemonError> {
        // Decode message
        let (container, _n) = Container::from(&data);
        let _id: Id = container.id().into();

        let mut data = data.to_vec();

        // Parse out message object
        // TODO: pass secret keys for encode / decode here
        let (message, _n) = match net::Message::parse(&mut data, self) {
            Ok(v) => v,
            Err(e) => {
                error!("Error decoding base message: {:?}", e);
                return Err(e.into());
            }
        };

        // TODO: handle crypto mode errors
        // (eg. message encoded with SYMMETRIC but no pubkey for derivation)

        // Upgrade to symmetric mode on incoming symmetric message
        // TODO: there needs to be another transition for this in p2p comms
        if message.flags().contains(Flags::SYMMETRIC_MODE) {
            self.peers().update(&message.from(), |p| {
                if !p.flags.contains(PeerFlags::SYMMETRIC_ENABLED) {
                    warn!(
                        "Enabling symmetric message crypto for peer: {}",
                        message.from()
                    );
                    p.flags |= PeerFlags::SYMMETRIC_ENABLED;
                }
            });
        }

        Ok(message)
    }

    pub fn net_encode(
        &mut self,
        id: Option<&Id>,
        mut msg: net::Message,
    ) -> Result<Bytes, DaemonError> {
        // Encode response
        let buff = vec![0u8; 4096];

        // Fetch cached keys if available, otherwise use service keys
        let (enc_key, sym) =
            match id.map(|id| (self.keys(id), self.peers().filter(id, |p| p.flags))) {
                Some((Some(k), Some(f))) if f.contains(PeerFlags::SYMMETRIC_ENABLED) => (k, true),
                // TODO: disabled to avoid attempting to encode using peer private key when symmetric mode is disabled
                // Not _entirely_ sure why this needed to be there at all...
                // Some((Some(k), _)) => (k, false),
                _ => (self.service().keys(), false),
            };

        if sym {
            *msg.flags_mut() |= Flags::SYMMETRIC_MODE;
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
    pub fn net_op(&mut self, peers: Vec<Peer>, req: net::Request) -> NetFuture {
        let req_id = req.id;

        // Setup response channels
        let mut reqs = HashMap::with_capacity(peers.len());
        let mut targets = Vec::with_capacity(peers.len());

        // Add individual requests to tracking
        for p in peers {
            // Create response channel
            let (tx, rx) = mpsc::channel(1);
            self.net_requests.insert((p.address().into(), req_id), tx);

            // Add to operation object
            reqs.insert(p.id(), rx);

            // Add as target for sending
            targets.push((p.address(), Some(p.id())));
        }

        // Create net operation
        let (tx, rx) = mpsc::channel(1);
        let op = NetOp {
            req: req.clone(),
            reqs,
            resps: HashMap::new(),
            done: tx,
            ts: Instant::now(),
            broadcast: false,
        };

        // Register operation
        self.net_ops.insert(req_id, op);

        // Issue request
        if let Err(e) = self.net_send(&targets, net::Message::Request(req)) {
            error!("FATAL network send error: {:?}", e);
        }

        // Return future
        NetFuture { rx }
    }

    pub fn net_broacast(&mut self, req: net::Request) -> NetFuture {
        let req_id = req.id;

        // Create net operation
        let (tx, rx) = mpsc::channel(1);
        let op = NetOp {
            req: req.clone(),
            reqs: HashMap::new(),
            resps: HashMap::new(),
            done: tx,
            ts: Instant::now(),
            broadcast: true,
        };

        // Register operation
        self.net_ops.insert(req_id, op);

        // Issue request
        // TODO: select broadcast address' based on availabile interfaces?
        let targets = [
            (IPV4_BROADCAST.into(), None),
            (IPV6_BROADCAST.into(), None),
        ];
        if let Err(e) = self.net_send(&targets, net::Message::Request(req)) {
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
    pub fn handle_net_resp(
        &mut self,
        addr: SocketAddr,
        resp: net::Response,
    ) -> Result<(), DaemonError> {
        let from = resp.from.clone();
        let req_id = resp.id;

        // Generic net message processing here
        let peer =
            match self.handle_base(&from, &addr.into(), &resp.common, Some(SystemTime::now())) {
                Some(p) => p,
                None => return Ok(()),
            };

        // Parse out and handle DHT responses
        if let Some(dht_resp) = self.net_to_dht_response(&resp.data) {
            let _ = self.handle_dht_resp(from.clone(), peer, req_id, dht_resp);

            return Ok(());
        }

        // Look for matching point-to-point requests
        if let Some(mut a) = self.net_requests.remove(&(addr.into(), req_id)) {
            trace!("Found pending request for id {} address: {}", req_id, addr);
            if let Err(e) = a.try_send(resp) {
                error!(
                    "Error forwarding message for id {} from {}: {:?}",
                    req_id, addr, e
                );
            }

            return Ok(())
        };

        // Look for matching broadcast requests
        if let Some(a) = self.net_ops.get_mut(&req_id) {
            if a.broadcast {
                trace!("Found pending broadcast request for id {}", req_id);
                a.resps.insert(from, resp);

                return Ok(())
            }

            return Ok(())
        }

        error!("Received response id {} with no pending request", req_id);

        // TODO: handle responses for three-way-ack support?

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

        // Generic net message processing here
        let peer = match self.handle_base(&from, &addr.into(), &req.common, Some(SystemTime::now()))
        {
            Some(p) => p,
            None => return Ok(()),
        };

        // Handle specific DHT messages
        let resp = if let Some(dht_req) = self.net_to_dht_request(&req.data) {
            let dht_resp = self.handle_dht_req(from.clone(), peer, req.id, dht_req)?;

            let net_resp = self.dht_to_net_response(dht_resp);

            Some(net::Response::new(
                own_id,
                req_id,
                net_resp,
                Flags::default(),
            ))

        // Handle delegated messages
        } else if req.flags.contains(Flags::CONSTRAINED)
            && self.handle_dsf_delegated(&peer, req_id, &req, tx.clone())?
        {
            None

        // Handle normal DSF messages
        } else {
            let dsf_resp = self.handle_dsf(from.clone(), peer, req.data)?;
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
        self.peers().update(&from, |p| p.info.sent += 1);

        trace!("returning response (to: {:?})\n {:?}", from, &resp);

        if let Err(_e) = tx.send(resp).await {
            error!("Error forwarding net response: {:?}", ());
        }

        Ok(())
    }

    /// Handles a base message, updating internal state for the sender
    pub(crate) fn handle_base(
        &mut self,
        id: &Id,
        address: &Address,
        c: &net::Common,
        _seen: Option<SystemTime>,
    ) -> Option<Peer> {
        trace!(
            "[DSF ({:?})] Handling base message from: {:?} address: {:?} public_key: {:?}",
            self.id(),
            id,
            address,
            c.public_key
        );

        // Skip RX of messages / loops
        // TODO: may need this to check tunnels for STUN or equivalents... a problem for later
        if *id == self.id() {
            warn!("handle_base called for self...");
            return None;
        }

        let mut peer_flags = PeerFlags::empty();
        if c.flags.contains(Flags::CONSTRAINED) {
            peer_flags |= PeerFlags::CONSTRAINED;
        }

        // Find or create (and push) peer
        let peer = self.peers().find_or_create(
            id.clone(),
            PeerAddress::Implicit(*address),
            c.public_key.clone(),
            peer_flags,
        );

        // Update key cache
        match (self.key_cache.contains_key(id), &c.public_key) {
            (false, Some(pk)) => {
                if let Ok(k) = self.service().keys().derive_peer(pk.clone()) {
                    self.key_cache.insert(id.clone(), k.clone());
                }
            }
            _ => (),
        }

        // Update peer info
        self.peers().update(&id, |p| {
            p.info.seen = Some(SystemTime::now());
            p.info.received += 1;
        });

        trace!(
            "[DSF ({:?})] Peer id: {:?} state: {:?} seen: {:?}",
            self.id(),
            id,
            peer.state(),
            peer.seen()
        );

        // Add public key if appropriate
        match (peer.state(), &c.public_key) {
            (PeerState::Unknown, Some(pk)) => {
                info!("Adding key: {:?} to peer: {:?}", pk, id);
                self.peers()
                    .update(&id, |p| p.info.state = PeerState::Known(pk.clone()));
            }
            _ => (),
        };

        // Update address if specified (and different from existing)
        if let Some(a) = c.remote_address {
            if a != peer.address() {
                info!("Setting explicit address {:?} for peer: {:?}", a, id);
                self.peers()
                    .update(&id, |p| p.info.address = PeerAddress::Explicit(a));
            }
        }

        Some(peer)
    }

    /// Handle a DSF type message
    fn handle_dsf(
        &mut self,
        from: Id,
        peer: Peer,
        req: net::RequestBody,
    ) -> Result<net::ResponseBody, DaemonError> {
        match req {
            net::RequestBody::Hello => Ok(net::ResponseBody::Status(net::Status::Ok)),
            net::RequestBody::Subscribe(service_id) => {
                info!(
                    "Subscribe request from: {} for service: {}",
                    from, service_id
                );
                let _service = match self.services().find(&service_id) {
                    Some(s) => s,
                    None => {
                        // Only known services can be subscribed
                        error!("no service found (id: {})", service_id);
                        return Ok(net::ResponseBody::Status(net::Status::InvalidRequest));
                    }
                };

                // Fetch pages for service
                let pages = {
                    match &self
                        .services()
                        .filter(&service_id, |s| s.primary_page.clone())
                        .flatten()
                    {
                        Some(p) => vec![p.clone()],
                        None => vec![],
                    }
                };

                // TODO: verify this is coming from an active upstream subscriber

                // TODO: update subscriber count?

                // TODO: update peer subscription information here
                self.subscribers()
                    .update_peer(&service_id, &peer.id(), |inst| {
                        inst.info.updated = Some(SystemTime::now());
                        inst.info.expiry = Some(SystemTime::now().add(Duration::from_secs(3600)));
                    })
                    .unwrap();

                Ok(net::ResponseBody::ValuesFound(service_id, pages))
            }
            net::RequestBody::Unsubscribe(service_id) => {
                info!(
                    "Unsubscribe request from: {} for service: {}",
                    from, service_id
                );

                self.subscribers().remove(&service_id, &peer.id()).unwrap();

                Ok(net::ResponseBody::Status(net::Status::Ok))
            }
            net::RequestBody::Query(id) => {
                info!("Query request from: {} for service: {}", from, id);
                let _service = match self.services().find(&id) {
                    Some(s) => s,
                    None => {
                        // Only known services can be registered
                        error!("no service found (id: {})", id);
                        return Ok(net::ResponseBody::Status(net::Status::InvalidRequest));
                    }
                };

                // TODO: fetch and return data

                info!("Query request complete");

                Err(DaemonError::Unimplemented)
            }
            net::RequestBody::Register(id, pages) => {
                info!("Register request from: {} for service: {}", from, id);
                // TODO: determine whether we should allow this service to be registered

                // Add to local service registry
                self.service_register(&id, pages)?;

                info!("Register request for service: {} complete", id);

                Ok(net::ResponseBody::Status(net::Status::Ok))
            }
            net::RequestBody::Unregister(id) => {
                info!("Unegister request from: {} for service: {}", from, id);
                // TODO: determine whether we should allow this service to be unregistered

                todo!()
            }
            net::RequestBody::PushData(id, data) => {
                info!("Data push from: {} for service: {}", from, id);

                // TODO: why find _then_ validate_pages, duplicated ops?!
                let _service = match self.services().find(&id) {
                    Some(s) => s,
                    None => {
                        // Only known services can be registered
                        error!("no service found (id: {})", id);
                        return Ok(net::ResponseBody::Status(net::Status::InvalidRequest));
                    }
                };

                // Validate incoming data prior to processing
                if let Err(e) = self.services().validate_pages(&id, &data) {
                    error!("Invalid data for service: {} ({:?})", id, e);
                    return Ok(net::ResponseBody::Status(net::Status::InvalidRequest));
                }

                // Pick out service pages

                for p in &data {
                    // Apply any updates to the service
                    if p.header().kind().is_page() {
                        self.services().update_inst(&id, |s| {
                            let _ = s.apply_update(p);
                        });
                    }

                    // Store data pages
                    if p.header().kind().is_data() {
                        if let Ok(info) = DataInfo::try_from(p) {
                            // TODO: this is (probably) one of the sloww bits
                            self.data().store_data(&info, p).unwrap();
                        };
                    }
                }

                // TODO: check this is _new_ data, otherwise ignore (avoid)

                // Generate data push message
                let req_id = rand::random();
                let req = net::Request::new(
                    self.id(),
                    req_id,
                    net::RequestBody::PushData(id.clone(), data),
                    Flags::default(),
                );

                // Generate peer list for data push
                // TODO: we should be cleverer about this to avoid
                // loops etc. (and prolly add a TTL if it can be repeated?)
                let peer_ids = self.subscribers().find_peers(&id)?;
                let peer_subs: Vec<_> = peer_ids
                    .iter()
                    .filter_map(|peer_id| self.peers().find(peer_id))
                    .collect();

                info!(
                    "Sending data push message id {} to: {:?}",
                    req_id, peer_subs
                );

                // Issue data push requests
                // TODO: we should probably wire the return here to send a delayed PublishInfo to the requester?
                // TODO: deadlock? yeees, deadlock is here or related to this call
                let _ = self.net_op(peer_subs, req);

                info!("Data push complete");

                Ok(net::ResponseBody::Status(net::Status::Ok))
            }
            _ => Err(DaemonError::Unimplemented),
        }
    }

    fn handle_dsf_delegated<T: 'static + Sink<NetResponse> + Unpin + Send>(
        &mut self,
        peer: &Peer,
        req_id: RequestId,
        req: &net::Request,
        mut tx: T,
    ) -> Result<bool, DaemonError> {
        // TODO: elect whether to accept delegated request

        let own_id = self.id();
        let own_pk = self.pub_key();
        let needs_pk = req.flags.contains(Flags::PUB_KEY_REQUEST);

        match &req.data {
            net::RequestBody::Locate(service_id) => {
                info!(
                    "Delegated locate request from: {} for service: {}",
                    peer.id(),
                    service_id
                );

                let opts = LocateOptions {
                    id: service_id.clone(),
                    local_only: false,
                };
                let service_id = service_id.clone();
                let loc = self.locate(opts)?;

                tokio::task::spawn(async move {
                    let resp = match loc.await {
                        Ok(v) => {
                            if let Some(p) = v.page {
                                info!("Locate ok (index: {})", v.page_version);
                                net::ResponseBody::ValuesFound(service_id, vec![p])
                            } else {
                                error!("Locate failed, no page found");
                                net::ResponseBody::Status(Status::Failed)
                            }
                        }
                        Err(e) => {
                            error!("Locate failed: {:?}", e);
                            net::ResponseBody::Status(Status::Failed)
                        }
                    };

                    let mut resp = net::Response::new(own_id, req_id, resp, Flags::default());

                    if needs_pk {
                        resp.common.public_key = Some(own_pk);
                    }

                    if let Err(_e) = tx.send(resp).await {
                        error!("Locate delegate TX error");
                    }
                });

                Ok(true)
            }
            net::RequestBody::Subscribe(service_id) => {
                info!(
                    "Delegated subscribe request from: {} for service: {}",
                    peer.id(),
                    service_id
                );

                // Add subscriber to tracking
                self.subscribers()
                    .update_peer(&service_id, &peer.id(), |inst| {
                        inst.info.updated = Some(SystemTime::now());
                        inst.info.expiry = Some(SystemTime::now().add(Duration::from_secs(3600)));
                        // Set QOS latency priority if flag is provided
                        if req.flags.contains(Flags::QOS_PRIO_LATENCY) {
                            inst.info.qos = QosPriority::Latency;
                        }
                    })
                    .unwrap();

                // Issue subscribe request to replicas
                let opts = SubscribeOptions {
                    service: ServiceIdentifier::id(service_id.clone()),
                };
                let sub = self.subscribe(opts)?;

                // Await subscription response
                tokio::task::spawn(async move {
                    let resp = match sub.await {
                        Ok(_v) => net::ResponseBody::Status(Status::Ok),
                        Err(e) => {
                            error!("Subscription failed: {:?}", e);
                            net::ResponseBody::Status(Status::Failed)
                        }
                    };

                    let mut resp = net::Response::new(own_id, req_id, resp, Flags::default());
                    if needs_pk {
                        resp.common.public_key = Some(own_pk);
                    }

                    if let Err(_e) = tx.send(resp).await {
                        error!("Subscribe delegate TX error");
                    }
                });

                Ok(true)
            }
            net::RequestBody::Register(service_id, pages) => {
                info!(
                    "Delegated register request from: {} for service: {}",
                    peer.id(),
                    service_id
                );

                // Add to local service registry
                self.service_register(&service_id, pages.clone())?;

                // Perform global registration
                let opts = RegisterOptions {
                    service: ServiceIdentifier::id(service_id.clone()),
                    no_replica: false,
                };
                let reg = self.register(opts)?;

                // Task to await registration completion
                tokio::task::spawn(async move {
                    let resp = match reg.await {
                        Ok(_v) => net::ResponseBody::Status(Status::Ok),
                        Err(e) => {
                            error!("Registration failed: {:?}", e);
                            net::ResponseBody::Status(Status::Failed)
                        }
                    };

                    let mut resp = net::Response::new(own_id, req_id, resp, Flags::default());
                    if needs_pk {
                        resp.common.public_key = Some(own_pk);
                    }

                    if let Err(_e) = tx.send(resp).await {
                        error!("Register delegate TX error");
                    }
                });

                Ok(true)
            }
            _ => Ok(false),
        }
    }
}
