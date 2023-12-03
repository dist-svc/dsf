use std::collections::HashMap;
use std::convert::TryFrom;
use std::future::Future;
use std::net::{IpAddr, SocketAddr};
use std::ops::Add;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Instant;
use std::time::{Duration, SystemTime};

use dsf_core::crypto::{Crypto, PubKey};
use dsf_core::options::Filters;
use kad::common::message;
use log::{debug, error, info, trace, warn};

use bytes::Bytes;

use tokio::time::timeout;

use futures::channel::mpsc;
use futures::prelude::*;
use futures::stream::StreamExt;

use tracing::{instrument, span, Level};

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

use crate::core::AsyncCore;
use crate::daemon::Dsf;
use crate::error::Error as DaemonError;

use crate::rpc::locate::ServiceRegistry;
use crate::rpc::register::RegisterService;
use crate::rpc::subscribe::PubSub;
use crate::rpc::Engine;
use crate::store::object::ObjectIdentifier;
use crate::sync::{Arc, Mutex};

use super::dht::AsyncDht;

/// Network interface abstraction, allows [`Dsf`] instance to be generic over interfaces
#[allow(async_fn_in_trait)]
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
        mut msg: NetMessage,
    ) -> Result<(), DaemonError> {
        let core = self.core.clone();
        let signing_id = self.id();
        let signing_key = self.service.private_key().unwrap();
        let public_key = self.service.public_key();
        let mut net_sink = self.net_sink.clone();

        let targets = targets.to_vec();

        // Fetch peer keys from cache
        let mut keys = HashMap::new();
        for (_addr, id) in &targets {
            if let Some(id) = id {
                if let Some(k) = self.key_cache.keys(id) {
                    keys.insert(id.clone(), k.clone());
                }
            }
        }

        // Spawn task to handle net encoding and sending
        tokio::task::spawn(async move {
            for (a, id) in targets {
                // Resolve target IDs to peers

                // Lookup keys in cache
                let peer_keys = id.as_ref().map(|id| keys.get(id)).flatten();

                // Lookup peer info so we can use this for symmetric mode determination
                // TODO(med): we should cache this state / pass these down to improve efficiency
                let peer_info = match id.as_ref() {
                    Some(id) => core.peer_get(id).await.ok(),
                    None => None,
                };

                // Resolve public key from cache or info
                let pub_key = match (
                    peer_info.as_ref().map(|i| i.state()),
                    peer_keys.map(|k| k.pub_key.clone()),
                ) {
                    (Some(PeerState::Known(pub_key)), _) => Some(pub_key.clone()),
                    (_, Some(pub_key)) => pub_key.clone(),
                    _ => None,
                };

                // Setup keys for message encoding
                let enc_keys = Keys {
                    // daemon private key for signing
                    pri_key: Some(signing_key.clone()),
                    // target public key for asymmetric encryption
                    pub_key: pub_key,
                    // Symmetric key must be in key cache
                    sec_key: peer_keys.map(|k| k.sec_key.clone()).flatten(),
                    ..Default::default()
                };

                // Enable symmetric mode if supported
                let sym = peer_info
                    .map(|p| p.flags.contains(PeerFlags::SYMMETRIC_ENABLED))
                    .unwrap_or(false);
                if sym {
                    *msg.flags_mut() |= Flags::SYMMETRIC_MODE;
                }

                // Temporary patch to always include public key in messages...
                // this should only be required for constrained services that may
                // not have capacity for caching peer keys etc.
                if !sym {
                    msg.set_public_key(public_key.clone());
                }

                // Encode message
                let buff = vec![0u8; 10 * 1024];

                let c = match &msg {
                    NetMessage::Request(req) => {
                        dsf_core::net::encode_request(&signing_id, req, &enc_keys, buff)
                    }
                    NetMessage::Response(resp) => {
                        dsf_core::net::encode_response(&signing_id, resp, &enc_keys, buff)
                    }
                };

                let encoded = match c {
                    Ok(c) => c.raw().to_vec(),
                    Err(e) => {
                        error!("Message encode failed: {e:?}");
                        continue;
                    }
                };

                // Forward to network sink
                if let Err(e) = net_sink.try_send((a, encoded.to_vec())) {
                    error!("Failed to send message to sink: {:?}", e);
                }
            }
        });

        Ok(())
    }
}

/// Generic network helper for [`Dsf`] implementation
impl<Net> Dsf<Net>
where
    Dsf<Net>: NetIf<Interface = Net>,
{
    pub async fn handle_net(&mut self, msg: crate::io::NetMessage) -> Result<(), DaemonError> {
        // Setup context for async-ified handling
        let engine = self.exec();
        let mut core = self.core.clone();
        let net = self.net.clone();
        let dht = self.dht.clone();
        let key_cache = self.key_cache.clone();

        let our_id = self.id();
        let our_keys = self.service().keys();

        // Spawn task to handle network operations in parallel
        tokio::task::spawn(async move {
            // Decode and verify/decrypt message
            let container = match Container::parse(msg.data.to_vec(), &key_cache) {
                Ok(v) => v,
                Err(e) => {
                    error!("Container parsing error: {e:?}");
                    return;
                }
            };

            trace!("RX: {:?}", container);

            let id: Id = container.id().into();
            let addr = Address::from(msg.address.clone());

            // Add newly discovered peer keys to cache, generating symmetric keys as we do
            if let Some(pub_key) = container.public_options_iter().pub_key() {
                let sym_keys = Crypto::kx(
                    our_keys.pub_key.as_ref().unwrap(),
                    our_keys.pri_key.as_ref().unwrap(),
                    &pub_key,
                )
                .unwrap();

                let k = Keys{ 
                    pub_key: Some(pub_key),
                    sym_keys: Some(sym_keys),
                    ..Default::default()
                };
                key_cache.create_update(&id, &k);
            }

            // Convert container to message object if applicable
            // TODO: pass secret keys for encode / decode here
            let mut message = None;
            if container.header().kind().is_message() {
                match net::Message::convert(container.clone(), &key_cache) {
                    Ok(v) => message = Some(v),
                    Err(e) => {
                        error!("Error converting net message: {:?}", e);
                        return;
                    }
                }
            }

            // DELEGATION: Handle unwrapped objects for constrained / delegated services
            if !container.header().kind().is_message() {
                // Handle raw object
                let resp = match crate::net::handle_net_raw(
                    &engine,
                    core.clone(),
                    &id,
                    container.to_owned(),
                )
                .await
                {
                    Ok(v) => v,
                    Err(e) => {
                        error!("handle_net_raw error: {e:?}");
                        return;
                    }
                };

                // TODO(high): Send response
            }

            let message = match message {
                Some(m) => m,
                None => {
                    error!("No message");
                    return;
                }
            };

            debug!("Handling message: {message:?}");

            // Generic net message processing, fetch and update peer information
            let from = message.from();
            let peer = match crate::net::handle_base(
                &our_id,
                core.clone(),
                &from,
                &addr.into(),
                &message.common().clone(),
            )
            .await
            {
                Some(p) => p,
                None => return,
            };

            debug!("Peer: {peer:?}");

            // TODO: handle crypto mode errors
            // (eg. message encoded with SYMMETRIC but no pubkey for derivation)

            // Upgrade to symmetric mode on incoming symmetric message
            // TODO: there needs to be another transition for this in p2p comms

            if message.flags().contains(Flags::SYMMETRIC_MODE) {
                // TODO: compute and cache symmetric keys here?

                let _ = core
                    .peer_update(
                        &message.from(),
                        Box::new(move |p| {
                            if !p.flags.contains(PeerFlags::SYMMETRIC_ENABLED) {
                                warn!("Enabling symmetric message crypto for peer: {}", p.id);
                                p.flags |= PeerFlags::SYMMETRIC_ENABLED;
                            }
                        }),
                    )
                    .await;
            }

            // Route requests and responses to appropriate handlers
            match message {
                NetMessage::Response(resp) => {
                    // Forward responses via AsyncNet router
                    if let Err(e) = net.handle_resp(addr, from, resp).await {
                        error!("Failed to forward response to mux: {e:?}");
                    }
                }
                NetMessage::Request(req) => {
                    // Handle requests
                    // TODO(HIGH): this must be in an async task to avoid blocking all other ops
                    let resp = match handle_net_req2(
                        engine,
                        core,
                        dht,
                        our_keys.pub_key.unwrap(),
                        peer,
                        msg.address,
                        req,
                    )
                    .await
                    {
                        Ok(resp) => resp,
                        Err(e) => {
                            error!("Error handling net request: {e:?}");
                            return;
                        }
                    };

                    // Send response
                    if let Err(e) = net.net_send(vec![(addr, Some(from))], resp.into()).await {
                        error!("Failed to forward net response: {e:?}");
                    }
                }
            };
        });

        Ok(())
    }

    pub async fn handle_net_req(
        &mut self,
        peer: PeerInfo,
        addr: SocketAddr,
        req: net::Request,
    ) -> Result<net::Response, DaemonError> {
        let our_pub_key = self.service().public_key();

        handle_net_req2(
            self.exec(),
            self.core.clone(),
            self.dht.clone(),
            our_pub_key,
            peer,
            addr,
            req,
        )
        .await
    }
}

/// Handle a received request message and generate a response
#[instrument(skip_all, fields(req_id = req.common.id))]
async fn handle_net_req2<T: Engine + 'static>(
    engine: T,
    mut core: AsyncCore,
    dht: AsyncDht,
    our_pub_key: PublicKey,
    peer: PeerInfo,
    addr: SocketAddr,
    req: net::Request,
) -> Result<net::Response, DaemonError> {
    let own_id = engine.id();

    let req_id = req.id;
    let flags = req.flags.clone();

    let from = req.from.clone();

    trace!(
        "handling request (from: {:?} / {})\n {:?}",
        from,
        addr,
        &req
    );

    // Handle specific DHT messages
    let mut resp = if let Some(dht_req) = super::dht::net_to_dht_request(&req.data) {
        let dht_resp = dht.handle_req(peer, dht_req).await?;
        let net_resp = super::dht::dht_to_net_response(dht_resp);

        net::Response::new(own_id, req_id, net_resp, Flags::default())

    // Handle DSF requests
    } else {
        let dsf_resp = crate::net::handle_dsf_req(
            engine,
            core.clone(),
            peer,
            req.data.clone(),
            req.flags.clone(),
        )
        .await?;

        net::Response::new(own_id, req_id, dsf_resp, Flags::default())
    };

    debug!("Response: {resp:?}");

    // Generic response processing here
    // TODO: this should probably be in the dsf tx path rather than here?

    // Ensure we're returning a public key if requested
    if flags.contains(Flags::PUB_KEY_REQUEST) {
        resp.common.public_key = Some(our_pub_key);
    }

    // Update peer info
    let _ = core.peer_update(&from, Box::new(|p| p.sent += 1)).await;

    trace!("returning response (to: {:?})\n {:?}", from, &resp);

    Ok(resp)
}

#[derive(Clone)]
pub struct KeyCache {
    keys: Arc<Mutex<HashMap<Id, Keys>>>,
}

impl KeyCache {
    /// Create a new key cache
    pub fn new() -> Self {
        Self {
            keys: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Create or update key entry
    pub fn create_update(&self, id: &Id, keys: &Keys) {
        let mut key_cache = self.keys.lock().unwrap();

        key_cache
            .entry(id.clone())
            .and_modify(|k| {
                if k.pub_key.is_none() && keys.pub_key.is_some() {
                    k.pub_key = keys.pub_key.clone();
                }
                if k.sec_key.is_none() && keys.sec_key.is_some() {
                    k.sec_key = keys.sec_key.clone();
                }
                if k.sym_keys.is_none() && keys.sym_keys.is_some() {
                    k.sym_keys = keys.sym_keys.clone();
                }
            })
            .or_insert_with(|| keys.clone());
    }
}

/// KeySource implementation for KeyCache
impl KeySource for KeyCache {
    fn keys(&self, id: &Id) -> Option<Keys> {
        self.keys.lock().unwrap().get(id).map(|k| k.clone())
    }

    fn update<F: FnMut(&mut Keys)>(&self, id: &Id, mut f: F) -> bool {
        let mut key_cache = self.keys.lock().unwrap();
        let mut e = key_cache.entry(id.clone()).or_default();

        f(&mut e);

        true
    }
}
