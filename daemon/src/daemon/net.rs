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

use tracing::{span, Level, instrument};

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
                if let Some(k) = self.key_cache.get(id) {
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
                let pub_key = match (peer_info.as_ref().map(|i| i.state()), peer_keys.map(|k| k.pub_key.clone())) {
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
                    sec_key: peer_keys.map(|k| k.sec_key.clone() ).flatten(),
                    ..Default::default()
                };

                // Enable symmetric mode if supported
                let sym = peer_info.map(|p| p.flags.contains(PeerFlags::SYMMETRIC_ENABLED)).unwrap_or(false);
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
                    NetMessage::Request(req) => dsf_core::net::encode_request(&signing_id, req, &enc_keys, buff),
                    NetMessage::Response(resp) => dsf_core::net::encode_response(&signing_id, resp, &enc_keys, buff),
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
        // Decode message
        let (container, _n) = Container::from(&msg.data);
        let header = container.header();
        let id: Id = container.id().into();

        let mut data = msg.data.to_vec();
        let addr = Address::from(msg.address.clone());

        debug!("RX: {:?}", container);

        // TODO(high): verify object prior to performing any operations

        // DELEGATION: Handle unwrapped objects for constrained / delegated services
        if !header.kind().is_message() {
            // Handle raw object
            let resp = match crate::net::handle_net_raw(self.exec(), self.core.clone(), &id, container.to_owned()).await {
               Ok(v) => v,
               Err(e) => {
                    error!("handle_net_raw error: {e:?}");
                    return Ok(())
               }
            };

            // TODO: Send response
        }

        // Convert container to message object
        // TODO: pass secret keys for encode / decode here
        let (message, _n) = match net::Message::parse(&mut data, self) {
            Ok(v) => v,
            Err(e) => {
                error!("Error decoding base message: {:?}", e);
                return Ok(());
            }
        };
        
        debug!("Handling message: {message:?}");

        // TODO: cache keys

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
                            warn!("Enabling symmetric message crypto for peer: {}", p.id);
                            p.flags |= PeerFlags::SYMMETRIC_ENABLED;
                        }
                    }),
                )
                .await;
        }

        debug!("Handle base");

        // Generic net message processing, fetch and update peer information
        let peer = match crate::net::handle_base(&self.id(), self.core.clone(), &from, &addr.into(), &message.common().clone())
            .await
        {
            Some(p) => p,
            None => return Ok(()),
        };

        debug!("Peer: {peer:?}");

        // Route requests and responses to appropriate handlers
        match message {
            NetMessage::Response(resp) => {
                // Forward responses via AsyncNet router
                self.net.handle_resp(addr, from, resp).await?
            }
            NetMessage::Request(req) => {
                // Handle requests
                // TODO(HIGH): this must be in an async task to avoid blocking all other ops
                let resp = self.handle_net_req(peer, msg.address, req).await?;

                // Send response
                self.net.net_send(vec![(addr, Some(from))], resp.into()).await?;
            }
        };

        Ok(())
    }

    /// Handle a received request message and generate a response
    #[instrument(skip_all, fields(req_id = req.common.id))]
    pub async fn handle_net_req(
        &mut self,
        peer: PeerInfo,
        addr: SocketAddr,
        req: net::Request,
    ) -> Result<net::Response, DaemonError> {
        let own_id = self.id();

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

        // Handle specific DHT messages
        let mut resp = if let Some(dht_req) = Self::net_to_dht_request(&req.data) {
            let dht_resp = self.handle_dht_req(from.clone(), peer, dht_req)?;
            let net_resp = Self::dht_to_net_response(dht_resp);

            net::Response::new(
                own_id,
                req_id,
                net_resp,
                Flags::default(),
            )

        // Handle DSF requests
        } else {
            let dsf_resp = crate::net::handle_dsf_req(self.exec(), self.core.clone(), peer, req.data.clone(), req.flags.clone()).await?;

            net::Response::new(
                own_id,
                req_id,
                dsf_resp,
                Flags::default(),
            )
        };

        debug!("Response: {resp:?}");

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

        Ok(resp)
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
