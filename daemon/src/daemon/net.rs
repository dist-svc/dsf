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

        // Generic net message processing, fetch and update peer information
        let peer = match crate::net::handle_base(self.exec(), &from, &addr.into(), &message.common().clone())
            .await
        {
            Some(p) => p,
            None => return Ok(()),
        };

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

    /// Execute a network operation for the given request
    ///
    /// This sends the provided request to the listed peers with retries and timeouts.
    pub async fn net_op(&mut self, mut peers: Vec<PeerInfo>, req: net::Request) -> HashMap<Id, (Address, net::Response)> {
        let targets: Vec<_> = peers.drain(..).map(|p| (p.address().clone(), Some(p.id)) ).collect();
        self.net.net_request(targets, req, Default::default()).await.unwrap()
    }

    pub async fn net_broadcast(&mut self, req: net::Request) -> HashMap<Id, (Address, net::Response)> {
        self.net.net_broadcast(req, Default::default()).await.unwrap()
    }

    /// Handle a received request message and generate a response
    pub async fn handle_net_req(
        &mut self,
        peer: PeerInfo,
        addr: SocketAddr,
        req: net::Request,
    ) -> Result<net::Response, DaemonError> {
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

        // Handle specific DHT messages
        let resp = if let Some(dht_req) = Self::net_to_dht_request(&req.data) {
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
