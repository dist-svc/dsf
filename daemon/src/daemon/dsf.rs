use crate::core::{AsyncCore, Core};
use crate::core::store::AsyncStore;
use crate::daemon::ops::Op;
use crate::sync::{Arc, Mutex};
use std::collections::HashMap;
use std::convert::TryFrom;
use std::time::{Duration, SystemTime};

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll, Waker};

use dsf_core::wire::Container;
use dsf_rpc::{PeerInfo, ServiceInfo};
use log::{debug, error, info, trace, warn};
use tracing::{span, Level};

use futures::channel::mpsc;
use futures::prelude::*;

use tokio::time::timeout;

use dsf_core::prelude::*;
use dsf_core::service::Publisher;

use kad::prelude::*;
use kad::table::NodeTable;

use crate::core::subscribers::SubscriberManager;

use super::net::{ByteSink, NetIf, NetOp, NetSink};

use crate::error::Error;
use crate::store::Store;

use super::dht::{dht_reducer, DsfDhtMessage};
use super::DsfOptions;

/// Re-export of Dht type used for DSF
pub type DsfDht = Dht<Id, PeerInfo, Data>;

pub struct Dsf<Net = NetSink> {
    /// DSF Configuration
    pub(crate) config: DsfOptions,

    /// Internal storage for daemon service
    service: Service,

    /// Last primary page for peer service
    last_primary: Option<Container>,

    /// Core management of services, peers, etc.
    core: AsyncCore,

    /// Distributed Database
    dht: DsfDht,

    /// Source for outgoing DHT requests
    dht_source: kad::dht::RequestReceiver<Id, PeerInfo, Container>,

    /// Local (database) storage
    pub(crate) store: AsyncStore,

    /// RPC request channel
    pub(crate) op_rx: mpsc::UnboundedReceiver<Op>,
    pub(crate) op_tx: mpsc::UnboundedSender<Op>,

    /// Tracking for network operations (collections of requests with retries etc.)
    pub(crate) net_ops: HashMap<RequestId, NetOp>,

    /// Tracking for individual network requests
    pub(crate) net_requests: HashMap<(Address, RequestId), mpsc::Sender<NetResponse>>,

    /// Sink for sending messages via the network
    pub(crate) net_sink: Net,

    pub(crate) net_resp_tx: mpsc::UnboundedSender<(Address, Option<Id>, NetMessage)>,
    pub(crate) net_resp_rx: mpsc::UnboundedReceiver<(Address, Option<Id>, NetMessage)>,

    /// Addresses for peer advertisement
    pub(crate) addresses: Vec<Address>,

    //pub(crate) net_source: Arc<Mutex<mpsc::Receiver<(Address, NetMessage)>>>,
    pub(crate) waker: Option<Waker>,

    pub(super) key_cache: HashMap<Id, Keys>,
}

impl<Net> Dsf<Net>
where
    Dsf<Net>: NetIf<Interface = Net>,
{
    /// Create a new daemon
    pub async fn new(
        config: DsfOptions,
        service: Service,
        store: AsyncStore,
        net_sink: Net,
    ) -> Result<Self, Error> {
        debug!("Creating new DSF instance");

        let id = service.id();

        // Create core, sets up service / peer / data storage and loads persistent data
        let core = Core::new(store.clone()).await?;
        let core = AsyncCore::new(core).await;

        // Instantiate DHT
        let (dht_sink, dht_source) = mpsc::channel(100);
        let mut dht = Dht::<Id, PeerInfo, Data>::standard(id, config.dht.clone(), dht_sink);
        dht.set_reducer(Box::new(dht_reducer));

        let (op_tx, op_rx) = mpsc::unbounded();

        let (net_resp_tx, net_resp_rx) = mpsc::unbounded();

        // Create DSF object
        let s = Self {
            config,
            service,
            last_primary: None,

            core,

            dht,
            dht_source,

            store,

            op_tx,
            op_rx,

            net_sink,
            net_requests: HashMap::new(),
            net_ops: HashMap::new(),
            net_resp_rx,
            net_resp_tx,
            addresses: Vec::new(),

            waker: None,
            key_cache: HashMap::new(),
        };

        Ok(s)
    }

    /// Fetch the daemon ID
    pub fn id(&self) -> Id {
        self.service.id()
    }

    /// Fetch a reference to the daemon service
    pub fn service(&mut self) -> &mut Service {
        &mut self.service
    }

    pub(crate) fn dht_mut(&mut self) -> &mut DsfDht {
        &mut self.dht
    }

    pub(crate) fn pub_key(&self) -> PublicKey {
        self.service.public_key()
    }

    pub(crate) fn wake(&self) {
        if let Some(w) = &self.waker {
            w.clone().wake();
        }
    }

    pub(crate) fn primary(&mut self, refresh: bool) -> Result<Container, Error> {
        // Check whether we have a cached primary page
        match self.last_primary.as_ref() {
            Some(c) if !c.expired() && !refresh => return Ok(c.clone()),
            _ => (),
        }

        // Otherwise, generate a new one
        // TODO: this should generate a peer page / contain peer contact info
        let (_, c) = self
            .service
            .publish_primary_buff(Default::default())
            .map_err(Error::Core)?;

        self.last_primary = Some(c.to_owned());

        Ok(c.to_owned())
    }

    /// Run an update of the daemon and all managed services
    pub async fn update(&mut self, force: bool) -> Result<(), Error> {
        info!("DSF update (forced: {:?})", force);

        let _interval = Duration::from_secs(10 * 60);

        // TODO: update registered, subscribed, replicated services

        //self.register(rpc::RegisterOptions{service: rpc::ServiceIdentifier{id: Some(inst.id), index: None}, no_replica: false })
        //self.locate(rpc::LocateOptions{id: inst.id}).await;
        //self.subscribe(rpc::SubscribeOptions{service: rpc::ServiceIdentifier{id: Some(inst.id), index: None}}).await;

        Ok(())
    }

    pub fn service_register(
        &mut self,
        id: &Id,
        pages: Vec<Container>,
    ) -> Result<ServiceInfo, Error> {
        debug!("Adding service: {} to store", id);

        debug!("found {} pages", pages.len());
        // Fetch primary page
        let primary_page = match pages.iter().find(|p| {
            let h = p.header();
            h.kind().is_page() && !h.flags().contains(Flags::SECONDARY) && &p.id() == id
        }) {
            Some(p) => p.clone(),
            None => return Err(Error::NotFound),
        };

        // Fetch replica pages
        let replicas: Vec<(Id, &Container)> = pages
            .iter()
            .filter(|p| {
                let h = p.header();
                h.kind().is_page()
                    && h.flags().contains(Flags::SECONDARY)
                    && h.application_id() == 0
                    && h.kind() == PageKind::Replica.into()
            })
            .filter_map(|p| {
                let peer_id = match p.info().map(|i| i.peer_id()) {
                    Ok(Some(v)) => v,
                    _ => return None,
                };

                Some((peer_id.clone(), p))
            })
            .collect();

        debug!("found {} replicas", replicas.len());

        if &primary_page.id() == id {
            debug!("Registering service for matching peer");
        }

        // Fetch service instance
        let info = match self.services.known(id) {
            true => {
                info!("updating existing service");

                // Apply update to known instance
                self.services
                    .update_inst(id, |s| {
                        // Apply primary page update
                        if s.apply_update(&primary_page).unwrap() {
                            s.primary_page = Some(primary_page.clone());
                            s.last_updated = Some(SystemTime::now());
                        }
                    })
                    .unwrap()
            }
            false => {
                info!("creating new service entry");

                // Create instance from page
                let service = match Service::load(&primary_page) {
                    Ok(s) => s,
                    Err(e) => return Err(e.into()),
                };

                // Register in service tracking
                self.services
                    .register(
                        service,
                        &primary_page,
                        ServiceState::Located,
                        Some(SystemTime::now()),
                    )
                    .unwrap()
            }
        };

        // Store primary page
        self.data().store_data(&primary_page)?;

        debug!("Updating replicas");

        // Update listed replicas
        for (peer_id, page) in &replicas {
            // TODO: handle this error condition properly
            if let Err(e) = self.replicas.create_or_update(id, peer_id, page) {
                error!("Failed to store replica information: {:?}", e);
            }
        }

        debug!("Service registered: {:?}", info);

        Ok(info)
    }
}

impl<Net> dsf_core::keys::KeySource for Dsf<Net>
where
    Dsf<Net>: NetIf<Interface = Net>,
{
    fn keys(&self, id: &Id) -> Option<dsf_core::keys::Keys> {
        // Short circuit if looking for our own keys
        if *id == self.id() {
            return Some(self.service.keys());
        }

        // Check key cache for matching keys
        if let Some(keys) = self.key_cache.get(id) {
            return Some(keys.clone());
        }

        // Find public key from source
        let (pub_key, sec_key) = if let Some(s) = self.services.find(id) {
            (Some(s.public_key), s.secret_key)
        } else if let Some(p) = self.peers.find(id) {
            if let PeerState::Known(pk) = p.state() {
                (Some(pk), None)
            } else {
                (None, None)
            }
        } else if let Some(e) = self.dht.nodetable().contains(id) {
            if let PeerState::Known(pk) = e.info().state() {
                (Some(pk), None)
            } else {
                (None, None)
            }
        } else {
            (None, None)
        };

        match pub_key {
            Some(pk) => {
                let mut keys = Keys::new(pk);
                if let Some(sk) = sec_key {
                    keys.sec_key = Some(sk);
                }
                Some(keys)
            }
            None => None,
        }
    }
}

impl<Net> Dsf<Net>
where
    Dsf<Net>: NetIf<Interface = Net>,
{
    fn poll_base(&mut self, ctx: &mut Context<'_>) -> Poll<Result<(), DsfError>> {
        // Poll for outgoing DHT messages
        if let Poll::Ready(Some((peers, body, mut tx))) = self.dht_source.poll_next_unpin(ctx) {
            let req_id = rand::random();

            // Convert from DHT to network request
            let body = Self::dht_to_net_request(body);
            let mut req = NetRequest::new(self.id(), req_id, body, Flags::PUB_KEY_REQUEST);
            req.common.public_key = Some(self.pub_key());

            let peers: Vec<_> = peers.iter().map(|p| p.info().clone()).collect();

            let disp: Vec<_> = peers.iter().map(|p| (p.id, p.address())).collect();

            debug!(
                "Issuing DHT {} request ({}) to {:?}",
                req.data, req_id, disp,
            );

            let exec = self.exec();
            let net = self.net_op(peers, req);

            // Spawn task to handle net response
            tokio::task::spawn(async move {
                // Await net response collection
                let resps = net.await;

                // TODO: Convert these to DHT messages
                let mut converted = HashMap::new();
                for (id, resp) in resps.iter() {
                    if let Some(r) = Self::net_to_dht_response(&exec, &resp.data).await {
                        converted.insert(id.clone(), r);
                    } else {
                        warn!("Unexpected response to DHT request: {:?}", resp);
                    }
                }

                // Forward to DHT
                if let Err(e) = tx.send(Ok(converted)).await {
                    error!("Failed to forward responses to DHT: {:?}", e);
                }
            });

            // Force re-wake
            ctx.waker().clone().wake();
        }

        // Poll on pending outgoing responses to be forwarded
        // (we need a channel or _something_ to make async responses viable, but, this is a bit grim)
        if let Poll::Ready(Some((addr, id, msg))) = self.net_resp_rx.poll_next_unpin(ctx) {
            if let Err(e) = self.net_send(&[(addr, id)], msg) {
                error!("Error sending outgoing response message: {:?}", e);
            }

            // Force re-wake
            ctx.waker().clone().wake();
        }

        // Poll on internal network operations
        self.poll_net_ops(ctx);

        // Poll on internal base operations
        let _ = self.poll_exec(ctx);

        // Poll on internal DHT
        let _ = self.dht_mut().poll_unpin(ctx);

        // TODO: poll on internal state / messages

        // Manage waking
        // TODO: propagate this, in a better manner

        // Always wake (terrible for CPU use but helps response times)
        //ctx.waker().clone().wake();

        // Store waker
        self.waker = Some(ctx.waker().clone());

        // Indicate we're still running
        Poll::Pending
    }
}

impl Future for Dsf<ByteSink> {
    type Output = Result<(), DsfError>;

    fn poll(mut self: Pin<&mut Self>, ctx: &mut Context<'_>) -> Poll<Self::Output> {
        self.poll_base(ctx)
    }
}

impl futures::future::FusedFuture for Dsf<ByteSink> {
    fn is_terminated(&self) -> bool {
        false
    }
}

impl Future for Dsf<NetSink> {
    type Output = Result<(), DsfError>;

    fn poll(mut self: Pin<&mut Self>, ctx: &mut Context<'_>) -> Poll<Self::Output> {
        self.poll_base(ctx)
    }
}

impl futures::future::FusedFuture for Dsf<NetSink> {
    fn is_terminated(&self) -> bool {
        false
    }
}
