use crate::daemon::ops::Op;
use crate::sync::{Arc, Mutex};
use std::collections::HashMap;
use std::convert::TryFrom;
use std::time::{Duration, SystemTime};

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll, Waker};

use dsf_core::wire::Container;
use log::{debug, error, info, trace, warn};
use tracing::{span, Level};

use futures::channel::mpsc;
use futures::prelude::*;

use tokio::time::timeout;

use dsf_core::prelude::*;
use dsf_core::service::Publisher;

use kad::prelude::*;
use kad::table::NodeTable;

use crate::core::data::DataManager;
use crate::core::peers::{Peer, PeerManager, PeerState};
use crate::core::replicas::ReplicaManager;
use crate::core::services::{ServiceInfo, ServiceManager, ServiceState};
use crate::core::subscribers::SubscriberManager;

use super::net::{ByteSink, NetIf, NetOp, NetSink};

use crate::error::Error;
use crate::store::Store;

use super::dht::{dht_reducer, DsfDhtMessage};
use super::DsfOptions;

/// Re-export of Dht type used for DSF
pub type DsfDht = Dht<Id, Peer, Data>;

pub struct Dsf<Net = NetSink> {
    /// Inernal storage for daemon service
    service: Service,

    /// Last primary page for peer service
    last_primary: Option<Container>,

    /// Peer manager
    peers: PeerManager,

    /// Service manager
    services: ServiceManager,

    /// Subscriber manager
    subscribers: SubscriberManager,

    /// Replica manager
    replicas: ReplicaManager,

    /// Data manager
    data: DataManager,

    /// Distributed Database
    dht: Dht<Id, Peer, Data>,

    /// Source for outgoing DHT requests
    dht_source: kad::dht::RequestReceiver<Id, Peer, Container>,

    /// Local (database) storage
    store: Store,

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
    pub fn new(
        config: DsfOptions,
        service: Service,
        store: Store,
        net_sink: Net,
    ) -> Result<Self, Error> {
        debug!("Creating new DSF instance");

        // Create managers
        //let store = Arc::new(Mutex::new(Store::new(&config.database_file)?));
        let peers = PeerManager::new(store.clone());
        let mut services = ServiceManager::new(store.clone());
        let data = DataManager::new(store.clone());

        let replicas = ReplicaManager::new();
        let subscribers = SubscriberManager::new();

        let id = service.id();

        // Load services etc.
        services.load(&id);

        // Instantiate DHT
        let (dht_sink, dht_source) = mpsc::channel(100);
        let dht = Dht::<Id, Peer, Data>::standard(id, config.dht, dht_sink);

        let (op_tx, op_rx) = mpsc::unbounded();

        let (net_resp_tx, net_resp_rx) = mpsc::unbounded();

        // Create DSF object
        let s = Self {
            service,
            last_primary: None,

            peers,
            services,
            subscribers,
            replicas,
            data,

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

    pub(crate) fn peers(&mut self) -> &mut PeerManager {
        &mut self.peers
    }

    pub(crate) fn services(&mut self) -> &mut ServiceManager {
        &mut self.services
    }

    pub(crate) fn replicas(&mut self) -> &mut ReplicaManager {
        &mut self.replicas
    }

    pub(crate) fn subscribers(&mut self) -> &mut SubscriberManager {
        &mut self.subscribers
    }

    pub(crate) fn data(&mut self) -> &mut DataManager {
        &mut self.data
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

    /// Store pages in the database at the provided ID
    #[cfg(tmp)]
    pub fn store(
        &mut self,
        id: &Id,
        pages: Vec<Container>,
    ) -> Result<kad::dht::StoreFuture<Id, Peer>, Error> {
        let span = span!(Level::DEBUG, "store", "{}", self.id());
        let _enter = span.enter();

        // Pre-sign new pages so encoding works
        // TODO: this should not be needed as pages are will be signed on creation
        let mut pages = pages.clone();
        for p in &mut pages {
            // We can only sign our own pages...
            if p.id() != self.id() {
                continue;
            }

            // TODO: ensure page is signed / verifiable
        }

        let (store, _req_id) = match self.dht.store(id.clone().into(), pages) {
            Ok(v) => v,
            Err(e) => {
                error!("Error starting DHT store: {:?}", e);
                return Err(Error::Unknown);
            }
        };

        Ok(store)
    }

    /// Search for pages in the database at the provided ID
    #[cfg(tmp)]
    pub async fn search(&mut self, id: &Id) -> Result<Vec<Container>, Error> {
        let span = span!(Level::DEBUG, "search", "{}", self.id());
        let _enter = span.enter();

        let (search, _req_id) = match self.dht.search(id.clone().into()) {
            Ok(v) => v,
            Err(e) => {
                error!("Error starting DHT search: {:?}", e);
                return Err(Error::Unknown);
            }
        };

        match search.await {
            Ok(d) => {
                let data = dht_reducer(id, &d);

                info!("Search complete ({} entries found)", data.len());
                // TODO: use search results
                if data.len() > 0 {
                    Ok(data)
                } else {
                    Err(Error::NotFound)
                }
            }
            Err(e) => {
                error!("Search failed: {:?}", e);
                Err(Error::NotFound)
            }
        }
    }

    /// Run an update of the daemon and all managed services
    pub async fn update(&mut self, force: bool) -> Result<(), Error> {
        info!("DSF update (forced: {:?})", force);

        let interval = Duration::from_secs(10 * 60);

        // Sync data storage
        self.services.sync();

        // Fetch instance lists for each operation
        let register_ops =
            self.services
                .updates_required(ServiceState::Registered, interval, force);
        for _o in &register_ops {
            //self.register(rpc::RegisterOptions{service: rpc::ServiceIdentifier{id: Some(inst.id), index: None}, no_replica: false })
        }

        let update_ops = self
            .services
            .updates_required(ServiceState::Located, interval, force);
        for _o in &update_ops {
            //self.locate(rpc::LocateOptions{id: inst.id}).await;
        }

        let subscribe_ops =
            self.services
                .updates_required(ServiceState::Subscribed, interval, force);
        for _o in &subscribe_ops {
            //self.locate(rpc::LocateOptions{id: inst.id}).await;
            //self.subscribe(rpc::SubscribeOptions{service: rpc::ServiceIdentifier{id: Some(inst.id), index: None}}).await;
        }

        Ok(())
    }

    /// Initialise a DSF instance
    ///
    /// This bootstraps using known peers then updates all tracked services
    #[cfg(nope)]
    pub async fn bootstrap(&mut self) -> Result<(), Error> {
        let peers = self.peers.list();

        info!("DSF bootstrap ({} peers)", peers.len());

        //let mut handles = vec![];

        // Build peer connect requests
        // TODO: switched to serial due to locking issue somewhere,
        // however, it should be possible to execute this in parallel
        let mut success: usize = 0;

        for (id, p) in peers {
            let timeout = Duration::from_millis(200).into();

            if let Ok(_) = self
                .connect(dsf_rpc::ConnectOptions {
                    address: p.address().into(),
                    id: Some(id.clone()),
                    timeout,
                })?
                .await
            {
                success += 1;
            }
        }

        // We're not really worried about failures here
        //let _ = timeout(Duration::from_secs(3), future::join_all(handles)).await;

        info!("DSF bootstrap connect done (contacted {} peers)", success);

        // Run updates
        self.update(true).await?;

        info!("DSF bootstrap update done");

        Ok(())
    }

    pub fn find_public_key(&self, id: &Id) -> Option<PublicKey> {
        if let Some(s) = self.services.find(id) {
            return Some(s.public_key);
        }

        if let Some(p) = self.peers.find(id) {
            if let PeerState::Known(pk) = p.state() {
                return Some(pk);
            }
        }

        if let Some(e) = self.dht.nodetable().contains(id) {
            if let PeerState::Known(pk) = e.info().state() {
                return Some(pk);
            }
        }

        None
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

            let disp: Vec<_> = peers.iter().map(|p| (p.id(), p.address())).collect();

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
        }

        // Poll on pending outgoing responses to be forwarded
        // (we need a channel or _something_ to make async responses viable, but, this is a bit grim)
        if let Poll::Ready(Some((addr, id, msg))) = self.net_resp_rx.poll_next_unpin(ctx) {
            if let Err(e) = self.net_send(&[(addr, id)], msg) {
                error!("Error sending outgoing response message: {:?}", e);
            }
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
