use crate::core::store::AsyncStore;
use crate::core::{AsyncCore, Core};
use crate::daemon::net::KeyCache;
use crate::daemon::ops::Op;
use crate::sync::{Arc, Mutex};
use std::collections::HashMap;
use std::convert::TryFrom;
use std::time::{Duration, SystemTime};

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll, Waker};

use dsf_core::wire::Container;
use dsf_rpc::{PeerFlags, PeerInfo, PeerState, ServiceInfo};
use log::{debug, error, info, trace, warn};
use tracing::{span, Level};

use futures::channel::mpsc;
use futures::prelude::*;

use tokio::time::timeout;

use dsf_core::prelude::*;
use dsf_core::service::Publisher;

use kad::prelude::*;
use kad::table::NodeTable;

use crate::error::Error;
use crate::store::Store;

use super::dht::{dht_reducer, AsyncDht, DsfDhtMessage};
use super::net::{ByteSink, NetIf, NetSink};
use super::net2::AsyncNet;
use super::DsfOptions;

/// Re-export of Dht type used for DSF
pub type DsfDht = Dht<Id, PeerInfo, Data>;

pub struct Dsf<Net = NetSink> {
    /// DSF Configuration
    pub(crate) config: DsfOptions,

    /// Internal storage for daemon service
    pub(crate) service: Service,

    /// Last primary page for peer service
    last_primary: Option<Container>,

    /// Core management of services, peers, etc.
    pub(super) core: AsyncCore,

    /// Local (database) storage
    pub(crate) store: AsyncStore,

    /// Network request router
    pub(crate) net: AsyncNet,

    net_out_rx: mpsc::UnboundedReceiver<(Vec<(Address, Option<Id>)>, NetMessage)>,

    /// Distributed Database
    pub(crate) dht: AsyncDht,

    /// Source for outgoing DHT requests
    dht_source: kad::dht::RequestReceiver<Id, PeerInfo, Container>,

    /// RPC request channel
    pub(crate) op_rx: mpsc::UnboundedReceiver<Op>,
    pub(crate) op_tx: mpsc::UnboundedSender<Op>,

    /// Sink for sending messages via the network
    pub(crate) net_sink: Net,

    /// Addresses for peer advertisement
    pub(crate) addresses: Vec<Address>,

    //pub(crate) net_source: Arc<Mutex<mpsc::Receiver<(Address, NetMessage)>>>,
    pub(crate) waker: Option<Waker>,

    pub(super) key_cache: KeyCache,
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
        let dht = AsyncDht::new(dht);

        let (op_tx, op_rx) = mpsc::unbounded();

        let (net_out_tx, net_out_rx) = mpsc::unbounded();
        let net = AsyncNet::new(net_out_tx);

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
            net,
            net_out_rx,

            addresses: Vec::new(),

            waker: None,

            // TODO: pre-fill key cache with known services and peers?
            key_cache: KeyCache::new(),
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

    pub(crate) fn pub_key(&self) -> PublicKey {
        self.service.public_key()
    }

    pub(crate) fn wake(&self) {
        if let Some(w) = &self.waker {
            w.clone().wake();
        }
    }

    pub(crate) fn primary(&mut self, refresh: bool) -> Result<Container, DsfError> {
        // Check whether we have a cached primary page
        match self.last_primary.as_ref() {
            Some(c) if !c.expired() && !refresh => return Ok(c.clone()),
            _ => (),
        }

        // Otherwise, generate a new one
        // TODO: this should generate a peer page / contain peer contact info
        let (_, c) = self.service.publish_primary_buff(Default::default())?;

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
}

impl<Net> Dsf<Net>
where
    Dsf<Net>: NetIf<Interface = Net>,
{
    fn poll_base(&mut self, ctx: &mut Context<'_>) -> Poll<Result<(), DsfError>> {
        // Poll for outgoing DHT messages
        // TODO(med): move this to daemon/dht.rs
        if let Poll::Ready(Some((peers, body, mut tx))) = self.dht_source.poll_next_unpin(ctx) {
            let req_id = rand::random();

            // Convert from DHT to network request
            let body = super::dht::dht_to_net_request(body);
            let mut req = NetRequest::new(self.id(), req_id, body, Flags::PUB_KEY_REQUEST);
            req.common.public_key = Some(self.pub_key());

            let targets: Vec<_> = peers
                .iter()
                .map(|p| (p.info().address().clone(), Some(p.info().id.clone())))
                .collect();

            debug!(
                "Issuing DHT {} request ({}) to {:?}",
                req.data, req_id, targets,
            );

            let net = self.net.clone();

            // Spawn task to handle net response
            tokio::task::spawn(async move {
                // Await net response collection
                let resps = match net.net_request(targets, req, Default::default()).await {
                    Ok(r) => r,
                    Err(e) => {
                        error!("Network request error: {e:?}");
                        return;
                    }
                };

                // TODO: Convert these to DHT messages
                let mut converted = HashMap::new();
                for (id, (_addr, resp)) in resps.iter() {
                    if let Some(r) = super::dht::net_to_dht_response(&resp.data).await {
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

        // Poll on internal network operations
        if let Poll::Ready(Some((targets, msg))) = self.net_out_rx.poll_next_unpin(ctx) {
            if let Err(e) = self.net_send(&targets, msg) {
                error!("Failed to send message: {e:?}");
            }

            // Force re-wake
            ctx.waker().clone().wake();
        }

        // Poll on internal base operations
        let _ = self.poll_exec(ctx);

        // TODO: poll on internal state / messages

        // Manage waking
        // TODO: propagate this, in a better manner

        // Always wake (terrible for CPU use but helps response times)
        ctx.waker().clone().wake();

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
