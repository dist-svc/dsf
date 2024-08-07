//! Core provides the core functionality of DSF,
//! managing services and peers as well as interacting with the database.
//!
//! Core is a singleton wrapped in [AsyncCore] for shared use, while
//! methods are re-exported via the [Engine] interface for use in higher-level logic.

use std::{collections::HashMap, iter::FromIterator, time::SystemTime};

use tokio::sync::{
    mpsc::{unbounded_channel, UnboundedSender},
    oneshot::{self, Sender as OneshotSender},
};
use tracing::{debug, error, info, trace, warn};

use dsf_core::prelude::*;
use dsf_rpc::{
    AuthInfo, AuthRole, DataInfo, PageBounds, PeerInfo, PeerState, ReplicaInfo, ServiceIdentifier,
    ServiceInfo, ServiceState, SubscriptionInfo, SubscriptionKind, TimeBounds,
};

use crate::{
    core::{
        peers::PeerInst,
        replicas::ReplicaInst,
        services::{build_service_info, ServiceInst},
        store::{AsyncStore, DataStore},
    },
    error::Error,
    store::object::ObjectIdentifier,
};

pub mod data;

pub mod peers;

pub mod replicas;

pub mod services;

pub mod subscribers;

pub mod store;

/// Core logic and storage
pub struct Core {
    /// Service managed or known by the daemon
    services: HashMap<Id, ServiceInst>,

    /// Peers known by the daemon
    peers: HashMap<Id, PeerInst>,

    /// Replicas of known services, collected by service ID
    replicas: HashMap<Id, Vec<ReplicaInst>>,

    /// Subscribers to known services, collected by service ID
    subscribers: HashMap<Id, Vec<SubscriptionInfo>>,

    /// Backing store for persistence
    store: AsyncStore,
}

/// Async [Core] task wrapper
///
/// This allows handles to be cloned and shared between async tasks
#[derive(Clone)]
pub struct AsyncCore {
    tasks: UnboundedSender<(CoreOp, OneshotSender<CoreRes>)>,
}

pub enum CoreOp {
    /// Fetch a service by identifier (Id, ShortId, or index)
    ServiceGet(ServiceIdentifier),
    /// Create a new local service
    ServiceCreate(Service, Vec<Container>),
    /// Register a service, associating replicas etc.
    ServiceRegister(Id, Vec<Container>),
    /// List available services
    ServiceList(PageBounds),
    /// Update the specified service using the provided function
    ServiceUpdate(Id, ServiceUpdateFn),

    /// Fetch a peer by identifier (Id, ShortId, or index)
    PeerGet(ServiceIdentifier),
    PeerList(PageBounds),
    PeerCreate(PeerInfo),
    PeerUpdate(Id, PeerUpdateFn),

    /// Fetch replicas for a given service ID
    ReplicaList(Id),
    /// Create or update replicas for a given service
    ReplicaCreateUpdate(Id, Vec<Container>),

    /// Fetch subscribers for a given service ID
    SubscriberList(Id),
    /// Update a subscription for a given service
    SubscriberCreateUpdate(SubscriptionInfo),
    /// Remove a subscription for a given service
    SubscriberRemove(Id, SubscriptionKind),

    ListData(ServiceIdentifier, PageBounds),
    GetData(ServiceIdentifier, ObjectIdentifier),
    StoreData(Id, Vec<Container>),

    GetObject(ServiceIdentifier, ObjectIdentifier),
    StoreObject(Id, Container),

    /// Fetch key information for a service or peer, used in network encode / decode etc.
    GetKeys(Id),

    /// List authorisations for a service
    ListAuth(ServiceIdentifier),

    /// Update (or remove) a service authorisation
    UpdateAuth(ServiceIdentifier, Id, AuthRole),

    /// Exit core task
    Exit,
}

impl core::fmt::Debug for CoreOp {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            CoreOp::ServiceGet(id) => f.debug_tuple("ServiceGet").field(id).finish(),
            CoreOp::ServiceCreate(svc, _pages) => {
                f.debug_tuple("ServiceCreate").field(svc).finish()
            }
            CoreOp::ServiceRegister(id, _pages) => {
                f.debug_tuple("ServiceRegister").field(id).finish()
            }
            CoreOp::ServiceList(bounds) => f.debug_tuple("ServiceList").field(bounds).finish(),
            CoreOp::ServiceUpdate(id, _) => f.debug_tuple("ServiceUpdate").field(id).finish(),

            CoreOp::PeerGet(ident) => f.debug_tuple("PeerGet").field(ident).finish(),
            CoreOp::PeerList(bounds) => f.debug_tuple("PeerList").field(bounds).finish(),
            CoreOp::PeerCreate(info) => f.debug_tuple("PeerCreate").field(info).finish(),
            CoreOp::PeerUpdate(id, _) => f.debug_tuple("PeerUpdate").field(id).finish(),

            CoreOp::ReplicaList(id) => f.debug_tuple("ReplicaList").field(id).finish(),
            CoreOp::ReplicaCreateUpdate(id, _pages) => {
                f.debug_tuple("ReplicaCreateUpdate").field(id).finish()
            }

            CoreOp::SubscriberList(id) => f.debug_tuple("SubscriberList").field(id).finish(),
            CoreOp::SubscriberCreateUpdate(info) => {
                f.debug_tuple("SubscriberCreateUpdate").field(info).finish()
            }
            CoreOp::SubscriberRemove(id, sub_kind) => f
                .debug_tuple("SubscriberRemove")
                .field(id)
                .field(sub_kind)
                .finish(),

            CoreOp::ListData(id, bounds) => {
                f.debug_tuple("ListData").field(id).field(bounds).finish()
            }
            CoreOp::GetData(id, ident) => f.debug_tuple("GetData").field(id).field(ident).finish(),
            CoreOp::StoreData(id, _objects) => f.debug_tuple("StoreData").field(id).finish(),

            CoreOp::GetObject(id, object) => {
                f.debug_tuple("GetObject").field(id).field(object).finish()
            }
            CoreOp::StoreObject(id, object) => f
                .debug_tuple("StoreObject")
                .field(id)
                .field(object)
                .finish(),
            CoreOp::GetKeys(id) => f.debug_tuple("GetKeys").field(id).finish(),
            CoreOp::UpdateAuth(s_id, p_id, role) => f
                .debug_tuple("UpdateAuth")
                .field(s_id)
                .field(p_id)
                .field(role)
                .finish(),
            CoreOp::ListAuth(s_id) => f.debug_tuple("ListAuth").field(s_id).finish(),

            CoreOp::Exit => f.debug_tuple("Exit").finish(),
        }
    }
}

pub type ServiceUpdateFn = Box<dyn Fn(&mut Service, &mut ServiceInfo) -> CoreRes + Send + 'static>;

pub type PeerUpdateFn = Box<dyn Fn(&mut PeerInfo) + Send + 'static>;

pub type SearchInfo = kad::dht::SearchInfo<Id>;

#[derive(PartialEq, Debug)]
pub enum CoreRes {
    Ok,

    Id(Id),
    Ids(Vec<Id>),

    Service(ServiceInfo),
    Services(Vec<ServiceInfo>),

    Peer(PeerInfo),
    Peers(Vec<PeerInfo>, Option<SearchInfo>),

    Replicas(Vec<ReplicaInfo>),
    Subscribers(Vec<SubscriptionInfo>),

    Pages(Vec<Container>, Option<SearchInfo>),
    Objects(Vec<(DataInfo, Container)>),

    Keys(Keys),
    Sig(Signature),
    Auths(AuthInfo),

    /// Network request responses by peer ID
    Responses(HashMap<Id, NetResponse>),

    NotFound,
    Error(DsfError),
}

impl CoreRes {
    pub fn pages(pages: Vec<Container>, info: Option<SearchInfo>) -> Self {
        Self::Pages(pages, info)
    }

    pub fn peers(peers: Vec<PeerInfo>, info: Option<SearchInfo>) -> Self {
        Self::Peers(peers, info)
    }
}

impl Core {
    /// Create a new core instance to manage services / peers / etc.
    pub async fn new(store: AsyncStore) -> Result<Self, Error> {
        // Load persistent information from the provided store

        // Load services
        let mut services = Self::load_services(&store).await?;
        let services = HashMap::from_iter(services.drain(..).map(|s| (s.id(), s)));

        // Load peers
        let mut peers = store.peer_load().await?;
        let peers = HashMap::from_iter(
            peers
                .drain(..)
                .map(|info| (info.id.clone(), PeerInst { info, dirty: false })),
        );

        // TODO(low): persist subscribers and replicas? though we can recover these from elsewhere

        Ok(Self {
            services,
            peers,
            subscribers: HashMap::new(),
            replicas: HashMap::new(),
            store,
        })
    }

    /// Async handler for [Core] operations
    async fn handle_op(&mut self, op: CoreOp) -> CoreRes {
        trace!("op: {op:?}");

        let res = match op {
            CoreOp::ServiceGet(ident) => self
                .service_get(&ident)
                .await
                .map(CoreRes::Service)
                .unwrap_or(CoreRes::NotFound),

            CoreOp::ServiceList(bounds) => self
                .service_list(bounds)
                .await
                .map(CoreRes::Services)
                .unwrap_or(CoreRes::NotFound),

            CoreOp::ServiceRegister(id, pages) => self
                .service_register(id, pages)
                .await
                .map(CoreRes::Service)
                .unwrap_or(CoreRes::NotFound),

            CoreOp::ServiceCreate(service, pages) => self
                .service_create(service, pages)
                .await
                .map(CoreRes::Service)
                .unwrap_or(CoreRes::NotFound),
            CoreOp::ServiceUpdate(service_id, f) => self.service_update(&service_id, f).await,

            CoreOp::ListData(service_id, page_bounds) => self
                .list_data(&service_id, &page_bounds, &TimeBounds::default())
                .await
                .map(CoreRes::Objects)
                .unwrap_or(CoreRes::NotFound),

            CoreOp::GetData(service_id, object_ident) => self
                .get_object(&service_id, object_ident)
                .await
                .map(|v| CoreRes::Objects(vec![v]))
                .unwrap_or(CoreRes::NotFound),

            CoreOp::StoreData(service_id, pages) => self
                .store_data(&service_id, pages)
                .await
                .map(|_| CoreRes::Ok)
                .unwrap_or(CoreRes::NotFound),

            CoreOp::GetObject(service_id, obj) => self
                .get_object(&service_id, obj)
                .await
                .map(|o| CoreRes::Objects(vec![o]))
                .unwrap_or(CoreRes::NotFound),

            CoreOp::StoreObject(service_id, obj) => self
                .store_data(&service_id, vec![obj])
                .await
                .map(|_o| CoreRes::Ok)
                .unwrap_or(CoreRes::NotFound),

            CoreOp::GetKeys(id) => self
                .get_keys(&id)
                .map(CoreRes::Keys)
                .unwrap_or(CoreRes::NotFound),

            CoreOp::PeerGet(ident) => self
                .peer_get(ident)
                .map(CoreRes::Peer)
                .unwrap_or(CoreRes::NotFound),
            CoreOp::PeerList(_bounds) => CoreRes::Peers(
                self.list_peers().drain(..).map(|(_id, p)| p).collect(),
                None,
            ),
            CoreOp::PeerCreate(info) => self
                .peer_create_or_update(info)
                .await
                .map(CoreRes::Peer)
                .unwrap_or(CoreRes::NotFound),
            CoreOp::PeerUpdate(peer_id, f) => self
                .peer_update(&peer_id, f)
                .await
                .map(|p| p.map(CoreRes::Peer).unwrap_or(CoreRes::NotFound))
                .unwrap_or(CoreRes::NotFound),

            CoreOp::ReplicaCreateUpdate(service_id, replicas) => self
                .create_or_update_replicas(&service_id, replicas)
                .map(CoreRes::Replicas)
                .unwrap_or(CoreRes::NotFound),
            CoreOp::ReplicaList(service_id) => self
                .find_replicas(&service_id)
                .map(CoreRes::Replicas)
                .unwrap_or(CoreRes::NotFound),

            CoreOp::SubscriberList(service_id) => self
                .find_subscribers(&service_id)
                .map(CoreRes::Subscribers)
                .unwrap_or(CoreRes::NotFound),
            CoreOp::SubscriberCreateUpdate(info) => self
                .subscriber_create_or_update(info)
                .map(|s| CoreRes::Subscribers(vec![s]))
                .unwrap_or(CoreRes::NotFound),
            CoreOp::SubscriberRemove(service_id, sub_kind) => self
                .subscriber_remove(&service_id, &sub_kind)
                .map(|_s| CoreRes::Ok)
                .unwrap_or(CoreRes::NotFound),

            CoreOp::UpdateAuth(service_id, peer_id, role) => self
                .service_auth_update(&service_id, &peer_id, role)
                .await
                .map(|a| CoreRes::Auths(a))
                .unwrap_or(CoreRes::NotFound),
            CoreOp::ListAuth(service_id) => self
                .service_auth_list(&service_id)
                .await
                .map(|a| CoreRes::Auths(a))
                .unwrap_or(CoreRes::NotFound),

            CoreOp::Exit => unimplemented!(),
        };

        trace!("res: {res:?}");

        res
    }

    fn get_keys(&self, id: &Id) -> Option<Keys> {
        if let Some(p) = self.peers.get(id) {
            if let PeerState::Known(k) = &p.info.state {
                return Some(Keys {
                    pub_key: Some(k.clone()),
                    ..Default::default()
                });
            }
        }

        if let Some(s) = self.services.get(id) {
            return Some(s.info.keys());
        }

        None
    }
}

impl AsyncCore {
    /// Create a new async [Core] task
    pub async fn new(mut core: Core) -> Self {
        // Setup control channels
        let (tx, mut rx) = unbounded_channel();
        let s = Self { tasks: tx };

        // Spawn core event handler task
        tokio::task::spawn(async move {
            // TODO: periodic sync task (update peers)

            // Wait for core operations
            while let Some((op, done)) = rx.recv().await {
                if let CoreOp::Exit = op {
                    debug!("Exiting AsyncCore");
                    break;
                }

                // Handle core operations
                let tx = core.handle_op(op).await;

                // Forward result back to waiting async tasks
                let _ = done.send(tx);
            }
        });

        // Return clone/shareable core handle
        s
    }

    pub async fn service_get<T: Into<ServiceIdentifier>>(
        &self,
        id: T,
    ) -> Result<ServiceInfo, DsfError> {
        let (tx, rx) = oneshot::channel();

        // Enqueue put operation
        if let Err(e) = self.tasks.send((CoreOp::ServiceGet(id.into()), tx)) {
            error!("Failed to enqueue service get operation: {e:?}");
            return Err(DsfError::IO);
        }

        // Await operation completion
        match rx.await {
            Ok(CoreRes::Service(info)) => Ok(info),
            Ok(CoreRes::Error(e)) => Err(e),
            _ => Err(DsfError::Unknown),
        }
    }

    /// Async dispatch to [Core::service_create]
    pub async fn service_create(
        &self,
        service: Service,
        pages: Vec<Container>,
    ) -> Result<ServiceInfo, DsfError> {
        let (tx, rx) = oneshot::channel();

        // Enqueue put operation
        if let Err(e) = self.tasks.send((CoreOp::ServiceCreate(service, pages), tx)) {
            error!("Failed to enqueue service create operation: {e:?}");
            return Err(DsfError::IO);
        }

        // Await operation completion
        match rx.await {
            Ok(CoreRes::Service(info)) => Ok(info),
            Ok(CoreRes::Error(e)) => Err(e),
            Err(_) => Err(DsfError::Unknown),
            _ => Err(DsfError::Unknown),
        }
    }

    /// Async dispatch to [Core::service_register]
    pub async fn service_register(
        &self,
        id: Id,
        pages: Vec<Container>,
    ) -> Result<ServiceInfo, DsfError> {
        let (tx, rx) = oneshot::channel();

        // Enqueue put operation
        if let Err(e) = self.tasks.send((CoreOp::ServiceRegister(id, pages), tx)) {
            error!("Failed to enqueue service register operation: {e:?}");
            return Err(DsfError::IO);
        }

        // Await operation completion
        match rx.await {
            Ok(CoreRes::Service(info)) => Ok(info),
            Ok(CoreRes::Error(e)) => Err(e),
            Ok(CoreRes::NotFound) => Err(DsfError::NotFound),
            _ => Err(DsfError::Unknown),
        }
    }

    /// Async dispatch to [Core::service_update]
    pub async fn service_update(
        &self,
        id: Id,
        f: ServiceUpdateFn,
    ) -> Result<ServiceInfo, DsfError> {
        let (tx, rx) = oneshot::channel();

        // Enqueue put operation
        if let Err(e) = self.tasks.send((CoreOp::ServiceUpdate(id, f), tx)) {
            error!("Failed to enqueue service update operation: {e:?}");
            return Err(DsfError::IO);
        }

        // Await operation completion
        match rx.await {
            Ok(CoreRes::Service(info)) => Ok(info),
            Ok(CoreRes::Error(e)) => Err(e),
            Ok(CoreRes::NotFound) => Err(DsfError::NotFound),
            _ => Err(DsfError::Unknown),
        }
    }

    /// Async dispatch to [Core::service_list]
    pub async fn service_list(&self, bounds: PageBounds) -> Result<Vec<ServiceInfo>, DsfError> {
        let (tx, rx) = oneshot::channel();

        // Enqueue put operation
        if let Err(e) = self.tasks.send((CoreOp::ServiceList(bounds), tx)) {
            error!("Failed to enqueue service list operation: {e:?}");
            return Err(DsfError::IO);
        }

        // Await operation completion
        match rx.await {
            Ok(CoreRes::Services(info)) => Ok(info),
            Ok(CoreRes::Error(e)) => Err(e),
            Ok(CoreRes::NotFound) => Err(DsfError::NotFound),
            _ => Err(DsfError::Unknown),
        }
    }

    /// Async dispatch to [Core::peer_get]
    pub async fn peer_get<T: Into<ServiceIdentifier>>(
        &self,
        ident: T,
    ) -> Result<PeerInfo, DsfError> {
        let (tx, rx) = oneshot::channel();

        // Enqueue put operation
        if let Err(e) = self.tasks.send((CoreOp::PeerGet(ident.into()), tx)) {
            error!("Failed to enqueue service get operation: {e:?}");
            return Err(DsfError::IO);
        }

        // Await operation completion
        match rx.await {
            Ok(CoreRes::Peer(info)) => Ok(info),
            Ok(CoreRes::Error(e)) => Err(e),
            Ok(CoreRes::NotFound) => Err(DsfError::NotFound),
            _ => Err(DsfError::Unknown),
        }
    }

    /// Async dispatch to [Core::peer_list]
    pub async fn peer_list(&self, bounds: PageBounds) -> Result<Vec<PeerInfo>, DsfError> {
        let (tx, rx) = oneshot::channel();

        // Enqueue put operation
        if let Err(e) = self.tasks.send((CoreOp::PeerList(bounds), tx)) {
            error!("Failed to enqueue peer list operation: {e:?}");
            return Err(DsfError::IO);
        }

        // Await operation completion
        match rx.await {
            Ok(CoreRes::Peers(info, _)) => Ok(info),
            Ok(CoreRes::Error(e)) => Err(e),
            Ok(CoreRes::NotFound) => Err(DsfError::NotFound),
            _ => Err(DsfError::Unknown),
        }
    }

    /// Update or create a peer with associated information
    pub async fn peer_create_or_update(&mut self, info: PeerInfo) -> Result<PeerInfo, DsfError> {
        let (tx, rx) = oneshot::channel();

        // Enqueue put operation
        if let Err(e) = self.tasks.send((CoreOp::PeerCreate(info), tx)) {
            error!("Failed to enqueue peer create_or_update operation: {e:?}");
            return Err(DsfError::IO);
        }

        // Await operation completion
        match rx.await {
            Ok(CoreRes::Peer(info)) => Ok(info),
            Ok(CoreRes::Error(e)) => Err(e),
            Ok(CoreRes::NotFound) => Err(DsfError::NotFound),
            _ => Err(DsfError::Unknown),
        }
    }

    /// Update an existing peer using the provided function
    pub async fn peer_update(
        &mut self,
        peer_id: &Id,
        f: PeerUpdateFn,
    ) -> Result<PeerInfo, DsfError> {
        let (tx, rx) = oneshot::channel();

        // Enqueue put operation
        if let Err(e) = self
            .tasks
            .send((CoreOp::PeerUpdate(peer_id.clone(), f), tx))
        {
            error!("Failed to enqueue peer update operation: {e:?}");
            return Err(DsfError::IO);
        }

        // Await operation completion
        let r = rx.await;

        match r {
            Ok(CoreRes::Peer(info)) => Ok(info),
            Ok(CoreRes::Peers(p, _)) if p.len() == 1 => Ok(p[0].clone()),
            Ok(CoreRes::Error(e)) => Err(e),
            _ => Err(DsfError::Unknown),
        }
    }

    /// Find all replicas for the provided service
    pub async fn replica_list(&mut self, service_id: Id) -> Result<Vec<ReplicaInfo>, DsfError> {
        let (tx, rx) = oneshot::channel();

        // Enqueue put operation
        if let Err(e) = self.tasks.send((CoreOp::ReplicaList(service_id), tx)) {
            error!("Failed to enqueue replica list operation: {e:?}");
            return Err(DsfError::IO);
        }

        // Await operation completion
        match rx.await {
            Ok(CoreRes::Replicas(info)) => Ok(info),
            Ok(CoreRes::Error(e)) => Err(e),
            _ => Err(DsfError::Unknown),
        }
    }

    /// Create or update a replica for the provided service
    pub async fn replica_create_or_update(
        &mut self,
        service_id: Id,
        pages: Vec<Container>,
    ) -> Result<Vec<ReplicaInfo>, DsfError> {
        let (tx, rx) = oneshot::channel();

        // Enqueue put operation
        if let Err(e) = self
            .tasks
            .send((CoreOp::ReplicaCreateUpdate(service_id, pages), tx))
        {
            error!("Failed to enqueue replica create update operation: {e:?}");
            return Err(DsfError::IO);
        }

        // Await operation completion
        match rx.await {
            Ok(CoreRes::Replicas(info)) => Ok(info),
            Ok(CoreRes::Error(e)) => Err(e),
            _ => Err(DsfError::Unknown),
        }
    }

    /// List all subscribers for the provided service
    pub async fn subscriber_list(
        &mut self,
        service_id: Id,
    ) -> Result<Vec<SubscriptionInfo>, DsfError> {
        let (tx, rx) = oneshot::channel();

        // Enqueue put operation
        if let Err(e) = self.tasks.send((CoreOp::SubscriberList(service_id), tx)) {
            error!("Failed to enqueue subscriber list operation: {e:?}");
            return Err(DsfError::IO);
        }

        // Await operation completion
        match rx.await {
            Ok(CoreRes::Subscribers(info)) => Ok(info),
            Ok(CoreRes::Error(e)) => Err(e),
            _ => Err(DsfError::Unknown),
        }
    }

    /// Create or update a subscriber instance
    pub async fn subscriber_create_or_update(
        &mut self,
        sub_info: SubscriptionInfo,
    ) -> Result<SubscriptionInfo, DsfError> {
        let (tx, rx) = oneshot::channel();

        // Enqueue put operation
        if let Err(e) = self
            .tasks
            .send((CoreOp::SubscriberCreateUpdate(sub_info), tx))
        {
            error!("Failed to enqueue subscriber update operation: {e:?}");
            return Err(DsfError::IO);
        }

        // Await operation completion
        match rx.await {
            Ok(CoreRes::Subscribers(info)) if info.len() == 1 => Ok(info[0].clone()),
            Ok(CoreRes::NotFound) => Err(DsfError::NotFound),
            Ok(CoreRes::Error(e)) => Err(e),
            _ => Err(DsfError::Unknown),
        }
    }

    /// Remove a subscriber instance
    pub async fn subscriber_remove(
        &mut self,
        service_id: Id,
        sub_kind: SubscriptionKind,
    ) -> Result<(), DsfError> {
        let (tx, rx) = oneshot::channel();

        // Enqueue put operation
        if let Err(e) = self
            .tasks
            .send((CoreOp::SubscriberRemove(service_id, sub_kind), tx))
        {
            error!("Failed to enqueue subscriber update operation: {e:?}");
            return Err(DsfError::IO);
        }

        // Await operation completion
        match rx.await {
            Ok(CoreRes::Ok) => Ok(()),
            Ok(CoreRes::NotFound) => Err(DsfError::NotFound),
            Ok(CoreRes::Error(e)) => Err(e),
            _ => Err(DsfError::Unknown),
        }
    }

    /// Fetch a stored object for a given service and object identifier
    pub async fn object_get<I: Into<ObjectIdentifier>>(
        &self,
        service_id: &ServiceIdentifier,
        object_ident: I,
    ) -> Result<(DataInfo, Container), DsfError> {
        let (tx, rx) = oneshot::channel();

        // Enqueue put operation
        if let Err(e) = self.tasks.send((
            CoreOp::GetObject(service_id.clone(), object_ident.into()),
            tx,
        )) {
            error!("Failed to enqueue service list operation: {e:?}");
            return Err(DsfError::IO);
        }

        // Await operation completion
        match rx.await {
            Ok(CoreRes::Objects(info)) if info.len() == 1 => Ok(info[0].clone()),
            Ok(CoreRes::NotFound) | Ok(CoreRes::Objects(_)) => Err(DsfError::NotFound),
            Ok(CoreRes::Error(e)) => Err(e),
            _ => Err(DsfError::Unknown),
        }
    }

    pub async fn data_store(&self, service_id: &Id, pages: Vec<Container>) -> Result<(), DsfError> {
        let (tx, rx) = oneshot::channel();

        // Enqueue put operation
        if let Err(e) = self
            .tasks
            .send((CoreOp::StoreData(service_id.clone(), pages), tx))
        {
            error!("Failed to enqueue store data operation: {e:?}");
            return Err(DsfError::IO);
        }

        // Await operation completion
        match rx.await {
            Ok(CoreRes::Ok) => Ok(()),
            Ok(CoreRes::Error(e)) => Err(e),
            _ => Err(DsfError::Unknown),
        }
    }

    pub async fn data_list(
        &self,
        ident: &ServiceIdentifier,
        bounds: &PageBounds,
    ) -> Result<Vec<(DataInfo, Container)>, DsfError> {
        let (tx, rx) = oneshot::channel();

        // Enqueue put operation
        if let Err(e) = self
            .tasks
            .send((CoreOp::ListData(ident.clone(), bounds.clone()), tx))
        {
            error!("Failed to enqueue list data operation: {e:?}");
            return Err(DsfError::IO);
        }

        // Await operation completion
        match rx.await {
            Ok(CoreRes::Objects(p)) => Ok(p),
            Ok(CoreRes::Error(e)) => Err(e),
            _ => Err(DsfError::Unknown),
        }
    }

    pub async fn keys_get(&self, some_id: &Id) -> Result<Keys, DsfError> {
        let (tx, rx) = oneshot::channel();

        // Enqueue put operation
        if let Err(e) = self.tasks.send((CoreOp::GetKeys(some_id.clone()), tx)) {
            error!("Failed to enqueue key get operation: {e:?}");
            return Err(DsfError::IO);
        }

        // Await operation completion
        match rx.await {
            Ok(CoreRes::Keys(k)) => Ok(k),
            Ok(CoreRes::Error(e)) => Err(e),
            Ok(CoreRes::NotFound) => Err(DsfError::NotFound),
            _ => Err(DsfError::Unknown),
        }
    }

    pub async fn service_auth_list(
        &self,
        service: &ServiceIdentifier,
    ) -> Result<AuthInfo, DsfError> {
        let (tx, rx) = oneshot::channel();

        // Enqueue put operation
        if let Err(e) = self.tasks.send((CoreOp::ListAuth(service.clone()), tx)) {
            error!("Failed to enqueue auth list operation: {e:?}");
            return Err(DsfError::IO);
        }

        // Await operation completion
        match rx.await {
            Ok(CoreRes::Auths(k)) => Ok(k),
            Ok(CoreRes::Error(e)) => Err(e),
            Ok(CoreRes::NotFound) => Err(DsfError::NotFound),
            _ => Err(DsfError::Unknown),
        }
    }

    pub async fn service_auth_update(
        &self,
        service: &ServiceIdentifier,
        peer_id: &Id,
        role: AuthRole,
    ) -> Result<AuthInfo, DsfError> {
        let (tx, rx) = oneshot::channel();

        // Enqueue put operation
        if let Err(e) = self.tasks.send((
            CoreOp::UpdateAuth(service.clone(), peer_id.clone(), role),
            tx,
        )) {
            error!("Failed to enqueue auth update operation: {e:?}");
            return Err(DsfError::IO);
        }

        // Await operation completion
        match rx.await {
            Ok(CoreRes::Auths(k)) => Ok(k),
            Ok(CoreRes::Error(e)) => Err(e),
            Ok(CoreRes::NotFound) => Err(DsfError::NotFound),
            _ => Err(DsfError::Unknown),
        }
    }
}
