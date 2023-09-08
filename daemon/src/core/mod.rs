use std::{collections::HashMap, iter::FromIterator, time::SystemTime};

use dsf_rpc::{PeerInfo, ServiceInfo, ReplicaInfo, SubscriptionInfo, PageBounds, DataInfo, TimeBounds, ServiceState};
use dsf_core::prelude::*;
use tokio::sync::{mpsc::{UnboundedSender, unbounded_channel}, oneshot::{Sender as OneshotSender, self}};
use tracing::{error, info, debug};

use crate::{
    core::{
        store::{AsyncStore, DataStore},
        services::{ServiceInst, build_service_info},
        replicas::ReplicaInst,
    },
    error::Error, rpc::UpdateFn, store::object::ObjectIdentifier,
};

pub mod data;

pub mod peers;

pub mod replicas;

pub mod services;

pub mod subscribers;

pub mod store;

// TODO: combine separate core modules / logic as the current ones are needlessly complicated
pub struct Core {
    /// Service managed or known by the daemon
    services: HashMap<Id, ServiceInst>,

    /// Peers known by the daemon
    peers: HashMap<Id, PeerInfo>,

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

#[derive(PartialEq, Debug)]
pub enum CoreOp {
    ServiceGet(Id),
    ServiceCreate(Service, Vec<Container>),
    ServiceRegister(Id, Vec<Container>),

    GetData{
        service_id: Id,
        page_bounds: PageBounds,
    },
    StoreData{
        service_id: Id,
        pages: Vec<Container>,
    },

    GetObject(Id, ObjectIdentifier),
    StoreObject(Id, Container),
}

#[derive(PartialEq, Debug)]
pub enum CoreRes {
    Service(ServiceInfo),
    Peer(PeerInfo),
    Data(Vec<(DataInfo, Container)>),
    Object(Container),
    NotFound,
    Error(Error),
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
        let peers = HashMap::from_iter(peers.drain(..).map(|s| (s.id.clone(), s)));

        // TODO: load subscribers and replicas?!

        Ok(Self{
            services,
            peers,
            subscribers: HashMap::new(),
            replicas: HashMap::new(),
            store,
        })
    }

    pub async fn service_get(&self, id: &Id) -> Option<ServiceInfo> {
        // Service are cached so can be fetched from in-memory storage
        self.services.get(id).map(|s| s.info.clone() )
    }

    pub async fn service_update(&mut self, info: &ServiceInfo) -> Result<ServiceInfo, Error> {
        // Update local replica

        // Update database version

        todo!()
    }

}

impl AsyncCore {
    /// Create a new async [Core] task
    pub async fn new(mut core: Core) -> Self {
        // Setup control channels
        let (tx, mut rx) = unbounded_channel();
        let s = Self{ tasks: tx };

        // Spawn event handler task
        tokio::task::spawn(async move {
            // Wait for core operations
            while let Some((op, done)) = rx.blocking_recv() {
                // Handle core operations
                let tx = Self::handle_op(&mut core, op).await;

                // Forward result back to waiting async tasks
                let _ = done.send(tx);
            }
        });

        s
    }

    /// Async handler for [Core] operations
    async fn handle_op(core: &mut Core, op: CoreOp) -> CoreRes {
        match op {
            CoreOp::ServiceGet(id) => core.service_get(&id).await
                .map(CoreRes::Service)
                .unwrap_or(CoreRes::NotFound),
            CoreOp::ServiceRegister(id, pages) => core.service_register(id, pages).await
                .map(CoreRes::Service)
                .unwrap_or(CoreRes::NotFound),
            CoreOp::ServiceCreate(service, pages) => core.service_create(service, pages).await
                .map(CoreRes::Service)
                .unwrap_or(CoreRes::NotFound),

            CoreOp::GetData { service_id, page_bounds } => core.fetch_data(&service_id, &page_bounds, &TimeBounds::default()).await.map(CoreRes::Data)
            .unwrap_or(CoreRes::NotFound),
            CoreOp::StoreData { service_id, pages } => todo!(),

            CoreOp::GetObject(service_id, obj) => core
                .get_object(&service_id, obj).await
                .map(|o| o.map(CoreRes::Object).unwrap_or(CoreRes::NotFound))
                .unwrap_or(CoreRes::NotFound),
            CoreOp::StoreObject(_, _) => todo!(),
        }
    }

    pub async fn service_get(&self, id: Id) -> Result<ServiceInfo, Error> {
        let (tx, rx) = oneshot::channel();

        // Enqueue put operation
        if let Err(e) = self.tasks.send((CoreOp::ServiceGet(id), tx)) {
            error!("Failed to enqueue service get operation: {e:?}");
            return Err(Error::Closed)
        }

        // Await operation completion
        match rx.await {
            Ok(CoreRes::Service(info)) => Ok(info),
            Ok(CoreRes::Error(e)) => Err(e),
            Err(_) => Err(Error::Unknown),
            _ => unreachable!()
        }
    }

    /// Async dispatch to [Core::service_create]
    pub async fn service_create(&self, service: Service, pages: Vec<Container>) -> Result<ServiceInfo, Error>  {
        let (tx, rx) = oneshot::channel();

        // Enqueue put operation
        if let Err(e) = self.tasks.send((CoreOp::ServiceCreate(service, pages), tx)) {
            error!("Failed to enqueue service create operation: {e:?}");
            return Err(Error::Closed)
        }

        // Await operation completion
        match rx.await {
            Ok(CoreRes::Service(info)) => Ok(info),
            Ok(CoreRes::Error(e)) => Err(e),
            Err(_) => Err(Error::Unknown),
            _ => unreachable!()
        }
    }

    /// Async dispatch to [Core::service_register]
    pub async fn service_register(&self, id: Id, pages: Vec<Container>) -> Result<ServiceInfo, Error>  {
        let (tx, rx) = oneshot::channel();

        // Enqueue put operation
        if let Err(e) = self.tasks.send((CoreOp::ServiceRegister(id, pages), tx)) {
            error!("Failed to enqueue service register operation: {e:?}");
            return Err(Error::Closed)
        }

        // Await operation completion
        match rx.await {
            Ok(CoreRes::Service(info)) => Ok(info),
            Ok(CoreRes::Error(e)) => Err(e),
            Err(_) => Err(Error::Unknown),
            _ => unreachable!()
        }
    }

}

impl KeySource for AsyncCore {
    fn keys(&self, id: &Id) -> Option<Keys> {
        todo!()
    }
}
