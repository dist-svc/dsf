use std::{collections::HashMap, iter::FromIterator, time::SystemTime};

use dsf_rpc::{PeerInfo, ServiceInfo, ReplicaInfo, SubscriptionInfo, PageBounds, DataInfo, TimeBounds, ServiceState, ServiceIdentifier};
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
    ServiceGet(ServiceIdentifier),
    ServiceCreate(Service, Vec<Container>),
    ServiceRegister(Id, Vec<Container>),
    ServiceList(PageBounds),
    ServiceUpdate(Id, UpdateFn),

    PeerGet(ServiceIdentifier),
    PeerList(PageBounds),
    PeerCreate(Id, Vec<Container>),
    PeerUpdate(Id, PeerUpdateFn),

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

pub type PeerUpdateFn =
    Box<dyn Fn(&mut PeerInfo) -> PeerInfo + Send + 'static>;

#[derive(PartialEq, Debug)]
pub enum CoreRes {
    Service(ServiceInfo),
    Services(Vec<ServiceInfo>),

    Peer(PeerInfo),
    Peers(Vec<PeerInfo>),
    
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

    /// Async handler for [Core] operations
    async fn handle_op(&mut self, op: CoreOp) -> CoreRes {
        match op {
            CoreOp::ServiceGet(ident) => self.service_get(&ident).await
                .map(CoreRes::Service)
                .unwrap_or(CoreRes::NotFound),

            CoreOp::ServiceList(bounds) => self.service_list(&bounds).await
                .map(CoreRes::Service)
                .unwrap_or(CoreRes::NotFound),

            CoreOp::ServiceRegister(id, pages) => self.service_register(id, pages).await
                .map(CoreRes::Service)
                .unwrap_or(CoreRes::NotFound),

            CoreOp::ServiceCreate(service, pages) => self.service_create(service, pages).await
                .map(CoreRes::Service)
                .unwrap_or(CoreRes::NotFound),

            CoreOp::GetData { service_id, page_bounds } => self.fetch_data(&service_id, &page_bounds, &TimeBounds::default()).await.map(CoreRes::Data)
            .unwrap_or(CoreRes::NotFound),

            CoreOp::StoreData { service_id, pages } => todo!(),

            CoreOp::GetObject(service_id, obj) => self
                .get_object(&service_id, obj).await
                .map(|o| o.map(CoreRes::Object).unwrap_or(CoreRes::NotFound))
                .unwrap_or(CoreRes::NotFound),

            CoreOp::StoreObject(_, _) => todo!(),

            // TODO: implement all core ops...
            _ => unimplemented!(),
        }
    }

}

impl AsyncCore {
    /// Create a new async [Core] task
    pub async fn new(mut core: Core) -> Self {
        // Setup control channels
        let (tx, mut rx) = unbounded_channel();
        let s = Self{ tasks: tx };

        // Spawn core event handler task
        tokio::task::spawn(async move {
            // Wait for core operations
            while let Some((op, done)) = rx.blocking_recv() {
                // Handle core operations
                let tx = core.handle_op(op).await;

                // Forward result back to waiting async tasks
                let _ = done.send(tx);
            }
        });

        // Return clone/shareable core handle
        s
    }

    pub async fn service_get<T: Into<ServiceIdentifier>>(&self, id: T) -> Result<ServiceInfo, Error> {
        let (tx, rx) = oneshot::channel();

        // Enqueue put operation
        if let Err(e) = self.tasks.send((CoreOp::ServiceGet(id.into()), tx)) {
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

    /// Async dispatch to [Core::service_update]
    pub async fn service_update(&self, id: Id, f: UpdateFn) -> Result<ServiceInfo, Error> {
        let (tx, rx) = oneshot::channel();

        // Enqueue put operation
        if let Err(e) = self.tasks.send((CoreOp::ServiceUpdate(id, f), tx)) {
            error!("Failed to enqueue service update operation: {e:?}");
            return Err(Error::Closed)
        }

        // Await operation completion
        match rx.await {
            Ok(CoreRes::Services(info)) => Ok(info),
            Ok(CoreRes::Error(e)) => Err(e),
            Err(_) => Err(Error::Unknown),
            _ => unreachable!()
        }
    }

    /// Async dispatch to [Core::service_list]
    pub async fn service_list(&self, bounds: PageBounds) -> Result<Vec<ServiceInfo>, Error>  {
        let (tx, rx) = oneshot::channel();

        // Enqueue put operation
        if let Err(e) = self.tasks.send((CoreOp::ServiceList(bounds), tx)) {
            error!("Failed to enqueue service list operation: {e:?}");
            return Err(Error::Closed)
        }

        // Await operation completion
        match rx.await {
            Ok(CoreRes::Services(info)) => Ok(info),
            Ok(CoreRes::Error(e)) => Err(e),
            Err(_) => Err(Error::Unknown),
            _ => unreachable!()
        }
    }

    /// Async dispatch to [Core::peer_get]
    pub async fn peer_get<T: Into<ServiceIdentifier>>(&self, ident: T) -> Result<PeerInfo, Error> {
        let (tx, rx) = oneshot::channel();

        // Enqueue put operation
        if let Err(e) = self.tasks.send((CoreOp::PeerGet(ident.into()), tx)) {
            error!("Failed to enqueue service get operation: {e:?}");
            return Err(Error::Closed)
        }

        // Await operation completion
        match rx.await {
            Ok(CoreRes::Peer(info)) => Ok(info),
            Ok(CoreRes::Error(e)) => Err(e),
            Err(_) => Err(Error::Unknown),
            _ => unreachable!()
        }
    }

     /// Async dispatch to [Core::peer_list]
     pub async fn peer_list(&self, bounds: PageBounds) -> Result<Vec<PeerInfo>, Error>  {
        let (tx, rx) = oneshot::channel();

        // Enqueue put operation
        if let Err(e) = self.tasks.send((CoreOp::PeerList(bounds), tx)) {
            error!("Failed to enqueue service list operation: {e:?}");
            return Err(Error::Closed)
        }

        // Await operation completion
        match rx.await {
            Ok(CoreRes::Services(info)) => Ok(info),
            Ok(CoreRes::Error(e)) => Err(e),
            Err(_) => Err(Error::Unknown),
            _ => unreachable!()
        }
    }

    /// Update or create a peer with associated information
    pub async fn peer_create_or_update(
        &mut self,
        info: PeerInfo,
    ) -> Result<PeerInfo, Error> {
        todo!()
    }

    /// Update or create a peer with associated information
    pub async fn peer_update(
        &mut self,
        peer_id: &Id,
        f: PeerUpdateFn,
    ) -> Result<PeerInfo, Error> {
        todo!()
    }

    /// Find all replicas for the provided service
    pub async fn replica_list(&mut self, id: Id) -> Result<Vec<ReplicaInfo>, Error> {
        todo!()
    }

    /// Create or update a replica for the provided service
    pub async fn replica_create_or_update(
        &mut self,
        svc_id: Id,
        peer_id: Id,
        page: Container,
    ) -> Result<ReplicaInfo, Error> {
        todo!()
    }

    /// List all subscribers for the provided service
    pub async fn subscriber_list(&mut self, id: Id) -> Result<Vec<SubscriptionInfo>, Error> {
        todo!()
    }

    /// Create or update a subscriber instance
    pub async fn subscriber_create_or_update(
        &mut self,
        sub_info: SubscriptionInfo,
    ) -> Result<ReplicaInfo, Error> {
        todo!()
    } 

    /// Remove a subscriber instance
    pub async fn subscriber_remove(
        &mut self,
        service_id: Id,
        peer_id: Id,
    ) -> Result<ReplicaInfo, Error> {
        todo!()
    } 


    pub async fn object_get<I: Into<ObjectIdentifier>>(
        &self,
        service_id: &Id,
        ident: I,
    ) -> Result<Option<Container>, Error> {
        let (tx, rx) = oneshot::channel();

        // Enqueue put operation
        if let Err(e) = self.tasks.send((CoreOp::GetObject(service_id.clone(), ident.into()), tx)) {
            error!("Failed to enqueue service list operation: {e:?}");
            return Err(Error::Closed)
        }

        // Await operation completion
        match rx.await {
            Ok(CoreRes::Object(info)) => Ok(Some(info)),
            Ok(CoreRes::NotFound) => Ok(None),
            Ok(CoreRes::Error(e)) => Err(e),
            Err(_) => Err(Error::Unknown),
            _ => unreachable!()
        }
    }

    pub async fn data_store(&self, service_id: &Id, pages: Vec<Container>) -> Result<(), Error> {
        let (tx, rx) = oneshot::channel();

        // Enqueue put operation
        if let Err(e) = self.tasks.send((CoreOp::StoreData(service_id.clone(), pages), tx)) {
            error!("Failed to enqueue store data operation: {e:?}");
            return Err(Error::Closed)
        }

        // Await operation completion
        match rx.await {
            Ok(CoreRes::Object(info)) => Ok(Some(info)),
            Ok(CoreRes::NotFound) => Ok(None),
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
