//! Operations used in the construction of higher-level RPCs

use std::collections::HashMap;

use dsf_core::wire::Container;
use futures::channel::mpsc;
use futures::Future;

use dsf_core::net::{Request as NetRequest, Response as NetResponse};
use dsf_core::prelude::{DsfError as CoreError, Id, NetRequestBody, Service};
use dsf_core::types::{CryptoHash, Signature};

use dsf_rpc::*;

use crate::core::peers::Peer;
use crate::core::replicas::ReplicaInst;
use crate::error::Error;

use super::connect::{ConnectOp, ConnectState};
use super::lookup::{LookupOp, LookupState};

use super::create::{CreateOp, CreateState};
use super::register::{RegisterOp, RegisterState};

use super::discover::{DiscoverOp, DiscoverState};
use super::publish::{PublishOp, PublishState};
use super::push::{PushOp, PushState};

use super::bootstrap::{BootstrapOp, BootstrapState};

pub type RpcSender = mpsc::Sender<Response>;

/// RPC operation container object
/// Used to track RPC operation kind / state / response etc.
pub struct RpcOperation {
    pub rpc_id: u64,
    pub kind: RpcKind,
    pub done: RpcSender,
}

#[derive(strum_macros::Display)]
pub enum RpcKind {
    Connect(ConnectOp),
    Lookup(LookupOp),

    Create(CreateOp),
    Register(RegisterOp),
    Publish(PublishOp),

    Discover(DiscoverOp),

    Push(PushOp),

    Bootstrap(BootstrapOp),
}

impl RpcKind {
    pub fn connect(opts: ConnectOptions) -> Self {
        RpcKind::Connect(ConnectOp {
            opts,
            state: ConnectState::Init,
        })
    }

    pub fn lookup(opts: peer::SearchOptions) -> Self {
        RpcKind::Lookup(LookupOp {
            opts,
            state: LookupState::Init,
        })
    }

    pub fn create(opts: CreateOptions) -> Self {
        RpcKind::Create(CreateOp {
            id: None,
            opts,
            state: CreateState::Init,
        })
    }

    pub fn register(opts: RegisterOptions) -> Self {
        RpcKind::Register(RegisterOp {
            opts,
            state: RegisterState::Init,
        })
    }

    pub fn publish(opts: PublishOptions) -> Self {
        RpcKind::Publish(PublishOp {
            opts,
            state: PublishState::Init,
        })
    }

    pub fn discover(opts: DiscoverOptions) -> Self {
        RpcKind::Discover(DiscoverOp {
            opts,
            state: DiscoverState::Init,
        })
    }

    pub fn push(opts: PushOptions) -> Self {
        RpcKind::Push(PushOp {
            opts,
            state: PushState::Init,
        })
    }

    pub fn bootstrap(opts: ()) -> Self {
        RpcKind::Bootstrap(BootstrapOp {
            opts,
            state: BootstrapState::Init,
        })
    }
}

/// Basic engine operation, used to construct higher-level functions
pub enum OpKind {
    DhtSearch(Id),
    DhtLocate(Id),
    DhtPut(Id, Vec<Container>),

    ServiceResolve(ServiceIdentifier),
    ServiceGet(Id),
    ServiceCreate(Service, Container),
    ServiceRegister(Id, Vec<Container>),
    ServiceUpdate(Id, UpdateFn),

    PeerGet(Id),

    ReplicaUpdate(Id, Vec<ReplicaInst>),
    ReplicaGet(Id),

    ObjectGet(Id, Signature),
    ObjectPut(Container),

    Net(NetRequestBody, Vec<Peer>),
}

impl core::fmt::Debug for OpKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::DhtLocate(id) => f.debug_tuple("DhtLocate").field(id).finish(),
            Self::DhtSearch(id) => f.debug_tuple("DhtSearch").field(id).finish(),
            Self::DhtPut(id, pages) => f.debug_tuple("DhtPut").field(id).field(pages).finish(),

            Self::ServiceResolve(arg0) => f.debug_tuple("ServiceResolve").field(arg0).finish(),
            Self::ServiceGet(id) => f.debug_tuple("ServiceGet").field(id).finish(),
            Self::ServiceCreate(s, _p) => f.debug_tuple("ServiceCreate").field(&s.id()).finish(),
            Self::ServiceRegister(id, _pages) => {
                f.debug_tuple("ServiceRegister").field(id).finish()
            }
            Self::ServiceUpdate(id, _f) => f.debug_tuple("ServiceUpdate").field(id).finish(),

            Self::PeerGet(id) => f.debug_tuple("PeerGet").field(id).finish(),

            Self::ReplicaGet(id) => f.debug_tuple("ReplicaGet").field(id).finish(),

            Self::ReplicaUpdate(id, info) => f
                .debug_tuple("ReplicaUpdate")
                .field(id)
                .field(info)
                .finish(),

            Self::ObjectGet(id, sig) => f.debug_tuple("ObjectGet").field(id).field(sig).finish(),
            Self::ObjectPut(o) => f.debug_tuple("ObjectPut").field(o).finish(),
            Self::Net(req, peers) => f.debug_tuple("Net").field(req).field(peers).finish(),
        }
    }
}

pub type UpdateFn =
    Box<dyn Fn(&mut Service, &mut ServiceState) -> Result<Res, CoreError> + Send + 'static>;

/// Basic engine response, used to construct higher-level functions
#[derive(Clone, PartialEq, Debug)]
pub enum Res {
    Id(Id),
    Service(Service),
    ServiceInfo(ServiceInfo),
    Pages(Vec<Container>),
    Peers(Vec<Peer>),
    Ids(Vec<Id>),
    Responses(HashMap<Id, NetResponse>),
    Sig(Signature),
    Replicas(Vec<ReplicaInst>),
}

/// Core engine implementation providing primitive operations for the construction of RPCs
#[async_trait::async_trait]
pub trait Engine: Sync + Send {
    //type Output: Future<Output=Result<Res, CoreError>> + Send;

    /// Base execute function, non-blocking, returns a future result
    async fn exec(&self, op: OpKind) -> Result<Res, CoreError>;

    /// Lookup a peer using the DHT
    async fn dht_locate(&self, id: Id) -> Result<Peer, CoreError> {
        match self.exec(OpKind::DhtLocate(id)).await? {
            Res::Peers(p) if p.len() > 0 => Ok(p[0].clone()),
            Res::Peers(_) => Err(CoreError::NotFound),
            _ => Err(CoreError::Unknown),
        }
    }

    /// Search for pages in the DHT
    async fn dht_search(&self, id: Id) -> Result<Vec<Container>, CoreError> {
        match self.exec(OpKind::DhtSearch(id)).await? {
            Res::Pages(p) => Ok(p),
            _ => Err(CoreError::Unknown),
        }
    }

    /// Store pages in the DHT
    async fn dht_put(&self, id: Id, pages: Vec<Container>) -> Result<Vec<Id>, CoreError> {
        match self.exec(OpKind::DhtPut(id, pages)).await? {
            Res::Ids(p) => Ok(p),
            _ => Err(CoreError::Unknown),
        }
    }

    /// Fetch peer information
    async fn peer_get(&self, id: Id) -> Result<Peer, CoreError> {
        match self.exec(OpKind::PeerGet(id)).await? {
            Res::Peers(p) if p.len() == 1 => Ok(p[0].clone()),
            _ => Err(CoreError::Unknown),
        }
    }

    /// Resolve a service index to ID
    async fn service_resolve(&self, identifier: ServiceIdentifier) -> Result<Service, CoreError> {
        match self.exec(OpKind::ServiceResolve(identifier)).await? {
            Res::Service(s) => Ok(s),
            _ => Err(CoreError::Unknown),
        }
    }

    /// Fetch a service object by ID
    async fn service_get(&self, service: Id) -> Result<ServiceInfo, CoreError> {
        match self.exec(OpKind::ServiceGet(service)).await? {
            Res::ServiceInfo(s) => Ok(s),
            _ => Err(CoreError::Unknown),
        }
    }

    /// Create a new service
    async fn service_create(
        &self,
        service: Service,
        primary_page: Container,
    ) -> Result<ServiceInfo, CoreError> {
        match self
            .exec(OpKind::ServiceCreate(service, primary_page))
            .await?
        {
            Res::ServiceInfo(s) => Ok(s),
            _ => Err(CoreError::Unknown),
        }
    }

    /// Register a newly discovered service from the provided pages
    async fn service_register(
        &self,
        id: Id,
        pages: Vec<Container>,
    ) -> Result<ServiceInfo, CoreError> {
        match self.exec(OpKind::ServiceRegister(id, pages)).await? {
            Res::ServiceInfo(s) => Ok(s),
            _ => Err(CoreError::Unknown),
        }
    }

    /// Execute an update function on a mutable service instance
    async fn service_update(&self, service: Id, f: UpdateFn) -> Result<Res, CoreError> {
        self.exec(OpKind::ServiceUpdate(service, f)).await
    }

    /// Fetch an object with the provided signature
    async fn object_get(&self, service: Id, sig: Signature) -> Result<Container, CoreError> {
        match self.exec(OpKind::ObjectGet(service, sig)).await? {
            Res::Pages(p) if p.len() == 1 => Ok(p[0].clone()),
            _ => Err(CoreError::NotFound),
        }
    }

    /// Store an object for the associated service
    async fn object_put(&self, data: Container) -> Result<Signature, CoreError> {
        match self.exec(OpKind::ObjectPut(data)).await? {
            Res::Sig(s) => Ok(s),
            _ => Err(CoreError::NotFound),
        }
    }

    /// Store an object for the associated service
    async fn replica_update(&self, id: Id, replicas: Vec<ReplicaInst>) -> Result<(), CoreError> {
        match self.exec(OpKind::ReplicaUpdate(id, replicas)).await? {
            Res::Id(_) => Ok(()),
            _ => Err(CoreError::NotFound),
        }
    }

    /// Issue a network request to the specified peers
    async fn net_req(
        &self,
        req: NetRequestBody,
        peers: Vec<Peer>,
    ) -> Result<HashMap<Id, NetResponse>, CoreError> {
        match self.exec(OpKind::Net(req, peers)).await? {
            Res::Responses(r) => Ok(r),
            _ => Err(CoreError::Unknown),
        }
    }
}
