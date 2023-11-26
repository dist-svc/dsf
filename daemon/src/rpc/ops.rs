//! Operations used in the construction of higher-level RPCs

use std::collections::HashMap;

use dsf_core::service::{PrimaryOptions, SecondaryOptions, TertiaryOptions};
use dsf_core::wire::Container;
use futures::channel::mpsc;
use futures::Future;

use dsf_core::{
    net::{Request as NetRequest, Response as NetResponse},
    prelude::{DsfError, Id, Keys, NetRequestBody, PageInfo, Service},
    types::{Address, CryptoHash, PublicKey, Signature},
};
use dsf_rpc::*;

use super::discover::{DiscoverOp, DiscoverState};
use crate::core::replicas::ReplicaInst;
use crate::core::{CoreRes, SearchInfo, ServiceUpdateFn};
use crate::error::Error;

pub type RpcSender = mpsc::Sender<Response>;

/// Basic engine operations, used to construct higher-level async functions
pub enum OpKind {
    /// Connect to the DHT via unknown peer
    DhtConnect(Address, Option<Id>),
    /// Search for pages at an address in the DHT
    DhtSearch(Id),
    /// Locate nearby peers for a DHT address
    DhtLocate(Id),
    /// Store pages at an address in the DHT
    DhtPut(Id, Vec<Container>),
    /// Update the DHT
    DhtUpdate,

    /// Resolve a service identifier to a service instance
    ServiceGet(ServiceIdentifier),
    ServiceCreate(Service, Container),
    ServiceRegister(Id, Vec<Container>),
    ServiceUpdate(Id, ServiceUpdateFn),
    ServiceList(ServiceListOptions),

    Publish(Id, PageInfo),

    /// Fetch subscribers for a known service
    SubscribersGet(Id),

    /// Create or update peer information
    PeerCreateUpdate(Id, PeerAddress, Option<PublicKey>, PeerFlags),
    /// Fetch peer information by peer ID
    PeerGet(Id),
    /// List known peers
    PeerList,

    /// Update available replicas for a service
    ReplicaUpdate(Id, Vec<Container>),
    /// Fetch replica information by peer ID
    ReplicaGet(Id),

    /// Fetch an object by service ID and signature
    ObjectGet(Id, Signature),
    /// Store an object
    ObjectPut(Container),

    /// Issue a network request to the listed peers
    Net(NetRequestBody, Vec<PeerInfo>),

    /// Issue a broadcast network request
    NetBcast(NetRequestBody),

    /// Fetch or generate a new primary page for the peer service
    Primary,
}

impl core::fmt::Debug for OpKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::DhtConnect(addr, id) => {
                f.debug_tuple("DhtConnect").field(addr).field(id).finish()
            }
            Self::DhtLocate(id) => f.debug_tuple("DhtLocate").field(id).finish(),
            Self::DhtSearch(id) => f.debug_tuple("DhtSearch").field(id).finish(),
            Self::DhtPut(id, pages) => f.debug_tuple("DhtPut").field(id).field(pages).finish(),
            Self::DhtUpdate => f.debug_tuple("DhtUpdate").finish(),

            Self::ServiceGet(id) => f.debug_tuple("ServiceGet").field(id).finish(),
            Self::ServiceList(opts) => f.debug_tuple("ServiceList").field(opts).finish(),

            Self::ServiceCreate(s, _p) => f.debug_tuple("ServiceCreate").field(&s.id()).finish(),
            Self::ServiceRegister(id, _pages) => {
                f.debug_tuple("ServiceRegister").field(id).finish()
            }
            Self::ServiceUpdate(id, _f) => f.debug_tuple("ServiceUpdate").field(id).finish(),

            Self::Publish(id, info) => f.debug_tuple("Publish").field(id).field(info).finish(),

            Self::PeerCreateUpdate(id, addr, pub_key, flags) => f
                .debug_tuple("PeerCreateUpdate")
                .field(id)
                .field(addr)
                .field(pub_key)
                .field(flags)
                .finish(),

            Self::PeerGet(id) => f.debug_tuple("PeerGet").field(id).finish(),
            Self::PeerList => f.debug_tuple("PeerList").finish(),
            Self::SubscribersGet(id) => f.debug_tuple("SubscribersGet").field(id).finish(),

            Self::ReplicaGet(id) => f.debug_tuple("ReplicaGet").field(id).finish(),

            Self::ReplicaUpdate(id, info) => f
                .debug_tuple("ReplicaUpdate")
                .field(id)
                .field(info)
                .finish(),

            Self::ObjectGet(id, sig) => f.debug_tuple("ObjectGet").field(id).field(sig).finish(),
            Self::ObjectPut(o) => f.debug_tuple("ObjectPut").field(o).finish(),

            Self::Net(req, peers) => f.debug_tuple("Net").field(req).field(peers).finish(),

            Self::NetBcast(req) => f.debug_tuple("NetBcast").field(req).finish(),

            Self::Primary => f.debug_tuple("Primary").finish(),
        }
    }
}

/// Core engine implementation providing primitive operations for the construction of RPCs
#[allow(async_fn_in_trait)]
pub trait Engine: Sync + Send {
    //type Output: Future<Output=Result<Res, DsfError>> + Send;

    /// Fetch own / peer ID
    fn id(&self) -> Id;

    /// Base execute function, non-blocking, returns a future result
    async fn exec(&self, op: OpKind) -> CoreRes;

    /// Connect to a peer to establish DHT connection
    async fn dht_connect(
        &self,
        addr: Address,
        id: Option<Id>,
    ) -> Result<(Vec<PeerInfo>, SearchInfo), DsfError> {
        match self.exec(OpKind::DhtConnect(addr, id)).await {
            CoreRes::Peers(p, i) => Ok((p, i.unwrap())),
            CoreRes::Error(e) => Err(e),
            _ => Err(DsfError::Unknown),
        }
    }

    /// Lookup a peer using the DHT
    async fn dht_locate(&self, id: Id) -> Result<(PeerInfo, SearchInfo), DsfError> {
        match self.exec(OpKind::DhtLocate(id)).await {
            CoreRes::Peers(p, i) if p.len() > 0 => Ok((p[0].clone(), i.unwrap())),
            CoreRes::Peers(_, _) => Err(DsfError::NotFound),
            CoreRes::Error(e) => Err(e),
            _ => Err(DsfError::Unknown),
        }
    }

    /// Search for pages in the DHT
    async fn dht_search(&self, id: Id) -> Result<(Vec<Container>, SearchInfo), DsfError> {
        match self.exec(OpKind::DhtSearch(id)).await {
            CoreRes::Pages(p, i) => Ok((p, i.unwrap())),
            CoreRes::Error(e) => Err(e),
            _ => Err(DsfError::Unknown),
        }
    }

    /// Store pages in the DHT
    async fn dht_put(
        &self,
        id: Id,
        pages: Vec<Container>,
    ) -> Result<(Vec<PeerInfo>, SearchInfo), DsfError> {
        match self.exec(OpKind::DhtPut(id, pages)).await {
            CoreRes::Peers(p, i) => Ok((p, i.unwrap())),
            CoreRes::Error(e) => Err(e),
            _ => Err(DsfError::Unknown),
        }
    }

    /// Force refresh of the DHT
    async fn dht_update(&self) -> Result<(), DsfError> {
        match self.exec(OpKind::DhtUpdate).await {
            CoreRes::Ok => Ok(()),
            CoreRes::Error(e) => Err(e),
            _ => Err(DsfError::Unknown),
        }
    }

    /// Create or update a peer
    async fn peer_create_update(
        &self,
        id: Id,
        addr: PeerAddress,
        pub_key: Option<PublicKey>,
        flags: PeerFlags,
    ) -> Result<PeerInfo, DsfError> {
        match self
            .exec(OpKind::PeerCreateUpdate(id, addr, pub_key, flags))
            .await
        {
            CoreRes::Peers(p, _) if p.len() == 1 => Ok(p[0].clone()),
            CoreRes::Error(e) => Err(e),
            _ => Err(DsfError::Unknown),
        }
    }

    /// Fetch peer information
    async fn peer_get(&self, id: Id) -> Result<PeerInfo, DsfError> {
        match self.exec(OpKind::PeerGet(id)).await {
            CoreRes::Peers(p, _) if p.len() == 1 => Ok(p[0].clone()),
            CoreRes::Error(e) => Err(e),
            _ => Err(DsfError::Unknown),
        }
    }

    /// List known peers
    async fn peer_list(&self) -> Result<Vec<PeerInfo>, DsfError> {
        match self.exec(OpKind::PeerList).await {
            CoreRes::Peers(p, _) => Ok(p),
            CoreRes::Error(e) => Err(e),
            _ => Err(DsfError::Unknown),
        }
    }

    /// Fetch a service object by ID
    async fn svc_get(&self, ident: ServiceIdentifier) -> Result<ServiceInfo, DsfError> {
        match self.exec(OpKind::ServiceGet(ident)).await {
            CoreRes::Service(s) => Ok(s),
            CoreRes::Error(e) => Err(e),
            _ => Err(DsfError::Unknown),
        }
    }

    /// Create a new service
    async fn svc_create(
        &self,
        service: Service,
        primary_page: Container,
    ) -> Result<ServiceInfo, DsfError> {
        match self
            .exec(OpKind::ServiceCreate(service, primary_page))
            .await
        {
            CoreRes::Service(s) => Ok(s),
            CoreRes::Error(e) => Err(e),
            _ => Err(DsfError::Unknown),
        }
    }

    /// List services using the specified filters
    async fn svc_list(&self, opts: ServiceListOptions) -> Result<Vec<ServiceInfo>, DsfError> {
        match self.exec(OpKind::ServiceList(opts)).await {
            CoreRes::Services(s) => Ok(s),
            CoreRes::Error(e) => Err(e),
            _ => Err(DsfError::Unknown),
        }
    }

    /// Register a newly discovered service from the provided pages
    async fn svc_register(&self, id: Id, pages: Vec<Container>) -> Result<ServiceInfo, DsfError> {
        match self.exec(OpKind::ServiceRegister(id, pages)).await {
            CoreRes::Service(s) => Ok(s),
            CoreRes::Error(e) => Err(e),
            _ => Err(DsfError::Unknown),
        }
    }

    /// Execute an update function on a mutable service instance
    async fn svc_update(&self, service: Id, f: ServiceUpdateFn) -> Result<CoreRes, DsfError> {
        let r = self.exec(OpKind::ServiceUpdate(service, f)).await;
        if let CoreRes::Error(e) = r {
            Err(e)
        } else {
            Ok(r)
        }
    }

    /// Fetch an object with the provided signature
    async fn object_get(&self, service: Id, sig: Signature) -> Result<Container, DsfError> {
        match self.exec(OpKind::ObjectGet(service, sig)).await {
            CoreRes::Pages(p, _) if p.len() == 1 => Ok(p[0].clone()),
            CoreRes::Error(e) => Err(e),
            _ => Err(DsfError::NotFound),
        }
    }

    /// Store an object for the associated service
    async fn object_put(&self, data: Container) -> Result<Signature, DsfError> {
        match self.exec(OpKind::ObjectPut(data)).await {
            CoreRes::Sig(s) => Ok(s),
            CoreRes::Error(e) => Err(e),
            _ => Err(DsfError::NotFound),
        }
    }

    /// Update replicas for a located service
    async fn replica_update(
        &self,
        id: Id,
        replicas: Vec<Container>,
    ) -> Result<Vec<ReplicaInfo>, DsfError> {
        match self.exec(OpKind::ReplicaUpdate(id, replicas)).await {
            CoreRes::Replicas(r) => Ok(r),
            CoreRes::Error(e) => Err(e),
            _ => Err(DsfError::NotFound),
        }
    }

    /// Fetch subscribers for a known service
    async fn subscribers_get(&self, id: Id) -> Result<Vec<Id>, DsfError> {
        match self.exec(OpKind::SubscribersGet(id)).await {
            CoreRes::Ids(v) => Ok(v),
            CoreRes::Error(e) => Err(e),
            _ => Err(DsfError::NotFound),
        }
    }

    /// Issue a network request to the specified peers
    async fn net_req(
        &self,
        req: NetRequestBody,
        peers: Vec<PeerInfo>,
    ) -> Result<HashMap<Id, NetResponse>, DsfError> {
        match self.exec(OpKind::Net(req, peers)).await {
            CoreRes::Responses(r) => Ok(r),
            CoreRes::Error(e) => Err(e),
            _ => Err(DsfError::Unknown),
        }
    }

    // Issue a broadcast network request
    async fn net_bcast(&self, req: NetRequestBody) -> Result<HashMap<Id, NetResponse>, DsfError> {
        match self.exec(OpKind::NetBcast(req)).await {
            CoreRes::Responses(r) => Ok(r),
            CoreRes::Error(e) => Err(e),
            _ => Err(DsfError::Unknown),
        }
    }
}
