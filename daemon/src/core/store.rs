use tokio::sync::{
    mpsc::unbounded_channel, mpsc::UnboundedSender, oneshot, oneshot::Sender as OneshotSender,
};
use tracing::{debug, error};

use dsf_core::prelude::*;
use dsf_rpc::{PageBounds, PeerInfo, ServiceInfo, ServiceListOptions};

use crate::store::{object::ObjectIdentifier, Backend, Store, StoreError};

/// Async datastore wrapper
///
/// This serialises sqlite database operations to avoid the need for connection
/// pooling while using a dedicated thread to avoid blocking executor threads
#[derive(Clone)]
pub struct AsyncStore {
    tasks: UnboundedSender<(StoreOp, OneshotSender<StoreRes>)>,
}

/// Async datastore operations
#[derive(Clone, PartialEq, Debug)]
pub enum StoreOp {
    ServiceGet(Id),
    ServiceUpdate(ServiceInfo),
    ServiceDelete(Id),
    ServiceLoad,

    PeerGet(Id),
    PeerUpdate(PeerInfo),
    PeerDelete(Id),
    PeerLoad,

    ObjectGet(Id, ObjectIdentifier, Keys),
    ObjectPut(Container),
    ObjectList(Id, PageBounds, Keys),
    ObjectDelete(Signature),
}

/// Async datastore results
#[derive(PartialEq, Debug)]
pub enum StoreRes {
    Ok,
    Object(Container),
    Objects(Vec<Container>),
    Service(ServiceInfo),
    Services(Vec<ServiceInfo>),
    Peer(PeerInfo),
    Peers(Vec<PeerInfo>),
    Err(StoreError),
}

impl AsyncStore {
    /// Create a new async [Store] task
    pub fn new<B: Backend + Send + 'static>(mut store: Store<B>) -> Result<Self, StoreError> {
        // Setup control channels
        let (tx, mut rx) = unbounded_channel();
        let s = Self { tasks: tx };

        // Setup blocking store interface task
        // TODO: rework to perform all ops this way? or only writes?
        std::thread::spawn(move || {
            // Wait for store commands
            while let Some((op, done)) = rx.blocking_recv() {
                // Handle store operations
                let tx = match Self::handle_op(&mut store, op) {
                    Ok(r) => r,
                    Err(e) => {
                        error!("Failed to store object: {e:?}");
                        StoreRes::Err(e)
                    }
                };

                // Forward result back to waiting async tasks
                let _ = done.send(tx);
            }
        });

        Ok(s)
    }

    fn handle_op<B: Backend>(store: &mut Store<B>, op: StoreOp) -> Result<StoreRes, StoreError> {
        // Handle store operations
        match op {
            StoreOp::ServiceGet(id) => store.find_service(&id).map(|v| {
                v.map(StoreRes::Service)
                    .unwrap_or(StoreRes::Err(StoreError::NotFound))
            }),
            StoreOp::ServiceUpdate(info) => store.save_service(&info).map(|_| StoreRes::Ok),
            StoreOp::ServiceDelete(id) => store.delete_service(&id).map(|_| StoreRes::Ok),
            StoreOp::ServiceLoad => store.load_services().map(StoreRes::Services),

            StoreOp::PeerGet(id) => store.find_peer(&id).map(|v| {
                v.map(StoreRes::Peer)
                    .unwrap_or(StoreRes::Err(StoreError::NotFound))
            }),
            StoreOp::PeerUpdate(info) => store.save_peer(&info).map(|_| StoreRes::Ok),
            StoreOp::PeerDelete(id) => store.delete_peer(&id).map(|_| StoreRes::Ok),
            StoreOp::PeerLoad => store.load_peers().map(StoreRes::Peers),

            StoreOp::ObjectPut(o) => store.save_object(&o).map(|_| StoreRes::Ok),
            StoreOp::ObjectGet(id, obj, keys) => store.load_object(&id, obj, &keys).map(|v| {
                v.map(StoreRes::Object)
                    .unwrap_or(StoreRes::Err(StoreError::NotFound))
            }),
            StoreOp::ObjectList(id, bounds, keys) => store
                .find_objects(
                    &id,
                    &keys,
                    bounds.offset.unwrap_or(0),
                    bounds.count.unwrap_or(10),
                )
                .map(StoreRes::Objects),
            StoreOp::ObjectDelete(sig) => store
                .delete_object(&sig)
                .map(|_| StoreRes::Ok),
        }
    }
}

#[allow(async_fn_in_trait)]
pub trait DataStore {
    /// Store a peer, updating the object if existing
    async fn peer_update(&self, _info: &PeerInfo) -> Result<(), StoreError>;

    /// Fetch a peer by ID
    async fn peer_get(&self, _id: &Id) -> Result<PeerInfo, StoreError>;

    /// Delete a peer by ID
    async fn peer_del(&self, _id: &Id) -> Result<(), StoreError>;

    /// Load all peers
    async fn peer_load(&self) -> Result<Vec<PeerInfo>, StoreError>;

    /// Store a service, updating the object if existing
    async fn service_update(&self, _info: &ServiceInfo) -> Result<(), StoreError>;

    /// Fetch a service by ID
    async fn service_get(&self, _id: &Id) -> Result<ServiceInfo, StoreError>;

    /// Delete a service by ID
    async fn service_del(&self, _id: &Id) -> Result<(), StoreError>;

    /// Load all services
    async fn service_load(&self) -> Result<Vec<ServiceInfo>, StoreError>;

    /// Store an object, linked to a service ID and signature
    async fn object_put<T: ImmutableData + Send>(
        &self,
        _page: Container<T>,
    ) -> Result<(), StoreError>;

    /// Fetch an object by signature
    async fn object_get(
        &self,
        id: &Id,
        obj: ObjectIdentifier,
        keys: &Keys,
    ) -> Result<Container, StoreError>;

    /// Fetch a list of objects for a given service
    async fn object_find(
        &self,
        id: &Id,
        keys: &Keys,
        bounds: &PageBounds,
    ) -> Result<Vec<Container>, StoreError>;

    /// Delete an object by signature
    async fn object_del(&self, _sig: &Signature) -> Result<(), StoreError>;
}

impl DataStore for AsyncStore {
    async fn peer_update(&self, info: &PeerInfo) -> Result<(), StoreError> {
        let (tx, rx) = oneshot::channel();

        // Enqueue put operation
        if let Err(e) = self.tasks.send((StoreOp::PeerUpdate(info.clone()), tx)) {
            error!("Failed to enqueue peer update operation: {e:?}");
            return Err(StoreError::Unknown);
        }

        // Await operation completion
        match rx.await {
            Ok(StoreRes::Ok) => Ok(()),
            Ok(StoreRes::Err(e)) => Err(e),
            Err(_) => Err(StoreError::Unknown),
            _ => unreachable!(),
        }
    }

    async fn peer_get(&self, id: &Id) -> Result<PeerInfo, StoreError> {
        let (tx, rx) = oneshot::channel();

        // Enqueue put operation
        if let Err(e) = self.tasks.send((StoreOp::PeerGet(id.clone()), tx)) {
            error!("Failed to enqueue peer get operation: {e:?}");
            return Err(StoreError::Unknown);
        }

        // Await operation completion
        match rx.await {
            Ok(StoreRes::Peer(info)) => Ok(info),
            Ok(StoreRes::Err(e)) => Err(e),
            Err(_) => Err(StoreError::Unknown),
            _ => unreachable!(),
        }
    }

    async fn peer_del(&self, peer_id: &Id) -> Result<(), StoreError> {
        let (tx, rx) = oneshot::channel();

        // Enqueue put operation
        if let Err(e) = self.tasks.send((StoreOp::PeerDelete(peer_id.clone()), tx)) {
            error!("Failed to enqueue peer delete operation: {e:?}");
            return Err(StoreError::Unknown);
        }

        // Await operation completion
        match rx.await {
            Ok(StoreRes::Ok) => Ok(()),
            Ok(StoreRes::Err(e)) => Err(e),
            Err(_) => Err(StoreError::Unknown),
            _ => unreachable!(),
        }
    }

    async fn peer_load(&self) -> Result<Vec<PeerInfo>, StoreError> {
        let (tx, rx) = oneshot::channel();

        // Enqueue put operation
        if let Err(e) = self.tasks.send((StoreOp::PeerLoad, tx)) {
            error!("Failed to enqueue peer load operation: {e:?}");
            return Err(StoreError::Unknown);
        }

        // Await operation completion
        match rx.await {
            Ok(StoreRes::Peers(info)) => Ok(info),
            Ok(StoreRes::Err(e)) => Err(e),
            Err(_) => Err(StoreError::Unknown),
            _ => unreachable!(),
        }
    }

    async fn service_update(&self, info: &ServiceInfo) -> Result<(), StoreError> {
        let (tx, rx) = oneshot::channel();

        // Enqueue put operation
        if let Err(e) = self.tasks.send((StoreOp::ServiceUpdate(info.clone()), tx)) {
            error!("Failed to enqueue service update operation: {e:?}");
            return Err(StoreError::Unknown);
        }

        // Await operation completion
        match rx.await {
            Ok(StoreRes::Ok) => Ok(()),
            Ok(StoreRes::Err(e)) => Err(e),
            Err(_) => Err(StoreError::Unknown),
            _ => unreachable!(),
        }
    }

    async fn service_get(&self, id: &Id) -> Result<ServiceInfo, StoreError> {
        let (tx, rx) = oneshot::channel();

        // Enqueue put operation
        if let Err(e) = self.tasks.send((StoreOp::ServiceGet(id.clone()), tx)) {
            error!("Failed to enqueue service get operation: {e:?}");
            return Err(StoreError::Unknown);
        }

        // Await operation completion
        match rx.await {
            Ok(StoreRes::Service(info)) => Ok(info),
            Ok(StoreRes::Err(e)) => Err(e),
            Err(_) => Err(StoreError::Unknown),
            _ => unreachable!(),
        }
    }

    async fn service_del(&self, service_id: &Id) -> Result<(), StoreError> {
        let (tx, rx) = oneshot::channel();

        // Enqueue put operation
        if let Err(e) = self.tasks.send((StoreOp::ServiceDelete(service_id.clone()), tx)) {
            error!("Failed to enqueue service delete operation: {e:?}");
            return Err(StoreError::Unknown);
        }

        // Await operation completion
        match rx.await {
            Ok(StoreRes::Ok) => Ok(()),
            Ok(StoreRes::Err(e)) => Err(e),
            Err(_) => Err(StoreError::Unknown),
            _ => unreachable!(),
        }
    }

    async fn service_load(&self) -> Result<Vec<ServiceInfo>, StoreError> {
        let (tx, rx) = oneshot::channel();

        // Enqueue put operation
        if let Err(e) = self.tasks.send((StoreOp::ServiceLoad, tx)) {
            error!("Failed to enqueue service get operation: {e:?}");
            return Err(StoreError::Unknown);
        }

        // Await operation completion
        match rx.await {
            Ok(StoreRes::Services(info)) => Ok(info),
            Ok(StoreRes::Err(e)) => Err(e),
            Err(_) => Err(StoreError::Unknown),
            _ => unreachable!(),
        }
    }

    async fn object_put<T: ImmutableData + Send>(&self, c: Container<T>) -> Result<(), StoreError> {
        let c = c.to_owned();
        let (tx, rx) = oneshot::channel();

        // Enqueue put operation
        if let Err(e) = self.tasks.send((StoreOp::ObjectPut(c), tx)) {
            error!("Failed to enqueue object store operation: {e:?}");
            return Err(StoreError::Unknown);
        }

        // Await operation completion
        match rx.await {
            Ok(StoreRes::Ok) => Ok(()),
            Ok(StoreRes::Err(e)) => Err(e),
            Err(_) => Err(StoreError::Unknown),
            _ => unreachable!(),
        }
    }

    async fn object_get(
        &self,
        id: &Id,
        obj: ObjectIdentifier,
        keys: &Keys,
    ) -> Result<Container, StoreError> {
        let (tx, rx) = oneshot::channel();

        // Enqueue put operation
        if let Err(e) = self
            .tasks
            .send((StoreOp::ObjectGet(id.clone(), obj, keys.clone()), tx))
        {
            error!("Failed to enqueue object get operation: {e:?}");
            return Err(StoreError::Unknown);
        }

        // Await operation completion
        match rx.await {
            Ok(StoreRes::Object(o)) => Ok(o),
            Ok(StoreRes::Err(e)) => Err(e),
            Err(_) => Err(StoreError::Unknown),
            _ => unreachable!(),
        }
    }

    async fn object_find(
        &self,
        id: &Id,
        keys: &Keys,
        bounds: &PageBounds,
    ) -> Result<Vec<Container>, StoreError> {
        let (tx, rx) = oneshot::channel();

        // Enqueue put operation
        if let Err(e) = self.tasks.send((
            StoreOp::ObjectList(id.clone(), bounds.clone(), keys.clone()),
            tx,
        )) {
            error!("Failed to enqueue object list operation: {e:?}");
            return Err(StoreError::Unknown);
        }

        // Await operation completion
        match rx.await {
            Ok(StoreRes::Objects(o)) => Ok(o),
            Ok(StoreRes::Err(e)) => Err(e),
            Err(_) => Err(StoreError::Unknown),
            _ => unreachable!(),
        }
    }

    async fn object_del(&self, sig: &Signature) -> Result<(), StoreError> {
        let (tx, rx) = oneshot::channel();

        // Enqueue put operation
        if let Err(e) = self.tasks.send((StoreOp::ObjectDelete(sig.clone()), tx)) {
            error!("Failed to enqueue object delete operation: {e:?}");
            return Err(StoreError::Unknown);
        }

        // Await operation completion
        match rx.await {
            Ok(StoreRes::Ok) => Ok(()),
            Ok(StoreRes::Err(e)) => Err(e),
            Err(_) => Err(StoreError::Unknown),
            _ => unreachable!(),
        }
    }
}
