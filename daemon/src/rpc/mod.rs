use std::task::{Context, Poll};

use dsf_core::wire::Container;
use kad::store::Datastore;
use log::{debug, error, info, warn};
use tracing::{span, Level};

use futures::channel::mpsc;
use futures::prelude::*;

use dsf_core::prelude::*;
use dsf_rpc::*;

use crate::{
    core::peers::Peer,
    daemon::{net::NetIf, Dsf},
    error::{CoreError, Error},
    rpc::lookup::PeerRegistry,
    rpc::search::NameService,
};

// Generic / shared operation types
pub mod ops;
use ops::*;

// Connect to an existing peer
pub mod connect;

// Lookup a peer in the database
pub mod lookup;

// Create and register new service
pub mod create;

// Register an existing service
pub mod register;

// Publish data for a given service
pub mod publish;

// Push DSF data for a given service
pub mod push;

// Locate an existing service
pub mod locate;

// Query for data from a service
pub mod query;

// Subscribe to a service
pub mod subscribe;

// Bootstrap daemon connectivity
pub mod bootstrap;

// Search using nameservices
pub mod search;

// Debug commands
pub mod debug;

// Local discovery
pub mod discover;

impl<Net> Dsf<Net>
where
    Dsf<Net>: NetIf<Interface = Net>,
{
    // Create a new RPC operation
    pub fn start_rpc(&mut self, req: Request, done: RpcSender) -> Result<(), Error> {
        let req_id = req.req_id();

        if let Some(waker) = self.waker.as_ref() {
            waker.clone().wake();
        }

        // Respond to non-async RPC requests immediately
        let resp = match req.kind() {
            RequestKind::Status => {
                let i = StatusInfo {
                    id: self.id(),
                    peers: self.peers().count(),
                    services: self.services().count(),
                };

                Some(ResponseKind::Status(i))
            }
            RequestKind::Peer(PeerCommands::List(_options)) => {
                let peers = self
                    .peers()
                    .list()
                    .drain(..)
                    .map(|(id, p)| (id, p.info()))
                    .collect();

                Some(ResponseKind::Peers(peers))
            }
            RequestKind::Peer(PeerCommands::Remove(options)) => {
                match self.resolve_peer_identifier(&options) {
                    Ok(id) => {
                        let p = self.peers().remove(&id);
                        match p {
                            Some(p) => Some(ResponseKind::Peer(p)),
                            None => Some(ResponseKind::None),
                        }
                    }
                    Err(_e) => Some(ResponseKind::None),
                }
            }
            RequestKind::Service(ServiceCommands::List(_options)) => {
                let s = self.services().list();
                Some(ResponseKind::Services(s))
            }
            RequestKind::Service(ServiceCommands::Info(options)) => {
                match self.resolve_identifier(&options.service) {
                    Ok(id) => Some(
                        self.services()
                            .find(&id)
                            .map(|i| ResponseKind::Service(i))
                            .unwrap_or(ResponseKind::None),
                    ),
                    Err(_e) => Some(ResponseKind::None),
                }
            }
            RequestKind::Service(ServiceCommands::SetKey(options)) => {
                match self.resolve_identifier(&options.service) {
                    Ok(id) => {
                        // TODO: Ehh?
                        let s = self.services().update_inst(&id, |s| {
                            s.service.set_secret_key(options.secret_key.clone());
                        });

                        match s {
                            Some(s) => Some(ResponseKind::Services(vec![s])),
                            None => Some(ResponseKind::None),
                        }
                    }
                    Err(_e) => Some(ResponseKind::None),
                }
            }
            RequestKind::Service(ServiceCommands::Remove(options)) => {
                match self.resolve_identifier(&options.service) {
                    Ok(id) => {
                        let s = self.services().remove(&id)?;

                        match s {
                            Some(s) => Some(ResponseKind::Service(s)),
                            None => Some(ResponseKind::None),
                        }
                    }
                    Err(_e) => Some(ResponseKind::None),
                }
            }
            RequestKind::Data(DataCommands::List(data::ListOptions {
                service,
                page_bounds,
                time_bounds,
            })) => match self.resolve_identifier(&service) {
                Ok(id) => {
                    let d = self.data().fetch_data(&id, &page_bounds, &time_bounds)?;
                    let i = d.iter().map(|i| i.info.clone()).collect();

                    Some(ResponseKind::Data(i))
                }
                Err(_e) => Some(ResponseKind::None),
            },

            _ => None,
        };

        if let Some(k) = resp {
            let r = Response::new(req_id, k);
            done.clone().try_send(r).unwrap();
            return Ok(());
        }

        // Otherwise queue up request for async execution
        let kind = match req.kind() {
            RequestKind::Peer(PeerCommands::Connect(opts)) => RpcKind::connect(opts),
            //RequestKind::Peer(PeerCommands::Search(opts)) => RpcKind::lookup(opts),
            //#[cfg(nope)]
            RequestKind::Peer(PeerCommands::Search(opts)) => {
                let mut exec = self.exec();

                tokio::task::spawn(async move {
                    debug!("Starting async lookup");
                    let i = match exec.peer_lookup(opts).await {
                        Ok(p) => ResponseKind::Peer(p),
                        Err(e) => ResponseKind::Error(e),
                    };
                    warn!("Async peer lookup result: {:?}", i);
                    if let Err(e) = done.clone().try_send(Response::new(req_id, i)) {
                        error!("Failed to send RPC response: {:?}", e);
                    }
                    warn!("Async response sent");
                });

                return Ok(());
            }
            RequestKind::Service(ServiceCommands::Create(opts)) => RpcKind::create(opts),
            RequestKind::Service(ServiceCommands::Register(opts)) => RpcKind::register(opts),
            RequestKind::Service(ServiceCommands::Locate(opts)) => RpcKind::locate(opts),
            RequestKind::Service(ServiceCommands::Subscribe(opts)) => RpcKind::subscribe(opts),
            RequestKind::Service(ServiceCommands::Discover(opts)) => RpcKind::discover(opts),
            RequestKind::Data(DataCommands::Publish(opts)) => RpcKind::publish(opts),
            //RequestKind::Data(DataCommands::Query(options)) => unimplemented!(),
            //RequestKind::Debug(DebugCommands::Update) => self.update(true).await.map(|_| ResponseKind::None)?,
            RequestKind::Ns(NsCommands::Register(opts)) => {
                let exec = self.exec();
                tokio::task::spawn(async move {
                    debug!("Starting async ns register");
                    let i = match exec.ns_register(opts).await {
                        Ok(i) => ResponseKind::Ns(i),
                        Err(e) => ResponseKind::Error(e),
                    };
                    warn!("Async ns register result: {:?}", i);
                    if let Err(e) = done.clone().try_send(Response::new(req_id, i)) {
                        error!("Failed to send RPC response: {:?}", e);
                    }
                    warn!("Async ns register response sent");
                });
                return Ok(());
            }
            RequestKind::Ns(NsCommands::Search(opts)) => {
                let exec = self.exec();
                tokio::task::spawn(async move {
                    debug!("Starting async ns search");
                    let i = match exec.ns_search(opts).await {
                        Ok(p) => ResponseKind::Pages(p),
                        Err(e) => ResponseKind::Error(e),
                    };
                    warn!("Async ns search result: {:?}", i);
                    if let Err(e) = done.clone().try_send(Response::new(req_id, i)) {
                        error!("Failed to send RPC response: {:?}", e);
                    }
                    warn!("Async ns search response sent");
                });
                return Ok(());
            }
            RequestKind::Debug(DebugCommands::Bootstrap) => RpcKind::bootstrap(()),
            _ => {
                error!("RPC operation {:?} unimplemented", req.kind());
                return Ok(());
            }
        };

        let op = RpcOperation { req_id, kind, done };

        // TODO: check we're not overwriting anything here

        debug!("Adding RPC op {} to tracking", req_id);
        self.rpc_ops.insert(req_id, op);

        Ok(())
    }

    // Poll on pending RPC operations
    // Context must be propagated through here to keep the waker happy
    pub fn poll_rpc(&mut self, ctx: &mut Context) -> Result<(), Error> {
        // Take RPC operations so we can continue using `&mut self`
        let mut rpc_ops: Vec<_> = self.rpc_ops.drain().collect();
        let mut ops_done = vec![];

        // Iterate through and update each operation
        for (_req_id, op) in &mut rpc_ops {
            let RpcOperation { kind, done, req_id } = op;

            let complete = match kind {
                RpcKind::Connect(connect) => {
                    self.poll_rpc_connect(*req_id, connect, ctx, done.clone())?
                }
                RpcKind::Locate(locate) => {
                    self.poll_rpc_locate(*req_id, locate, ctx, done.clone())?
                }
                RpcKind::Register(register) => {
                    self.poll_rpc_register(*req_id, register, ctx, done.clone())?
                }
                RpcKind::Create(create) => {
                    self.poll_rpc_create(*req_id, create, ctx, done.clone())?
                }
                RpcKind::Lookup(lookup) => {
                    self.poll_rpc_lookup(*req_id, lookup, ctx, done.clone())?
                }
                RpcKind::Bootstrap(bootstrap) => {
                    self.poll_rpc_bootstrap(*req_id, bootstrap, ctx, done.clone())?
                }
                RpcKind::Publish(publish) => {
                    self.poll_rpc_publish(*req_id, publish, ctx, done.clone())?
                }
                RpcKind::Subscribe(subscribe) => {
                    self.poll_rpc_subscribe(*req_id, subscribe, ctx, done.clone())?
                }
                _ => {
                    error!("Unsuported async RPC: {}", kind);
                    return Ok(());
                }
            };

            if complete {
                ops_done.push(req_id.clone());
            }
        }

        // Re-add updated operations
        for (req_id, op) in rpc_ops {
            self.rpc_ops.insert(req_id, op);
        }

        // Remove completed operations
        for d in ops_done {
            self.rpc_ops.remove(&d);
        }

        Ok(())
    }

    /// Shared helper for resolving service identifiers
    pub(super) fn resolve_identifier(
        &mut self,
        identifier: &ServiceIdentifier,
    ) -> Result<Id, Error> {
        // Short circuit if ID specified or error if none
        let index = match (&identifier.id, identifier.index) {
            (Some(id), _) => return Ok(id.clone()),
            (None, None) => {
                error!("service id or index must be specified");
                return Err(Error::UnknownService);
            }
            (_, Some(index)) => index,
        };

        match self.services().index_to_id(index) {
            Some(id) => Ok(id),
            None => {
                error!("no service matching index: {}", index);
                Err(Error::UnknownService)
            }
        }
    }

    /// Shared helper for resolving peer identifiers
    pub(super) fn resolve_peer_identifier(
        &mut self,
        identifier: &ServiceIdentifier,
    ) -> Result<Id, Error> {
        // Short circuit if ID specified or error if none
        let index = match (&identifier.id, identifier.index) {
            (Some(id), _) => return Ok(id.clone()),
            (None, None) => {
                error!("service id or index must be specified");
                return Err(Error::Core(CoreError::NoPeerId));
            }
            (_, Some(index)) => index,
        };

        match self.peers().index_to_id(index) {
            Some(id) => Ok(id),
            None => {
                error!("no peer matching index: {}", index);
                Err(Error::Core(CoreError::UnknownPeer))
            }
        }
    }

    pub(crate) fn exec(&self) -> ExecHandle {
        ExecHandle {
            req_id: rand::random(),
            tx: self.op_tx.clone(),
            waker: self.waker.as_ref().map(|w| w.clone()),
        }
    }

    pub fn poll_exec(&mut self, ctx: &mut Context) -> Result<(), Error> {
        // Check for incoming / new operations
        if let Poll::Ready(Some(mut op)) = self.op_rx.poll_next_unpin(ctx) {
            debug!("New op request: {:?}", op);

            let op_id = op.req_id;

            match op.kind {
                OpKind::ServiceResolve(i) => {
                    let r = self
                        .resolve_identifier(&i)
                        .map(|id| self.services().find_copy(&id).unwrap())
                        .map(|s| Ok(Res::Service(s)))
                        .unwrap_or(Err(CoreError::NotFound));

                    if let Err(e) = op.done.try_send(r) {
                        error!("Failed to send operation response: {:?}", e);
                    };
                }
                OpKind::ServiceGet(id) => {
                    let r = self
                        .services()
                        .find_copy(&id)
                        .map(|s| Ok(Res::Service(s)))
                        .unwrap_or(Err(CoreError::NotFound));

                    if let Err(e) = op.done.try_send(r) {
                        error!("Failed to send operation response: {:?}", e);
                    };
                }
                OpKind::ServiceCreate(id, pages) => {
                    let r = self
                        .service_register(&id, pages)
                        .map(|i| Ok(Res::ServiceInfo(i)))
                        // TODO: fix this error type
                        .unwrap_or(Err(CoreError::Unknown));

                    if let Err(e) = op.done.try_send(r) {
                        error!("Failed to send operation response: {:?}", e);
                    }
                }
                OpKind::ServiceUpdate(id, f) => {
                    let r = self
                        .services()
                        .with(&id, |inst| f(&mut inst.service))
                        .unwrap_or(Err(CoreError::NotFound));

                    if let Err(e) = op.done.try_send(r) {
                        error!("Failed to send operation response: {:?}", e);
                    }
                }
                OpKind::DhtLocate(ref id) => match self.dht_mut().locate(id.clone()) {
                    Ok((s, _id)) => {
                        op.state = OpState::DhtLocate(s);
                        self.ops.insert(op_id, op);
                    }
                    Err(e) => {
                        error!("Failed to create search operation: {:?}", e);
                    }
                },
                OpKind::DhtSearch(ref id) => match self.dht_mut().search(id.clone()) {
                    Ok((s, _id)) => {
                        op.state = OpState::DhtSearch(s);
                        self.ops.insert(op_id, op);
                    }
                    Err(e) => {
                        error!("Failed to create search operation: {:?}", e);
                    }
                },
                OpKind::DhtPut(ref id, ref pages) => {
                    match self.dht_mut().store(id.clone(), pages.clone()) {
                        Ok((p, _id)) => {
                            op.state = OpState::DhtPut(p);
                            self.ops.insert(op_id, op);
                        }
                        Err(e) => {
                            error!("Failed to create search operation: {:?}", e);
                        }
                    }
                }
                OpKind::PeerGet(ref id) => {
                    let r = self
                        .peers()
                        .find(id)
                        .map(|p| Ok(Res::Peers(vec![p])))
                        .unwrap_or(Err(CoreError::NotFound));

                    if let Err(e) = op.done.try_send(r) {
                        error!("Failed to send operation response: {:?}", e);
                    };
                }
                OpKind::ObjectGet(ref id, sig) => {
                    let mut page = None;

                    // Attempt to fetch from services in memory
                    match self
                        .services()
                        .with(id, |s| s.primary_page.clone())
                        .flatten()
                    {
                        Some(p) if p.signature() == sig => page = Some(p),
                        _ => (),
                    }
                    match self
                        .services()
                        .with(id, |s| s.replica_page.clone())
                        .flatten()
                    {
                        Some(p) if p.signature() == sig => page = Some(p),
                        _ => (),
                    }

                    // Otherwise fallback to db
                    if page.is_none() {
                        todo!("Implement store object fetch");
                        //let o = self.store.
                    }

                    // And return the response object
                    let r = match page {
                        Some(p) => Ok(Res::Pages(vec![p])),
                        _ => Err(CoreError::NotFound),
                    };
                    if let Err(e) = op.done.try_send(r) {
                        error!("Failed to send operation response: {:?}", e);
                    };
                }
            }
        }

        // Poll on currently executing operations
        let mut ops_done = vec![];
        for (op_id, Op { state, done, .. }) in self.ops.iter_mut() {
            if let Poll::Ready(r) = state.poll_unpin(ctx) {
                debug!("Op: {} complete with result: {:?}", op_id, r);

                if let Err(_e) = done.try_send(r) {
                    error!("Error sending result to op: {}", op_id);
                }

                ops_done.push(*op_id);
            }
        }

        // Remove complete operations
        for op_id in &ops_done {
            let _ = self.ops.remove(op_id);
        }

        Ok(())
    }
}

pub struct ExecHandle {
    req_id: u64,
    tx: mpsc::UnboundedSender<Op>,
    waker: Option<core::task::Waker>,
}

/// Operations used when constructing higher level RPCs
pub struct Op {
    req_id: u64,
    kind: OpKind,
    done: mpsc::Sender<Result<Res, CoreError>>,
    state: OpState,
}

impl core::fmt::Debug for Op {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Op")
            .field("req_id", &self.req_id)
            .field("kind", &self.kind)
            .field("state", &self.state)
            .finish()
    }
}

/// Operation state storage for DHT interactions
enum OpState {
    None,
    DhtLocate(kad::dht::LocateFuture<Id, Peer>),
    DhtSearch(kad::dht::SearchFuture<Container>),
    DhtPut(kad::dht::StoreFuture<Id, Peer>),
}

impl Future for OpState {
    type Output = Result<Res, CoreError>;

    fn poll(self: std::pin::Pin<&mut Self>, ctx: &mut Context<'_>) -> Poll<Self::Output> {
        let r = match self.get_mut() {
            OpState::None => unreachable!(),
            OpState::DhtLocate(locate) => match locate.poll_unpin(ctx) {
                Poll::Ready(Ok(peer)) => Ok(Res::Peers(vec![peer.info().clone()])),
                Poll::Ready(Err(_e)) => Err(CoreError::Unknown),
                _ => return Poll::Pending,
            },
            OpState::DhtSearch(get) => match get.poll_unpin(ctx) {
                Poll::Ready(Ok(pages)) => Ok(Res::Pages(pages)),
                Poll::Ready(Err(_e)) => Err(CoreError::Unknown),
                _ => return Poll::Pending,
            },
            OpState::DhtPut(put) => match put.poll_unpin(ctx) {
                Poll::Ready(Ok(peers)) => {
                    let peers = peers.iter().map(|e| e.info().clone()).collect();
                    Ok(Res::Peers(peers))
                }
                Poll::Ready(Err(_e)) => Err(CoreError::Unknown),
                _ => return Poll::Pending,
            },
        };

        Poll::Ready(r)
    }
}

impl core::fmt::Debug for OpState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::None => write!(f, "None"),
            Self::DhtLocate(_) => f.debug_tuple("DhtLocate").finish(),
            Self::DhtSearch(_) => f.debug_tuple("DhtSearch").finish(),
            Self::DhtPut(_) => f.debug_tuple("DhtPut").finish(),
        }
    }
}

#[async_trait::async_trait]
impl Engine for ExecHandle {
    async fn exec(&self, kind: OpKind) -> Result<Res, CoreError> {
        let (done_tx, mut done_rx) = mpsc::channel(1);
        let mut tx = self.tx.clone();

        let req = Op {
            req_id: self.req_id,
            kind,
            done: done_tx,
            state: OpState::None,
        };

        // Add message to operation queue
        if let Err(_e) = tx.send(req).await {
            // TODO: Cancelled
            error!("RPC exec channel closed");
            return Err(CoreError::Unknown);
        }

        // Trigger waker if available (not sure why this isn't happening automatically..?)
        if let Some(waker) = self.waker.as_ref() {
            waker.clone().wake();
        }

        // Await response message
        match done_rx.next().await {
            Some(Ok(r)) => Ok(r),
            Some(Err(e)) => Err(e),
            // TODO: Cancelled
            None => {
                warn!("RPC response channel closed");
                Err(CoreError::Unknown)
            }
        }
    }
}
