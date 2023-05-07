use std::{
    task::{Context, Poll},
    time::SystemTime,
};

use dsf_rpc::{ServiceIdentifier, ServiceState};
use futures::channel::mpsc;
use futures::prelude::*;
use kad::prelude::DhtError;
use log::{debug, error, info, warn};

use dsf_core::{
    error::Error as CoreError,
    prelude::NetRequest,
    service::Service,
    types::{Flags, Id},
    wire::Container,
};

use crate::{
    core::peers::Peer,
    daemon::{net::NetIf, Dsf},
    error::Error,
    rpc::{Engine, OpKind, Res},
};

use super::net::NetFuture;

impl<Net> Dsf<Net>
where
    Dsf<Net>: NetIf<Interface = Net>,
{
    /// Create an [ExecHandle] for executing async RPCs
    pub(crate) fn exec(&self) -> ExecHandle {
        ExecHandle {
            peer_id: self.id(),
            req_id: rand::random(),
            tx: self.op_tx.clone(),
            waker: self.waker.as_ref().map(|w| w.clone()),
        }
    }

    /// Poll on executable engine ([OpKind]) operations
    ///
    /// These are used to compose higher-level RPC functions
    pub fn poll_exec(&mut self, ctx: &mut Context) -> Result<(), Error> {
        // Check for incoming / new operations
        if let Poll::Ready(Some(mut op)) = self.op_rx.poll_next_unpin(ctx) {
            debug!("New op request: {:?}", op);

            let op_id = op.req_id;

            match op.kind {
                OpKind::Primary => {
                    let r = self
                        .primary()
                        .map(|p| Res::Pages(vec![p]))
                        .map_err(|_| CoreError::Unknown);

                    if let Err(e) = op.done.try_send(r) {
                        error!("Failed to send operation response: {:?}", e);
                    };
                }
                OpKind::ServiceResolve(i) => {
                    let r = self
                        .resolve_service(&i)
                        .map(|s| Ok(Res::Service(s)))
                        .unwrap_or(Err(CoreError::NotFound));

                    if let Err(e) = op.done.try_send(r) {
                        error!("Failed to send operation response: {:?}", e);
                    };
                }
                OpKind::ServiceGet(id) => {
                    let r = self
                        .services()
                        .find(&id)
                        .map(|s| Ok(Res::ServiceInfo(s)))
                        .unwrap_or(Err(CoreError::NotFound));

                    if let Err(e) = op.done.try_send(r) {
                        error!("Failed to send operation response: {:?}", e);
                    };
                }
                OpKind::ServiceCreate(service, primary_page) => {
                    let r = self
                        .services()
                        .register(
                            service,
                            &primary_page,
                            ServiceState::Created,
                            Some(SystemTime::now()),
                        )
                        .map(|i| Ok(Res::ServiceInfo(i)))
                        // TODO: fix this error type
                        .unwrap_or(Err(CoreError::Unknown));

                    if let Err(e) = op.done.try_send(r) {
                        error!("Failed to send operation response: {:?}", e);
                    }
                }
                OpKind::ServiceRegister(id, pages) => {
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
                        .with(&id, |inst| f(&mut inst.service, &mut inst.state))
                        .unwrap_or(Err(CoreError::NotFound));

                    if let Err(e) = op.done.try_send(r) {
                        error!("Failed to send operation response: {:?}", e);
                    }
                }
                OpKind::SubscribersGet(ref id) => {
                    let r = self
                        .subscribers()
                        .find_peers(id)
                        .map(Res::Ids)
                        .map_err(|_e| CoreError::Unknown);

                    if let Err(e) = op.done.try_send(r) {
                        error!("Failed to send operation response: {:?}", e);
                    }
                }
                OpKind::Publish(ref _id, _info) => {
                    todo!()
                }
                OpKind::DhtConnect(ref addr, ref id) => {
                    let id = id.clone();
                    // TODO: refactor this ungainly mess
                    match self.dht_mut().connect_start() {
                        Ok((s, req_id, dht_req)) => {
                            let addr = addr.clone();
                            op.state = OpState::DhtConnect(s);
                            self.ops.insert(op_id, op);

                            let net_req_body = self.dht_to_net_request(dht_req);
                            let mut net_req = NetRequest::new(
                                self.id(),
                                req_id,
                                net_req_body,
                                Flags::ADDRESS_REQUEST | Flags::PUB_KEY_REQUEST,
                            );
                            net_req.common.public_key = Some(self.service().public_key());

                            if let Err(e) =
                                self.net_send(&[(addr.clone(), id.clone())], net_req.into())
                            {
                                error!("Failed to send connect message: {:?}", e);
                            }
                        }
                        Err(e) => {
                            error!("Failed to create connect operation: {:?}", e);
                        }
                    }
                }
                OpKind::DhtLocate(ref id) => match self.dht_mut().locate(id.clone()) {
                    Ok((s, _id)) => {
                        op.state = OpState::DhtLocate(s);
                        self.ops.insert(op_id, op);
                    }
                    Err(e) => {
                        error!("Failed to create locate operation: {:?}", e);
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
                OpKind::PeerList => {
                    let r = self.peers().list().drain(..).map(|(_id, p)| p).collect();

                    if let Err(e) = op.done.try_send(Ok(Res::Peers(r))) {
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
                OpKind::ObjectPut(data) => {
                    // TODO: Lookup matching service

                    // Write data to local store
                    let r = match self.data().store_data(&data) {
                        Ok(_) => Ok(Res::Sig(data.signature())),
                        Err(_) => {
                            error!("Failed to store data: {:?}", data);
                            Err(CoreError::Unknown)
                        }
                    };
                    if let Err(e) = op.done.try_send(r) {
                        error!("Failed to send operation response: {:?}", e);
                    };
                }
                OpKind::ReplicaGet(id) => {
                    let r = match self.replicas().find(&id) {
                        Ok(v) => Ok(Res::Replicas(v)),
                        Err(e) => {
                            error!("Failed to locate replicas for service: {:?}", id);
                            Err(e)
                        }
                    };
                    if let Err(e) = op.done.try_send(r) {
                        error!("Failed to send operation response: {:?}", e);
                    };
                }
                OpKind::ReplicaUpdate(ref id, replicas) => {
                    let resp = Ok(Res::Id(id.clone()));
                    for r in replicas {
                        match self
                            .replicas()
                            .create_or_update(&id, &r.info.peer_id, &r.page)
                        {
                            Ok(v) => v,
                            Err(e) => {
                                // TODO: propagate error?
                                error!("Failed to update replica: {:?}", e);
                            }
                        }
                    }
                    if let Err(e) = op.done.try_send(resp) {
                        error!("Failed to send operation response: {:?}", e);
                    };
                }
                OpKind::Net(ref body, ref peers) => {
                    let req =
                        NetRequest::new(self.id(), rand::random(), body.clone(), Flags::default());

                    let s = self.net_op(peers.clone(), req);
                    op.state = OpState::Net(s);
                    self.ops.insert(op_id, op);
                }
                OpKind::NetBcast(ref body) => {
                    let mut req = NetRequest::new(
                        self.id(),
                        rand::random(),
                        body.clone(),
                        Flags::PUB_KEY_REQUEST | Flags::ADDRESS_REQUEST,
                    );
                    req.set_public_key(self.service().public_key());

                    let s = self.net_broadcast(req);
                    op.state = OpState::Net(s);
                    self.ops.insert(op_id, op);
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

    /// Shared helper for resolving service identifiers
    pub(crate) fn resolve_identifier(
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
    pub(crate) fn resolve_peer_identifier(
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

    fn resolve_service(&mut self, ident: &ServiceIdentifier) -> Option<Service> {
        let id = match self.resolve_identifier(ident) {
            Ok(v) => v,
            Err(_) => return None,
        };

        self.services().find_copy(&id)
    }
}

/// Handle for executing basic operations via daemon
pub struct ExecHandle {
    peer_id: Id,
    req_id: u64,
    tx: mpsc::UnboundedSender<Op>,
    waker: Option<core::task::Waker>,
}

#[async_trait::async_trait]
impl Engine for ExecHandle {
    fn id(&self) -> Id {
        self.peer_id.clone()
    }

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

/// Operation state storage for async interactions
enum OpState {
    None,
    DhtConnect(kad::dht::ConnectFuture<Id, Peer>),
    DhtLocate(kad::dht::LocateFuture<Id, Peer>),
    DhtSearch(kad::dht::SearchFuture<Container>),
    DhtPut(kad::dht::StoreFuture<Id, Peer>),
    Net(NetFuture),
}

impl Future for OpState {
    type Output = Result<Res, CoreError>;

    fn poll(self: std::pin::Pin<&mut Self>, ctx: &mut Context<'_>) -> Poll<Self::Output> {
        let r = match self.get_mut() {
            OpState::None => unreachable!(),
            OpState::DhtConnect(connect) => match connect.poll_unpin(ctx) {
                Poll::Ready(Ok(peers)) => {
                    Ok(Res::Peers(peers.iter().map(|v| v.info().clone()).collect()))
                }
                Poll::Ready(Err(e)) => Err(dht_to_core_error(e)),
                _ => return Poll::Pending,
            },
            OpState::DhtLocate(locate) => match locate.poll_unpin(ctx) {
                Poll::Ready(Ok(peer)) => Ok(Res::Peers(vec![peer.info().clone()])),
                Poll::Ready(Err(e)) => Err(dht_to_core_error(e)),
                _ => return Poll::Pending,
            },
            OpState::DhtSearch(get) => match get.poll_unpin(ctx) {
                Poll::Ready(Ok(pages)) => Ok(Res::Pages(pages)),
                Poll::Ready(Err(e)) => Err(dht_to_core_error(e)),
                _ => return Poll::Pending,
            },
            OpState::DhtPut(put) => match put.poll_unpin(ctx) {
                Poll::Ready(Ok(peers)) => {
                    let peers = peers.iter().map(|e| e.info().clone()).collect();
                    Ok(Res::Peers(peers))
                }
                Poll::Ready(Err(e)) => Err(dht_to_core_error(e)),
                _ => return Poll::Pending,
            },
            OpState::Net(send) => match send.poll_unpin(ctx) {
                Poll::Ready(r) => Ok(Res::Responses(r)),
                _ => return Poll::Pending,
            },
        };

        Poll::Ready(r)
    }
}

/// Map from [kad::Error] to [dsf_core::error::Error] type
fn dht_to_core_error(e: DhtError) -> CoreError {
    match e {
        DhtError::Unimplemented => CoreError::Unimplemented,
        DhtError::InvalidResponse => CoreError::InvalidResponse,
        DhtError::InvalidResponseId => CoreError::InvalidResponse,
        DhtError::Timeout => CoreError::Timeout,
        DhtError::NoPeers => CoreError::NoPeersFound,
        DhtError::NotFound => CoreError::NotFound,
        DhtError::Io(_) => CoreError::IO,
        _ => CoreError::Unknown,
    }
}

/// [Debug] impl for [OpState]
impl core::fmt::Debug for OpState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::None => write!(f, "None"),
            Self::DhtConnect(_) => f.debug_tuple("DhtConnect").finish(),
            Self::DhtLocate(_) => f.debug_tuple("DhtLocate").finish(),
            Self::DhtSearch(_) => f.debug_tuple("DhtSearch").finish(),
            Self::DhtPut(_) => f.debug_tuple("DhtPut").finish(),
            Self::Net(_) => f.debug_tuple("Net").finish(),
        }
    }
}
