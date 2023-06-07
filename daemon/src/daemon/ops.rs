use std::{
    task::{Context, Poll},
    time::SystemTime,
};

use dsf_rpc::{ServiceIdentifier, ServiceState};
use futures::channel::mpsc;
use futures::prelude::*;
use kad::{
    dht::{Base, Connect, Lookup, Search, Store},
    prelude::{DhtEntry, DhtError},
};
use log::{debug, error, info, warn};

use dsf_core::{
    error::Error as CoreError,
    prelude::{NetMessage, NetRequest, NetRequestBody},
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
        if let Poll::Ready(Some(op)) = self.op_rx.poll_next_unpin(ctx) {
            debug!("New op request: {:?}", op);

            let Op {
                req_id: _,
                kind,
                mut done,
            } = op;

            match kind {
                OpKind::Primary => {
                    let r = self
                        .primary(false)
                        .map(|p| Res::Pages(vec![p]))
                        .map_err(|_| CoreError::Unknown);

                    if let Err(e) = done.try_send(r) {
                        error!("Failed to send operation response: {:?}", e);
                    };
                }
                OpKind::ServiceResolve(i) => {
                    let r = self
                        .resolve_service(&i)
                        .map(|s| Ok(Res::Service(s)))
                        .unwrap_or(Err(CoreError::NotFound));

                    if let Err(e) = done.try_send(r) {
                        error!("Failed to send operation response: {:?}", e);
                    };
                }
                OpKind::ServiceGet(id) => {
                    let r = self
                        .services()
                        .find(&id)
                        .map(|s| Ok(Res::ServiceInfo(s)))
                        .unwrap_or(Err(CoreError::NotFound));

                    if let Err(e) = done.try_send(r) {
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

                    if let Err(e) = done.try_send(r) {
                        error!("Failed to send operation response: {:?}", e);
                    }
                }
                OpKind::ServiceRegister(id, pages) => {
                    let r = self
                        .service_register(&id, pages)
                        .map(|i| Ok(Res::ServiceInfo(i)))
                        // TODO: fix this error type
                        .unwrap_or(Err(CoreError::Unknown));

                    if let Err(e) = done.try_send(r) {
                        error!("Failed to send operation response: {:?}", e);
                    }
                }
                // Handle update of peer service
                // TODO: does this need to be a special case / could
                // we put this in the normal service collection?
                OpKind::ServiceUpdate(id, f) if id == self.id() => {
                    let svc = self.service();
                    let mut state = ServiceState::Created;
                    let r = f(svc, &mut state);

                    if let Err(e) = done.try_send(r) {
                        error!("Failed to send operation response: {:?}", e);
                    }
                }
                OpKind::ServiceUpdate(id, f) => {
                    let r = self
                        .services()
                        .with(&id, |inst| f(&mut inst.service, &mut inst.state))
                        .unwrap_or(Err(CoreError::NotFound));

                    if let Err(e) = done.try_send(r) {
                        error!("Failed to send operation response: {:?}", e);
                    }
                }
                OpKind::SubscribersGet(id) => {
                    let r = self
                        .subscribers()
                        .find_peers(&id)
                        .map(Res::Ids)
                        .map_err(|_e| CoreError::Unknown);

                    if let Err(e) = done.try_send(r) {
                        error!("Failed to send operation response: {:?}", e);
                    }
                }
                OpKind::Publish(_id, _info) => {
                    todo!()
                }
                // TODO: improve OpKind so this logic can move to connect RPC
                OpKind::DhtConnect(addr, id) => {
                    let dht = self.dht_mut().get_handle();
                    let exec = self.exec();

                    debug!("Start connect to: {addr:?}");

                    // Issue connect message to peer
                    let req_id = rand::random();
                    let mut req = NetRequest::new(
                        self.id(),
                        req_id,
                        NetRequestBody::Hello,
                        Flags::ADDRESS_REQUEST | Flags::PUB_KEY_REQUEST,
                    );
                    req.common.public_key = Some(self.service().public_key());

                    let (tx, mut rx) = mpsc::channel(1);
                    self.net_requests.insert((addr.clone(), req_id), tx);

                    if let Err(e) = self.net_send(&[(addr.clone(), id.clone())], req.into()) {
                        error!("Failed to send connect message: {:?}", e);
                        // TODO: remove net request / bail out?
                    }

                    tokio::task::spawn(async move {
                        // Wait for net response
                        let resp = match rx.next().await {
                            Some(v) => v,
                            None => {
                                error!("No response from peer: {:?}", addr);
                                return;
                            }
                        };

                        debug!("Received response {:?}", resp);

                        // Fetch peer information (created on message RX)
                        let from = resp.common.from;
                        let peer = match exec.peer_get(from.clone()).await {
                            Ok(v) => v,
                            Err(_e) => {
                                error!("Missing peer instance for id: {from:#?}");
                                return;
                            }
                        };

                        let r = match dht
                            .connect(vec![DhtEntry::new(from.clone(), peer)], Default::default())
                            .await
                        {
                            Ok((p, _i)) => Ok(Res::Peers(p.iter().map(|p| p.info().clone()).collect())),
                            Err(e) => {
                                error!("DHT connect error: {e:?}");
                                Err(CoreError::Unknown)
                            }
                        };

                        if let Err(e) = done.send(r).await {
                            error!("Failed to forward DHT lookup result: {:?}", e);
                        }
                    });
                }
                OpKind::DhtLocate(id) => {
                    let dht = self.dht_mut().get_handle();

                    tokio::task::spawn(async move {
                        let r = match dht.lookup(id.clone(), Default::default()).await {
                            Ok((p, _i)) => Ok(Res::Peers(vec![p.info().clone()])),
                            Err(e) => {
                                error!("DHT lookup error: {e:?}");
                                Err(dht_to_core_error(e))
                            }
                        };

                        if let Err(e) = done.send(r).await {
                            error!("Failed to forward DHT lookup result: {:?}", e);
                        }
                    });
                }
                OpKind::DhtSearch(id) => {
                    let dht = self.dht_mut().get_handle();

                    tokio::task::spawn(async move {
                        let r = match dht.search(id.clone(), Default::default()).await {
                            Ok((p, _i)) => Ok(Res::Pages(p)),
                            Err(e) => {
                                error!("DHT search error: {e:?}");
                                Err(dht_to_core_error(e))
                            }
                        };

                        if let Err(e) = done.send(r).await {
                            error!("Failed to forward DHT search result: {:?}", e);
                        }
                    });
                }
                OpKind::DhtPut(id, pages) => {
                    let dht = self.dht_mut().get_handle();

                    tokio::task::spawn(async move {
                        let r = match dht
                            .store(id.clone(), pages.clone(), Default::default())
                            .await
                        {
                            Ok((p, _i)) => Ok(Res::Peers(p.iter().map(|p| p.info().clone()).collect())),
                            Err(e) => {
                                error!("DHT store error: {e:?}");
                                Err(dht_to_core_error(e))
                            }
                        };

                        if let Err(e) = done.send(r).await {
                            error!("Failed to forward DHT store result: {:?}", e);
                        }
                    });
                }
                OpKind::DhtUpdate => {
                    let dht = self.dht_mut().get_handle();
                    tokio::task::spawn(async move {
                        debug!("Start DHT update");

                        let r = match dht.update(true).await {
                            Ok(_p) => Ok(Res::Ok),
                            Err(e) => {
                                error!("DHT update error: {e:?}");
                                Err(dht_to_core_error(e))
                            }
                        };

                        if let Err(e) = done.send(r).await {
                            error!("Failed to forward DHT update result: {:?}", e);
                        }
                    });
                }
                OpKind::PeerCreateUpdate(id, address, pub_key, flags) => {
                    let p = self.peers().find_or_create(id, address, pub_key, flags);

                    if let Err(e) = done.try_send(Ok(Res::Peers(vec![p]))) {
                        error!("Failed to send operation response: {:?}", e);
                    };
                }
                OpKind::PeerGet(id) => {
                    let r = self
                        .peers()
                        .find(&id)
                        .map(|p| Ok(Res::Peers(vec![p])))
                        .unwrap_or(Err(CoreError::NotFound));

                    if let Err(e) = done.try_send(r) {
                        error!("Failed to send operation response: {:?}", e);
                    };
                }
                OpKind::PeerList => {
                    let r = self.peers().list().drain(..).map(|(_id, p)| p).collect();

                    if let Err(e) = done.try_send(Ok(Res::Peers(r))) {
                        error!("Failed to send operation response: {:?}", e);
                    };
                }
                OpKind::ObjectGet(id, sig) => {
                    let mut page = None;

                    // Attempt to fetch from services in memory
                    match self
                        .services()
                        .with(&id, |s| s.primary_page.clone())
                        .flatten()
                    {
                        Some(p) if p.signature() == sig => page = Some(p),
                        _ => (),
                    }
                    match self
                        .services()
                        .with(&id, |s| s.replica_page.clone())
                        .flatten()
                    {
                        Some(p) if p.signature() == sig => page = Some(p),
                        _ => (),
                    }

                    // Otherwise fallback to db
                    if page.is_none() {
                        if let Some(d) = self.data().get_object(&id, &sig)? {
                            page = Some(d.page);
                        }
                    }

                    // And return the response object
                    let r = match page {
                        Some(p) => Ok(Res::Pages(vec![p])),
                        _ => Err(CoreError::NotFound),
                    };
                    if let Err(e) = done.try_send(r) {
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
                    if let Err(e) = done.try_send(r) {
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
                    if let Err(e) = done.try_send(r) {
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
                    if let Err(e) = done.try_send(resp) {
                        error!("Failed to send operation response: {:?}", e);
                    };
                }
                OpKind::Net(ref body, ref peers) => {
                    let req =
                        NetRequest::new(self.id(), rand::random(), body.clone(), Flags::default());
                    let s = self.net_op(peers.clone(), req);

                    tokio::task::spawn(async move {
                        let r = s.await;

                        if let Err(e) = done.send(Ok(Res::Responses(r))).await {
                            error!("Failed to forward net response: {:?}", e);
                        }
                    });
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

                    tokio::task::spawn(async move {
                        let r = s.await;

                        if let Err(e) = done.send(Ok(Res::Responses(r))).await {
                            error!("Failed to forward net response: {:?}", e);
                        }
                    });
                }
            }

            ctx.waker().clone().wake();
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
#[derive(Debug)]
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
}

impl core::fmt::Debug for Op {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Op")
            .field("req_id", &self.req_id)
            .field("kind", &self.kind)
            .finish()
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
