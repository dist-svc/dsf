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
use dsf_rpc::{PeerInfo, PeerAddress};

use crate::{
    core::{
        Core,
        store::{DataStore, StoreOp, StoreRes},
    },
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

            let core = self.core.clone();

            let Op {
                req_id: _,
                kind,
                mut done,
            } = op;

            match kind {
                OpKind::Primary => {
                    let r = self
                        .primary(false)
                        .map(|p| Res::Pages(vec![p], None))
                        .map_err(|_| CoreError::Unknown);

                    if let Err(e) = done.try_send(r) {
                        error!("Failed to send operation response: {:?}", e);
                    };
                }
                OpKind::ServiceGet(ident) => {
                    tokio::task::spawn(async move {
                        let r = core.service_get(ident.clone()).await
                            .map(|s| Ok(Res::ServiceInfo(s)))
                            .unwrap_or(Err(CoreError::NotFound));

                        if let Err(e) = done.try_send(r) {
                            error!("Failed to send operation response: {:?}", e);
                        };
                    });
                }
                OpKind::ServiceCreate(service, primary_page) => {
                    tokio::task::spawn(async move {
                        let r = core
                            .service_create(service, vec![primary_page])
                            .await
                            .map(|i| Res::ServiceInfo(i))
                            // TODO: fix this error type
                            .map_err(|e| CoreError::Unknown);

                        if let Err(e) = done.try_send(r) {
                            error!("Failed to send operation response: {:?}", e);
                        }
                    });
                }
                OpKind::ServiceRegister(id, pages) => {
                    tokio::task::spawn(async move {
                        let r = core
                            .service_register(id.clone(), pages)
                            .await
                            .map(|i| Res::ServiceInfo(i))
                            // TODO: fix this error type
                            .map_err(|e| CoreError::Unknown);

                        if let Err(e) = done.try_send(r) {
                            error!("Failed to send operation response: {:?}", e);
                        }
                    });
                }
                // Handle special-case update of our own / peer service
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
                OpKind::ServiceList(opts) => {

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
                    todo!("Implement publish RPC op")
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
                            Ok((p, i)) => Ok(Res::Peers(
                                p.iter().map(|p| p.info().clone()).collect(),
                                Some(i),
                            )),
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
                            Ok((p, i)) => Ok(Res::Peers(vec![p.info().clone()], Some(i))),
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
                            Ok((p, i)) => Ok(Res::Pages(p, Some(i))),
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
                            Ok((p, i)) => Ok(Res::Peers(
                                p.iter().map(|p| p.info().clone()).collect(),
                                Some(i),
                            )),
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
                    let peer_state = match pub_key {
                        Some(k) => PeerState::Known(k),
                        None => PeerState::Unknown,
                    };

                    let mut peer_info = PeerInfo::new(id, address, peer_state, 0, None);
                    peer_info.flags = flags;
                    let p = self.core.peer_find_or_create(peer_info).await?;

                    if let Err(e) = done.try_send(Ok(Res::Peers(vec![p], None))) {
                        error!("Failed to send operation response: {:?}", e);
                    };
                }
                OpKind::PeerGet(id) => {
                    let r = self
                        .core
                        .peer_get(id.into())
                        .await
                        .map(|p| Ok(Res::Peers(vec![p], None)))
                        .unwrap_or(Err(CoreError::NotFound));

                    if let Err(e) = done.try_send(r) {
                        error!("Failed to send operation response: {:?}", e);
                    };
                }
                OpKind::PeerList => {
                    let r = self.core.peer_list(Default::default()).await?;

                    if let Err(e) = done.try_send(Ok(Res::Peers(r, None))) {
                        error!("Failed to send operation response: {:?}", e);
                    };
                }
                OpKind::ObjectGet(id, sig) => {
                    let mut page = None;

                    // Fetch object
                    let page = self.core.object_get(i&d, sig.into()).await?;

                    // And return the response object if found
                    let r = match page {
                        Some(p) => Ok(Res::Pages(vec![p], None)),
                        _ => Err(CoreError::NotFound),
                    };
                    if let Err(e) = done.try_send(r) {
                        error!("Failed to send operation response: {:?}", e);
                    };
                }
                OpKind::ObjectPut(data) => {
                    // TODO: Lookup matching service / check prior to put

                    let store = self.store.clone();
                    let mut done = done.clone();

                    tokio::task::spawn(async move {
                        // Write data to local store
                        let res = store.object_put(data).await;

                        // Map results and forward to caller
                        let resp = match res {
                            Ok(_) => Ok(Res::Sig(data.signature())),
                            // TODO: fix store error returns
                            Err(_e) => Err(CoreError::Unknown),
                        };
                        if let Err(e) = done.try_send(resp) {
                            error!("Failed to forward net response: {:?}", e);
                        }
                    });
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

}

/// Handle for executing basic operations via daemon
#[derive(Debug)]
pub struct ExecHandle {
    peer_id: Id,
    req_id: u64,
    tx: mpsc::UnboundedSender<Op>,
    waker: Option<core::task::Waker>,
}

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
