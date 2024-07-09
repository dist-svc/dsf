use std::{
    collections::HashMap,
    iter::FromIterator,
    task::{Context, Poll},
    time::SystemTime,
};

use dsf_rpc::{ServiceIdentifier, ServiceInfo, ServiceState};
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
    types::{Flags, Id, ServiceKind},
    wire::Container,
};
use dsf_rpc::{PeerAddress, PeerInfo, PeerState, SubscriptionInfo};

use crate::{
    core::{
        store::{DataStore, StoreOp, StoreRes},
        Core, CoreRes,
    },
    daemon::{net::NetIf, Dsf},
    error::Error,
    rpc::{Engine, OpKind},
};

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
            waker: self.waker.clone(),
        }
    }

    /// Poll on executable engine ([OpKind]) operations
    ///
    /// These are used to compose higher-level RPC functions
    pub fn poll_exec(&mut self, ctx: &mut Context) -> Result<(), Error> {
        // Check for incoming / new operations
        if let Poll::Ready(Some(op)) = self.op_rx.poll_next_unpin(ctx) {
            debug!("New op request: {:?}", op);

            let mut core = self.core.clone();

            let Op {
                req_id: _,
                kind,
                mut done,
            } = op;

            match kind {
                OpKind::Info => todo!("Info op not yet implemented"),
                OpKind::Primary => {
                    let r = match self.primary(false) {
                        Ok(p) => CoreRes::Pages(vec![p], None),
                        Err(e) => CoreRes::Error(e),
                    };

                    if let Err(e) = done.try_send(r) {
                        error!("Failed to send operation response: {:?}", e);
                    };
                }
                OpKind::ServiceGet(ident) => {
                    tokio::task::spawn(async move {
                        let r = match core.service_get(ident.clone()).await {
                            Ok(s) => CoreRes::Service(s),
                            Err(e) => CoreRes::Error(e),
                        };

                        if let Err(e) = done.try_send(r) {
                            error!("Failed to send operation response: {:?}", e);
                        };
                    });
                }
                OpKind::ServiceCreate(service, primary_page) => {
                    tokio::task::spawn(async move {
                        let r = match core.service_create(service, vec![primary_page]).await {
                            Ok(s) => CoreRes::Service(s),
                            Err(e) => CoreRes::Error(e),
                        };

                        if let Err(e) = done.try_send(r) {
                            error!("Failed to send operation response: {:?}", e);
                        }
                    });
                }
                OpKind::ServiceRegister(id, pages) => {
                    tokio::task::spawn(async move {
                        let r = match core.service_register(id.clone(), pages).await {
                            Ok(s) => CoreRes::Service(s),
                            Err(e) => CoreRes::Error(e),
                        };

                        if let Err(e) = done.try_send(r) {
                            error!("Failed to send operation response: {:?}", e);
                        }
                    });
                }
                // Handle special-case update of our own / peer service
                OpKind::ServiceUpdate(id, f) if id == self.id() => {
                    let mut info = ServiceInfo::from(&self.service);
                    let r = f(&mut self.service, &mut info);

                    if let Err(e) = done.try_send(r) {
                        error!("Failed to send operation response: {:?}", e);
                    }
                }
                OpKind::ServiceUpdate(id, f) => {
                    tokio::task::spawn(async move {
                        let r = match core.service_update(id, f).await {
                            Ok(s) => CoreRes::Service(s),
                            Err(e) => CoreRes::Error(e),
                        };

                        if let Err(e) = done.try_send(r) {
                            error!("Failed to send operation response: {:?}", e);
                        }
                    });
                }
                OpKind::ServiceList(opts) => {
                    // TODO(med): apply service kind filters
                    tokio::task::spawn(async move {
                        let r = match core.service_list(opts.bounds).await {
                            Ok(s) => CoreRes::Services(s),
                            Err(e) => CoreRes::Error(e),
                        };

                        if let Err(e) = done.try_send(r) {
                            error!("Failed to send operation response: {:?}", e);
                        }
                    });
                }
                OpKind::SubscribersGet(id) => {
                    tokio::task::spawn(async move {
                        let r = match core.subscriber_list(id.clone()).await {
                            Ok(s) => CoreRes::Subscribers(s),
                            Err(e) => CoreRes::Error(e),
                        };

                        if let Err(e) = done.try_send(r) {
                            error!("Failed to send operation response: {:?}", e);
                        }
                    });
                }
                OpKind::Publish(_id, _info) => {
                    todo!("Implement publish RPC op")
                }
                // TODO: improve OpKind so this logic can move to connect RPC
                OpKind::DhtConnect(addr, id) => {
                    let dht = self.dht.clone();

                    let exec = self.exec();
                    let net = self.net.clone();

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

                    let targets = [(addr, id.clone())].to_vec();

                    tokio::task::spawn(async move {
                        // Issue network hello request
                        let mut r = match net.net_request(targets, req, Default::default()).await {
                            Ok(v) if !v.is_empty() => v,
                            _ => {
                                error!("No response from peer: {:?}", addr);
                                return;
                            }
                        };

                        let (_peer_id, (_addr, resp)) = match r.drain().next() {
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

                        // Perform DHT connect
                        let dht = dht.get_handle().await.unwrap();
                        let r = match dht
                            .connect(vec![DhtEntry::new(from.clone(), peer)], Default::default())
                            .await
                        {
                            Ok((p, i)) => CoreRes::Peers(
                                p.iter().map(|p| p.info().clone()).collect(),
                                Some(i),
                            ),
                            Err(e) => {
                                error!("DHT connect error: {e:?}");
                                CoreRes::Error(CoreError::Unknown)
                            }
                        };

                        if let Err(e) = done.send(r).await {
                            error!("Failed to forward DHT lookup result: {:?}", e);
                        }
                    });
                }
                OpKind::DhtLocate(id) => {
                    let dht = self.dht.clone();

                    tokio::task::spawn(async move {
                        let dht = dht.get_handle().await.unwrap();

                        let r = match dht.lookup(id.clone(), Default::default()).await {
                            Ok((p, i)) => CoreRes::Peers(vec![p.info().clone()], Some(i)),
                            Err(e) => {
                                error!("DHT lookup error: {e:?}");
                                CoreRes::Error(dht_to_core_error(e))
                            }
                        };

                        if let Err(e) = done.send(r).await {
                            error!("Failed to forward DHT lookup result: {:?}", e);
                        }
                    });
                }
                OpKind::DhtSearch(id) => {
                    let dht = self.dht.clone();

                    tokio::task::spawn(async move {
                        let dht = dht.get_handle().await.unwrap();

                        let r = match dht.search(id.clone(), Default::default()).await {
                            Ok((p, i)) => CoreRes::Pages(p, Some(i)),
                            Err(e) => {
                                error!("DHT search error: {e:?}");
                                CoreRes::Error(dht_to_core_error(e))
                            }
                        };

                        if let Err(e) = done.send(r).await {
                            error!("Failed to forward DHT search result: {:?}", e);
                        }
                    });
                }
                OpKind::DhtPut(id, pages) => {
                    let dht = self.dht.clone();

                    tokio::task::spawn(async move {
                        let dht = dht.get_handle().await.unwrap();

                        let r = match dht
                            .store(id.clone(), pages.clone(), Default::default())
                            .await
                        {
                            Ok((p, i)) => CoreRes::Peers(
                                p.iter().map(|p| p.info().clone()).collect(),
                                Some(i),
                            ),
                            Err(e) => {
                                error!("DHT store error: {e:?}");
                                CoreRes::Error(dht_to_core_error(e))
                            }
                        };

                        if let Err(e) = done.send(r).await {
                            error!("Failed to forward DHT store result: {:?}", e);
                        }
                    });
                }
                OpKind::DhtUpdate => {
                    let dht = self.dht.clone();

                    tokio::task::spawn(async move {
                        debug!("Start DHT update");
                        let dht = dht.get_handle().await.unwrap();

                        let r = match dht.update(true).await {
                            Ok(_p) => CoreRes::Ok,
                            Err(e) => {
                                error!("DHT update error: {e:?}");
                                CoreRes::Error(dht_to_core_error(e))
                            }
                        };

                        if let Err(e) = done.send(r).await {
                            error!("Failed to forward DHT update result: {:?}", e);
                        }
                    });
                }
                OpKind::PeerCreateUpdate(peer_info) => {
                    tokio::task::spawn(async move {
                        let r = match core.peer_create_or_update(peer_info).await {
                            Ok(p) => CoreRes::Peers(vec![p], None),
                            Err(e) => CoreRes::Error(e),
                        };

                        if let Err(e) = done.try_send(r) {
                            error!("Failed to send operation response: {:?}", e);
                        };
                    });
                }
                OpKind::PeerGet(id) => {
                    tokio::task::spawn(async move {
                        let r = match core.peer_get(&id).await {
                            Ok(p) => CoreRes::Peers(vec![p], None),
                            Err(e) => CoreRes::Error(e),
                        };

                        if let Err(e) = done.try_send(r) {
                            error!("Failed to send operation response: {:?}", e);
                        };
                    });
                }
                OpKind::PeerInfo(ident) => {
                    tokio::task::spawn(async move {
                        let r = match core.peer_get(ident).await {
                            Ok(p) => CoreRes::Peers(vec![p], None),
                            Err(e) => CoreRes::Error(e),
                        };

                        if let Err(e) = done.try_send(r) {
                            error!("Failed to send operation response: {:?}", e);
                        };
                    });
                }
                OpKind::PeerList(_opts) => {
                    tokio::task::spawn(async move {
                        let r = match core.peer_list(Default::default()).await {
                            Ok(p) => CoreRes::Peers(p, None),
                            Err(e) => CoreRes::Error(e),
                        };

                        if let Err(e) = done.try_send(r) {
                            error!("Failed to send operation response: {:?}", e);
                        };
                    });
                }
                OpKind::ObjectGet(ident, sig) => {
                    tokio::task::spawn(async move {
                        // Fetch object
                        let page = core.object_get(&ident, &sig).await;

                        // And return the response object if found
                        let r = match page {
                            Ok(p) => CoreRes::Objects(vec![p]),
                            _ => CoreRes::Error(CoreError::NotFound),
                        };
                        if let Err(e) = done.try_send(r) {
                            error!("Failed to send operation response: {:?}", e);
                        };
                    });
                }
                OpKind::ObjectPut(data) => {
                    // TODO: Lookup matching service / check prior to put

                    let store = self.store.clone();
                    let mut done = done.clone();

                    tokio::task::spawn(async move {
                        // Write data to local store
                        let res = store.object_put(data.clone()).await;

                        // Map results and forward to caller
                        let resp = match res {
                            Ok(_) => CoreRes::Sig(data.signature()),
                            // TODO: fix store error returns
                            Err(_e) => CoreRes::Error(CoreError::Unknown),
                        };
                        if let Err(e) = done.try_send(resp) {
                            error!("Failed to forward net response: {:?}", e);
                        }
                    });
                }
                OpKind::ObjectList(ident, bounds) => {
                    // Grab service credentials

                    tokio::task::spawn(async move {
                        // Fetch objects
                        let page = core.data_list(&ident, &bounds).await;

                        // And return the response object if found
                        let r = match page {
                            Ok(p) => CoreRes::Objects(p),
                            _ => CoreRes::Error(CoreError::NotFound),
                        };
                        if let Err(e) = done.try_send(r) {
                            error!("Failed to send operation response: {:?}", e);
                        };
                    });
                }
                OpKind::ReplicaGet(id) => {
                    tokio::task::spawn(async move {
                        let r = match core.replica_list(id.clone()).await {
                            Ok(v) => CoreRes::Replicas(v),
                            Err(e) => {
                                error!("Failed to locate replicas for service {id:?}: {e:?}");
                                CoreRes::Error(CoreError::Unknown)
                            }
                        };
                        if let Err(e) = done.try_send(r) {
                            error!("Failed to send operation response: {:?}", e);
                        };
                    });
                }
                OpKind::ReplicaUpdate(id, replicas) => {
                    tokio::task::spawn(async move {
                        let resp = match core.replica_create_or_update(id.clone(), replicas).await {
                            // TODO: return updated replica info?
                            Ok(r) => CoreRes::Replicas(r),
                            Err(e) => {
                                // TODO: propagate error?
                                error!("Failed to update replica: {:?}", e);
                                CoreRes::Error(e)
                            }
                        };
                        if let Err(e) = done.try_send(resp) {
                            error!("Failed to send operation response: {:?}", e);
                        };
                    });
                }
                OpKind::AuthList(service) => {
                    tokio::task::spawn(async move {
                        let resp = match core.service_auth_list(&service).await {
                            // TODO: return updated replica info?
                            Ok(r) => CoreRes::Auths(r),
                            Err(e) => {
                                // TODO: propagate error?
                                error!("Failed to list authorisations: {:?}", e);
                                CoreRes::Error(e)
                            }
                        };
                        if let Err(e) = done.try_send(resp) {
                            error!("Failed to send operation response: {:?}", e);
                        };
                    });
                }
                OpKind::AuthUpdate(service, peer_id, role) => {
                    tokio::task::spawn(async move {
                        let resp = match core.service_auth_update(&service, &peer_id, role).await {
                            // TODO: return updated replica info?
                            Ok(r) => CoreRes::Auths(r),
                            Err(e) => {
                                // TODO: propagate error?
                                error!("Failed to list authorisations: {:?}", e);
                                CoreRes::Error(e)
                            }
                        };
                        if let Err(e) = done.try_send(resp) {
                            error!("Failed to send operation response: {:?}", e);
                        };
                    });
                }
                OpKind::Net(ref body, ref peers) => {
                    let net = self.net.clone();

                    let req =
                        NetRequest::new(self.id(), rand::random(), body.clone(), Flags::default());
                    let targets: Vec<_> = peers
                        .iter()
                        .map(|p| (*p.address(), Some(p.id.clone())))
                        .collect();

                    tokio::task::spawn(async move {
                        let r = match net.net_request(targets, req, Default::default()).await {
                            Ok(v) => {
                                let v = HashMap::from_iter(
                                    v.iter().map(|(k, v)| (k.clone(), v.1.clone())),
                                );
                                CoreRes::Responses(v)
                            }
                            Err(_e) => CoreRes::Error(CoreError::Unknown),
                        };

                        if let Err(e) = done.send(r).await {
                            error!("Failed to forward net response: {:?}", e);
                        }
                    });
                }
                OpKind::NetBcast(ref body) => {
                    let net = self.net.clone();

                    let mut req = NetRequest::new(
                        self.id(),
                        rand::random(),
                        body.clone(),
                        Flags::PUB_KEY_REQUEST | Flags::ADDRESS_REQUEST,
                    );
                    req.set_public_key(self.service().public_key());

                    tokio::task::spawn(async move {
                        let r = match net.net_broadcast(req, Default::default()).await {
                            Ok(v) => {
                                let v = HashMap::from_iter(
                                    v.iter().map(|(k, v)| (k.clone(), v.1.clone())),
                                );
                                CoreRes::Responses(v)
                            }
                            Err(_e) => CoreRes::Error(CoreError::Unknown),
                        };

                        if let Err(e) = done.send(r).await {
                            error!("Failed to forward net response: {:?}", e);
                        }
                    });
                }
            }

            // Force wake next tick
            ctx.waker().wake_by_ref();
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

    async fn exec(&self, kind: OpKind) -> CoreRes {
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
            return CoreRes::Error(CoreError::IO);
        }

        // Trigger waker if available (not sure why this isn't happening automatically..?)
        if let Some(waker) = self.waker.as_ref() {
            waker.wake_by_ref();
        }

        // Await response message
        match done_rx.next().await {
            Some(r) => r,
            // TODO: Cancelled
            None => {
                warn!("RPC response channel closed");
                CoreRes::Error(CoreError::Unknown)
            }
        }
    }
}

/// Operations used when constructing higher level RPCs
pub struct Op {
    req_id: u64,
    kind: OpKind,
    done: mpsc::Sender<CoreRes>,
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
