use std::{
    task::{Context, Poll},
    time::SystemTime,
};

use dsf_core::wire::Container;
use kad::{prelude::DhtError, store::Datastore};
use log::{debug, error, info, warn};
use tracing::{span, Level};

use futures::channel::mpsc;
use futures::prelude::*;

use dsf_core::prelude::*;
use dsf_rpc::*;

use crate::{
    core::{peers::Peer, services::ServiceInst},
    daemon::{
        net::{NetFuture, NetIf},
        Dsf,
    },
    error::{CoreError, Error},
    rpc::{
        bootstrap::Bootstrap, connect::Connect, create::CreateService, discover::Discover,
        locate::ServiceRegistry, lookup::PeerRegistry, ns::NameService, publish::PublishData,
        register::RegisterService, replicate::ReplicateService, subscribe::PubSub, sync::SyncData,
    },
};

// Generic / shared operation types
mod ops;
pub use ops::*;

pub mod bootstrap;
pub mod connect;
pub mod create;
pub mod discover;
pub mod locate;
pub mod lookup;
pub mod ns;
pub mod publish;
pub mod push;
pub mod register;
pub mod replicate;
pub mod subscribe;
pub mod sync;

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
                let mut peers: Vec<_> = self
                    .peers()
                    .list()
                    .drain(..)
                    .map(|(id, p)| (id, p.info()))
                    .collect();

                peers.sort_by_key(|(_, p)| p.index);

                Some(ResponseKind::Peers(peers))
            }
            RequestKind::Peer(PeerCommands::Info(options)) => {
                match self.resolve_peer_identifier(&options) {
                    Ok(id) => {
                        let p = self.peers().info(&id);
                        match p {
                            Some(p) => Some(ResponseKind::Peer(p)),
                            None => Some(ResponseKind::None),
                        }
                    }
                    Err(_e) => Some(ResponseKind::None),
                }
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
            RequestKind::Service(ServiceCommands::List(opts)) => {
                let mut services = self.services().list();
                services.sort_by_key(|s| s.index);

                // TODO: Filter by application ID
                if let Some(_a) = opts.application_id {}

                // Filter by service kind
                if let Some(k) = opts.kind {
                    services = services.drain(..).filter(|s| s.kind == k).collect();
                }

                // TODO: clear keys unless specifically requested
                Some(ResponseKind::Services(services))
            }
            RequestKind::Service(ServiceCommands::Info(options)) => {
                match self.resolve_identifier(&options.service) {
                    Ok(id) => Some(
                        self.services()
                            .find(&id)
                            .map(|i| {
                                // TODO: Clear keys unless specifically requested
                                ResponseKind::Service(i)
                            })
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
            RequestKind::Data(DataCommands::List(data::DataListOptions {
                service,
                page_bounds,
                time_bounds,
            })) => match self.resolve_identifier(&service) {
                Ok(id) => {
                    // Fetch data
                    let d = self.data().fetch_data(&id, &page_bounds, &time_bounds)?;

                    // Lookup private key
                    let _k = self.services().find(&id).map(|s| s.private_key).flatten();

                    let i = d.iter().map(|i| i.info.clone()).collect();

                    Some(ResponseKind::Data(i))
                }
                Err(_e) => Some(ResponseKind::None),
            },

            RequestKind::Data(DataCommands::Get(data::FetchOptions { service, page_sig })) => {
                match self.resolve_identifier(&service) {
                    Ok(id) => {
                        // Fetch data
                        let d = self.data().get_object(&id, &page_sig)?;

                        // Lookup private key
                        let _k = self.services().find(&id).map(|s| s.private_key).flatten();

                        // Build info array
                        let i = d.iter().map(|i| i.info.clone()).collect();

                        Some(ResponseKind::Data(i))
                    }
                    Err(_e) => Some(ResponseKind::None),
                }
            }

            RequestKind::Debug(DebugCommands::SetAddress { addr }) => {
                self.addresses = vec![addr.into()];

                // Update service address
                self.service().update(|_b, o, _p| {
                    let mut opts: Vec<_> = o.drain(..).filter(|o| !o.is_address_v4()).collect();
                    opts.push(Options::address(addr));
                    *o = opts;
                })?;

                // Re-generate primary page
                let c = self.primary(true)?;

                // Return the primary page
                Some(ResponseKind::Page(c))
            }

            RequestKind::Debug(DebugCommands::DhtNodes) => {
                use kad::table::NodeTable;

                let peers: Vec<_> = self
                    .dht_mut()
                    .nodetable()
                    .entries()
                    .map(|e| (e.id().clone(), e.info().info.clone()))
                    .collect();

                Some(ResponseKind::Peers(peers))
            }

            _ => None,
        };

        if let Some(k) = resp {
            let r = Response::new(req_id, k);
            done.clone().try_send(r).unwrap();
            return Ok(());
        }

        // Otherwise queue up request for async execution
        let mut exec = self.exec();

        match req.kind() {
            RequestKind::Peer(c) => {
                tokio::task::spawn(async move {
                    debug!("Starting async peer op");

                    let r = match c {
                        PeerCommands::Search(opts) => {
                            exec.peer_lookup(opts).await.map(ResponseKind::Peer)
                        }
                        PeerCommands::Connect(opts) => {
                            exec.connect(opts).await.map(ResponseKind::Connected)
                        }
                        _ => unimplemented!("{:?} not yet implemented", c),
                    };

                    debug!("Async peer rpc result: {:?}", r);
                    let r = match r {
                        Ok(r) => r,
                        Err(e) => ResponseKind::Error(e),
                    };

                    if let Err(e) = done.clone().try_send(Response::new(req_id, r)) {
                        error!("Failed to send RPC response: {:?}", e);
                    }
                });

                return Ok(());
            }
            RequestKind::Data(c) => {
                tokio::task::spawn(async move {
                    debug!("Starting async data rpc: {:?}", c);
                    let r = match c {
                        DataCommands::Publish(opts) => {
                            exec.publish(opts).await.map(|v| ResponseKind::Published(v))
                        }
                        DataCommands::Sync(opts) => exec.sync(opts).await.map(ResponseKind::Sync),
                        _ => unimplemented!(),
                    };

                    debug!("Async data rpc result: {:?}", r);
                    let r = match r {
                        Ok(r) => r,
                        Err(e) => ResponseKind::Error(e),
                    };

                    if let Err(e) = done.clone().try_send(Response::new(req_id, r)) {
                        error!("Failed to send RPC response: {:?}", e);
                    }
                });
                return Ok(());
            }
            RequestKind::Service(c) => {
                tokio::task::spawn(async move {
                    debug!("Starting async service rpc: {:?}", c);
                    let r = match c {
                        ServiceCommands::Create(opts) => exec
                            .service_create(opts)
                            .await
                            .map(|v| ResponseKind::Service(v)),
                        ServiceCommands::Locate(opts) => exec
                            .service_locate(opts)
                            .await
                            .map(|v| ResponseKind::Located(vec![v])),
                        ServiceCommands::Register(opts) => exec
                            .service_register(opts)
                            .await
                            .map(|v| ResponseKind::Registered(v)),
                        ServiceCommands::Replicate(opts) => exec
                            .service_replicate(opts)
                            .await
                            .map(|v| ResponseKind::Registered(v)),
                        ServiceCommands::Subscribe(opts) => {
                            exec.subscribe(opts).await.map(ResponseKind::Subscribed)
                        }
                        ServiceCommands::Discover(opts) => {
                            exec.discover(opts).await.map(ResponseKind::Services)
                        }
                        _ => unimplemented!(),
                    };

                    debug!("Async service rpc result: {:?}", r);
                    let r = match r {
                        Ok(r) => r,
                        Err(e) => ResponseKind::Error(e),
                    };

                    if let Err(e) = done.clone().try_send(Response::new(req_id, r)) {
                        error!("Failed to send RPC response: {:?}", e);
                    }
                });
                return Ok(());
            }
            RequestKind::Ns(c) => {
                tokio::task::spawn(async move {
                    debug!("Starting NS op: {:?}", c);
                    // Run NS operation
                    let r = match c {
                        NsCommands::Create(opts) => {
                            exec.ns_create(opts).await.map(ResponseKind::Service)
                        }
                        NsCommands::Register(opts) => {
                            exec.ns_register(opts).await.map(ResponseKind::Ns)
                        }
                        NsCommands::Search(opts) => {
                            exec.ns_search(opts).await.map(ResponseKind::Located)
                        }
                    };
                    let r = match r {
                        Ok(v) => v,
                        Err(e) => {
                            error!("NS operation failed: {:?}", e);
                            ResponseKind::Error(e)
                        }
                    };
                    if let Err(e) = done.clone().try_send(Response::new(req_id, r)) {
                        error!("Failed to send RPC response: {:?}", e);
                    }
                });
                return Ok(());
            }
            RequestKind::Debug(c) => {
                tokio::task::spawn(async move {
                    debug!("Starting async debug task");
                    let r = match c {
                        DebugCommands::Search { id } => match exec.dht_search(id).await {
                            Ok(i) => ResponseKind::Pages(i),
                            Err(e) => ResponseKind::Error(e),
                        },
                        DebugCommands::Bootstrap => match exec.bootstrap().await {
                            Ok(i) => ResponseKind::Bootstrap(i),
                            Err(e) => ResponseKind::Error(e),
                        },
                        DebugCommands::Update => match exec.dht_update().await {
                            Ok(_i) => ResponseKind::None,
                            Err(e) => ResponseKind::Error(e),
                        },
                        _ => ResponseKind::Error(DsfError::Unimplemented),
                    };

                    if let Err(e) = done.clone().try_send(Response::new(req_id, r)) {
                        error!("Failed to send RPC response: {:?}", e);
                    }
                });
                return Ok(());
            }
            _ => {
                error!("RPC operation {:?} not yet implemented", req.kind());
                return Ok(());
            }
        }
    }
}
