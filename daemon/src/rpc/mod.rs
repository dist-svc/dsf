//! RPC contains high-level DSF functionality, implemented on top of the
//! [Engine] abstraction to allow unit testing of each function.

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
    core::services::ServiceInst,
    daemon::{net::NetIf, Dsf},
    error::{CoreError, Error},
    rpc::{
        bootstrap::Bootstrap, connect::Connect, control::Control, create::CreateService, data::PublishData,
        discover::Discover, locate::ServiceRegistry, lookup::PeerRegistry, ns::NameService,
        register::RegisterService, replicate::ReplicateService, subscribe::PubSub, sync::SyncData,
    },
};

// Generic / shared operation types
mod ops;
pub use ops::*;

pub mod bootstrap;
pub mod connect;
pub mod control;
pub mod create;
pub mod data;
pub mod discover;
pub mod locate;
pub mod lookup;
pub mod ns;
pub mod register;
pub mod replicate;
pub mod subscribe;
pub mod sync;

/// Async RPC handler abstraction
#[allow(async_fn_in_trait)]
pub trait Rpc {
    /// Handle an RPC request, returning an RPC response
    async fn handle_rpc(&mut self, req: Request) -> Response;
}

/// Async RPC handler implementation for [Engine] types
impl<T: Engine> Rpc for T {
    async fn handle_rpc(&mut self, req: Request) -> Response {
        let req_id = req.req_id();

        match req.kind() {
            RequestKind::Status => {
                debug!("Status request");

                // TODO(med): fill in status information
                let info = StatusInfo {
                    id: self.id(),
                    peers: 0,
                    services: 0,
                    version: crate::VERSION.to_string(),
                };

                Response::new(req_id, ResponseKind::Status(info))
            }
            RequestKind::Peer(c) => {
                debug!("Starting async peer op");

                let r = match c {
                    PeerCommands::Search(opts) => self
                        .peer_lookup(opts)
                        .await
                        .map(|p| ResponseKind::Peers(vec![p])),
                    PeerCommands::Connect(opts) => {
                        self.connect(opts).await.map(ResponseKind::Connected)
                    }
                    PeerCommands::Block(_) => todo!("Block command not yet implemented"),
                    PeerCommands::Unblock(_) => todo!("Unblock command not yet implemented"),
                    PeerCommands::List(opts) => self.peer_list(opts).await.map(ResponseKind::Peers),
                    PeerCommands::Info(opts) => self
                        .peer_info(opts)
                        .await
                        .map(|p| ResponseKind::Peers(vec![p])),
                    PeerCommands::Remove(_) => todo!("Remove command not yet implemented"),
                };

                debug!("Async peer rpc result: {:?}", r);
                let r = match r {
                    Ok(r) => r,
                    Err(e) => ResponseKind::Error(e),
                };

                Response::new(req_id, r)
            }
            RequestKind::Service(c) => {
                debug!("Starting async service rpc: {:?}", c);
                let r = match c {
                    ServiceCommands::List(opts) => self
                        .svc_list(opts)
                        .await
                        .map(ResponseKind::Services)
                        .map_err(DsfError::from),
                    ServiceCommands::Info(opts) => self
                        .svc_get(opts.service)
                        .await
                        .map(|s| ResponseKind::Services(vec![s])),
                    ServiceCommands::Create(opts) => self
                        .service_create(opts)
                        .await
                        .map(|s| ResponseKind::Services(vec![s])),
                    ServiceCommands::Locate(opts) => self
                        .service_locate(opts)
                        .await
                        .map(|v| ResponseKind::Located(vec![v])),
                    ServiceCommands::Register(opts) => self
                        .service_register(opts)
                        .await
                        .map(ResponseKind::Registered),
                    ServiceCommands::Replicate(opts) => self
                        .service_replicate(opts)
                        .await
                        .map(ResponseKind::Registered),
                    ServiceCommands::Subscribe(opts) => {
                        self.subscribe(opts).await.map(ResponseKind::Subscribed)
                    }
                    ServiceCommands::Unsubscribe(opts) => {
                        self.unsubscribe(opts).await.map(|_| ResponseKind::None)
                    }
                    ServiceCommands::Discover(opts) => {
                        self.discover(opts).await.map(ResponseKind::Services)
                    }
                    ServiceCommands::SetKey(_opts) => todo!("SetKey needs reimplementation"),
                    ServiceCommands::Remove(_opts) => todo!("Remove not yet implemented"),
                    ServiceCommands::GetKey(_opts) => todo!("GetKey needs reimplementation"),
                    ServiceCommands::AuthList(opts) => {
                        self.svc_auth_list(opts).await.map(ResponseKind::Auths)
                    }
                    ServiceCommands::AuthUpdate(opts) => self
                        .svc_auth_update(opts.service, opts.peer_id, opts.role)
                        .await
                        .map(ResponseKind::Auths),
                };

                debug!("Async service rpc result: {:?}", r);
                let r = match r {
                    Ok(r) => r,
                    Err(e) => ResponseKind::Error(e),
                };

                Response::new(req_id, r)
            }
            RequestKind::Data(c) => {
                debug!("Starting async data rpc: {:?}", c);
                let r = match c {
                    DataCommands::Publish(opts) => {
                        self.data_publish(opts).await.map(ResponseKind::Published)
                    }
                    DataCommands::Sync(opts) => self.sync(opts).await.map(ResponseKind::Sync),
                    DataCommands::List(opts) => self
                        .object_list(opts.service, opts.page_bounds)
                        .await
                        .map(ResponseKind::Objects),
                    DataCommands::Get(opts) => self
                        .object_get(opts.service, opts.page_sig)
                        .await
                        .map(|o| ResponseKind::Objects(vec![o])),
                    DataCommands::Query {} => todo!("Data Query not yet implemented"),
                    DataCommands::Push(opts) => {
                        self.data_push(opts).await.map(ResponseKind::Published)
                    }
                };

                debug!("Async data rpc result: {:?}", r);
                let r = match r {
                    Ok(r) => r,
                    Err(e) => ResponseKind::Error(e),
                };

                Response::new(req_id, r)
            }
            RequestKind::Control(c) => {
                debug!("Starting async control rpc: {:?}", c);

                let r = match c {
                    ControlCommands::Write(opts) => {
                        self.control(opts).await.map(|_| ResponseKind::None)
                    }
                };

                debug!("Async control rpc result: {:?}", r);
                let r = match r {
                    Ok(r) => r,
                    Err(e) => ResponseKind::Error(e),
                };

                Response::new(req_id, r)
            }
            RequestKind::Ns(c) => {
                debug!("Starting NS op: {:?}", c);
                // Run NS operation
                let r = match c {
                    NsCommands::Create(opts) => self
                        .ns_create(opts)
                        .await
                        .map(|s| ResponseKind::Services(vec![s])),
                    NsCommands::Adopt(opts) => self
                        .ns_adopt(opts)
                        .await
                        .map(|s| ResponseKind::Services(vec![s])),
                    NsCommands::Register(opts) => {
                        self.ns_register(opts).await.map(ResponseKind::NsRegister)
                    }
                    NsCommands::Search(opts) => {
                        self.ns_search(opts).await.map(ResponseKind::NsSearch)
                    }
                };
                let r = match r {
                    Ok(v) => v,
                    Err(e) => {
                        error!("NS operation failed: {:?}", e);
                        ResponseKind::Error(e)
                    }
                };

                Response::new(req_id, r)
            }
            RequestKind::Debug(c) => {
                debug!("Starting async debug task");
                let r = match c {
                    DebugCommands::Search { id } => match self.dht_search(id).await {
                        Ok((p, _i)) => ResponseKind::Pages(p),
                        Err(e) => ResponseKind::Error(e),
                    },
                    DebugCommands::Bootstrap => match self.bootstrap().await {
                        Ok(i) => ResponseKind::Bootstrap(i),
                        Err(e) => ResponseKind::Error(e),
                    },
                    DebugCommands::Update => match self.dht_update().await {
                        Ok(_i) => ResponseKind::None,
                        Err(e) => ResponseKind::Error(e),
                    },
                    _ => ResponseKind::Error(DsfError::Unimplemented),
                };

                Response::new(req_id, r)
            }
            _ => {
                error!("RPC operation {:?} not yet implemented", req.kind());

                Response::new(req_id, ResponseKind::Error(DsfError::Unimplemented))
            }
        }
    }
}

impl<Net> Dsf<Net>
where
    Dsf<Net>: NetIf<Interface = Net>,
{
    // Create a new RPC operation
    pub fn start_rpc(&mut self, req: Request, mut done: RpcSender) -> Result<(), Error> {
        // Force wake next tick
        if let Some(waker) = self.waker.as_ref() {
            waker.wake_by_ref();
        }

        // Setup async rpc operation
        let mut exec = self.exec();
        tokio::task::spawn(async move {
            // Handle RPC request
            let resp = exec.handle_rpc(req).await;

            // Forward response
            if let Err(e) = done.send(resp).await {
                error!("Failed to forward RPC response: {e:?}");
            }
        });

        Ok(())
    }
}
