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

/// Async RPC handler abstraction
pub trait Rpc {
    /// Handle an RPC request, returning an RPC response
    async fn handle_rpc(&mut self, req: Request) -> Response;
}

/// Async RPC handler implementation for [Engine] types
impl <T: Engine> Rpc for T {
    async fn handle_rpc(&mut self, req: Request) -> Response {
        let req_id = req.req_id();

        match req.kind() {
            RequestKind::Peer(c) => {
                debug!("Starting async peer op");

                let r = match c {
                    PeerCommands::Search(opts) => self.peer_lookup(opts).await.map(ResponseKind::Peer),
                    PeerCommands::Connect(opts) => self.connect(opts).await.map(ResponseKind::Connected),
                    PeerCommands::Block(_) => todo!(),
                    PeerCommands::Unblock(_) => todo!(),
                    PeerCommands::List(_) => todo!(),
                    PeerCommands::Info(_) => todo!(),
                    PeerCommands::Remove(_) => todo!(),
                };

                debug!("Async peer rpc result: {:?}", r);
                let r = match r {
                    Ok(r) => r,
                    Err(e) => ResponseKind::Error(e),
                };

                return Response::new(req_id, r);
            }
            RequestKind::Service(c) => {
                debug!("Starting async service rpc: {:?}", c);
                let r = match c {
                    ServiceCommands::List(opts) => self
                        .svc_list(opts)
                        .await
                        .map(ResponseKind::Services)
                        .map_err(DsfError::from),
                    ServiceCommands::Info(_) => todo!(),
                    ServiceCommands::Create(opts) => self
                        .service_create(opts)
                        .await
                        .map(ResponseKind::Service),
                    ServiceCommands::Locate(opts) => self
                        .service_locate(opts)
                        .await
                        .map(|v| ResponseKind::Located(vec![v])),
                    ServiceCommands::Register(opts) => self
                        .service_register(opts)
                        .await
                        .map(|v| ResponseKind::Registered(v)),
                    ServiceCommands::Replicate(opts) => self
                        .service_replicate(opts)
                        .await
                        .map(|v| ResponseKind::Registered(v)),
                    ServiceCommands::Subscribe(opts) => self.subscribe(opts).await.map(ResponseKind::Subscribed),
                    ServiceCommands::Unsubscribe(_) => todo!(),
                    ServiceCommands::Discover(opts) => self.discover(opts).await.map(ResponseKind::Services),
                    ServiceCommands::SetKey(opts) => todo!(),
                    ServiceCommands::Remove(opts) => todo!(),
                    ServiceCommands::GetKey(_) => todo!(),
                };

                debug!("Async service rpc result: {:?}", r);
                let r = match r {
                    Ok(r) => r,
                    Err(e) => ResponseKind::Error(e),
                };

                return Response::new(req_id, r);
            }
            RequestKind::Data(c) => {
                debug!("Starting async data rpc: {:?}", c);
                let r = match c {
                    DataCommands::Publish(opts) => self.publish(opts).await.map(|v| ResponseKind::Published(v)),
                    DataCommands::Sync(opts) => self.sync(opts).await.map(ResponseKind::Sync),
                    DataCommands::List(_) => todo!(),
                    DataCommands::Query {  } => todo!(),
                    DataCommands::Push(_) => todo!(),
                    DataCommands::Get(_) => todo!(),
                };

                debug!("Async data rpc result: {:?}", r);
                let r = match r {
                    Ok(r) => r,
                    Err(e) => ResponseKind::Error(e),
                };

                return Response::new(req_id, r)
            }
            RequestKind::Ns(c) => {
                debug!("Starting NS op: {:?}", c);
                // Run NS operation
                let r = match c {
                    NsCommands::Create(opts) => self.ns_create(opts).await.map(ResponseKind::Service),
                    NsCommands::Register(opts) => self.ns_register(opts).await.map(ResponseKind::NsRegister),
                    NsCommands::Search(opts) => self.ns_search(opts).await.map(ResponseKind::NsSearch),
                };
                let r = match r {
                    Ok(v) => v,
                    Err(e) => {
                        error!("NS operation failed: {:?}", e);
                        ResponseKind::Error(e)
                    }
                };

                return Response::new(req_id, r);
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

               return Response::new(req_id, r);
            }
            _ => {
                error!("RPC operation {:?} not yet implemented", req.kind());

                return Response::new(req_id, ResponseKind::Error(DsfError::Unimplemented));
            }
        }
    }
}

impl<Net> Dsf<Net>
where
    Dsf<Net>: NetIf<Interface = Net>,
{
    // Create a new RPC operation
    pub fn start_rpc(&mut self, req: Request, done: RpcSender) -> Result<(), Error> {
        let req_id = req.req_id();

        // Force wake next tick
        if let Some(waker) = self.waker.as_ref() {
            waker.clone().wake();
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

        return Ok(());
    }
}
