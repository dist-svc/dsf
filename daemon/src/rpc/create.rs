use std::convert::TryFrom;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::SystemTime;

use futures::channel::mpsc;
use futures::prelude::*;

use log::{debug, error, info, warn};
use tracing::{span, Level};

use dsf_core::options::Options;
use dsf_core::prelude::*;
use dsf_core::service::Publisher;

use dsf_rpc::{self as rpc, CreateOptions, RegisterOptions, ServiceIdentifier};

use crate::daemon::{net::NetIf, Dsf};
use crate::error::Error;

use crate::core::peers::Peer;
use crate::core::services::ServiceState;
use crate::core::services::*;

use super::ops::*;

pub enum CreateState {
    Init,
    Pending(kad::dht::StoreFuture<Id, Peer>),
    Done,
    Error,
}

pub struct CreateOp {
    pub(crate) id: Option<Id>,
    pub(crate) opts: CreateOptions,
    pub(crate) state: CreateState,
}

pub struct CreateFuture {
    rx: mpsc::Receiver<rpc::Response>,
}

impl Future for CreateFuture {
    type Output = Result<ServiceInfo, DsfError>;

    fn poll(mut self: Pin<&mut Self>, ctx: &mut Context<'_>) -> Poll<Self::Output> {
        let resp = match self.rx.poll_next_unpin(ctx) {
            Poll::Ready(Some(r)) => r,
            _ => return Poll::Pending,
        };

        match resp.kind() {
            rpc::ResponseKind::Service(r) => Poll::Ready(Ok(r)),
            rpc::ResponseKind::Error(e) => Poll::Ready(Err(e.into())),
            _ => Poll::Pending,
        }
    }
}

impl<Net> Dsf<Net>
where
    Dsf<Net>: NetIf<Interface = Net>,
{
    /// Create (and publish) a new CreateOptions
    pub fn create(&mut self, options: CreateOptions) -> Result<CreateFuture, DsfError> {
        let req_id = rand::random();

        let (tx, rx) = mpsc::channel(1);

        // Create connect object
        let op = RpcOperation {
            req_id,
            kind: RpcKind::create(options),
            done: tx,
        };

        // Add to tracking
        debug!("Adding RPC op {} to tracking", req_id);
        self.rpc_ops.insert(req_id, op);

        Ok(CreateFuture { rx })
    }

    pub fn poll_rpc_create(
        &mut self,
        req_id: u64,
        create_op: &mut CreateOp,
        ctx: &mut Context,
        mut done: mpsc::Sender<rpc::Response>,
    ) -> Result<bool, DsfError> {
        let CreateOp { id, opts, state } = create_op;

        match state {
            CreateState::Init => {
                info!("Creating service: {:?}", opts);
                let mut sb = ServiceBuilder::generic();

                if let Some(kind) = opts.page_kind {
                    sb = sb.kind(kind);
                }

                // Attach a body if provided
                if let Some(body) = &opts.body {
                    sb = sb.body(body.clone());
                } else {
                    sb = sb.body(vec![]);
                }

                // Append supplied public and private options
                sb = sb.public_options(opts.public_options.clone());
                sb = sb.private_options(opts.private_options.clone());

                // Append addresses as private options
                sb = sb.private_options(
                    opts.addresses
                        .iter()
                        .map(|v| Options::address(v.clone()))
                        .collect(),
                );

                // TODO: append metadata
                for _m in &opts.metadata {
                    //TODO
                }

                // If the service is not public, encrypt the object
                if !opts.public {
                    sb = sb.encrypt();
                }

                debug!("Generating service");
                let mut service = sb.build().unwrap();
                *id = Some(service.id());

                debug!("Generating service page");
                // TODO: revisit this
                let buff = vec![0u8; 1024];
                let (_n, primary_page) = service.publish_primary(Default::default(), buff).unwrap();

                //primary_page.raw = Some(buff[..n].to_vec());

                // Register service in local database
                debug!("Storing service information");
                self.services()
                    .register(service, &primary_page, ServiceState::Created, None)
                    .unwrap();

                let pages = vec![primary_page];

                // TODO: Write pages to storage

                // Register to database if enabled
                if opts.register {
                    // Store pages
                    let (store, _req_id) = match self.dht_mut().store(id.clone().unwrap(), pages) {
                        Ok(r) => r,
                        Err(e) => {
                            error!("DHT store error: {:?}", e);
                            return Err(DsfError::Unknown);
                        }
                    };

                    *state = CreateState::Pending(store);
                    Ok(false)
                } else {
                    // Return info for newly created service
                    let info = self
                        .services()
                        .update_inst(id.as_ref().unwrap(), |s| ())
                        .unwrap();

                    let resp = rpc::Response::new(req_id, rpc::ResponseKind::Service(info));
                    done.try_send(resp).unwrap();

                    *state = CreateState::Done;

                    Ok(false)
                }
            }
            CreateState::Pending(store) => {
                match store.poll_unpin(ctx) {
                    Poll::Ready(Ok(v)) => {
                        debug!("DHT store complete! {:?}", v);

                        // Update service
                        let info = self
                            .services()
                            .update_inst(id.as_ref().unwrap(), |s| {
                                s.state = ServiceState::Registered;
                                s.last_updated = Some(SystemTime::now());
                            })
                            .unwrap();

                        // Return info
                        let resp = rpc::Response::new(req_id, rpc::ResponseKind::Service(info));
                        done.try_send(resp).unwrap();

                        *state = CreateState::Done;

                        Ok(false)
                    }
                    Poll::Ready(Err(kad::prelude::DhtError::NoPeers)) => {
                        warn!("No peers for registration");

                        let info = self
                            .services()
                            .update_inst(id.as_ref().unwrap(), |s| ())
                            .unwrap();

                        let resp = rpc::Response::new(req_id, rpc::ResponseKind::Service(info));
                        done.try_send(resp).unwrap();

                        *state = CreateState::Done;

                        Ok(false)
                    }
                    Poll::Ready(Err(e)) => {
                        warn!("DHT store error: {:?}", e);

                        let resp = rpc::Response::new(
                            req_id,
                            rpc::ResponseKind::Error(dsf_core::error::Error::Unknown),
                        );

                        done.try_send(resp).unwrap();

                        *state = CreateState::Error;

                        Ok(false)
                    }
                    _ => Ok(false),
                }
            }
            CreateState::Done => Ok(true),
            CreateState::Error => Ok(true),
        }
    }
}
