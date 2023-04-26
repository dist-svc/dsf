//! Register operation, used to enrol a service in the database

use std::convert::TryFrom;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::SystemTime;

use futures::channel::mpsc;
use futures::prelude::*;
use log::{debug, error, info, trace, warn};
use tracing::{span, Level};

use dsf_core::options::Options;
use dsf_core::prelude::*;
use dsf_rpc::{self as rpc, RegisterInfo, RegisterOptions};

use crate::core::peers::Peer;
use crate::core::services::ServiceState;

use super::ops::*;
use crate::daemon::{net::NetIf, Dsf};
use crate::error::Error as DsfError;

pub enum RegisterState {
    Init,
    Pending(kad::dht::StoreFuture<Id, Peer>),
    Done,
    Error,
}

pub struct RegisterOp {
    pub(crate) opts: RegisterOptions,
    pub(crate) state: RegisterState,
}

pub struct RegisterFuture {
    rx: mpsc::Receiver<rpc::Response>,
}

unsafe impl Send for RegisterFuture {}

impl Future for RegisterFuture {
    type Output = Result<RegisterInfo, DsfError>;

    fn poll(mut self: Pin<&mut Self>, ctx: &mut Context<'_>) -> Poll<Self::Output> {
        let resp = match self.rx.poll_next_unpin(ctx) {
            Poll::Ready(Some(r)) => r,
            _ => return Poll::Pending,
        };

        match resp.kind() {
            rpc::ResponseKind::Registered(r) => Poll::Ready(Ok(r)),
            rpc::ResponseKind::Error(e) => Poll::Ready(Err(e.into())),
            _ => Poll::Pending,
        }
    }
}

impl<Net> Dsf<Net>
where
    Dsf<Net>: NetIf<Interface = Net>,
{
    /// Register a locally known service
    pub fn register(&mut self, options: RegisterOptions) -> Result<RegisterFuture, DsfError> {
        let req_id = rand::random();

        let (tx, rx) = mpsc::channel(1);

        // Create connect object
        let op = RpcOperation {
            req_id,
            kind: RpcKind::register(options),
            done: tx,
        };

        // Add to tracking
        debug!("Adding RPC op {} (register) to tracking", req_id);
        self.rpc_ops.insert(req_id, op);

        Ok(RegisterFuture { rx })
    }

    pub fn poll_rpc_register(
        &mut self,
        req_id: u64,
        register_op: &mut RegisterOp,
        ctx: &mut Context,
        mut done: mpsc::Sender<rpc::Response>,
    ) -> Result<bool, DsfError> {
        let span = span!(Level::DEBUG, "register");
        let _enter = span.enter();

        let RegisterOp { opts, state } = register_op;

        let id = self.resolve_identifier(&opts.service)?;

        match state {
            RegisterState::Init => {
                info!("Register: {:?}", &opts.service);

                // Fetch the known service from the service list
                if self.services().find(&id).is_none() {
                    // Only known services can be registered
                    error!("unknown service (id: {})", id);
                    // TODO: this shouldn't be a return
                    return Err(DsfError::UnknownService.into());
                };

                let mut pages = vec![];
                let mut page_version = 0u16;
                //let mut _replica_version = None;

                // Generate pages / update service instance
                // TODO: should be viable to replicate _non hosted_ services, this logic may not support this
                let service_info = self.services().update_inst(&id, |s| {
                    debug!("Fetching/generating service page");
                    match s.publish(false) {
                        Ok(p) => {
                            debug!("Using page index: {}", p.header().index());
                            page_version = p.header().index();
                            pages.push(p.clone());
                        }
                        Err(e) => {
                            error!("Error generating primary page: {:?}", e);
                            return;
                        }
                    };
                });

                let service_info = match service_info {
                    Some(v) => v,
                    None => return Err(DsfError::UnknownService.into()),
                };

                // Generate replica page unless disabled
                let replica_version = if !opts.no_replica {
                    // Check if we have an existing replica
                    let existing = self
                        .services()
                        .with(&id, |s| s.replica_page.clone())
                        .flatten();

                    let last_version = existing.as_ref().map(|p| p.header().index()).unwrap_or(0);

                    let replica_page = match existing {
                        Some(p) if !p.expired() => {
                            debug!("Using existing replica page");
                            p
                        }
                        _ => {
                            debug!("Generating new replica page");

                            // Setup replica options
                            let opts = SecondaryOptions {
                                page_kind: PageKind::Replica.into(),
                                version: last_version + 1,
                                public_options: &[Options::public_key(self.service().public_key())],
                                ..Default::default()
                            };

                            // Encode / sign page so this is valid for future propagation
                            let buff = vec![0u8; 1024];
                            let (_n, rp) =
                                self.service().publish_secondary(&id, opts, buff).unwrap();

                            // Update service instance
                            self.services().update_inst(&id, |s| {
                                s.replica_page = Some(rp.to_owned());
                                s.changed = true;
                            });

                            rp
                        }
                    };

                    let replica_version = replica_page.header().index();
                    pages.push(replica_page);

                    Some(replica_version)
                } else {
                    None
                };

                debug!("Registering service");
                trace!("Pages: {:?}", pages);

                if self.peers().seen_count() == 0 {
                    warn!("No active peers, registration only effects local node");

                    let i = RegisterInfo {
                        // TODO: fix page and replica versions
                        page_version: service_info.index as u16,
                        replica_version,
                        peers: 0,
                    };

                    let resp = rpc::Response::new(req_id, rpc::ResponseKind::Registered(i));
                    done.try_send(resp).unwrap();

                    *state = RegisterState::Done;
                    return Ok(true);
                }

                // Store pages
                let (store, _req_id) = match self.dht_mut().store(id, pages) {
                    Ok(r) => r,
                    Err(e) => {
                        error!("DHT store error: {:?}", e);
                        return Err(DsfError::Unknown);
                    }
                };

                *state = RegisterState::Pending(store);
                Ok(false)
            }
            RegisterState::Pending(store) => {
                match store.poll_unpin(ctx) {
                    Poll::Ready(Ok(v)) => {
                        debug!("DHT store complete! {:?}", v);

                        // Update service
                        self.services().update_inst(&id, |s| {
                            s.state = ServiceState::Registered;
                            s.last_updated = Some(SystemTime::now());
                        });

                        // TODO: push updated registration to known subscribers?

                        // Return info
                        let i = RegisterInfo {
                            // TODO: fix page and replica versions
                            page_version: 0,
                            replica_version: None,
                            peers: v.len(),
                        };

                        let resp = rpc::Response::new(req_id, rpc::ResponseKind::Registered(i));
                        done.try_send(resp).unwrap();

                        *state = RegisterState::Done;

                        Ok(true)
                    }
                    Poll::Ready(Err(e)) => {
                        error!("DHT store error: {:?}", e);

                        let resp = rpc::Response::new(
                            req_id,
                            rpc::ResponseKind::Error(dsf_core::error::Error::Unknown),
                        );
                        done.try_send(resp).unwrap();

                        *state = RegisterState::Error;

                        Ok(true)
                    }
                    _ => Ok(false),
                }
            }
            RegisterState::Done => Ok(true),
            RegisterState::Error => Ok(true),
        }
    }
}
