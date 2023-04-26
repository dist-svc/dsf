//! Locate operation, looks up a service by ID using the database

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::SystemTime;

use dsf_core::wire::Container;
use futures::channel::mpsc;
use futures::prelude::*;

use log::{debug, error, info, warn};
use tracing::{span, Level};

use dsf_core::prelude::*;
use dsf_rpc::{self as rpc, LocateInfo, LocateOptions};

use crate::daemon::{net::NetIf, Dsf};
use crate::error::Error;

use crate::core::peers::Peer;
use crate::core::services::ServiceState;

use super::ops::*;

pub enum LocateState {
    Init,
    Pending(kad::dht::SearchFuture<Container>),
    Done,
    Error,
}

pub struct LocateOp {
    pub(crate) opts: LocateOptions,
    pub(crate) state: LocateState,
}

pub struct LocateFuture {
    rx: mpsc::Receiver<rpc::Response>,
}

impl Future for LocateFuture {
    type Output = Result<LocateInfo, DsfError>;

    fn poll(mut self: Pin<&mut Self>, ctx: &mut Context<'_>) -> Poll<Self::Output> {
        let resp = match self.rx.poll_next_unpin(ctx) {
            Poll::Ready(Some(r)) => r,
            _ => return Poll::Pending,
        };

        match resp.kind() {
            rpc::ResponseKind::Located(r) => Poll::Ready(Ok(r)),
            rpc::ResponseKind::Error(e) => Poll::Ready(Err(e.into())),
            _ => Poll::Pending,
        }
    }
}

impl<Net> Dsf<Net>
where
    Dsf<Net>: NetIf<Interface = Net>,
{
    pub fn locate(&mut self, options: LocateOptions) -> Result<LocateFuture, Error> {
        let req_id = rand::random();

        let (tx, rx) = mpsc::channel(1);

        // Create connect object
        let op = RpcOperation {
            req_id,
            kind: RpcKind::locate(options),
            done: tx,
        };

        // Add to tracking
        debug!("Adding RPC op {} to tracking", req_id);
        self.rpc_ops.insert(req_id, op);

        Ok(LocateFuture { rx })
    }

    pub fn poll_rpc_locate(
        &mut self,
        req_id: u64,
        create_op: &mut LocateOp,
        ctx: &mut Context,
        mut done: mpsc::Sender<rpc::Response>,
    ) -> Result<bool, DsfError> {
        let LocateOp { opts, state } = create_op;

        match state {
            LocateState::Init => {
                // Short-circuit for owned services
                match self.services().find(&opts.id) {
                    Some(service_info) if service_info.origin => {
                        let i = LocateInfo {
                            origin: true,
                            updated: false,
                            page_version: service_info.index as u16,
                            page: self
                                .services()
                                .filter(&opts.id, |s| s.primary_page.clone())
                                .flatten(),
                        };

                        let resp = rpc::Response::new(req_id, rpc::ResponseKind::Located(i));
                        done.try_send(resp).unwrap();

                        *state = LocateState::Done;
                    }
                    _ => (),
                }

                // Initiate search via DHT
                let (search, _) = match self.dht_mut().search(opts.id.clone()) {
                    Ok(r) => r,
                    Err(e) => {
                        error!("DHT store error: {:?}", e);
                        return Err(DsfError::Unknown);
                    }
                };

                *state = LocateState::Pending(search);
                Ok(false)
            }
            LocateState::Pending(search) => {
                match search.poll_unpin(ctx) {
                    Poll::Ready(Ok(v)) => {
                        debug!("DHT search complete! {:?}", v);

                        // Register or update service
                        let service_info = match self.service_register(&opts.id, v) {
                            Ok(i) => i,
                            Err(e) => {
                                error!("Error registering located service: {:?}", e);
                                // TODO: handle errors better?
                                *state = LocateState::Error;
                                return Ok(true);
                            }
                        };

                        // Return info
                        // TODO: only return updated when _new_ data is returned
                        let info = LocateInfo {
                            origin: false,
                            updated: true,
                            page_version: service_info.index as u16,
                            page: self
                                .services()
                                .filter(&opts.id, |s| s.primary_page.clone())
                                .flatten(),
                        };
                        let resp = rpc::Response::new(req_id, rpc::ResponseKind::Located(info));
                        done.try_send(resp).unwrap();

                        *state = LocateState::Done;

                        Ok(false)
                    }
                    Poll::Ready(Err(e)) => {
                        error!("DHT search error: {:?}", e);

                        // Check local registry
                        if let Some(i) = self.services().find(&opts.id) {
                            // Return info
                            let info = LocateInfo {
                                origin: false,
                                updated: false,
                                page_version: i.index as u16,
                                page: self
                                    .services()
                                    .filter(&opts.id, |s| s.primary_page.clone())
                                    .flatten(),
                            };
                            let resp = rpc::Response::new(req_id, rpc::ResponseKind::Located(info));
                            done.try_send(resp).unwrap();

                            *state = LocateState::Done;

                            return Ok(false);
                        }

                        // Otherwise, fail
                        let resp = rpc::Response::new(
                            req_id,
                            rpc::ResponseKind::Error(dsf_core::error::Error::Unknown),
                        );
                        done.try_send(resp).unwrap();

                        *state = LocateState::Error;

                        Ok(false)
                    }
                    Poll::Pending => Ok(false),
                }
            }
            _ => Ok(true),
        }
    }
}

#[async_trait::async_trait]
pub trait ServiceRegistry {
    /// Lookup a peer
    async fn service_locate(&self, options: LocateOptions) -> Result<LocateInfo, DsfError>;
}

#[async_trait::async_trait]
impl<T: Engine> ServiceRegistry for T {
    async fn service_locate(&self, opts: LocateOptions) -> Result<LocateInfo, DsfError> {
        info!("Locating service: {:?}", opts);

        // Check for existing / local information
        let local = self.service_get(opts.id.clone()).await;
        let local = match local {
            Ok(i) => {
                let page = match i.primary_page {
                    Some(sig) => Some(self.object_get(opts.id.clone(), sig).await?),
                    None => None,
                };

                Some(LocateInfo {
                    origin: i.origin,
                    updated: false,
                    page_version: i.index as u16,
                    page,
                })
            }
            _ => None,
        };

        // Short-circuit for owned services
        match &local {
            Some(i) if i.origin => return Ok(i.clone()),
            _ => (),
        }

        // Otherwise, lookup via DHT
        let pages = self.dht_search(opts.id.clone()).await;
        let pages = match (pages, &local) {
            (Ok(p), _) => p,
            (_, Some(i)) => {
                warn!("DHT search failed, using local service info");
                return Ok(i.clone());
            }
            (Err(e), _) => {
                error!("DHT search failed: {:?}", e);
                return Err(e.into());
            }
        };

        debug!("Found pages: {:?}", pages);

        debug!("Registering service {}", opts.id);

        // Register new service
        let i = self.service_register(opts.id.clone(), pages).await?;

        debug!("Registered: {:?}", i);

        let primary_page = match i.primary_page {
            Some(sig) => Some(self.object_get(opts.id.clone(), sig).await?),
            None => None,
        };

        debug!("Found page: {:?}", primary_page);

        // Return info
        let info = LocateInfo {
            origin: i.origin,
            updated: true,
            page_version: i.index as u16,
            // TODO: fetch related page
            page: primary_page,
        };

        Ok(info)
    }
}
