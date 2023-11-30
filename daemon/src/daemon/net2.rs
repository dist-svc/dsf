use std::{collections::HashMap, time::Duration};

use futures::{channel::mpsc::{UnboundedReceiver, UnboundedSender, unbounded as unbounded_channel}, StreamExt as _, SinkExt as _};
use tokio::{
    select,
    sync::{
        oneshot::Sender as OneshotSender,
    },
};
use tracing::{debug, error, info, instrument, trace, warn};

use dsf_core::{
    net::*,
    prelude::*,
    types::address::{IPV4_BROADCAST, IPV6_BROADCAST},
};
use dsf_rpc::PeerInfo;

use crate::error::Error;

/// Async network controller
/// 
/// This handles routing for network requests and responses using a decoupled async
/// task so these can be executed from an async context without blocking other
/// network operations.
#[derive(Clone)]
pub struct AsyncNet {
    /// Channel for sending executing network requests
    ctl: UnboundedSender<NetCtl>,
}

/// Network request object
#[derive(Debug)]
enum NetCtl {
    Register(u16, UnboundedSender<(Address, Id, Response)>),
    Unregister(u16),
    Send(Vec<(Address, Option<Id>)>, Message),
    Handle(Address, Id, Response),
    Exit,
}

/// Options for configuring network requests
pub struct NetRequestOpts {
    /// Timeout awaiting network responses
    pub timeout: Duration,
    /// Number of retries for each network request
    pub retries: usize,
}

impl Default for NetRequestOpts {
    fn default() -> Self {
        Self {
            timeout: Duration::from_secs(2),
            retries: 3,
        }
    }
}

impl AsyncNet {
    /// Create a new async network routing task
    /// 
    /// This creates the singleton manager task and returns `.clone()`able handles for sharing
    pub fn new(sender: UnboundedSender<(Vec<(Address, Option<Id>)>, Message)>) -> Self {
        // Setup control channel
        let (tx, mut rx) = unbounded_channel();

        // Setup network mux task
        tokio::task::spawn(async move {
            debug!("Starting async network router");

            let mut handles = HashMap::new();

            // Handle network requests
            while let Some(op) = rx.next().await {
                match op {
                    NetCtl::Register(req_id, resp) => {
                        trace!("Adding response handler for {req_id}");
                        handles.insert(req_id, resp);
                    }
                    NetCtl::Unregister(req_id) => {
                        trace!("Removing response handler for {req_id}");
                        handles.remove(&req_id);
                    }
                    NetCtl::Send(targets, msg) => {
                        trace!("Sending {} to {}", msg.common().id, targets.len());
                        if let Err(e) = sender.send((targets, msg)).await {
                            error!("Network send failed: {e:?}");
                            break;
                        }
                    }
                    NetCtl::Handle(addr, id, resp) => {
                        trace!("Routing response for {} from {addr:?}", resp.id);
                        match handles.get(&resp.id) {
                            Some(h) => {
                                h.send((addr, id, resp));
                            }
                            None => {
                                warn!("No handler for response {}", resp.id);
                            }
                        }
                    }
                    NetCtl::Exit => {
                        debug!("Exiting async network router");
                        break;
                    }
                }
            }
        });

        Self { ctl: tx }
    }

    /// Issue a request to the specified targets
    #[instrument(skip_all, fields(req_id = req.id))]
    pub async fn net_request(
        &self,
        mut targets: Vec<(Address, Option<Id>)>,
        req: Request,
        opts: NetRequestOpts,
    ) -> Result<HashMap<Id, (Address, Response)>, Error> {
        let req_id = req.id;
        let mut ctl = self.ctl.clone();

        // Setup response channel and register with network router
        let (tx, mut rx) = unbounded_channel();

        if let Err(e) = ctl.send(NetCtl::Register(req_id, tx)).await {
            error!("Failed to register net request: {e:?}");
            return Err(Error::Closed);
        }

        let mut responses = HashMap::new();

        for i in 0..opts.retries {
            debug!("retry {i} to {} targets", targets.len());

            // Send request to targets
            if let Err(e) = ctl.send(NetCtl::Send(targets.clone(), req.clone().into())).await {
                error!("Failed to send net request: {e:?}");

                // Close router channel
                let _ = ctl.send(NetCtl::Unregister(req_id)).await;

                return Err(Error::Closed);
            }

            // Await responses from router
            loop {
                select! {
                    // Handle responses
                    Some((addr, id, resp)) = rx.next() => {
                        // Add to response map
                        responses.insert(id, (addr.clone(), resp));
                        // Remove responder from target list for next retry
                        targets.retain(|(a, _i)| *a != addr);

                        // Break when all targets have responded
                        if targets.len() == 0 {
                            break;
                        }
                    }
                    // Handle timeouts
                    _ = tokio::time::sleep(opts.timeout) => {
                        break;
                    }
                }
            }
        }

        // Close router channel
        let _ = ctl.send(NetCtl::Unregister(req_id)).await;

        debug!("received {} responses", responses.len());

        // Return responses
        Ok(responses)
    }

    /// Issue a broadcast request
    #[instrument(skip_all, fields(req_id = req.id))]
    pub async fn net_broadcast(
        &self,
        req: Request,
        opts: NetRequestOpts,
    ) -> Result<HashMap<Id, (Address, Response)>, Error> {
        let mut ctl = self.ctl.clone();
        let req_id = req.id;

        // Setup response channel and register with network router
        let (tx, mut rx) = unbounded_channel();
        if let Err(e) = ctl.send(NetCtl::Register(req_id, tx)).await {
            error!("Failed to register net request: {e:?}");
            return Err(Error::Closed);
        }

        let targets = [(IPV4_BROADCAST.into(), None), (IPV6_BROADCAST.into(), None)];
        let mut responses = HashMap::new();

        for i in 0..opts.retries {
            debug!("broadcast retry {i}");

            // Send request to targets
            if let Err(e) = ctl.send(NetCtl::Send(targets.to_vec(), req.clone().into())).await {
                error!("Failed to send net request: {e:?}");

                // Close router channel
                let _ = ctl.send(NetCtl::Unregister(req_id)).await;

                return Err(Error::Closed);
            }

            // Await responses from router
            loop {
                select! {
                    // Handle responses
                    Some((addr, id, resp)) = rx.next() => {
                        // Add to response map
                        responses.insert(id, (addr, resp));
                    }
                    // Handle timeouts
                    _ = tokio::time::sleep(opts.timeout) => {
                        break;
                    }
                }
            }
        }

        // Close router channel
        let _ = ctl.send(NetCtl::Unregister(req_id)).await;

        debug!("received {} responses", responses.len());

        // Return responses
        Ok(responses)
    }

    /// Issue an untracked message to the specified targets
    #[instrument(skip_all, fields(req_id = msg.common().id))]
    pub async fn net_send(
        &self,
        targets: Vec<(Address, Option<Id>)>,
        msg: Message,
    ) -> Result<(), Error> {
        let mut ctl = self.ctl.clone();

        ctl.send(NetCtl::Send(targets, msg))
            .await
            .map_err(|_| Error::Closed )
    }

    /// Handle incoming responses (routed back to request contexts)
    pub async fn handle_resp(&self, addr: Address, id: Id, resp: Response) -> Result<(), Error> {
        let mut ctl = self.ctl.clone();

        ctl
            .send(NetCtl::Handle(addr, id, resp))
            .await
            .map_err(|_| Error::Closed)
    }
}

// TODO(med): add tests for this module
