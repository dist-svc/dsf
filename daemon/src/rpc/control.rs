//! Control operation, used to issue control messages to known services

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::SystemTime;

use dsf_core::options::Filters;
use futures::channel::mpsc;
use futures::future::join_all;
use futures::prelude::*;

use log::{debug, error, info, trace, warn};
use rpc::{PeerInfo, QosPriority, ReplicaInfo, UnsubscribeOptions};
use tracing::{span, Level};

use dsf_core::error::Error as CoreError;
use dsf_core::net;
use dsf_core::prelude::*;
use dsf_rpc::{
    self as rpc, ControlWriteOptions, ServiceState, SubscribeOptions, SubscriptionInfo,
    SubscriptionKind,
};

use crate::rpc::data::{publish_data, push_data};
use crate::{
    core::{replicas::ReplicaInst, CoreRes},
    daemon::Dsf,
    error::Error,
};

use super::{ops::*, subscribe::find_replicas};

pub enum SubscribeState {
    Init,
    Searching,
    Locating,
    Pending,
    Error(Error),
    Done,
}

#[allow(async_fn_in_trait)]
pub trait Control {
    /// Write a control message to a known service
    async fn control(&self, options: ControlWriteOptions) -> Result<(), DsfError>;
}

impl<T: Engine> Control for T {
    async fn control(&self, options: ControlWriteOptions) -> Result<(), DsfError> {
        info!("Sending control to service: {:?}", options);

        // Lookup local service information
        let target = match self.svc_get(options.service.clone()).await {
            Ok(v) => v,
            Err(e) => {
                error!(
                    "Failed to find information for service {:?}: {:?}",
                    options.service, e
                );
                return Err(e);
            }
        };

        // TODO: generate control message
        let our_id = self.id();

        // Route message by connection type

        // TODO: probably tidier to route via IPC for managed services / clients.
        // (handle case where a service holds it's own keys but uses the daemon for comms)

        // For locally managed services we can directly apply the update.
        if target.private_key.is_some() {
            // TODO: this

            debug!("Generating updated data block");

            // Generate and publish updated data block
            let block =
                publish_data(self, &target, options.data, vec![Options::peer_id(our_id)]).await?;

            debug!("Publishing updated data block {}", block.signature());

            // Push updated data to subscribers
            push_data(&self, &target, vec![block]).await?;

            debug!("Data push complete");

            return Ok(());
        }

        // Otherwise use network communication

        // Find local peers or delegated replicas
        let target_peer = match self.peer_get(target.id.clone()).await {
            Ok(p) => {
                debug!("Found local peer matching service ID, attempting direct control");

                p
            }
            _ => {
                debug!("No local peers, searching for delegated replicas.");

                // Resolve replicas via DHT
                let replicas = find_replicas(self, target.id.clone()).await?;
                debug!("Located {} replicas", replicas.len());

                // Filter by delegation proof
                let peer_id = match replicas.iter().find(|r| r.authorative) {
                    Some(r) => {
                        debug!("Located authorative replica: {}", r.peer_id);

                        r.peer_id.clone()
                    }
                    None if replicas.len() == 1 => {
                        debug!(
                            "No authorative replica, trying with only available peer: {}",
                            replicas[0].page_id
                        );

                        replicas[0].peer_id.clone()
                    }
                    None => {
                        debug!("No authorative replica found ({} replicas)", replicas.len());

                        return Err(DsfError::NoReplicasFound);
                    }
                };

                // Lookup non-local peer
                // TODO: skip this if peer is already known
                let (target_peer, _) = self.dht_locate(peer_id).await?;

                target_peer
            }
        };

        debug!("Issuing control message to target peer: {:?}", target_peer);

        // Generate control message
        let req = net::RequestBody::Control(options.kind, target.id, options.data.clone());

        // Issue request
        let resps = self.net_req(req, vec![target_peer.clone()]).await?;
        debug!("responses: {:?}", resps);

        // Handle responses
        let resp = match resps.get(&target_peer.id) {
            Some(r) => r,
            None => return Err(DsfError::NoResponse),
        };

        match resp.data {
            net::ResponseBody::Status(s) if s == net::Status::Ok => {
                debug!("Request OK!");
                Ok(())
            }
            _ => {
                debug!("Request failed (response: {:?})", resp.data);
                Err(DsfError::InvalidResponse)
            }
        }
    }
}
