//! Subscribe operation, used to subscribe to a known service,
//! optionally creating a service replica

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
use dsf_rpc::{self as rpc, ServiceState, SubscribeOptions, SubscriptionInfo, SubscriptionKind};

use crate::{
    core::{replicas::ReplicaInst, CoreRes},
    daemon::Dsf,
    error::Error,
};

use super::ops::*;

pub enum SubscribeState {
    Init,
    Searching,
    Locating,
    Pending,
    Error(Error),
    Done,
}

#[allow(async_fn_in_trait)]
pub trait PubSub {
    /// Subscribe to a known service
    async fn subscribe(&self, options: SubscribeOptions)
        -> Result<Vec<SubscriptionInfo>, DsfError>;

    /// Unsubscribe from a known service
    async fn unsubscribe(&self, options: UnsubscribeOptions) -> Result<(), DsfError>;
}

impl<T: Engine> PubSub for T {
    async fn subscribe(
        &self,
        options: SubscribeOptions,
    ) -> Result<Vec<SubscriptionInfo>, DsfError> {
        info!("Subscribing to service: {:?}", options);

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

        // TODO: do not allow subscription to ourself?
        // or do we need this for delegated calls?

        // TODO: handle case where we have no peers
        // TODO: handle case where we already have replica info?

        let mut peers = vec![];

        // Attempt direct peer connection if available
        // TODO: this needs to be configurable / balanced based on QoS etc.
        if let Ok(p) = self.peer_get(target.id.clone()).await {
            debug!("Found peer matching service ID, attempting direct subscription");
            peers.push(p);
        } else {
            // Resolve replicas via DHT
            let replicas = find_replicas(self, target.id.clone()).await?;

            debug!("Located {} replicas", replicas.len());

            // Lookup peer services for available replicas
            // TODO: skip if no known peers / not connected to DHT?
            for r in &replicas {
                let peer = match self.peer_get(r.peer_id.clone()).await {
                    Ok(v) => v,
                    Err(e) => {
                        error!("Failed to lookup replica peer {}: {:?}", r.peer_id, e);
                        continue;
                    }
                };

                peers.push(peer)
            }
        }

        debug!("Issuing subscribe request to {} peers", peers.len());

        // Issue subscription requests
        let subs = do_subscribe(self, target.id.clone(), &peers).await?;

        debug!("Updating service {}", target.id);

        // Update local service state
        self.svc_update(
            target.id.clone(),
            Box::new(|svc, info| {
                info.state = ServiceState::Subscribed;
                CoreRes::Service(info.clone())
            }),
        )
        .await?;

        // TODO: track subscriptions in local store

        // TODO: replicate if enabled

        debug!("Subscribe okay!");

        Ok(subs)
    }

    async fn unsubscribe(&self, _options: UnsubscribeOptions) -> Result<(), DsfError> {
        todo!("unsubscribe not yet implemented")
    }
}

pub(super) async fn find_replicas<E: Engine>(
    e: &E,
    target_id: Id,
) -> Result<Vec<ReplicaInfo>, DsfError> {
    debug!("Searching for service {:#} via DHT", target_id);

    // Fetch service and replica information from DHT
    let (pages, _info) = match e.dht_search(target_id.clone()).await {
        Ok(v) => v,
        Err(e) => {
            error!("Failed to perform DHT lookup for replica pages: {:?}", e);
            return Err(e);
        }
    };

    debug!("Search complete, found {} pages", pages.len());

    // TODO: filter for primary pages / annotations & update

    // Filter for replica pages & update
    let replica_pages: Vec<_> = pages
        .iter()
        .filter(|p| {
            let h = p.header();
            // TODO: expand these checks
            h.flags().contains(Flags::SECONDARY)
        }).cloned()
        .collect();

    // Update replica tracking in engine
    let replicas = e.replica_update(target_id, replica_pages).await?;

    Ok(replicas)
}

async fn do_subscribe<E: Engine>(
    e: &E,
    target_id: Id,
    peers: &[PeerInfo],
) -> Result<Vec<SubscriptionInfo>, DsfError> {
    // Issue subscription requests
    let req = net::RequestBody::Subscribe(target_id);
    let resps = e.net_req(req, peers.to_vec()).await?;
    debug!("responses: {:?}", resps);

    // Check responses
    let mut subs = vec![];
    for (peer_id, r) in &resps {
        let sub_id = match &r.data {
            net::ResponseBody::Status(s) if *s == net::Status::Ok => peer_id.clone(),
            net::ResponseBody::ValuesFound(service_id, _pages) => service_id.clone(),
            _ => continue,
        };

        subs.push(SubscriptionInfo {
            service_id: sub_id,
            kind: SubscriptionKind::Peer(peer_id.clone()),
            updated: Some(SystemTime::now()),
            expiry: None,
            qos: QosPriority::None,
        })
    }

    Ok(subs)
}
