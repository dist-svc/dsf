//! Publish operation, publishes data using a known service,
//! distributing updates to any active subscribers

use std::collections::HashMap;
use std::convert::TryFrom;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use dsf_core::base::DataBody;
use dsf_core::options::Filters;
use dsf_core::wire::Container;
use futures::channel::mpsc;
use futures::prelude::*;

use log::{debug, error, info, warn};
use rpc::{ServiceInfo, SyncInfo, PeerInfo};
use tracing::{span, Level};

use dsf_core::prelude::*;

use dsf_core::net;
use dsf_core::service::{DataOptions, Publisher};
use dsf_rpc::{self as rpc, DataInfo, SyncOptions};

use crate::daemon::net::{NetFuture, NetIf};
use crate::daemon::Dsf;
use crate::error::Error;
use crate::rpc::push::push_data;
use crate::rpc::subscribe::find_replicas;

use super::ops::*;


pub trait SyncData {
    /// Sync data for a subscribed service
    async fn sync(&self, options: SyncOptions) -> Result<SyncInfo, DsfError>;
}

impl<T: Engine> SyncData for T {
    async fn sync(&self, options: SyncOptions) -> Result<SyncInfo, DsfError> {
        info!("Sync: {:?}", &options);

        // Resolve service id / index to a service instance
        let info = self.svc_get(options.service).await?;

        // Resolve replicas / peers for connection
        let mut peers = vec![];

        // Attempt direct peer connection if available
        // TODO: this needs to be configurable / balanced based on QoS etc.
        if let Ok(p) = self.peer_get(info.id.clone()).await {
            debug!("Found peer matching service ID, attempting direct subscription");
            peers.push(p);
        } else {
            // Resolve replicas via DHT
            let replicas = find_replicas(self, info.id.clone()).await?;

            debug!("Located {} replicas", replicas.len());

            // Lookup peer services for available replicas
            // TODO: skip if no known peers / not connected to DHT?
            for r in &replicas {
                let peer = match self.peer_get(r.info.peer_id.clone()).await {
                    Ok(v) => v,
                    Err(e) => {
                        error!("Failed to lookup replica peer {}: {:?}", r.info.peer_id, e);
                        continue;
                    }
                };

                peers.push(peer)
            }
        }

        debug!("Syncing using {} peers", peers.len());

        // Request data from replicas
        let mut objects = HashMap::new();

        // First, query for the latest published object
        // TODO: reduce to subset of available peers
        let req = NetRequestBody::Query(info.id.clone(), None);
        let resps = self.net_req(req, peers.clone()).await?;

        let mut last_index = 0;
        let mut last_sig = None;

        // Process responses into object list
        for (_peer_id, resp) in resps {
            // Match on PullData responses
            let data = match resp.data {
                NetResponseBody::PullData(id, data) if id == info.id => data,
                _ => continue,
            };

            // Build hashmap of unique objects
            for o in data {
                let index = o.header().index();

                if last_index < index {
                    last_index = index;
                    last_sig = Some(o.signature());
                }

                if objects.contains_key(&index) {
                    objects.insert(index, o);
                }
            }
        }

        debug!("Current index: {}", last_index);
        let mut num_synced = 0;

        // Store initial / latest object
        if last_sig.is_some() {
            if let Some(o) = objects.get(&last_index) {
                if let Err(e) = self.object_put(o.clone()).await {
                    error!("Failed to store object {:?}: {:?}", o, e);
                } else {
                    num_synced += 1;
                }
            }
        }

        if last_index == 0 {
            warn!("Service reports index 0, retry");
            return Err(DsfError::InvalidResponse);
        }

        // TODO: reduce peers based on initial response

        for i in (0..last_index - 1).rev() {
            // Check if we have recently received the object
            if objects.contains_key(&i) {
                num_synced += 1;

                continue;
            }

            // Check whether the object is available locally
            if let Some(sig) = &last_sig {
                match self.object_get(info.id.clone(), sig.clone()).await {
                    Ok(o) if o.header().index() == i => {
                        // Update prior sig and object state
                        last_sig = Filters::prev_sig(&o.public_options_iter());
                        objects.insert(o.header().index(), o);

                        num_synced += 1;

                        continue;
                    }
                    _ => (),
                }
            }

            // Otherwise, request the object from peers
            match issue_query(self, info.id.clone(), Some(i), &peers).await {
                Ok(o) if o.header().index() == i => {
                    debug!("Received object: {:?}", o);
                    // Update prior sig and object state
                    last_sig = Filters::prev_sig(&o.public_options_iter());
                    objects.insert(o.header().index(), o.clone());

                    // TODO: add object to store
                    if let Err(e) = self.object_put(o).await {
                        error!("Failed to store new object: {:?}", e);
                    } else {
                        num_synced += 1;
                    }

                    continue;
                }
                _ => (),
            }

            warn!("Failed to fetch object {}", i);
            break;
        }

        // Update service using received info
        let mut last_page = None;
        for (_, o) in objects.iter() {
            let h = o.header();

            if !h.kind().is_page() {
                continue;
            }

            match last_page {
                None => last_page = Some(o),
                Some(l) if l.header().index() < h.index() => last_page = Some(o),
                _ => (),
            }
        }
        if let Some(p) = last_page {
            self.svc_register(info.id.clone(), vec![p.to_owned()]).await?;
        }

        Ok(SyncInfo {
            total: last_index as usize,
            synced: num_synced,
        })
    }
}

async fn issue_query<E: Engine + Sync + Send>(
    e: &E,
    target_id: Id,
    index: Option<u32>,
    peers: &[PeerInfo],
) -> Result<Container, DsfError> {
    let req = NetRequestBody::Query(target_id.clone(), index);

    if peers.len() == 0 {
        return Err(DsfError::NoPeersFound);
    }

    // Issue request to _one of the_ peers
    // TODO: random order? prefer QoS / latencies?
    for p in peers {
        debug!(
            "Query for service {:#} data {:?} via peer {:?}",
            target_id, index, p
        );

        let resps = match e.net_req(req.clone(), peers.to_vec()).await {
            Ok(v) if v.len() == 1 => v,
            Ok(_v) => {
                debug!("No response for peer {:?}", p);
                continue;
            }
            Err(e) => {
                debug!("Query failed for peer {:?}: {:?}", p, e);
                continue;
            }
        };

        let (_peer_id, resp) = resps.iter().next().unwrap();

        // Match on responses
        let mut data = match &resp.data {
            NetResponseBody::PullData(id, data) if id == &target_id => data.to_vec(),
            _ => {
                debug!("Unexepected response from peer {:?}: {:?}", p, resps);
                continue;
            }
        };

        if data.len() == 0 {
            debug!("No data in response");
            continue;
        }
        data.sort_by_key(|d| d.header().index());

        debug!("Received data: {:?}", data);

        // Look for matching object by index
        if let Some(i) = index {
            for d in data {
                if d.header().index() == i {
                    return Ok(d.to_owned());
                }
            }

            debug!("No object matching index {}", i);
            continue;
        }

        // Otherwise, return latest object
        return Ok(data[data.len() - 1].to_owned());
    }

    return Err(DsfError::NotFound);
}
