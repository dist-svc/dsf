//! Peer model, information, and map
//! This module is used to provide a single map of PeerManager peers for sharing between DSF components

use crate::core::store::DataStore;
use crate::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicUsize, Ordering};

use std::collections::{hash_map::Entry, HashMap};

use log::{debug, error, info, trace, warn};

use dsf_core::prelude::*;
use dsf_rpc::{PeerAddress, PeerFlags, PeerInfo, PeerState, ServiceIdentifier};

use super::{store::AsyncStore, Core};
use crate::error::Error;

/// HACK: alias to mitigate breakage from Peer -> PeerInfo migration
pub type Peer = PeerInfo;

pub struct PeerInst {
    pub info: PeerInfo,
    pub dirty: bool,
}

impl Core {
    pub fn peer_get(&self, ident: ServiceIdentifier) -> Option<PeerInfo> {
        if let Some(id) = &ident.id {
            return self.peers.get(id).map(|p| p.info.clone());
        }

        if let Some(short_id) = &ident.short_id {
            return self
                .peers
                .iter()
                .find(|(_id, s)| s.info.short_id == *short_id)
                .map(|(_id, p)| p.info.clone());
        }

        if let Some(index) = &ident.index {
            return self
                .peers
                .iter()
                .find(|(_id, s)| s.info.index == *index)
                .map(|(_id, p)| p.info.clone());
        }

        None
    }

    pub async fn peer_create_or_update(&mut self, info: PeerInfo) -> Result<PeerInfo, Error> {
        // Update and return existing peer
        if let Some(PeerInst { info: p, dirty }) = self.peers.get_mut(&info.id) {
            // Update address on change
            // TODO(med): support multiple peer addresses / peer address prioritisations
            p.update_address(info.address);

            if let PeerState::Known(k) = info.state {
                p.set_state(PeerState::Known(k))
            }

            // TODO(med) what other fields should we update here..? all of them..?

            if let Some(seen) = info.seen {
                p.seen = Some(seen);
            }

            p.flags = info.flags;

            *dirty = true;

            return Ok(p.clone());
        }

        // Create new peer
        debug!(
            "Creating new peer instance id: ({:?} addr: {:?}, state: {:?})",
            info.id, info.address, info.state
        );

        // Store in peer cache
        self.peers.insert(
            info.id.clone(),
            PeerInst {
                info: info.clone(),
                dirty: false,
            },
        );

        // Write non-transient peers to store on creation
        #[cfg(feature = "store")]
        if !info.flags.contains(PeerFlags::TRANSIENT) {
            if let Err(e) = self.store.peer_update(&info).await {
                error!("Error writing peer {} to db: {:?}", info.id, e);
            }
        }

        Ok(info)
    }

    pub async fn remove_peer(&mut self, id: &Id) -> Option<PeerInfo> {
        let peer = match self.peers.remove(id) {
            Some(v) => v,
            None => return None,
        };

        #[cfg(feature = "store")]
        if let Err(e) = self.store.peer_del(&peer.info.id).await {
            error!("Error removing peer from db: {:?}", e);
        }

        Some(peer.info)
    }

    pub fn peer_count(&self) -> usize {
        self.peers.len()
    }

    pub fn peer_seen_count(&self) -> usize {
        self.peers
            .iter()
            .filter(|(_id, p)| {
                p.info.seen.is_some() && !p.info.flags.contains(PeerFlags::CONSTRAINED)
            })
            .count()
    }

    pub fn list_peers(&self) -> Vec<(Id, PeerInfo)> {
        self.peers
            .iter()
            .map(|(id, p)| (id.clone(), p.info.clone()))
            .collect()
    }

    pub fn peer_index_to_id(&self, index: usize) -> Option<Id> {
        self.peers
            .iter()
            .find(|(_id, p)| p.info.index == index)
            .map(|(id, _s)| id.clone())
    }

    /// Update a peer instance (if found)
    pub async fn peer_update<F>(&mut self, id: &Id, mut f: F) -> Result<Option<PeerInfo>, DsfError>
    where
        F: FnMut(&mut PeerInfo),
    {
        // Look for matching peer
        let PeerInst { info: p, dirty } = match self.peers.get_mut(id) {
            Some(p) => p,
            None => return Ok(None),
        };

        // Cache unchanged peer info
        let old_peer = p.clone();

        // Run update function
        f(p);

        // Set dirty flag
        *dirty = true;

        // Return updated info
        Ok(Some(p.clone()))
    }
}
