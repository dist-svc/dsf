//! Peer model, information, and map
//! This module is used to provide a single map of PeerManager peers for sharing between DSF components

use crate::core::store::DataStore;
use crate::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicUsize, Ordering};

use std::collections::{hash_map::Entry, HashMap};

use log::{debug, error, info, trace, warn};

use dsf_core::prelude::*;
use dsf_rpc::{PeerInfo, PeerAddress, PeerFlags, PeerState};

use super::{Core, store::AsyncStore};


impl Core {
    pub fn find_peer(&self, id: &Id) -> Option<PeerInfo> {
        self.peers.get(id).map(|p| p.clone())
    }

    pub async fn find_or_create_peer(
        &mut self,
        id: Id,
        address: PeerAddress,
        key: Option<PublicKey>,
        flags: PeerFlags,
    ) -> PeerInfo {
        // Update and return existing peer
        if let Some(p) = self.peers.get_mut(&id) {
            p.update_address(address);

            if let Some(k) = key {
                p.set_state(PeerState::Known(k))
            }

            return p.clone();
        }

        // Create new peer

        let state = match key {
            Some(k) => PeerState::Known(k),
            None => PeerState::Unknown,
        };

        debug!(
            "Creating new peer instance id: ({:?} addr: {:?}, state: {:?})",
            id, address, state
        );

        let index = self.index;
        self.index += 1;

        let peer = PeerInfo::new(id.clone(), address, state, index, None);

        self.peers.insert(id.clone(), peer.clone());

        // Write non-transient peers to store
        #[cfg(feature = "store")]
        if !peer.flags.contains(PeerFlags::TRANSIENT) {
            if let Err(e) = self.store.peer_update(&peer).await {
                error!("Error writing peer {} to db: {:?}", id, e);
            }
        }

        peer
    }

    pub async fn remove_peer(&mut self, id: &Id) -> Option<PeerInfo> {
        let peer = match self.peers.remove(id) {
            Some(v) => v,
            None => return None,
        };

        #[cfg(feature = "store")]
        if let Err(e) = self.store.peer_del(&peer.id).await {
            error!("Error removing peer from db: {:?}", e);
        }

        Some(peer)        
    }

    pub fn peer_count(&self) -> usize {
        self.peers.len()
    }

    pub fn peer_seen_count(&self) -> usize {
        self.peers
            .iter()
            .filter(|(_id, p)| p.seen.is_some() && !p.flags.contains(PeerFlags::CONSTRAINED))
            .count()
    }

    pub fn list_peers(&self) -> Vec<(Id, PeerInfo)> {
        self.peers
            .iter()
            .map(|(id, p)| (id.clone(), p.clone()))
            .collect()
    }

    pub fn peer_index_to_id(&self, index: usize) -> Option<Id> {
        self.peers
            .iter()
            .find(|(_id, p)| p.index == index)
            .map(|(id, _s)| id.clone())
    }

    /// Update a peer instance (if found)
    pub fn update<F>(&mut self, id: &Id, mut f: F) -> Option<PeerInfo>
    where
        F: FnMut(&mut Peer),
    {
        match self.peers.get_mut(id) {
            Some(p) => {
                (f)(p);
                Some(p.info())
            }
            None => None,
        }
    }

}
