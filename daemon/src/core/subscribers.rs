use crate::sync::{Arc, Mutex};
use std::collections::HashMap;
use std::time::SystemTime;

use dsf_rpc::QosPriority;
use log::{debug, error, trace};

use futures::channel::mpsc;

use dsf_core::types::Id;

pub use dsf_rpc::{SubscriptionInfo, SubscriptionKind};

// TODO: isolate unixmessage from core subscriber somehow
use crate::io::unix::UnixMessage;
use super::{Core, Error};


#[derive(Clone, Debug)]
pub struct UnixSubscriber {
    pub connection_id: u32,
    pub sender: mpsc::Sender<UnixMessage>,
}


impl Core {

    /// Fetch subscribers for a given service
    pub fn find_subscribers(&self, service_id: &Id) -> Result<Vec<SubscriptionInfo>, Error> {
        match self.subscribers.get(service_id) {
            Some(v) => Ok(v.clone()),
            None => Ok(vec![]),
        }
    }

    pub fn find_peer_subscribers(&self, service_id: &Id) -> Result<Vec<Id>, Error> {
        // Fetch all subscribers
        let subs = self.find_subscribers(service_id)?;

        // Filter for peer (network) subscribers
        let subs = subs.iter().filter_map(|s| {
            if let SubscriptionKind::Peer(peer_id) = &s.kind {
                Some(peer_id.clone())
            } else {
                None
            }
        });

        // Return filtered subscribers
        Ok(subs.collect())
    }

    /// Update a specified peer subscription (for remote clients)
    pub fn update_peer<F: Fn(&mut SubscriptionInfo)>(
        &mut self,
        service_id: &Id,
        peer_id: &Id,
        f: F,
    ) -> Result<(), Error> {
        let subscribers = self.subscribers.entry(service_id.clone()).or_insert(vec![]);

        // Find subscriber in list
        let mut subscriber = subscribers.iter_mut().find(|s| {
            if let SubscriptionKind::Peer(i) = &s.kind {
                return i == peer_id;
            }
            false
        });

        // Create new subscriber if not found
        if subscriber.is_none() {
            let s = SubscriptionInfo {
                service_id: service_id.clone(),
                kind: SubscriptionKind::Peer(peer_id.clone()),

                updated: Some(SystemTime::now()),
                expiry: None,
                qos: QosPriority::None,
            };

            subscribers.push(s);

            let n = subscribers.len();
            subscriber = Some(&mut subscribers[n - 1]);
        }

        // Call update function
        if let Some(mut s) = subscriber {
            f(&mut s);
        }

        Ok(())
    }

    /// Update a specified socket subscription (for local clients)
    pub fn update_socket<F: Fn(&mut SubscriptionInfo)>(
        &mut self,
        service_id: &Id,
        socket_id: u32,
        f: F,
    ) -> Result<(), Error> {
        let subscribers = self.subscribers.entry(service_id.clone()).or_insert(vec![]);

        let mut subscriber = subscribers.iter_mut().find(|s| {
            if let SubscriptionKind::Socket(i) = &s.kind {
                return *i == socket_id;
            }
            false
        });

        // Create new subscriber if not found
        if subscriber.is_none() {
            let s = SubscriptionInfo {
                service_id: service_id.clone(),
                kind: SubscriptionKind::Socket(socket_id),

                updated: Some(SystemTime::now()),
                expiry: None,
                qos: QosPriority::None,
            };

            subscribers.push(s);

            let n = subscribers.len();
            subscriber = Some(&mut subscribers[n - 1]);
        }

        if let Some(mut s) = subscriber {
            f(&mut s);
        }

        Ok(())
    }

    /// Remove a subscription
    pub fn remove(&mut self, service_id: &Id, peer_id: &Id) -> Result<(), Error> {
        trace!("remove sub lock");
        let subscribers = self.subscribers.entry(service_id.clone()).or_insert(vec![]);

        let remove = subscribers
            .iter()
            .enumerate()
            .find_map(|(i, s)| match &s.kind {
                SubscriptionKind::Peer(id) if id == peer_id => Some(i),
                _ => None,
            });

        if let Some(i) = remove {
            subscribers.remove(i);
        }

        Ok(())
    }
}

// TODO: write tests for this
