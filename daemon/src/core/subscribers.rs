use crate::sync::{Arc, Mutex};
use std::collections::HashMap;
use std::time::SystemTime;

use dsf_rpc::QosPriority;
use log::{debug, error, trace};

use futures::channel::mpsc;

use dsf_core::types::Id;

pub use dsf_rpc::{SubscriptionInfo, SubscriptionKind};

use crate::error::Error;

// TODO: isolate unixmessage from core subscriber somehow
use crate::io::unix::UnixMessage;

/// Subscribers are those connected to a service hosted or replicated by this daemon
#[derive(Clone)]
pub struct SubscriberInst {
    pub info: SubscriptionInfo,
}

#[derive(Clone, Debug)]
pub struct UnixSubscriber {
    pub connection_id: u32,
    pub sender: mpsc::Sender<UnixMessage>,
}

/// Subscriber Manager manages local, delegated, and RPC service Subscribers
pub struct SubscriberManager {
    subs: HashMap<Id, Vec<SubscriberInst>>,
}

impl SubscriberManager {
    pub fn new() -> Self {
        Self {
            subs: HashMap::new(),
        }
    }

    /// Fetch subscribers for a given service
    pub fn find(&self, service_id: &Id) -> Result<Vec<SubscriberInst>, Error> {
        match self.subs.get(service_id) {
            Some(v) => Ok(v.clone()),
            None => Ok(vec![]),
        }
    }

    pub fn find_peers(&self, service_id: &Id) -> Result<Vec<Id>, Error> {
        let subs = match self.subs.get(service_id) {
            Some(v) => v,
            None => return Ok(vec![]),
        };

        let subs = subs.iter().filter_map(|s| {
            if let SubscriptionKind::Peer(peer_id) = &s.info.kind {
                Some(peer_id.clone())
            } else {
                None
            }
        });

        Ok(subs.collect())
    }

    /// Update a specified peer subscription (for remote clients)
    pub fn update_peer<F: Fn(&mut SubscriberInst)>(
        &mut self,
        service_id: &Id,
        peer_id: &Id,
        f: F,
    ) -> Result<(), Error> {
        let subscribers = self.subs.entry(service_id.clone()).or_insert(vec![]);

        // Find subscriber in list
        let mut subscriber = subscribers.iter_mut().find(|s| {
            if let SubscriptionKind::Peer(i) = &s.info.kind {
                return i == peer_id;
            }
            false
        });

        // Create new subscriber if not found
        if subscriber.is_none() {
            let s = SubscriberInst {
                info: SubscriptionInfo {
                    service_id: service_id.clone(),
                    kind: SubscriptionKind::Peer(peer_id.clone()),

                    updated: Some(SystemTime::now()),
                    expiry: None,
                    qos: QosPriority::None,
                },
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
    pub fn update_socket<F: Fn(&mut SubscriberInst)>(
        &mut self,
        service_id: &Id,
        socket_id: u32,
        f: F,
    ) -> Result<(), Error> {
        let subscribers = self.subs.entry(service_id.clone()).or_insert(vec![]);

        let mut subscriber = subscribers.iter_mut().find(|s| {
            if let SubscriptionKind::Socket(i) = &s.info.kind {
                return *i == socket_id;
            }
            false
        });

        // Create new subscriber if not found
        if subscriber.is_none() {
            let s = SubscriberInst {
                info: SubscriptionInfo {
                    service_id: service_id.clone(),
                    kind: SubscriptionKind::Socket(socket_id),

                    updated: Some(SystemTime::now()),
                    expiry: None,
                    qos: QosPriority::None,
                },
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
        let subscribers = self.subs.entry(service_id.clone()).or_insert(vec![]);

        let remove = subscribers
            .iter()
            .enumerate()
            .find_map(|(i, s)| match &s.info.kind {
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
