//! mDNS plugin, provides mDNS support to the DSF daemon
//!

use crate::sync::{Arc, Mutex};
use std::collections::HashMap;
use std::io;
use std::net::{IpAddr, SocketAddr};
use std::str::FromStr;
use std::time::{Duration, SystemTime};

use futures::prelude::*;
use log::debug;
use mdns::RecordKind;
use tokio::task::{self, JoinHandle};

use dsf_core::types::Id;

const SERVICE_NAME: &str = "_dsf._udp.local";

/// Commands that are handled by the MDNS actor
#[derive(Clone, Debug)]
pub enum MdnsCommand {
    Register(u16),
    Discover,
    Deregister,
}

/// Updates published by the MDNS actor
#[derive(Clone, Debug)]
pub enum MdnsUpdate {
    Discovered(Vec<SocketAddr>),
}

#[derive(Clone, Debug, PartialEq)]
pub enum MdnsState {
    Disabled,
    Registered(u16),
}

#[derive(Clone, Debug, PartialEq)]
pub enum MdnsError {
    CreateError,
    RegisterFailed,
    DiscoverFailed,
}

pub struct MdnsPlugin {
    id: Id,

    responder: libmdns::Responder,
    registered_services: Vec<libmdns::Service>,

    discover_handle: Option<JoinHandle<Result<(), mdns::Error>>>,
    discovered_services: Arc<Mutex<HashMap<SocketAddr, SystemTime>>>,
}

impl MdnsPlugin {
    /// Create a new mDNS plugin
    pub fn new(id: Id) -> Result<Self, io::Error> {
        let responder = libmdns::Responder::new()?;

        let registered_services = vec![];
        let discovered_services = Arc::new(Mutex::new(HashMap::new()));

        Ok(Self {
            id,
            responder,
            registered_services,
            discovered_services,
            discover_handle: None,
        })
    }

    /// Attempt to register a service on the provided port
    pub async fn register(&mut self, port: u16) -> Result<(), MdnsError> {
        debug!("registering service");

        // Register service
        let svc = self.responder.register(
            SERVICE_NAME.to_owned(),
            "DSF Daemon".to_owned(),
            port,
            &[&format!("id={}", self.id)],
        );

        self.registered_services.push(svc);

        Ok(())
    }

    /// Remove all registered services
    pub fn deregister(&mut self) {
        self.registered_services.clear();
    }

    /// Enable mDNS discovery
    /// TODO: update the mdns library to work with modern futures
    pub fn start_discovery(&mut self) -> Result<(), mdns::Error> {
        debug!("starting discovery");

        // Create discovery object
        let discovery = mdns::discover::all(SERVICE_NAME, Duration::from_secs(30))?;
        let services = self.discovered_services.clone();

        let h = task::spawn(async move {
            let stream = discovery.listen();
            let mut stream = Box::pin(stream);

            // Listen for peers
            while let Some(Ok(r)) = stream.next().await {
                // Filter out fields
                let addr = r.records().filter_map(to_addr).next();
                let port = r.records().filter_map(to_srv).next();
                let id = r.records().filter_map(to_id).next();

                // Check we have an address and a port
                let v = match (addr, port) {
                    (Some(a), Some(p)) => Some((SocketAddr::new(a, p), id)),
                    _ => None,
                };

                // Track service
                if let Some((addr, _id)) = &v {
                    debug!("discovered service: {:?}", v);

                    // Add to discovered services array
                    let mut s = services.lock().unwrap();
                    let e = s.entry(*addr).or_insert(SystemTime::now());
                    *e = SystemTime::now();

                    // TODO: emit this
                }
            }

            Ok(())
        });

        self.discover_handle = Some(h);

        Ok(())
    }
}

fn to_addr(record: &mdns::Record) -> Option<IpAddr> {
    match record.kind {
        RecordKind::A(addr) => Some(IpAddr::V4(addr)),
        RecordKind::AAAA(addr) => Some(IpAddr::V6(addr)),
        _ => None,
    }
}

fn to_srv(record: &mdns::Record) -> Option<u16> {
    match record.kind {
        RecordKind::SRV {
            priority: _,
            weight: _,
            port,
            target: _,
        } => Some(port),
        _ => None,
    }
}

fn to_id(record: &mdns::Record) -> Option<Id> {
    match &record.kind {
        RecordKind::TXT(values) => values.iter().filter_map(|v| Id::from_str(v).ok()).next(),
        _ => None,
    }
}
