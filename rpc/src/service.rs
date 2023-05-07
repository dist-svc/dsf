use std::net::SocketAddr;
use std::time::SystemTime;

use serde::{Deserialize, Serialize};
use strum::{Display, EnumString};

use clap::{Parser, Subcommand};
use dsf_core::{options::Options, prelude::Service, wire::Container};

use dsf_core::types::*;

pub use crate::helpers::{try_load_file, try_parse_key_value};
use crate::ServiceIdentifier;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
//#[cfg_attr(feature = "diesel", derive(diesel::Queryable))]
//#[cfg_attr(feature = "diesel", table_name="services")]
pub struct ServiceInfo {
    pub id: Id,
    pub index: usize,

    pub kind: ServiceKind,
    pub state: ServiceState,

    pub public_key: PublicKey,
    // TODO: rework to avoid (ever) returning private / secret keys
    // (requires ServiceInfo replacement for database...)
    pub private_key: Option<PrivateKey>,
    pub secret_key: Option<SecretKey>,

    pub last_updated: Option<SystemTime>,

    pub primary_page: Option<Signature>,
    pub replica_page: Option<Signature>,

    pub subscribers: usize,
    pub replicas: usize,
    pub origin: bool,
    pub subscribed: bool,
}

impl From<&Service> for ServiceInfo {
    /// Create a default service info object for a service.
    /// Note that fields undefined within the service will be zero-initialised
    fn from(svc: &Service) -> Self {
        let kind = match svc.kind() {
            PageKind::Name => ServiceKind::Name,
            PageKind::Peer => ServiceKind::Peer,
            _ => ServiceKind::Generic,
        };

        Self {
            id: svc.id(),
            index: svc.version() as usize,
            state: ServiceState::Created,
            kind,
            public_key: svc.public_key(),
            private_key: svc.private_key(),
            secret_key: svc.secret_key(),

            last_updated: None,
            primary_page: None,
            replica_page: None,

            subscribers: 0,
            replicas: 0,
            origin: svc.is_origin(),
            subscribed: false,
        }
    }
}

#[derive(Clone, Copy, PartialEq, Debug, Serialize, Deserialize, Display)]
#[cfg_attr(feature = "std", derive(EnumString))]
pub enum ServiceState {
    Created,
    Registered,
    Located,
    Subscribed,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Subcommand)]
pub enum ServiceCommands {
    #[clap()]
    /// List known services
    List(ListOptions),

    #[clap()]
    /// Create a new service
    Create(CreateOptions),

    #[clap()]
    /// Locate an existing service
    Locate(LocateOptions),

    #[clap()]
    /// Fetch information for a service
    Info(InfoOptions),

    #[clap()]
    /// Register an existing / known service
    Register(RegisterOptions),

    #[clap()]
    /// Subscribe to a known service
    Subscribe(SubscribeOptions),

    #[clap()]
    /// Unsubscribe from a known service
    Unsubscribe(UnsubscribeOptions),

    /// Discover local services
    Discover(DiscoverOptions),

    #[clap()]
    /// Set the encryption/decryption key for a given service
    SetKey(SetKeyOptions),

    #[clap()]
    /// Fetch the encryption/decryption key for a given service
    GetKey(ServiceIdentifier),

    #[clap()]
    /// Remove a service from the service list (and database if specified)
    Remove(RemoveOptions),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Parser)]
pub struct ListOptions {
    #[clap(long = "application-id")]
    /// Application ID for filtering
    pub application_id: Option<u16>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Parser, Default)]
pub struct CreateOptions {
    #[clap(short = 'i', long = "application-id", default_value = "0")]
    /// Application ID    
    pub application_id: u16,

    #[clap(short = 'k', long = "page-kind", value_enum)]
    /// Page Kind (defaults to Generic)
    pub page_kind: Option<PageKind>,

    #[clap(name = "body", value_parser = try_load_file)]
    /// Service Page Body (loaded from the specified file)
    pub body: Option<Data>,

    #[clap(short = 'a', long = "address")]
    /// Service Addresses
    pub addresses: Vec<SocketAddr>,

    #[clap(short = 'm', long = "metadata", value_parser = try_parse_key_value)]
    /// Service Metadata key:value pairs
    pub metadata: Vec<(String, String)>,

    #[clap(long)]
    /// Service metadata / options
    pub public_options: Vec<Options>,

    #[clap(long)]
    /// Service metadata / options
    pub private_options: Vec<Options>,

    #[clap(short = 'p', long = "public")]
    /// Indicate the service should be public (unencrypted)
    pub public: bool,

    #[clap(long = "register")]
    /// Indicate the service should be registered and replicated following creation
    pub register: bool,
}

pub type Data = Vec<u8>;

impl CreateOptions {
    pub fn and_register(mut self) -> Self {
        self.register = true;
        self
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CreateInfo {
    pub id: Id,
    pub secret_key: Option<SecretKey>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Parser)]
pub struct RegisterOptions {
    #[clap(flatten)]
    pub service: ServiceIdentifier,

    #[clap(long = "no-replica")]
    /// Do not become a replica for the registered service
    pub no_replica: bool,
}

impl RegisterOptions {
    pub fn new(id: Id) -> Self {
        Self {
            service: ServiceIdentifier {
                id: Some(id),
                index: None,
            },
            no_replica: false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RegisterInfo {
    pub page_version: u32,
    pub replica_version: Option<u32>,
    pub peers: usize,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Parser)]
pub struct LocateOptions {
    #[clap(short = 'i', long = "id")]
    /// ID of the service to locate
    pub id: Id,

    #[clap(long = "local-only")]
    /// Search only in the local datastore
    pub local_only: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Parser)]
pub struct DiscoverOptions {
    /// Application-specific body for filtering
    pub body: Option<Data>,

    /// Otions for filtering
    pub filters: Vec<Options>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct LocateInfo {
    pub id: Id,
    pub origin: bool,
    pub updated: bool,
    pub page_version: u16,
    #[serde(skip)]
    pub page: Option<Container>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Parser)]
pub struct InfoOptions {
    #[clap(flatten)]
    pub service: ServiceIdentifier,
}

impl From<ServiceIdentifier> for InfoOptions {
    fn from(service: ServiceIdentifier) -> Self {
        Self { service }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Parser)]
pub struct SubscribeOptions {
    #[clap(flatten)]
    pub service: ServiceIdentifier,
}

impl From<ServiceIdentifier> for SubscribeOptions {
    fn from(service: ServiceIdentifier) -> Self {
        Self { service }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Parser)]
pub struct UnsubscribeOptions {
    #[clap(flatten)]
    pub service: ServiceIdentifier,
}

impl From<ServiceIdentifier> for UnsubscribeOptions {
    fn from(service: ServiceIdentifier) -> Self {
        Self { service }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum SubscriptionKind {
    Peer(Id),
    Socket(u32),
    None,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SubscriptionInfo {
    pub service_id: Id,
    pub kind: SubscriptionKind,

    pub updated: Option<SystemTime>,
    pub expiry: Option<SystemTime>,

    pub qos: QosPriority,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum QosPriority {
    None = 0,
    Latency = 1,
}

impl SubscriptionInfo {
    pub fn new(service_id: Id, kind: SubscriptionKind) -> Self {
        Self {
            service_id,
            kind,
            updated: None,
            expiry: None,
            qos: QosPriority::None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Parser)]
pub struct SetKeyOptions {
    #[clap(flatten)]
    pub service: ServiceIdentifier,

    #[clap(short = 's', long = "secret-key")]
    /// Secret key for service access
    pub secret_key: Option<SecretKey>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Parser)]
pub struct RemoveOptions {
    #[clap(flatten)]
    pub service: ServiceIdentifier,

    #[clap(long)]
    /// Attempt to remove an owned service from the network
    pub purge: bool,
}
