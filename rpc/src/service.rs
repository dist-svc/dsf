use std::time::SystemTime;
use std::{collections::HashMap, net::SocketAddr};

use serde::{Deserialize, Serialize};
use strum::{Display, EnumString};

use clap::{Parser, Subcommand};
use dsf_core::{
    options::Options,
    prelude::{Keys, Service},
    wire::Container,
};

use dsf_core::types::*;

pub use crate::helpers::{try_load_file, try_parse_key_value};
use crate::{PageBounds, ServiceIdentifier};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
//#[cfg_attr(feature = "diesel", derive(diesel::Queryable))]
//#[cfg_attr(feature = "diesel", table_name="services")]
pub struct ServiceInfo {
    pub id: Id,
    pub short_id: ShortId,

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

    pub flags: ServiceFlags,
}

bitflags::bitflags! {
    #[derive(Serialize, Deserialize, Default)]
    pub struct ServiceFlags: u16 {
        const ORIGIN = (1 << 0);
        const ENCRYPTED = (1 << 1);
        const SUBSCRIBED = (1 << 2);
        const REPLICATED = (1 << 3);
    }
}

impl ServiceInfo {
    pub fn keys(&self) -> Keys {
        Keys {
            pub_key: Some(self.public_key.clone()),
            pri_key: self.private_key.clone(),
            sec_key: self.secret_key.clone(),
            sym_keys: None,
        }
    }
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

        let mut flags = ServiceFlags::empty();
        if svc.is_origin() {
            flags |= ServiceFlags::ORIGIN;
        }
        if svc.encrypted() {
            flags |= ServiceFlags::ENCRYPTED;
        }

        Self {
            id: svc.id(),
            short_id: svc.id().into(),
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
            flags,
        }
    }
}

#[derive(Clone, Copy, PartialEq, Debug, Serialize, Deserialize, Display)]
#[cfg_attr(feature = "std", derive(EnumString))]
#[strum(serialize_all = "snake_case")]
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
    List(ServiceListOptions),

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
    /// Register an existing / known service in the database
    Register(RegisterOptions),

    #[clap()]
    /// Replicate an existing / known service
    Replicate(RegisterOptions),

    #[clap()]
    /// Subscribe to a known service
    Subscribe(SubscribeOptions),

    #[clap()]
    /// Unsubscribe from a known service
    Unsubscribe(UnsubscribeOptions),

    #[clap()]
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

    #[clap()]
    /// List authorisations for a service
    AuthList(ServiceIdentifier),

    #[clap()]
    /// Add/Update/Remove authorisations for a service
    AuthUpdate(AuthUpdateOptions),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Parser)]
pub struct ServiceListOptions {
    #[clap(long)]
    /// Application ID for filtering
    pub application_id: Option<u16>,

    #[clap(long)]
    /// Service type for filtering
    pub kind: Option<ServiceKind>,

    #[clap(flatten)]
    /// Bounds for listing
    pub bounds: PageBounds,
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
                short_id: None,
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

    #[clap(long)]
    /// Search only in the local datastore
    pub local_only: bool,

    #[clap(long)]
    /// Do not persist located service
    pub no_persist: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Parser)]
pub struct DiscoverOptions {
    /// Application ID for discovery
    #[clap(long)]
    pub application_id: u16,

    /// Application-specific body for filtering
    #[clap(long)]
    pub body: Option<Data>,

    /// Options for filtering
    #[clap(long)]
    pub filters: Vec<Options>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct LocateInfo {
    pub id: Id,
    pub flags: ServiceFlags,
    pub updated: bool,
    pub page_version: u32,
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

/// Authorisation information for a service
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AuthInfo {
    /// Service referenced
    pub service_id: Id,
    /// List of authorisations
    pub auths: HashMap<Id, AuthRole>,
}

/// Authorisation kind (read / write / etc.)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Parser, Display)]
#[cfg_attr(feature = "std", derive(EnumString))]
#[strum(serialize_all = "snake_case")]
pub enum AuthRole {
    None,
    Read,
    Write,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Parser)]
pub struct AuthUpdateOptions {
    /// Service to add authorisation to
    #[clap(flatten)]
    pub service: ServiceIdentifier,

    /// Peer to authorise
    pub peer_id: Id,

    /// Authorisation type
    #[clap(default_value_t=AuthRole::Write)]
    pub role: AuthRole,
}
