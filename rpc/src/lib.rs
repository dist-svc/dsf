use std::time::SystemTime;

use clap::Parser;
use futures::prelude::*;
use rand::random;
use serde::{Deserialize, Serialize};

use dsf_core::{api::ServiceHandle, error::Error, types::*, wire::Container};

pub mod config;
pub use config::*;

pub mod data;
pub use data::*;

pub mod debug;
pub use debug::*;

pub mod peer;
pub use peer::*;

pub mod service;
pub use service::*;

pub mod replica;
pub use replica::*;

pub mod subscriber;
pub use subscriber::*;

pub mod page;
pub use page::*;

pub mod name;
pub use name::*;

pub mod display;

mod helpers;

/// API trait implements RPC API for the daemon (or delegation)
pub trait Rpc {
    type Error;

    fn exec(&mut self, req: Request) -> Box<dyn Future<Output = Result<Response, Self::Error>>>;
}

/// RPC Request container for requests from a client to the daemon
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Request {
    req_id: u64,
    kind: RequestKind,
}

impl Request {
    pub fn new(kind: RequestKind) -> Self {
        Self {
            req_id: random(),
            kind,
        }
    }

    pub fn req_id(&self) -> u64 {
        self.req_id
    }

    pub fn kind(&self) -> RequestKind {
        self.kind.clone()
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Parser)]
pub struct ServiceIdentifier {
    #[clap(short = 'i', long = "id", group = "identifier")]
    /// Global service ID
    pub id: Option<Id>,

    #[clap(short = 'n', long = "index", group = "identifier")]
    /// Local service index
    pub index: Option<usize>,
}

impl ServiceIdentifier {
    pub fn id(id: Id) -> Self {
        Self {
            id: Some(id),
            index: None,
        }
    }

    pub fn index(index: usize) -> Self {
        Self {
            id: None,
            index: Some(index),
        }
    }
}

impl From<ServiceHandle> for ServiceIdentifier {
    fn from(h: ServiceHandle) -> Self {
        Self::id(h.id)
    }
}

impl From<Id> for ServiceIdentifier {
    fn from(id: Id) -> Self {
        Self::id(id)
    }
}

/// Paginator object supports paginating responses from the daemon
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Parser, Default)]
pub struct PageBounds {
    #[clap(long = "count")]
    /// Maximum number of responses to return
    pub count: Option<usize>,

    #[clap(long = "offset")]
    /// Offset of returned results
    pub offset: Option<usize>,
}

/// Time bounded object supports limiting queries by time
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Parser, Default)]
pub struct TimeBounds {
    /// Start time for data query
    #[clap(long, value_parser = timestamp_from_str)]
    pub from: Option<SystemTime>,

    /// End time for data query
    #[clap(long, value_parser = timestamp_from_str)]
    pub until: Option<SystemTime>,
}

/// Specific request kinds for issuing requests to the daemon from the client
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, clap::Subcommand)]
pub enum RequestKind {
    /// Checks the status of the DSF daemon
    Status,

    /// Subcommand for managing and interacting with peers
    #[clap(subcommand)]
    Peer(PeerCommands),

    /// Subcommand for managing and interacting with services
    #[clap(subcommand)]
    Service(ServiceCommands),

    /// Subcommand for managing and interacting with name services
    #[clap(subcommand)]
    Ns(NsCommands),

    /// Object requests
    #[clap(subcommand)]
    Page(PageCommands),

    /// Subcommand for managing data
    #[clap(subcommand)]
    Data(DataCommands),

    /// Subcommand for managing subscribers
    #[clap(subcommand)]
    Subscriber(SubscriberCommands),

    /// Subcommand for managing runtime daemon configuration
    #[clap(subcommand)]
    Config(ConfigCommands),

    /// Subcommand for exposing debug information
    #[clap(subcommand)]
    Debug(DebugCommands),

    /// Stream data from a given service
    #[clap()]
    Stream(SubscribeOptions),
}

/// Response container for replies from the daemon to the client
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Response {
    req_id: u64,
    kind: ResponseKind,
}

impl Response {
    pub fn new(req_id: u64, kind: ResponseKind) -> Self {
        Self { req_id, kind }
    }

    pub fn req_id(&self) -> u64 {
        self.req_id
    }

    pub fn kind(&self) -> ResponseKind {
        self.kind.clone()
    }
}

/// Specific response kinds for processing responses from the daemon
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ResponseKind {
    None,

    Status(StatusInfo),
    Connected(ConnectInfo),

    Peer(PeerInfo),
    Peers(Vec<(Id, PeerInfo)>),

    Service(ServiceInfo),
    Services(Vec<ServiceInfo>),
    Registered(RegisterInfo),
    Located(Vec<LocateInfo>),

    Subscribed(Vec<SubscriptionInfo>),

    Published(PublishInfo),

    Datastore(Vec<(Id, Vec<Vec<u8>>)>),

    Ns(NsRegisterInfo),

    Data(Vec<DataInfo>),

    Pages(Vec<Container>),

    Page(Container),

    //Value(String),
    Unrecognised,

    Error(Error),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StatusInfo {
    pub id: Id,
    pub peers: usize,
    pub services: usize,
}

pub use dsf_core::base::Body;

/// Parse a timestamp from a provided string
fn timestamp_from_str(s: &str) -> Result<SystemTime, chrono_english::DateError> {
    let t =
        chrono_english::parse_date_string(s, chrono::Local::now(), chrono_english::Dialect::Uk)?;
    Ok(t.into())
}
