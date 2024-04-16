use std::time::Duration;

use clap::{Parser, Subcommand};
use serde::{Deserialize, Serialize};

use dsf_core::{prelude::*, types::CryptoHash};

use crate::{LocateInfo, ServiceIdentifier};

/// Name service commands
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Subcommand)]
pub enum NsCommands {
    /// Create a new name service
    Create(NsCreateOptions),

    /// Adopt an existing name service
    Adopt(NsAdoptOptions),

    /// Search using the specified name service
    Search(NsSearchOptions),

    /// Register using the specified name service
    Register(NsRegisterOptions),
}

/// Options used to create a name service
#[derive(Clone, PartialEq, Debug, Serialize, Deserialize, Parser)]
pub struct NsCreateOptions {
    #[clap(long)]
    /// Namespace for new name service
    pub name: String,

    #[clap(long)]
    /// Create a name service for public use
    pub public: bool,
}

/// Options used to adopt a name service
#[derive(Clone, PartialEq, Debug, Serialize, Deserialize, Parser)]
pub struct NsAdoptOptions {
    #[clap(long)]
    /// Name service ID
    pub id: Id,
}

/// Options used for name searching
#[derive(Clone, PartialEq, Debug, Serialize, Deserialize, Parser)]
pub struct NsSearchOptions {
    #[clap(flatten)]
    /// NameServer filter / selection
    pub ns: ServiceIdentifier,

    #[clap(long, group = "filters")]
    /// Service name for search operation
    pub name: Option<String>,

    #[clap(long, group = "filters")]
    /// Searchable options for generic application matching
    pub options: Option<Options>,

    #[clap(long, group = "filters")]
    /// Searchable hashes for application-specific matching
    pub hash: Option<CryptoHash>,

    #[clap(long)]
    /// Do not persist located service(s)
    pub no_persist: bool,
}

/// Options used for name registration
#[derive(Clone, PartialEq, Debug, Serialize, Deserialize, Parser)]
pub struct NsRegisterOptions {
    #[clap(flatten)]
    /// NameServer filter / selection
    pub ns: ServiceIdentifier,

    /// ID of service to register
    pub target: Id,

    #[clap(long)]
    /// Service name for use in registry (equivalent to --options=name:)
    pub name: Option<String>,

    #[clap(long)]
    #[serde(default)]
    /// Options for general TID generation
    pub options: Vec<Options>,

    #[clap(long)]
    #[serde(default)]
    /// Hashes for application-specific TID derivation
    pub hashes: Vec<CryptoHash>,
}

/// NameService search response
#[derive(Clone, PartialEq, Debug, Serialize, Deserialize)]
pub struct NsSearchInfo {
    /// NameService used for searching
    pub ns: Id,
    /// TID computed for search operation
    pub hash: Id,
    /// Matching services
    pub matches: Vec<LocateInfo>,

    /// DHT search information
    pub info: DhtInfo,
}

/// NameService register response
#[derive(Clone, PartialEq, Debug, Serialize, Deserialize)]
pub struct NsRegisterInfo {
    /// NameService used for registration
    pub ns: Id,
    /// NameService prefix
    pub prefix: Option<String>,

    /// Name of registered service (corresponds to Options::name)
    pub name: Option<String>,

    /// TIDs used for service registration
    pub tids: Vec<Id>,

    /// DHT store information
    pub info: Vec<DhtInfo>,
}

/// DHT operation information
#[derive(Clone, PartialEq, Debug, Serialize, Deserialize)]
pub struct DhtInfo {
    /// Search depth (DHT iterations)
    pub depth: usize,
    /// Search duration
    pub elapsed: Duration,
}
