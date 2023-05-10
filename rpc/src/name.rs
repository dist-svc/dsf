use clap::{Parser, Subcommand};
use serde::{Deserialize, Serialize};

use dsf_core::{prelude::*, types::CryptoHash};

use crate::ServiceIdentifier;

/// Name service commands
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Subcommand)]
pub enum NsCommands {
    /// Create a new name service
    Create(NsCreateOptions),

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
    /// Options for general TID generation
    pub options: Vec<Options>,

    #[clap(long)]
    /// Hashes for application-specific TID derivation
    pub hashes: Vec<CryptoHash>,
}

#[derive(Clone, PartialEq, Debug, Serialize, Deserialize)]
pub struct NsRegisterInfo {
    pub ns: Id,
    pub prefix: Option<String>,

    pub name: Option<String>,
    pub hashes: Vec<CryptoHash>,
}
