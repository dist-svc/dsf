use clap::Parser;
use serde::{Deserialize, Serialize};

use dsf_core::types::*;

use crate::ServiceIdentifier;

#[derive(Clone, Debug, PartialEq, Parser, Serialize, Deserialize)]
pub enum PageCommands {
    /// Fetch a page by signature
    Fetch(FetchOptions),
}

#[derive(Clone, Debug, PartialEq, Parser, Serialize, Deserialize)]
pub struct FetchOptions {
    /// Service identifier
    #[clap(flatten)]
    pub service: ServiceIdentifier,

    /// Page signature
    #[clap(long)]
    pub page_sig: Signature,
}
