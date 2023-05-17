use clap::{Parser, Subcommand};
use serde::{Deserialize, Serialize};

use crate::ServiceIdentifier;
use dsf_core::types::Id;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Subcommand)]
pub enum DebugCommands {
    /// Datastore debug commands
    #[clap(subcommand)]
    Store(DatastoreCommands),

    /// Search for entries in the DHT
    Search {
        #[clap()]
        id: Id,
    },

    /// Force an update of the daemon
    Update,

    /// Invoke bootstrapping
    Bootstrap,

    /// Fetch current NodeTable
    NodeTable,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Parser)]
pub enum DhtCommands {
    /// Find a peer with a given ID
    Peer(ServiceIdentifier),

    /// Find data at a certain ID
    Data(ServiceIdentifier),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Subcommand)]
pub enum DatastoreCommands {}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]

pub struct BootstrapInfo {
    pub connected: usize,
    pub registrations: usize,
    pub subscriptions: usize,
}
