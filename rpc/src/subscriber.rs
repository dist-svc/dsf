use clap::Parser;

use serde::{Deserialize, Serialize};
use strum::Display;

/// PeerState defines the state of a peer
#[derive(Debug, Clone, Parser, PartialEq, Serialize, Deserialize, Display)]
pub enum SubscriberCommands {
    /// List all subscribers
    List,
}
