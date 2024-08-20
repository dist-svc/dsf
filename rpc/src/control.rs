use clap::Parser;

use serde::{Deserialize, Serialize};
use strum::Display;

use crate::{data::Data, helpers::data_from_str, ServiceIdentifier};

/// PeerState defines the state of a peer
#[derive(Debug, Clone, Parser, PartialEq, Serialize, Deserialize, Display)]
pub enum ControlCommands {
    /// Write a control message to the provided service
    Write(ControlWriteOptions),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Parser)]
pub struct ControlWriteOptions {
    #[clap(flatten)]
    pub service: ServiceIdentifier,

    #[clap(short, long)]
    /// Application identifier
    pub kind: u16,

    #[clap(short, long, value_parser = data_from_str)]
    /// Data body (base64 encoded for string parsing)
    pub data: Data,
}

