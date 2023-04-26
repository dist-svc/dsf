use std::net::SocketAddr;

use clap::Parser;
use serde::{Deserialize, Serialize};

use crate::helpers::try_parse_sock_addr;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Parser)]
pub enum ConfigCommands {
    #[clap(name = "add-address")]
    /// TODO: Register an external address for use by the daemon
    AddAddress(SocketAddress),

    #[clap(name = "remove-address")]
    /// TODO: De-register an external address for the daemon
    RemoveAddress(SocketAddress),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Parser)]
pub struct SocketAddress {
    #[clap(value_parser = try_parse_sock_addr)]
    /// Peer socket address
    pub address: SocketAddr,
}
