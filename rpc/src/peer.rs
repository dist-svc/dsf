use std::net::SocketAddr;
use std::time::{Duration, SystemTime};

use clap::Parser;
use serde::{Deserialize, Serialize};
use strum::Display;

use dsf_core::types::*;

use crate::helpers::{parse_duration, try_parse_sock_addr};
use crate::ServiceIdentifier;

/// PeerState defines the state of a peer
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Display)]
pub enum PeerState {
    /// A peer that has not been contacted exists in the Unknown state
    Unknown,
    /// Once public keys have been exchanged this moves to the Known state
    Known(PublicKey),
    //Peered(Service),
}

/// PeerState defines the state of a peer
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum PeerAddress {
    /// Implicit address
    Implicit(Address),
    /// Explicit / requested address
    Explicit(Address),
}

bitflags::bitflags!(
    /// Flags for peer information
    #[derive(Default, Serialize, Deserialize)]
    pub struct PeerFlags: u16 {
        /// Message flag indicating symmetric encryption is available
        const SYMMETRIC_AVAILABLE = (1 << 0);
        /// Message flag indicating symmetric encryption is in use
        const SYMMETRIC_ENABLED = (1 << 1);
        /// Message and object flag indicating device is constrained
        const CONSTRAINED = (1 << 2);
        /// Message and object flag indicating peer identity is transient and should not be persisted
        const TRANSIENT = (1 << 3);

        // Mask for persistent bit flags
        const PERSISTED = Self::CONSTRAINED.bits();
    }
);

/// PeerInfo object for storage and exchange of peer information
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "diesel", derive(diesel::Queryable))]
pub struct PeerInfo {
    pub id: Id,
    pub short_id: ShortId,
    pub index: usize,
    pub address: PeerAddress,
    pub state: PeerState,
    pub seen: Option<SystemTime>,
    pub flags: PeerFlags,

    pub sent: u64,
    pub received: u64,
    pub blocked: bool,
}

impl PeerInfo {
    pub fn new(
        id: Id,
        address: PeerAddress,
        state: PeerState,
        index: usize,
        seen: Option<SystemTime>,
    ) -> Self {
        let short_id = ShortId::from(&id);
        Self {
            id,
            short_id,
            index,
            address,
            state,
            seen,
            flags: PeerFlags::empty(),
            sent: 0,
            received: 0,
            blocked: false,
        }
    }

    /// Fetch the address of a peer
    pub fn address(&self) -> &Address {
        match &self.address {
            PeerAddress::Explicit(e) => e,
            PeerAddress::Implicit(i) => i,
        }
    }

    pub fn update_address(&mut self, addr: PeerAddress) {
        use PeerAddress::*;

        match (&self.address, &addr) {
            (_, Explicit(_)) => self.address = addr,
            (Implicit(_), Implicit(_)) => self.address = addr,
            _ => (),
        }
    }

    /// Fetch the state of a peer
    pub fn state(&self) -> &PeerState {
        &self.state
    }

    // Set the state of a peer
    pub fn set_state(&mut self, state: PeerState) {
        self.state = state;
    }

    pub fn seen(&self) -> Option<SystemTime> {
        self.seen
    }

    pub fn set_seen(&mut self, seen: SystemTime) {
        self.seen = Some(seen);
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, clap::Parser)]
pub enum PeerCommands {
    /// List known peers
    List(PeerListOptions),

    /// Connects to a known peer
    Connect(ConnectOptions),

    /// Fetches information for a given peer
    Info(ServiceIdentifier),

    /// Searches the database for a peer
    Search(SearchOptions),

    /// Removes a known peer from the database
    Remove(ServiceIdentifier),

    /// TODO: Blocks a peer
    Block(ServiceIdentifier),

    /// TODO: Unblocks a peer
    Unblock(ServiceIdentifier),
}

/// ConnectOptions passed to connect function
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Parser)]
pub struct ConnectOptions {
    #[clap(value_parser = try_parse_sock_addr)]
    /// Socket address for connection attempt
    pub address: SocketAddr,

    #[clap(short, long)]
    /// ID of the remote node
    pub id: Option<Id>,

    #[clap(short, long, value_parser = parse_duration)]
    /// ID of the remote node
    pub timeout: Option<Duration>,
}

// Peer list options
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Parser)]
pub struct PeerListOptions {}

/// ConnectOptions passed to connect function
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Parser)]
pub struct SearchOptions {
    #[clap(short, long)]
    /// ID of the peer to find
    pub id: Id,

    #[clap(short, long, value_parser = parse_duration)]
    /// ID of the remote node
    pub timeout: Option<Duration>,
}

/// ConnectInfo returned by connect function
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ConnectInfo {
    pub id: Id,
    pub peers: usize,
}
