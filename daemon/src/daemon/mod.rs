use clap::Parser;

use kad::prelude::*;

pub mod dht;

pub mod net;

pub mod ops;

pub mod dsf;
pub use dsf::*;

#[cfg(test)]
mod tests;

/// DSF Instance Configuration Options
#[derive(Clone, Debug, PartialEq, Parser)]
pub struct DsfOptions {
    #[clap(flatten)]
    pub dht: DhtConfig,
}

impl Default for DsfOptions {
    fn default() -> Self {
        DsfOptions {
            dht: DhtConfig::default(),
        }
    }
}

/// DSF System Event
/// Used to prompt asynchronous system updates
#[derive(Clone, Debug, PartialEq)]
pub enum Event {}
