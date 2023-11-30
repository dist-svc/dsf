use std::{str::FromStr, time::Duration};

use clap::Parser;

use kad::prelude::*;

pub mod dht;

pub mod net;
mod net2;

pub mod ops;

pub mod dsf;
pub use dsf::*;

#[cfg(test)]
mod tests;

/// DSF Instance Configuration Options
#[derive(Clone, Debug, PartialEq, Parser)]
pub struct DsfOptions {
    /// Timeout for network requests
    #[clap(long, default_value="3s", value_parser=parse_human_duration, env)]
    pub net_timeout: Duration,

    #[clap(flatten)]
    pub dht: DhtConfig,
}

impl Default for DsfOptions {
    fn default() -> Self {
        DsfOptions {
            dht: DhtConfig::default(),
            net_timeout: Duration::from_millis(500),
        }
    }
}

/// DSF System Event
/// Used to prompt asynchronous system updates
#[derive(Clone, Debug, PartialEq)]
pub enum Event {}

/// Helper to parse durations from string
fn parse_human_duration(s: &str) -> Result<Duration, humantime::DurationError> {
    let h = humantime::Duration::from_str(s)?;
    Ok(h.into())
}
