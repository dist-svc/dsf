use clap::Parser;
use serde::{Deserialize, Serialize};

use dsf_core::{base::Body, options::Filters, prelude::MaybeEncrypted, types::*, wire::Container};

use crate::helpers::data_from_str;
use crate::{PageBounds, ServiceIdentifier, TimeBounds};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DataInfo {
    pub service: Id,

    pub index: u16,
    pub body: Body,

    pub previous: Option<Signature>,
    pub signature: Signature,
}

impl std::convert::TryFrom<&Container> for DataInfo {
    type Error = std::convert::Infallible;

    fn try_from(page: &Container) -> Result<DataInfo, Self::Error> {
        let body = match page.encrypted() {
            true => MaybeEncrypted::Encrypted(page.body_raw().to_vec()),
            false => MaybeEncrypted::Cleartext(page.body_raw().to_vec()),
        };

        Ok(DataInfo {
            service: page.id(),
            index: page.header().index(),
            body,
            previous: page.public_options_iter().prev_sig(),
            signature: page.signature(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Parser)]
pub enum DataCommands {
    #[clap(name = "list")]
    /// List service data
    List(ListOptions),

    #[clap(name = "sync")]
    /// Synchronize service data
    Update {},

    #[clap(name = "query")]
    /// Fetch data from a service
    Query {},

    #[clap(name = "publish")]
    /// Publish data to a service
    Publish(PublishOptions),

    #[clap(name = "push")]
    /// Push pre-signed data for a known server
    Push(PushOptions),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Parser)]
pub struct ListOptions {
    #[clap(flatten)]
    pub service: ServiceIdentifier,

    #[clap(flatten)]
    #[serde(default)]
    pub page_bounds: PageBounds,

    #[clap(flatten)]
    #[serde(default)]
    pub time_bounds: TimeBounds,
}

pub type Data = Vec<u8>;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Parser)]
pub struct PublishOptions {
    #[clap(flatten)]
    pub service: ServiceIdentifier,

    #[clap(short, long, default_value = "0")]
    /// Data page kind (defaults to generic)
    pub kind: u16,

    #[clap(short, long, value_parser = data_from_str)]
    /// Data body as a string
    pub data: Option<Data>,
}

impl PublishOptions {
    pub fn new(id: Id) -> Self {
        Self {
            service: ServiceIdentifier {
                id: Some(id),
                index: None,
            },
            kind: 0,
            data: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Parser)]
pub struct PushOptions {
    #[clap(flatten)]
    pub service: ServiceIdentifier,

    #[clap(short, long, value_parser = data_from_str)]
    /// Base64 encoded (pre-signed) DSF object
    pub data: Data,
}

impl PushOptions {
    pub fn new(id: Id, data: Vec<u8>) -> Self {
        Self {
            service: ServiceIdentifier {
                id: Some(id),
                index: None,
            },
            data,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PublishInfo {
    pub index: u16,
    //pub sig: Signature,
}
