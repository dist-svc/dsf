use clap::Parser;
use dsf_core::base::{DecodeOwned, Encode};
use dsf_core::options::OptionsIter;
use dsf_core::prelude::{DsfError, KeySource, Options};
use serde::{Deserialize, Serialize};

use dsf_core::{options::Filters, prelude::MaybeEncrypted, types::*, wire::Container};

use crate::helpers::data_from_str;
use crate::{PageBounds, ServiceIdentifier, TimeBounds};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DataInfo<B: Encode = Vec<u8>> {
    pub service: Id,
    pub kind: Kind,
    pub index: u32,

    pub body: MaybeEncrypted<B>,
    pub public_options: Vec<Options>,
    pub private_options: MaybeEncrypted<Vec<Options>>,

    pub previous: Option<Signature>,
    pub signature: Signature,
}

impl DataInfo {
    /// Parse [DataInfo] from a container, decrypting encrypted fields
    /// using the provided keys if available
    pub fn from_block<T: ImmutableData>(
        c: &Container<T>,
        keys: &impl KeySource,
    ) -> Result<Self, DsfError> {
        let id = c.id();
        let sec_key = keys.sec_key(&id);

        let (body, private_options) = match (c.encrypted(), sec_key.as_ref()) {
            (true, None) => (
                MaybeEncrypted::Encrypted(c.body_raw().to_vec()),
                MaybeEncrypted::Encrypted(c.private_options_raw().to_vec()),
            ),
            (true, Some(sk)) => {
                let mut buff = [0u8; 2048];
                let (body, private_opts) = c.decrypt_to(sk, &mut buff)?;

                let private_opts = OptionsIter::new(private_opts).collect();

                (
                    MaybeEncrypted::Cleartext(body.to_vec()),
                    MaybeEncrypted::Cleartext(private_opts),
                )
            }
            (false, _) => (
                MaybeEncrypted::Cleartext(c.body_raw().to_vec()),
                MaybeEncrypted::Cleartext(c.private_options_iter().collect()),
            ),
        };

        Ok(DataInfo {
            service: id,
            kind: c.header().kind(),
            index: c.header().index(),
            body,
            public_options: c.public_options_iter().collect(),
            private_options,
            previous: c.public_options_iter().prev_sig(),
            signature: c.signature(),
        })
    }

    /// Convert [DataInfo] object using application page or data encoding
    pub fn convert<B>(
        &self,
    ) -> Result<DataInfo<<B as DecodeOwned>::Output>, <B as DecodeOwned>::Error>
    where
        B: Encode + DecodeOwned,
        <B as DecodeOwned>::Output: Encode,
    {
        use MaybeEncrypted::*;

        let body = match &self.body {
            Cleartext(d) => {
                let (b, _) = B::decode_owned(&d)?;
                Cleartext(b)
            }
            Encrypted(e) => Encrypted(e.clone()),
            None => None,
        };

        Ok(DataInfo {
            service: self.service.clone(),
            kind: self.kind,
            index: self.index,
            body,
            public_options: self.public_options.clone(),
            private_options: self.private_options.clone(),
            previous: self.previous.clone(),
            signature: self.signature.clone(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Parser)]
pub enum DataCommands {
    #[clap(name = "list")]
    /// List service data
    List(ListOptions),

    #[clap(name = "query")]
    /// Fetch data from a service
    Query {},

    #[clap(name = "publish")]
    /// Publish data to a service
    Publish(PublishOptions),

    #[clap(name = "push")]
    /// Push pre-signed data for a known service
    Push(PushOptions),

    #[clap(name = "sync")]
    /// Sync data for a known service
    Sync(SyncOptions),

    #[clap(name = "get")]
    /// Fetch a specific data object
    Get(FetchOptions),
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
    pub kind: u8,

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
    /// Base58 encoded (pre-signed) DSF object
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
    pub index: u32,
    pub sig: Signature,
    pub subscribers: usize,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Parser)]
pub struct SyncOptions {
    #[clap(flatten)]
    pub service: ServiceIdentifier,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SyncInfo {
    /// Total object available
    pub total: usize,
    /// Number of synced objects
    pub synced: usize,
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
