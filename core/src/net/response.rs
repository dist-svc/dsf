use core::convert::TryFrom;
use core::ops::Deref;

#[cfg(feature = "alloc")]
use alloc::vec::Vec;

use byteorder::{ByteOrder, NetworkEndian};
use encdec::{Decode, DecodeExt, Encode};
use slice_ext::SplitBefore;

use crate::error::Error;
use crate::keys::KeySource;
use crate::options::{Filters, Options};
use crate::types::*;
use crate::wire::Container;

use super::Common;

/// Generic Response message
#[derive(Clone, Debug)]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub struct Response {
    pub common: Common,
    pub data: ResponseBody,
}

/// Response message kinds
#[derive(Clone, PartialEq, Debug, strum::Display)]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub enum ResponseBody {
    Status(Status),
    NodesFound(Id, Vec<(Id, Address, PublicKey)>),
    ValuesFound(Id, Vec<Container>),
    NoResult,
    PullData(Id, Vec<Container>),
}

#[derive(Clone, Debug, Encode, Decode)]
pub struct Status2 {}
#[derive(Clone, Debug, Encode, Decode)]
pub struct NodesFound {}
#[derive(Clone, Debug, Encode, Decode)]
pub struct ValuesFound {}
#[derive(Clone, Debug, Encode, Decode)]
pub struct NoResult {}
#[derive(Clone, Debug, Encode, Decode)]
pub struct PullData {}

/// Convert response kind object to protocol message enumeration
impl From<&ResponseBody> for ResponseKind {
    fn from(r: &ResponseBody) -> Self {
        match r {
            ResponseBody::Status(_) => ResponseKind::Status,
            ResponseBody::NodesFound(_, _) => ResponseKind::NodesFound,
            ResponseBody::ValuesFound(_, _) => ResponseKind::ValuesFound,
            ResponseBody::NoResult => ResponseKind::NoResult,
            ResponseBody::PullData(_, _) => ResponseKind::PullData,
        }
    }
}

mod status {
    pub const OK: u32 = 0x00;
    pub const INVALID_REQUEST: u32 = 0x01;
    pub const FAILED: u32 = 0x02;
}

/// Status response codes
#[derive(Copy, Clone, PartialEq, Debug)]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub enum Status {
    Ok,
    InvalidRequest,
    Failed,
    Unknown(u32),
}

impl From<u32> for Status {
    fn from(v: u32) -> Self {
        match v {
            status::OK => Status::Ok,
            status::INVALID_REQUEST => Status::InvalidRequest,
            status::FAILED => Status::Failed,
            _ => Status::Unknown(v),
        }
    }
}

impl From<&Status> for u32 {
    fn from(s: &Status) -> u32 {
        match s {
            Status::Ok => status::OK,
            Status::InvalidRequest => status::INVALID_REQUEST,
            Status::Failed => status::FAILED,
            Status::Unknown(v) => *v,
        }
    }
}

impl Deref for Response {
    type Target = Common;

    fn deref(&self) -> &Common {
        &self.common
    }
}

impl Response {
    pub fn new(from: Id, id: RequestId, data: ResponseBody, mut flags: Flags) -> Response {
        flags.remove(Flags::SYMMETRIC_DIR);
        let common = Common {
            from,
            id,
            flags,
            public_key: None,
            remote_address: None,
        };
        Response { common, data }
    }

    pub fn flags(&mut self) -> &mut Flags {
        &mut self.common.flags
    }

    pub fn with_remote_address(mut self, addr: Address) -> Self {
        self.common.remote_address = Some(addr);
        self
    }

    pub fn set_public_key(&mut self, pk: PublicKey) {
        self.common.public_key = Some(pk);
    }

    pub fn with_public_key(mut self, pk: PublicKey) -> Self {
        self.common.public_key = Some(pk);
        self
    }
}

impl PartialEq for Response {
    fn eq(&self, b: &Self) -> bool {
        self.from == b.from && self.flags == b.flags && self.data == b.data
    }
}

impl Response {
    pub fn convert<T: ImmutableData, K: KeySource>(
        base: Container<T>,
        key_source: &K,
    ) -> Result<Response, Error> {
        let header = base.header();

        if base.encrypted() {
            error!("Attempted to convert encrypted container to request");
            return Err(Error::CryptoError);
        }

        let body = base.body_raw();

        let remote_address = None;

        let public_options: Vec<_> = base.public_options_iter().collect();
        //let _private_options = base.private_options().to_vec();
        let public_key = Filters::pub_key(&public_options.iter());

        let kind = match ResponseKind::try_from(header.kind()) {
            Ok(k) => k,
            Err(_) => return Err(Error::InvalidResponseKind),
        };

        let data = match kind {
            ResponseKind::Status => {
                let status = NetworkEndian::read_u32(body);
                ResponseBody::Status(status.into())
            }
            ResponseKind::NoResult => ResponseBody::NoResult,
            ResponseKind::NodesFound => {
                let mut id = Id::default();
                id.copy_from_slice(&body[..ID_LEN]);

                // Build options array from body
                let options = Options::decode_iter(&body[ID_LEN..])
                    .collect::<Result<Vec<Options>, Error>>()
                    .unwrap();

                let nodes: Vec<_> = (&options[..])
                    .split_before(|o| match o {
                        Options::PeerId(_) => true,
                        _ => false,
                    })
                    .filter_map(|opts| {
                        let id = Filters::peer_id(&opts.iter());
                        let addr = Filters::address(&opts.iter());
                        let key = Filters::pub_key(&opts.iter());

                        match (id, addr, key) {
                            (Some(id), Some(addr), Some(key)) => Some((id, addr, key)),
                            // TODO: warn here
                            _ => None,
                        }
                    })
                    .collect();

                ResponseBody::NodesFound(id, nodes)
            }
            ResponseKind::ValuesFound => {
                let mut id = Id::default();
                id.copy_from_slice(&body[0..ID_LEN]);

                let pages = Container::decode_pages(&body[ID_LEN..], key_source)?;

                ResponseBody::ValuesFound(id, pages)
            }
            ResponseKind::PullData => {
                let mut id = Id::default();
                id.copy_from_slice(&body[0..ID_LEN]);

                let pages = Container::decode_pages(&body[ID_LEN..], key_source)?;

                ResponseBody::PullData(id, pages)
            }
        };

        // Fetch other message specific options
        let common = Common {
            from: base.id(),
            id: header.index(),
            flags: header.flags(),
            public_key,
            remote_address,
        };
        Ok(Response { common, data })
    }
}
