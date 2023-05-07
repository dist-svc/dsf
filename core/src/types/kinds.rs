use core::convert::TryFrom;

use encdec::{DecodeOwned, Encode};
use modular_bitfield::prelude::*;
use num_enum::{IntoPrimitive, TryFromPrimitive};
use strum::{Display, EnumString};

use crate::error::Error;

/// [ServiceKind] enumerates different types of services
#[derive(PartialEq, Debug, Clone, Display, EnumString)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub enum ServiceKind {
    Generic,
    Peer,
    Name,
    Unknown,
}

impl From<ServiceKind> for PageKind {
    fn from(value: ServiceKind) -> Self {
        match value {
            ServiceKind::Peer => PageKind::Peer,
            ServiceKind::Name => PageKind::Name,
            _ => PageKind::Generic,
        }
    }
}

impl From<PageKind> for ServiceKind {
    fn from(value: PageKind) -> Self {
        match value {
            PageKind::Peer => ServiceKind::Peer,
            PageKind::Name => ServiceKind::Name,
            _ => ServiceKind::Generic,
        }
    }
}

/// [Kind] combines BaseKind and SubKinds
#[derive(Copy, Clone, PartialEq, Debug)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub struct Kind {
    /// Base object type
    pub base_kind: BaseKind,
    /// Sub object type
    pub sub_kind: SubKind,
}

impl core::fmt::Display for Kind {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        let index = self.sub_kind.index();
        let app = self.sub_kind.app();

        write!(f, "{} ", self.base_kind)?;

        match self.base_kind {
            BaseKind::Page => match PageKind::try_from(index) {
                Ok(k) if !app => write!(f, "({k})")?,
                _ => write!(f, "({index:02x})")?,
            },
            BaseKind::Block => match DataKind::try_from(index) {
                Ok(k) if !app => write!(f, "({k})")?,
                _ => write!(f, "({index:02x})")?,
            },
            BaseKind::Request => match RequestKind::try_from(index) {
                Ok(k) if !app => write!(f, "({k})")?,
                _ => write!(f, "({index:02x})")?,
            },
            BaseKind::Response => match ResponseKind::try_from(index) {
                Ok(k) if !app => write!(f, "({k})")?,
                _ => write!(f, "({index:02x})")?,
            },
        }

        Ok(())
    }
}

pub enum Kind2 {
    Page(PageKind),
    Data(DataKind),
    Request(RequestKind),
    Response(ResponseKind),
}

impl From<Kind> for u16 {
    fn from(value: Kind) -> Self {
        u16::from_le_bytes([value.base_kind as u8, value.sub_kind.bytes[0]])
    }
}

impl Encode for Kind {
    type Error = Error;

    fn encode_len(&self) -> Result<usize, Self::Error> {
        Ok(2)
    }

    fn encode(&self, buff: &mut [u8]) -> Result<usize, Self::Error> {
        if buff.len() < 2 {
            return Err(Error::BufferLength);
        }

        buff[0] = self.base_kind as u8;
        buff[1] = self.sub_kind.bytes[0];

        Ok(2)
    }
}

impl DecodeOwned for Kind {
    type Output = Self;

    type Error = Error;

    fn decode_owned(buff: &[u8]) -> Result<(Self::Output, usize), Self::Error> {
        if buff.len() < 2 {
            return Err(Error::BufferLength);
        }

        let base_kind = BaseKind::try_from(buff[0]).map_err(|_| Error::InvalidPageKind)?;

        let sub_kind = SubKind::from_bytes([buff[1]]);

        Ok((
            Self {
                base_kind,
                sub_kind,
            },
            2,
        ))
    }
}

/// [BaseKind] differentiates between pages, blocks, and messages
#[derive(Copy, Clone, PartialEq, Debug, Display, EnumString, TryFromPrimitive)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
#[repr(u8)]
pub enum BaseKind {
    /// Page object (for storage in the DHT)
    Page = 0,
    /// Data object (for pub/sub/distribution)
    Block = 1,
    /// Request message
    Request = 2,
    /// Response message
    Response = 3,
}

/// [SubKind] identifies sub-object types (eg. types of page / block / request / response)
#[bitfield]
#[derive(Copy, Clone, PartialEq, Debug)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
#[repr(u8)]
pub struct SubKind {
    /// Object kind index
    pub index: B7,
    /// Application flag, indicates object is application defined / external to DSF
    pub app: bool,
}

impl Kind {
    pub fn is_application(&self) -> bool {
        self.sub_kind.app()
    }

    pub fn is_page(&self) -> bool {
        self.base_kind == BaseKind::Page
    }

    pub fn is_request(&self) -> bool {
        self.base_kind == BaseKind::Request
    }

    pub fn is_response(&self) -> bool {
        self.base_kind == BaseKind::Response
    }

    pub fn is_message(&self) -> bool {
        self.is_request() || self.is_response()
    }

    pub fn is_data(&self) -> bool {
        self.base_kind == BaseKind::Block
    }

    pub fn page(index: u8) -> Self {
        Kind {
            base_kind: BaseKind::Page,
            sub_kind: SubKind::new().with_index(index),
        }
    }

    pub fn request(index: u8) -> Self {
        Kind {
            base_kind: BaseKind::Request,
            sub_kind: SubKind::new().with_index(index),
        }
    }

    pub fn response(index: u8) -> Self {
        Kind {
            base_kind: BaseKind::Response,
            sub_kind: SubKind::new().with_index(index),
        }
    }

    pub fn data(index: u8) -> Self {
        Kind {
            base_kind: BaseKind::Block,
            sub_kind: SubKind::new().with_index(index),
        }
    }
}

// Error parsing kind values
#[derive(Clone, PartialEq, Debug, Copy)]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub enum KindError {
    InvalidKind(Kind),
    Unrecognized(Kind),
}

/// [PageKind] describes DSF-specific page kinds for encoding and decoding
#[derive(
    PartialEq,
    Debug,
    Clone,
    Copy,
    IntoPrimitive,
    TryFromPrimitive,
    Display,
    EnumString,
    strum::EnumVariantNames,
)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
#[cfg_attr(feature = "clap", derive(clap::ValueEnum))]
#[repr(u8)]
pub enum PageKind {
    /// Basic / default page, primary
    Generic = 0x00,

    /// Peer page, primary, encodes connection information for peers
    Peer = 0x01,

    /// Replica page, secondary, links a service to a replicating peer w/ QoS information
    Replica = 0x02,

    /// Name page, primary, defines a name service
    Name = 0x03,

    /// Service link page, tertiary, published by name service, links a hashed value to a third party service
    ServiceLink = 0x04,

    /// Block link page, tertiary, published by name services, links a hashed value to a block published by the name service
    BlockLink = 0x05,

    /// Private page kind, do not parse
    Private = 0x7F,
}

impl TryFrom<Kind> for PageKind {
    type Error = KindError;

    fn try_from(v: Kind) -> Result<Self, Self::Error> {
        // Check kind mask
        if v.base_kind != BaseKind::Page {
            return Err(KindError::InvalidKind(v));
        }

        // Convert to page kind
        match PageKind::try_from(v.sub_kind.index()) {
            Ok(v) => Ok(v),
            Err(_e) => Err(KindError::InvalidKind(v)),
        }
    }
}

impl From<PageKind> for Kind {
    fn from(p: PageKind) -> Kind {
        Kind {
            base_kind: BaseKind::Page,
            sub_kind: SubKind::new().with_index(p as u8),
        }
    }
}

/// [RequestKind] enumerates request message types
#[derive(Copy, Clone, PartialEq, Debug, EnumString, Display, TryFromPrimitive)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
#[repr(u8)]
pub enum RequestKind {
    Hello = 0x00,
    Ping = 0x01,
    FindNodes = 0x02,
    FindValues = 0x03,
    Store = 0x04,
    Subscribe = 0x05,
    Query = 0x06,
    PushData = 0x07,
    Unsubscribe = 0x08,
    Register = 0x09,
    Unregister = 0x0a,
    Discover = 0x0b,
    Locate = 0x0c,
}

impl From<RequestKind> for Kind {
    fn from(k: RequestKind) -> Self {
        Kind {
            base_kind: BaseKind::Request,
            sub_kind: SubKind::new().with_index(k as u8),
        }
    }
}

impl TryFrom<Kind> for RequestKind {
    type Error = KindError;

    fn try_from(value: Kind) -> Result<Self, Self::Error> {
        if value.base_kind != BaseKind::Request || value.sub_kind.app() {
            return Err(KindError::InvalidKind(value));
        }

        RequestKind::try_from(value.sub_kind.index()).map_err(|_| KindError::Unrecognized(value))
    }
}

/// [RequestKind] enumerates response message types
#[derive(Copy, Clone, PartialEq, Debug, EnumString, Display, TryFromPrimitive)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
#[repr(u8)]
pub enum ResponseKind {
    Status = 0x00,
    NoResult = 0x01,
    NodesFound = 0x02,
    ValuesFound = 0x03,
    PullData = 0x04,
}

impl From<ResponseKind> for Kind {
    fn from(k: ResponseKind) -> Self {
        Kind {
            base_kind: BaseKind::Response,
            sub_kind: SubKind::new().with_index(k as u8),
        }
    }
}

impl TryFrom<Kind> for ResponseKind {
    type Error = KindError;

    fn try_from(value: Kind) -> Result<Self, Self::Error> {
        if value.base_kind != BaseKind::Response || value.sub_kind.app() {
            return Err(KindError::InvalidKind(value));
        }

        ResponseKind::try_from(value.sub_kind.index()).map_err(|_| KindError::Unrecognized(value))
    }
}

/// [DataKind] enumerates data object types
#[derive(Copy, Clone, PartialEq, Debug, EnumString, Display, TryFromPrimitive)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
#[repr(u8)]
pub enum DataKind {
    /// Generic data object
    Generic = 0x00,
    /// Name service data object
    Name = 0x01,
    /// Per replica data object
    Replica = 0x02,
}

impl From<DataKind> for Kind {
    fn from(k: DataKind) -> Self {
        Kind {
            base_kind: BaseKind::Block,
            sub_kind: SubKind::new().with_index(k as u8),
        }
    }
}

impl TryFrom<Kind> for DataKind {
    type Error = KindError;

    fn try_from(value: Kind) -> Result<Self, Self::Error> {
        if value.base_kind != BaseKind::Block || value.sub_kind.app() {
            return Err(KindError::InvalidKind(value));
        }

        DataKind::try_from(value.sub_kind.index()).map_err(|_| KindError::Unrecognized(value))
    }
}

#[cfg(test)]
mod tests {
    use encdec::Decode;

    use super::*;

    #[test]
    fn test_page_kinds() {
        let tests = vec![
            // Pages
            (PageKind::Generic, [0b0000_0000, 0b0000_0000]),
            (PageKind::Peer, [0b0000_0000, 0b0000_0001]),
            (PageKind::Replica, [0b0000_0000, 0b0000_0010]),
            (PageKind::Name, [0b0000_0000, 0b0000_0011]),
            (PageKind::ServiceLink, [0b0000_0000, 0b0000_0100]),
            (PageKind::BlockLink, [0b0000_0000, 0b0000_0101]),
            (PageKind::Private, [0b0000_0000, 0b0111_1111]),
        ];

        test_kind_coersions(&tests, |k| {
            assert_eq!(k.is_data(), false);
            assert_eq!(k.is_page(), true);
            assert_eq!(k.is_message(), false);
            assert_eq!(k.is_request(), false);
            assert_eq!(k.is_response(), false);
        });
    }

    #[test]
    fn test_request_kinds() {
        let tests = vec![
            (RequestKind::Hello, [0b0000_0010, 0b0000_0000]),
            (RequestKind::Ping, [0b0000_0010, 0b0000_0001]),
            (RequestKind::FindNodes, [0b0000_0010, 0b0000_0010]),
            (RequestKind::FindValues, [0b0000_0010, 0b0000_0011]),
            (RequestKind::Store, [0b0000_0010, 0b0000_0100]),
            (RequestKind::Subscribe, [0b0000_0010, 0b0000_0101]),
            (RequestKind::Query, [0b0000_0010, 0b0000_0110]),
            (RequestKind::PushData, [0b0000_0010, 0b0000_0111]),
            (RequestKind::Unsubscribe, [0b0000_0010, 0b0000_1000]),
            (RequestKind::Register, [0b0000_0010, 0b0000_1001]),
            (RequestKind::Unregister, [0b0000_0010, 0b0000_1010]),
            (RequestKind::Discover, [0b0000_0010, 0b0000_1011]),
        ];

        test_kind_coersions(&tests, |k| {
            assert_eq!(k.is_data(), false);
            assert_eq!(k.is_page(), false);
            assert_eq!(k.is_message(), true);
            assert_eq!(k.is_request(), true);
            assert_eq!(k.is_response(), false);
        });
    }

    #[test]
    fn test_response_kinds() {
        let tests = vec![
            (ResponseKind::Status, [0b0000_0011, 0b0000_0000]),
            (ResponseKind::NoResult, [0b0000_0011, 0b0000_0001]),
            (ResponseKind::NodesFound, [0b0000_0011, 0b0000_0010]),
            (ResponseKind::ValuesFound, [0b0000_0011, 0b0000_0011]),
            (ResponseKind::PullData, [0b0000_0011, 0b0000_0100]),
        ];

        test_kind_coersions(&tests, |k| {
            assert_eq!(k.is_data(), false);
            assert_eq!(k.is_page(), false);
            assert_eq!(k.is_message(), true);
            assert_eq!(k.is_request(), false);
            assert_eq!(k.is_response(), true);
        });
    }

    #[test]
    fn test_data_kinds() {
        let tests = vec![(DataKind::Generic, [0b0000_0001, 0b0000_0000])];

        test_kind_coersions(&tests, |k| {
            assert_eq!(k.is_data(), true);
            assert_eq!(k.is_page(), false);
            assert_eq!(k.is_message(), false);
            assert_eq!(k.is_request(), false);
            assert_eq!(k.is_response(), false);
        });
    }

    fn test_kind_coersions<K>(tests: &[(K, [u8; 2])], check: impl Fn(&Kind))
    where
        K: Copy + TryFrom<Kind> + PartialEq + core::fmt::Debug,
        <K as TryFrom<Kind>>::Error: core::fmt::Debug,
        Kind: From<K>,
    {
        for (t, b) in tests {
            println!("t: {:02x?}, v: {:02x?}", t, b);

            // Convert to kind
            let v = Kind::from(*t);

            // Check flags
            check(&v);

            // Test encoding
            let mut buff = [0u8; 2];
            v.encode(&mut buff).unwrap();
            assert_eq!(&buff, b, "encode {:?} mismatch", t);

            // Test decoding
            let (v1, _) = Kind::decode(&buff).unwrap();
            assert_eq!(v, v1);

            // Test reverse conversion
            let d = K::try_from(v).unwrap();
            assert_eq!(t, &d, "decode {:?} failed", t);
        }
    }
}
