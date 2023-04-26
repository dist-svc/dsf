use core::convert::TryFrom;

use modular_bitfield::prelude::*;
use num_enum::{IntoPrimitive, TryFromPrimitive};

/// [ServiceKind] enumerates different types of services
#[derive(PartialEq, Debug, Clone)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub enum ServiceKind {
    Generic,
    Peer,
    Replica,
    Registry,
    Private,
    Unknown(i16),
}

/// [Kind] identifies object types
#[bitfield]
#[derive(Copy, Clone, PartialEq, Debug)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
#[repr(u16)]
pub struct Kind {
    /// Object kind index
    pub index: B13,
    /// Application flag, indicates object is application defined / external to DSF
    pub app: bool,
    /// Base object kind specifier
    pub base: BaseKind,
}

/// [BaseKind] differentiates between pages, blocks, and messages
#[derive(BitfieldSpecifier, Copy, Clone, PartialEq, Debug)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
#[bits = 2]
pub enum BaseKind {
    /// Page object (for storage in the DHT)
    Page = 0,
    /// Data object
    Block = 1,
    /// Request message
    Request = 2,
    /// Response message
    Response = 3,
}

impl Kind {
    pub fn is_application(&self) -> bool {
        self.app()
    }

    pub fn is_page(&self) -> bool {
        self.base() == BaseKind::Page
    }

    pub fn is_request(&self) -> bool {
        self.base() == BaseKind::Request
    }

    pub fn is_response(&self) -> bool {
        self.base() == BaseKind::Response
    }

    pub fn is_message(&self) -> bool {
        self.is_request() || self.is_response()
    }

    pub fn is_data(&self) -> bool {
        self.base() == BaseKind::Block
    }

    pub fn page(index: u16) -> Self {
        Kind::new().with_base(BaseKind::Page).with_index(index)
    }

    pub fn request(index: u16) -> Self {
        Kind::new().with_base(BaseKind::Request).with_index(index)
    }

    pub fn response(index: u16) -> Self {
        Kind::new().with_base(BaseKind::Response).with_index(index)
    }

    pub fn data(index: u16) -> Self {
        Kind::new().with_base(BaseKind::Block).with_index(index)
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
    strum::Display,
    strum::EnumString,
    strum::EnumVariantNames,
)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
#[cfg_attr(feature = "clap", derive(clap::ValueEnum))]
#[repr(u16)]
pub enum PageKind {
    /// Basic / default page, primary
    Generic = 0x0000,

    /// Peer page, primary, encodes connection information for peers
    Peer = 0x0001,

    /// Replica page, secondary, links a service to a replicating peer w/ QoS information
    Replica = 0x0002,

    /// Name page, primary, defines a name service
    Name = 0x0003,

    /// Service link page, tertiary, published by name service, links a hashed value to a third party service
    ServiceLink = 0x0004,

    /// Block link page, tertiary, published by name services, links a hashed value to a block published by the name service
    BlockLink = 0x0005,

    /// Private page kind, do not parse
    Private = 0x0FFF,
}

impl TryFrom<Kind> for PageKind {
    type Error = KindError;

    fn try_from(v: Kind) -> Result<Self, Self::Error> {
        // Check kind mask
        if v.base() != BaseKind::Page {
            return Err(KindError::InvalidKind(v));
        }

        // Convert to page kind
        match PageKind::try_from(v.index()) {
            Ok(v) => Ok(v),
            Err(_e) => Err(KindError::InvalidKind(v)),
        }
    }
}

impl Into<Kind> for PageKind {
    fn into(self) -> Kind {
        Kind::new()
            .with_base(BaseKind::Page)
            .with_index(self as u16)
    }
}

/// [RequestKind] enumerates request message types
#[derive(BitfieldSpecifier, Copy, Clone, PartialEq, Debug, strum::EnumString, strum::Display)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
#[bits = 13]
pub enum RequestKind {
    Hello = 0x0000,
    Ping = 0x0001,
    FindNodes = 0x0002,
    FindValues = 0x0003,
    Store = 0x0004,
    Subscribe = 0x0005,
    Query = 0x0006,
    PushData = 0x0007,
    Unsubscribe = 0x0008,
    Register = 0x0009,
    Unregister = 0x000a,
    Discover = 0x000b,
    Locate = 0x000c,
}

impl From<RequestKind> for Kind {
    fn from(k: RequestKind) -> Self {
        Kind::new()
            .with_base(BaseKind::Request)
            .with_index(k as u16)
    }
}

impl TryFrom<Kind> for RequestKind {
    type Error = KindError;

    fn try_from(value: Kind) -> Result<Self, Self::Error> {
        if value.base() != BaseKind::Request || value.app() {
            return Err(KindError::InvalidKind(value));
        }

        RequestKind::from_bytes(value.index()).map_err(|_| KindError::Unrecognized(value))
    }
}

/// [RequestKind] enumerates response message types
#[derive(BitfieldSpecifier, Copy, Clone, PartialEq, Debug, strum::EnumString, strum::Display)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
#[bits = 13]
pub enum ResponseKind {
    Status = 0x0000,
    NoResult = 0x0001,
    NodesFound = 0x0002,
    ValuesFound = 0x0003,
    PullData = 0x0004,
}

impl From<ResponseKind> for Kind {
    fn from(k: ResponseKind) -> Self {
        Kind::new()
            .with_base(BaseKind::Response)
            .with_index(k as u16)
    }
}

impl TryFrom<Kind> for ResponseKind {
    type Error = KindError;

    fn try_from(value: Kind) -> Result<Self, Self::Error> {
        if value.base() != BaseKind::Response || value.app() {
            return Err(KindError::InvalidKind(value));
        }

        ResponseKind::from_bytes(value.index()).map_err(|_| KindError::Unrecognized(value))
    }
}

/// [DataKind] enumerates data object types
#[derive(BitfieldSpecifier, Copy, Clone, PartialEq, Debug)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
#[bits = 13]

pub enum DataKind {
    Generic = 0x0000,
}

impl From<DataKind> for Kind {
    fn from(k: DataKind) -> Self {
        Kind::new().with_base(BaseKind::Block).with_index(k as u16)
    }
}

impl TryFrom<Kind> for DataKind {
    type Error = KindError;

    fn try_from(value: Kind) -> Result<Self, Self::Error> {
        if value.base() != BaseKind::Block || value.app() {
            return Err(KindError::InvalidKind(value));
        }

        DataKind::from_bytes(value.index()).map_err(|_| KindError::Unrecognized(value))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_page_kinds() {
        let tests = vec![
            // Pages
            (
                PageKind::Generic,
                Kind::from_bytes([0b0000_0000, 0b0000_0000]),
            ),
            (PageKind::Peer, Kind::from_bytes([0b0000_0001, 0b0000_0000])),
            (
                PageKind::Replica,
                Kind::from_bytes([0b0000_0010, 0b0000_0000]),
            ),
            (PageKind::Name, Kind::from_bytes([0b0000_0011, 0b0000_0000])),
            (
                PageKind::ServiceLink,
                Kind::from_bytes([0b0000_0100, 0b0000_0000]),
            ),
            (
                PageKind::BlockLink,
                Kind::from_bytes([0b0000_0101, 0b0000_0000]),
            ),
            (
                PageKind::Private,
                Kind::from_bytes([0b1111_1111, 0b0000_1111]),
            ),
        ];

        for (t, v) in tests {
            println!("page t: {:?}, v: {:#08b}", t, u16::from(v));
            assert_eq!(v.is_page(), true);

            let k: Kind = t.into();
            assert_eq!(v, k, "error converting {:?}", t);

            let d = PageKind::try_from(v).expect("error parsing");
            assert_eq!(t, d, "error decoding {:?}", t);
        }
    }

    #[test]
    fn test_request_kinds() {
        let tests = vec![
            (
                RequestKind::Hello,
                Kind::from_bytes([0b0000_0000, 0b1000_0000]),
            ),
            (
                RequestKind::Ping,
                Kind::from_bytes([0b0000_0001, 0b1000_0000]),
            ),
            (
                RequestKind::FindNodes,
                Kind::from_bytes([0b0000_0010, 0b1000_0000]),
            ),
            (
                RequestKind::FindValues,
                Kind::from_bytes([0b0000_0011, 0b1000_0000]),
            ),
            (
                RequestKind::Store,
                Kind::from_bytes([0b0000_0100, 0b1000_0000]),
            ),
            (
                RequestKind::Subscribe,
                Kind::from_bytes([0b0000_0101, 0b1000_0000]),
            ),
            (
                RequestKind::Query,
                Kind::from_bytes([0b0000_0110, 0b1000_0000]),
            ),
            (
                RequestKind::PushData,
                Kind::from_bytes([0b0000_0111, 0b1000_0000]),
            ),
            (
                RequestKind::Unsubscribe,
                Kind::from_bytes([0b0000_1000, 0b1000_0000]),
            ),
            (
                RequestKind::Register,
                Kind::from_bytes([0b0000_1001, 0b1000_0000]),
            ),
            (
                RequestKind::Unregister,
                Kind::from_bytes([0b0000_1010, 0b1000_0000]),
            ),
            (
                RequestKind::Discover,
                Kind::from_bytes([0b0000_1011, 0b1000_0000]),
            ),
        ];

        for (t, v) in tests {
            println!("request t: {:02x?}, v: {:#b}", t, u16::from(v));
            assert_eq!(v.is_message(), true);
            assert_eq!(v.is_request(), true);

            let k = Kind::from(t);
            assert_eq!(v, k, "error converting {:02x?} into: {:02x?}", t, k);

            let d = RequestKind::try_from(v).expect("error parsing request kind");
            assert_eq!(t, d, "error decoding {:02x?}", t);
        }
    }

    #[test]
    fn test_response_kinds() {
        let tests = vec![
            (
                ResponseKind::Status,
                Kind::from_bytes([0b0000_0000, 0b1100_0000]),
            ),
            (
                ResponseKind::NoResult,
                Kind::from_bytes([0b0000_0001, 0b1100_0000]),
            ),
            (
                ResponseKind::NodesFound,
                Kind::from_bytes([0b0000_0010, 0b1100_0000]),
            ),
            (
                ResponseKind::ValuesFound,
                Kind::from_bytes([0b0000_0011, 0b1100_0000]),
            ),
            (
                ResponseKind::PullData,
                Kind::from_bytes([0b0000_0100, 0b1100_0000]),
            ),
        ];

        for (t, v) in tests {
            println!("message t: {:02x?}, v: {:#b}", t, u16::from(v));
            assert_eq!(v.is_message(), true);
            assert_eq!(v.is_response(), true);

            let k = Kind::from(t);
            assert_eq!(v, k, "error converting {:02x?} into: {:02x?}", t, k);

            let d = ResponseKind::try_from(v).expect("error parsing response kind");
            assert_eq!(t, d, "error decoding {:02x?}", t);
        }
    }

    #[test]
    fn test_data_kinds() {
        let tests = vec![(
            DataKind::Generic,
            Kind::from_bytes([0b0000_0000, 0b0100_0000]),
        )];

        for (t, v) in tests {
            println!("data t: {:02x?}, v: {:#b}", t, u16::from(v));
            assert_eq!(v.is_data(), true);

            let k: Kind = t.into();
            assert_eq!(v, k, "error converting {:02x?}", t);

            let d = DataKind::try_from(v).expect("error parsing data kind");
            assert_eq!(t, d, "error decoding {:02x?}", t);
        }
    }
}
