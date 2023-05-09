use core::convert::TryFrom;

use encdec::{DecodeOwned, Encode};
use num_enum::{IntoPrimitive, TryFromPrimitive};
use strum::{Display, EnumString};

use crate::error::Error;

/// [ServiceKind] enumerates different types of services
#[derive(PartialEq, Debug, Clone, Display, EnumString)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
#[strum(serialize_all = "snake_case")]
pub enum ServiceKind {
    Generic,
    Peer,
    Name,
    Application,
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

#[derive(Debug, Copy, Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub enum Kind {
    Page { app: bool, variant: u8 },
    Data { app: bool, variant: u8 },
    Request { variant: u8 },
    Response { variant: u8 },
}

#[derive(Debug, Copy, Clone, PartialEq, EnumString, Display)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
#[strum(serialize_all = "snake_case")]
pub enum BaseKind {
    Page,
    Data,
    Request,
    Response,
}

impl Kind {
    pub fn variant(&self) -> u8 {
        match self {
            Kind::Page { variant, .. }
            | Kind::Data { variant, .. }
            | Kind::Request { variant }
            | Kind::Response { variant } => *variant,
        }
    }

    pub fn base(&self) -> BaseKind {
        match self {
            Kind::Page { .. } => BaseKind::Page,
            Kind::Data { .. } => BaseKind::Data,
            Kind::Request { .. } => BaseKind::Request,
            Kind::Response { .. } => BaseKind::Response,
        }
    }

    pub fn is_application(&self) -> bool {
        match self {
            Self::Page { app, .. } | Self::Data { app, .. } if *app => true,
            _ => false,
        }
    }

    pub fn is_page(&self) -> bool {
        match self {
            Self::Page { .. } => true,
            _ => false,
        }
    }

    pub fn is_data(&self) -> bool {
        match self {
            Self::Data { .. } => true,
            _ => false,
        }
    }

    pub fn is_request(&self) -> bool {
        match self {
            Self::Request { .. } => true,
            _ => false,
        }
    }

    pub fn is_response(&self) -> bool {
        match self {
            Self::Response { .. } => true,
            _ => false,
        }
    }

    pub fn is_message(&self) -> bool {
        self.is_request() || self.is_response()
    }

    pub fn page(app: bool, variant: u8) -> Self {
        Self::Page { app, variant }
    }

    pub fn data(app: bool, variant: u8) -> Self {
        Self::Data { app, variant }
    }

    pub fn request(variant: u8) -> Self {
        Self::Request { variant }
    }

    pub fn response(variant: u8) -> Self {
        Self::Response { variant }
    }
}

impl From<u8> for Kind {
    fn from(value: u8) -> Self {
        match (value >> 6) & 0b11 {
            0b00 => Kind::Page {
                app: (value >> 5) & 0b1 != 0,
                variant: value & 0b0001_1111,
            },
            0b01 => Kind::Data {
                app: (value >> 5) & 0b1 != 0,
                variant: value & 0b0001_1111,
            },
            0b10 => Kind::Request {
                variant: value & 0b0011_1111,
            },
            0b11 => Kind::Response {
                variant: value & 0b0011_1111,
            },
            _ => unreachable!(),
        }
    }
}

impl From<Kind> for u8 {
    fn from(value: Kind) -> Self {
        let mut b = 0;
        match value {
            Kind::Page { app, variant } => {
                b |= 0b00 << 6;
                if app {
                    b |= 1 << 5;
                }
                b |= variant & 0b0001_1111;
            }
            Kind::Data { app, variant } => {
                b |= 0b01 << 6;
                if app {
                    b |= 1 << 5;
                }
                b |= variant & 0b0001_1111;
            }
            Kind::Request { variant } => {
                b |= 0b10 << 6;
                b |= variant & 0b0011_1111;
            }
            Kind::Response { variant } => {
                b |= 0b11 << 6;
                b |= variant & 0b0011_1111;
            }
        }
        b
    }
}

impl Encode for Kind {
    type Error = Error;

    fn encode_len(&self) -> Result<usize, Self::Error> {
        Ok(1)
    }

    fn encode(&self, buff: &mut [u8]) -> Result<usize, Self::Error> {
        if buff.len() < 1 {
            return Err(Error::BufferLength);
        }

        buff[0] = (*self).into();

        Ok(1)
    }
}

impl DecodeOwned for Kind {
    type Output = Self;

    type Error = Error;

    fn decode_owned(buff: &[u8]) -> Result<(Self::Output, usize), Self::Error> {
        if buff.len() < 1 {
            return Err(Error::BufferLength);
        }

        let s = Self::from(buff[0]);

        Ok((s, 1))
    }
}

impl core::fmt::Display for Kind {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Kind::Page { app, variant } => match PageKind::try_from(*variant) {
                Ok(k) if !app => write!(f, "Page({k})")?,
                _ => write!(f, "Page({variant:02x})")?,
            },
            Kind::Data { app, variant } => match DataKind::try_from(*variant) {
                Ok(k) if !app => write!(f, "Data({k})")?,
                _ => write!(f, "Data({variant:02x})")?,
            },
            Kind::Request { variant } => match RequestKind::try_from(*variant) {
                Ok(k) => write!(f, "Request({k})")?,
                _ => write!(f, "Request({variant:02x})")?,
            },
            Kind::Response { variant } => match ResponseKind::try_from(*variant) {
                Ok(k) => write!(f, "Response({k})")?,
                _ => write!(f, "Response({variant:02x})")?,
            },
        }

        Ok(())
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

    /// Application specific primary page
    Application = 0b0010_0000,
}

impl TryFrom<Kind> for PageKind {
    type Error = KindError;

    fn try_from(v: Kind) -> Result<Self, Self::Error> {
        // Check kind mask
        let (app, variant) = match v {
            Kind::Page { app, variant } => (app, variant),
            _ => return Err(KindError::InvalidKind(v)),
        };

        // Convert to page kind
        if app {
            return Ok(PageKind::Application);
        }

        match PageKind::try_from(variant) {
            Ok(v) => Ok(v),
            Err(_e) => Err(KindError::InvalidKind(v)),
        }
    }
}

impl From<PageKind> for Kind {
    fn from(p: PageKind) -> Kind {
        let app = matches!(p, PageKind::Application);
        Kind::page(app, p as u8)
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
        Kind::request(k as u8)
    }
}

impl TryFrom<Kind> for RequestKind {
    type Error = KindError;

    fn try_from(value: Kind) -> Result<Self, Self::Error> {
        if !value.is_request() {
            return Err(KindError::InvalidKind(value));
        }

        RequestKind::try_from(value.variant()).map_err(|_| KindError::Unrecognized(value))
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
        Kind::response(k as u8)
    }
}

impl TryFrom<Kind> for ResponseKind {
    type Error = KindError;

    fn try_from(value: Kind) -> Result<Self, Self::Error> {
        if !value.is_response() {
            return Err(KindError::InvalidKind(value));
        }

        ResponseKind::try_from(value.variant()).map_err(|_| KindError::Unrecognized(value))
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

    /// Application data object
    Application = 0b0010_0000,
}

impl From<DataKind> for Kind {
    fn from(k: DataKind) -> Self {
        let app = matches!(k, DataKind::Application);
        Kind::data(app, k as u8)
    }
}

impl TryFrom<Kind> for DataKind {
    type Error = KindError;

    fn try_from(value: Kind) -> Result<Self, Self::Error> {
        // Check kind mask
        let (app, variant) = match value {
            Kind::Data { app, variant } => (app, variant),
            _ => return Err(KindError::InvalidKind(value)),
        };

        // Convert to data kind
        if app {
            return Ok(DataKind::Application);
        }

        DataKind::try_from(variant).map_err(|_| KindError::Unrecognized(value))
    }
}

#[cfg(test)]
mod tests {
    use encdec::Decode;

    use super::*;

    #[test]
    fn test_kinds() {
        let tests = vec![
            (Kind::page(false, 0x04), [0b0000_0100]),
            (Kind::page(true, 0x05), [0b0010_0101]),
            (Kind::data(false, 0x06), [0b0100_0110]),
            (Kind::data(true, 0x08), [0b0110_1000]),
            (Kind::request(0x19), [0b1001_1001]),
            (Kind::response(0x2a), [0b1110_1010]),
        ];

        test_kind_coersions(&tests, |_| ())
    }

    #[test]
    fn test_page_kinds() {
        let tests = vec![
            // Pages
            (PageKind::Generic, [0b0000_0000]),
            (PageKind::Peer, [0b0000_0001]),
            (PageKind::Replica, [0b0000_0010]),
            (PageKind::Name, [0b0000_0011]),
            (PageKind::ServiceLink, [0b0000_0100]),
            (PageKind::BlockLink, [0b0000_0101]),
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
    fn test_data_kinds() {
        let tests = vec![(DataKind::Generic, [0b0100_0000])];

        test_kind_coersions(&tests, |k| {
            assert_eq!(k.is_data(), true);
            assert_eq!(k.is_page(), false);
            assert_eq!(k.is_message(), false);
            assert_eq!(k.is_request(), false);
            assert_eq!(k.is_response(), false);
        });
    }

    #[test]
    fn test_request_kinds() {
        let tests = vec![
            (RequestKind::Hello, [0b1000_0000]),
            (RequestKind::Ping, [0b1000_0001]),
            (RequestKind::FindNodes, [0b1000_0010]),
            (RequestKind::FindValues, [0b1000_0011]),
            (RequestKind::Store, [0b1000_0100]),
            (RequestKind::Subscribe, [0b1000_0101]),
            (RequestKind::Query, [0b1000_0110]),
            (RequestKind::PushData, [0b1000_0111]),
            (RequestKind::Unsubscribe, [0b1000_1000]),
            (RequestKind::Register, [0b1000_1001]),
            (RequestKind::Unregister, [0b1000_1010]),
            (RequestKind::Discover, [0b1000_1011]),
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
            (ResponseKind::Status, [0b1100_0000]),
            (ResponseKind::NoResult, [0b1100_0001]),
            (ResponseKind::NodesFound, [0b1100_0010]),
            (ResponseKind::ValuesFound, [0b1100_0011]),
            (ResponseKind::PullData, [0b1100_0100]),
        ];

        test_kind_coersions(&tests, |k| {
            assert_eq!(k.is_data(), false);
            assert_eq!(k.is_page(), false);
            assert_eq!(k.is_message(), true);
            assert_eq!(k.is_request(), false);
            assert_eq!(k.is_response(), true);
        });
    }

    fn test_kind_coersions<K>(tests: &[(K, [u8; 1])], check: impl Fn(&Kind))
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
            let mut buff = [0u8; 1];
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
