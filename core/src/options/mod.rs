//! Options are used to support extension of protocol objects
//! with DSF and application-specific optional fields.

use core::convert::TryFrom;
use core::fmt::Debug;

use byteorder::{ByteOrder, LittleEndian};
use encdec::{Decode, Encode};
use heapless::String;
use num_enum::{IntoPrimitive, TryFromPrimitive};

use crate::crypto::{Crypto, PubKey};
use crate::error::Error;
use crate::types::{
    Address, AddressV4, AddressV6, DateTime, Id, Ip, PrivateKey, PublicKey, Queryable, Signature,
    ID_LEN, PUBLIC_KEY_LEN, SIGNATURE_LEN,
};

mod helpers;
pub use helpers::{Filters, OptionsIter, OptionsParseError};

/// Option header length
const OPTION_HEADER_LEN: usize = 4;

pub const MAX_OPTION_LEN: usize = 64;

/// DSF defined option fields
#[derive(PartialEq, Debug, Clone)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub enum Options {
    None,
    PubKey(PublicKey),
    PeerId(Id),
    PrevSig(Signature),
    Kind(OptionString),
    Name(OptionString),

    IPv4(AddressV4),
    IPv6(AddressV6),

    Issued(DateTime),
    Expiry(DateTime),
    Limit(u32),
    Metadata(Metadata),
    Coord(Coordinates),

    Manufacturer(OptionString),
    Serial(OptionString),
    Building(OptionString),
    Room(OptionString),

    Index(u32),

    DelegationProof(Signature),
}

#[derive(
    PartialEq,
    Debug,
    Clone,
    Copy,
    IntoPrimitive,
    TryFromPrimitive,
    strum::EnumString,
    strum::Display,
)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
#[repr(u16)]
pub enum OptionKind {
    None = 0x0000,
    PubKey = 0x0001,          // Public Key
    PeerId = 0x0002,          // Peer ID, owner of secondary page or delegated origin
    PrevSig = 0x0003,         // Previous object signature
    Kind = 0x0004,            // Service KIND in utf-8
    Name = 0x0005,            // Service NAME in utf-8
    IpAddrV4 = 0x0006,        // IPv4 service address
    IpAddrV6 = 0x0007,        // IPv6 service address
    Issued = 0x0008,          // ISSUED option defines object creation time
    Expiry = 0x0009,          // EXPIRY option defines object expiry time
    Limit = 0x000a,           // LIMIT option defines maximum number of objects to return
    Meta = 0x000b,            // META option supports generic metadata key:value pairs
    Building = 0x000c,        // Building name / number (string)
    Room = 0x000d,            // Room name / number (string)
    Coord = 0x000e,           // Coordinates (lat, lng, alt)
    Manufacturer = 0x000f,    // Manufacturer name (string)
    Serial = 0x0010,          // Device serial (string)
    Index = 0x0011,           // Object index
    DelegationProof = 0x0012, // Delegation proof
}

impl From<&Options> for OptionKind {
    /// Fetch a protocol [`OptionKind`] enum from a concrete [`Options`] object
    fn from(o: &Options) -> Self {
        match o {
            Options::None => OptionKind::None,
            Options::PubKey(_) => OptionKind::PubKey,
            Options::PeerId(_) => OptionKind::PeerId,
            Options::PrevSig(_) => OptionKind::PrevSig,
            Options::Kind(_) => OptionKind::Kind,
            Options::Name(_) => OptionKind::Name,
            Options::IPv4(_) => OptionKind::IpAddrV4,
            Options::IPv6(_) => OptionKind::IpAddrV6,
            Options::Issued(_) => OptionKind::Issued,
            Options::Expiry(_) => OptionKind::Expiry,
            Options::Limit(_) => OptionKind::Limit,
            Options::Metadata(_) => OptionKind::Meta,
            Options::Coord(_) => OptionKind::Coord,
            Options::Building(_) => OptionKind::Building,
            Options::Room(_) => OptionKind::Room,
            Options::Manufacturer(_) => OptionKind::Manufacturer,
            Options::Serial(_) => OptionKind::Serial,
            Options::Index(_) => OptionKind::Index,
            Options::DelegationProof(_) => OptionKind::DelegationProof,
        }
    }
}

/// Helpers to create options instances
impl Options {
    pub fn name(value: impl AsRef<str>) -> Options {
        Options::Name(value.as_ref().into())
    }

    pub fn kind(value: &str) -> Options {
        Options::Kind(value.into())
    }

    pub fn prev_sig(value: &Signature) -> Options {
        Options::PrevSig(value.clone())
    }

    pub fn meta(key: &str, value: &str) -> Options {
        Options::Metadata(Metadata {
            key: heapless::String::from(key),
            value: heapless::String::from(value),
        })
    }

    pub fn issued<T: Into<DateTime>>(now: T) -> Options {
        Options::Issued(now.into())
    }

    pub fn expiry<T: Into<DateTime>>(when: T) -> Options {
        Options::Expiry(when.into())
    }

    pub fn peer_id(id: Id) -> Options {
        Options::PeerId(id)
    }

    pub fn public_key(public_key: PublicKey) -> Options {
        Options::PubKey(public_key)
    }

    pub fn address<T: Into<Address>>(address: T) -> Options {
        let addr: Address = address.into();

        match addr.ip {
            Ip::V4(ip) => Options::IPv4(AddressV4::new(ip, addr.port)),
            Ip::V6(ip) => Options::IPv6(AddressV6::new(ip, addr.port)),
        }
    }

    pub fn address_v4<T: Into<AddressV4>>(address: T) -> Options {
        Options::IPv4(address.into())
    }

    pub fn is_address_v4(&self) -> bool {
        match self {
            Self::IPv4(_) => true,
            _ => false,
        }
    }

    pub fn address_v6<T: Into<AddressV6>>(address: T) -> Options {
        Options::IPv6(address.into())
    }

    pub fn pub_key(public_key: PublicKey) -> Options {
        Options::PubKey(public_key)
    }

    pub fn building(value: impl AsRef<str>) -> Options {
        Options::Building(value.as_ref().into())
    }

    pub fn room(value: impl AsRef<str>) -> Options {
        Options::Room(value.as_ref().into())
    }

    pub fn proof(sig: Signature) -> Options {
        Options::DelegationProof(sig.clone())
    }

    /// Indicates an option is filterable and should be considered
    /// when filtering service responses
    pub fn filterable(&self) -> bool {
        match self {
            Self::Name(..)
            | Self::Kind(..)
            | Self::Building(..)
            | Self::Room(..)
            | Self::Manufacturer(..)
            | Self::Serial(..) => true,
            _ => false,
        }
    }
}

/// Parse parses a control option from the given scope
impl<'a> Decode<'a> for Options {
    type Output = Options;
    type Error = Error;

    fn decode(data: &'a [u8]) -> Result<(Self::Output, usize), Self::Error> {
        if data.len() < OPTION_HEADER_LEN {
            return Err(Error::InvalidOptionLength);
        }

        let option_kind = LittleEndian::read_u16(&data[0..2]);
        let option_len = LittleEndian::read_u16(&data[2..4]) as usize;

        trace!(
            "Parse option kind: 0x{:02x} length: {}",
            option_kind,
            option_len
        );

        if (OPTION_HEADER_LEN + option_len) > data.len() {
            warn!(
                "Option length ({}) exceeds buffer length ({}) for kind: {}",
                option_len,
                data.len(),
                option_kind
            );
            return Err(Error::InvalidOptionLength);
        }

        let d = &data[OPTION_HEADER_LEN..][..option_len];

        // Convert to option kind
        let k = match OptionKind::try_from(option_kind) {
            Ok(v) => v,
            Err(_e) => {
                // TODO: return raw / unsupported / application option data
                return Ok((Options::None, option_len + OPTION_HEADER_LEN));
            }
        };

        let r = match k {
            OptionKind::None => Ok(Options::None),
            OptionKind::PubKey => PublicKey::try_from(d).map(Options::PubKey),
            OptionKind::PeerId => Id::try_from(d).map(Options::PeerId),
            OptionKind::PrevSig => Signature::try_from(d).map(Options::PrevSig),
            OptionKind::Kind => OptionString::decode(d).map(|(v, _)| Options::Kind(v)),
            OptionKind::Name => OptionString::decode(d).map(|(v, _)| Options::Name(v)),

            OptionKind::IpAddrV4 => {
                let mut ip = [0u8; 4];

                ip.copy_from_slice(&d[0..4]);
                let port = LittleEndian::read_u16(&d[4..6]);

                Ok(Options::address_v4(AddressV4::new(ip, port)))
            }
            OptionKind::IpAddrV6 => {
                let mut ip = [0u8; 16];

                ip.copy_from_slice(&d[0..16]);
                let port = LittleEndian::read_u16(&d[16..18]);

                Ok(Options::address_v6(AddressV6::new(ip, port)))
            }

            OptionKind::Meta => {
                let s = core::str::from_utf8(d).map_err(|_| Error::InvalidOption)?;
                let mut sp = s.split('|');

                match (sp.next(), sp.next()) {
                    (Some(key), Some(value)) => Ok(Options::meta(key, value)),
                    _ => Err(Error::InvalidOption),
                }
            }

            OptionKind::Issued => Ok(Options::Issued(DateTime::from_secs(
                LittleEndian::read_u64(d),
            ))),
            OptionKind::Expiry => Ok(Options::Expiry(DateTime::from_secs(
                LittleEndian::read_u64(d),
            ))),
            OptionKind::Limit => Ok(Options::Limit(LittleEndian::read_u32(d))),

            OptionKind::Coord => Ok(Options::Coord(Coordinates {
                lat: LittleEndian::read_f32(&d[0..]),
                lng: LittleEndian::read_f32(&d[4..]),
                alt: LittleEndian::read_f32(&d[8..]),
            })),

            OptionKind::Building => OptionString::decode(d).map(|(v, _)| Options::Building(v)),
            OptionKind::Room => OptionString::decode(d).map(|(v, _)| Options::Room(v)),
            OptionKind::Manufacturer => {
                OptionString::decode(d).map(|(v, _)| Options::Manufacturer(v))
            }
            OptionKind::Serial => OptionString::decode(d).map(|(v, _)| Options::Serial(v)),
            OptionKind::Index => Ok(Options::Index(LittleEndian::read_u32(d))),
            OptionKind::DelegationProof => Signature::try_from(d).map(Options::DelegationProof),
        };

        let o = match r {
            Ok(r) => r,
            Err(e) => {
                error!(
                    "Failed to parse option kind: {} (0x{:02x}), len: {}: ",
                    k, option_kind, option_len
                );
                return Err(e);
            }
        };

        Ok((o, OPTION_HEADER_LEN + option_len))
    }
}

impl Encode for Options {
    type Error = Error;

    fn encode_len(&self) -> Result<usize, Self::Error> {
        let n = match self {
            Options::None => 0,
            Options::PubKey(_) => PUBLIC_KEY_LEN,
            Options::PeerId(_) => ID_LEN,
            Options::PrevSig(_) => SIGNATURE_LEN,
            Options::Kind(s)
            | Options::Name(s)
            | Options::Building(s)
            | Options::Room(s)
            | Options::Manufacturer(s)
            | Options::Serial(s) => s.as_bytes().len(),
            Options::IPv4(_) => 6,
            Options::IPv6(_) => 18,
            Options::Issued(_) | Options::Expiry(_) => 8,
            Options::Limit(_) => 4,
            Options::Metadata(m) => m.key.len() + m.value.len() + 1,
            Options::Coord(_) => 3 * 4,
            Options::Index(_) => 4,
            Options::DelegationProof(_) => SIGNATURE_LEN,
        };

        Ok(OPTION_HEADER_LEN + n)
    }

    fn encode(&self, data: &mut [u8]) -> Result<usize, Self::Error> {
        // Set kind
        let kind = OptionKind::from(self);
        LittleEndian::write_u16(&mut data[0..], kind as u16);

        // Encode data
        let n = match self {
            Options::PubKey(pub_key) => {
                data[OPTION_HEADER_LEN..][..PUBLIC_KEY_LEN].copy_from_slice(pub_key);
                PUBLIC_KEY_LEN
            }
            Options::PeerId(peer_id) => {
                data[OPTION_HEADER_LEN..][..ID_LEN].copy_from_slice(peer_id);
                ID_LEN
            }
            Options::PrevSig(sig) => {
                data[OPTION_HEADER_LEN..][..SIGNATURE_LEN].copy_from_slice(sig);
                SIGNATURE_LEN
            }
            Options::Kind(s)
            | Options::Name(s)
            | Options::Building(s)
            | Options::Room(s)
            | Options::Manufacturer(s)
            | Options::Serial(s) => {
                let len = s.as_bytes().len();
                data[OPTION_HEADER_LEN..][..len].copy_from_slice(s.as_bytes());
                len
            }
            Options::Limit(n) => {
                LittleEndian::write_u32(&mut data[4..], *n);
                4
            }
            Options::IPv4(v) => {
                data[OPTION_HEADER_LEN..][..4].copy_from_slice(&v.ip);
                LittleEndian::write_u16(&mut data[OPTION_HEADER_LEN + 4..], v.port);
                6
            }
            Options::IPv6(v) => {
                data[OPTION_HEADER_LEN..][..16].copy_from_slice(&v.ip);
                LittleEndian::write_u16(&mut data[OPTION_HEADER_LEN + 16..], v.port);

                18
            }
            Options::Issued(v) | Options::Expiry(v) => {
                LittleEndian::write_u64(&mut data[4..], v.as_secs());
                8
            }
            Options::Metadata(Metadata { key, value }) => {
                let mut n = 0;

                let key = key.as_bytes();
                data[OPTION_HEADER_LEN + n..][..key.len()].copy_from_slice(key);
                n += key.len();

                data[OPTION_HEADER_LEN + n] = b'|';
                n += 1;

                let val = value.as_bytes();
                data[OPTION_HEADER_LEN + n..][..val.len()].copy_from_slice(val);
                n += val.len();

                n
            }
            Options::Coord(v) => {
                LittleEndian::write_f32(&mut data[4..8], v.lat);
                LittleEndian::write_f32(&mut data[8..12], v.lng);
                LittleEndian::write_f32(&mut data[12..16], v.alt);

                3 * 4
            }
            Options::Index(n) => {
                LittleEndian::write_u32(&mut data[4..], *n);
                4
            }
            Options::DelegationProof(sig) => {
                data[OPTION_HEADER_LEN..][..SIGNATURE_LEN].copy_from_slice(sig);
                SIGNATURE_LEN
            }
            _ => todo!(),
        };

        // Write option length
        LittleEndian::write_u16(&mut data[2..], n as u16);

        trace!("Encoded option {:?}, value length: {}", kind, n);

        Ok(OPTION_HEADER_LEN + n)
    }
}

/// Implementation for queryable options
impl Queryable for Options {
    fn hash<H: crate::types::CryptoHasher>(&self, h: &mut H) -> bool {
        // First by option kind
        h.update(&(OptionKind::from(self) as u16).to_le_bytes());

        // Then by option data
        match self {
            // String based options
            Options::Name(v)
            | Options::Kind(v)
            | Options::Manufacturer(v)
            | Options::Serial(v)
            | Options::Building(v)
            | Options::Room(v) => {
                h.update(&(v.as_bytes().len() as u16).to_le_bytes());
                h.update(v.as_bytes());
                true
            }
            _ => false,
        }
    }
}

#[derive(PartialEq, Debug, Clone)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub struct Coordinates {
    pub lat: f32,
    pub lng: f32,
    pub alt: f32,
}

#[derive(PartialEq, Debug, Clone)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
pub struct Metadata {
    pub key: heapless::String<16>,
    pub value: heapless::String<48>,
}

#[cfg(feature = "defmt")]
impl defmt::Format for Metadata {
    fn format(&self, fmt: defmt::Formatter) {
        let (k, v): (&str, &str) = (&self.key, &self.value);
        defmt::write!(fmt, "{}:{}", k, v)
    }
}

#[derive(PartialEq, Debug, Clone)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
pub struct DelegationProof(pub Signature);

impl DelegationProof {
    /// Sign a delegation proof for the provided peer and origin service version
    pub fn sign(svc_pri_key: &PrivateKey, svc_version: u32, peer_id: &Id) -> DelegationProof {
        let mut b = [0u8; ID_LEN + 4];
        b[..ID_LEN].copy_from_slice(peer_id.as_bytes());
        b[ID_LEN..].copy_from_slice(&svc_version.to_le_bytes());

        Self(Crypto::pk_sign(svc_pri_key, &b).unwrap())
    }

    /// Verify a delegation proof for the provided peer and origin service version
    pub fn verify(&self, svc_pub_key: &PublicKey, svc_version: u32, peer_id: &Id) -> bool {
        let mut b = [0u8; ID_LEN + 4];
        b[..ID_LEN].copy_from_slice(peer_id.as_bytes());
        b[ID_LEN..].copy_from_slice(&svc_version.to_le_bytes());

        Crypto::pk_verify(svc_pub_key, &self.0, &b).unwrap()
    }
}

#[derive(PartialEq, Debug, Clone)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
pub struct OptionString(heapless::String<MAX_OPTION_LEN>);

impl OptionString {
    pub fn as_bytes(&self) -> &[u8] {
        self.0.as_bytes()
    }
}

impl From<&str> for OptionString {
    fn from(s: &str) -> Self {
        Self(String::from(s))
    }
}

impl AsRef<str> for OptionString {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl core::fmt::Display for OptionString {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str(&self.0)
    }
}

impl Encode for OptionString {
    type Error = Error;

    fn encode_len(&self) -> Result<usize, Self::Error> {
        Ok(self.0.as_bytes().len())
    }

    fn encode(&self, buff: &mut [u8]) -> Result<usize, Self::Error> {
        let b = self.0.as_bytes();
        if buff.len() < b.len() {
            return Err(Error::BufferLength);
        }

        buff[..b.len()].copy_from_slice(b);

        Ok(b.len())
    }
}

impl<'a> Decode<'a> for OptionString {
    type Output = Self;

    type Error = Error;

    fn decode(buff: &'a [u8]) -> Result<(Self::Output, usize), Self::Error> {
        let s = core::str::from_utf8(buff).map_err(|_| Error::InvalidOption)?;
        Ok((Self(s.into()), s.as_bytes().len()))
    }
}

#[cfg(feature = "defmt")]
impl defmt::Format for OptionString {
    fn format(&self, fmt: defmt::Formatter) {
        let s: &str = &self.0;
        defmt::write!(fmt, "{}", s)
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    use std::net::{Ipv4Addr, Ipv6Addr, SocketAddrV4, SocketAddrV6};
    use std::time::SystemTime;
    use std::vec::Vec;

    use encdec::{decode::DecodeExt, encode::EncodeExt};

    #[test]
    fn encode_decode_option_types() {
        #[cfg(feature = "simplelog")]
        let _ = simplelog::SimpleLogger::init(
            simplelog::LevelFilter::Debug,
            simplelog::Config::default(),
        );

        let tests = [
            Options::PubKey([1u8; PUBLIC_KEY_LEN].into()),
            Options::PeerId([2u8; ID_LEN].into()),
            Options::kind("test-kind"),
            Options::name("test-name"),
            Options::address_v4(SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), 8080)),
            Options::address_v6(SocketAddrV6::new(
                Ipv6Addr::new(0x2001, 0xdb8, 0, 0, 0, 0, 0, 1),
                8080,
                0,
                0,
            )),
            Options::meta("test-key", "test-value"),
            Options::issued(SystemTime::now()),
            Options::expiry(SystemTime::now()),
            Options::Limit(13),
            Options::Index(1234),
            Options::DelegationProof([3u8; SIGNATURE_LEN].into()),
        ];

        for o in tests.iter() {
            println!("Encode/Decode: {:?}", o);

            let mut data = vec![0u8; 1024];
            let n1 = o
                .encode(&mut data)
                .unwrap_or_else(|_| panic!("Error encoding {:?}", o));

            let (decoded, n2) =
                Options::decode(&data).unwrap_or_else(|_| panic!("Error decoding {:?}", o));

            assert_eq!(
                n1, n2,
                "Mismatch between encoded and decoded lengths for object: {:?}",
                o
            );
            assert_eq!(o, &decoded, "Mismatch between original and decode objects");
        }
    }

    #[test]
    fn encode_decode_option_list() {
        #[cfg(feature = "simplelog")]
        let _ = simplelog::SimpleLogger::init(
            simplelog::LevelFilter::Debug,
            simplelog::Config::default(),
        );

        let tests = vec![
            Options::PubKey([1u8; PUBLIC_KEY_LEN].into()),
            Options::PeerId([2u8; ID_LEN].into()),
            Options::PrevSig([3u8; SIGNATURE_LEN].into()),
            Options::kind("test-kind"),
            Options::name("test-name"),
            Options::address_v4(SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), 8080)),
            Options::address_v6(SocketAddrV6::new(
                Ipv6Addr::new(0x2001, 0xdb8, 0, 0, 0, 0, 0, 1),
                8080,
                0,
                0,
            )),
            Options::meta("test-key", "test-value"),
            Options::issued(SystemTime::now()),
            Options::expiry(SystemTime::now()),
        ];

        let mut data = vec![0u8; 1024];
        let n1 =
            Options::encode_iter(tests.iter(), &mut data).expect("Error encoding options vector");

        let encoded = &data[0..n1];
        let decoded: Vec<_> = Options::decode_iter(encoded)
            .collect::<Result<Vec<_>, Error>>()
            .expect("Error decoding options vector");

        assert_eq!(
            &tests, &decoded,
            "Mismatch between original and decode vectors"
        );
    }

    #[test]
    fn sign_verify_delegation_proof() {
        let (svc_pub_key, svc_pri_key) = Crypto::new_pk().unwrap();
        let svc_version = 1234u32;

        let peer_id = Id::from([2u8; ID_LEN]);

        let proof = DelegationProof::sign(&svc_pri_key, svc_version, &peer_id);

        // Succeds with valid inputs
        assert_eq!(proof.verify(&svc_pub_key, svc_version, &peer_id), true);

        // Fails with invalid inputs
        assert_eq!(
            proof.verify(&svc_pub_key, svc_version, &Id::from([3u8; ID_LEN])),
            false
        );
        assert_eq!(proof.verify(&svc_pub_key, svc_version + 1, &peer_id), false);
        let (wrong_pub_key, _) = Crypto::new_pk().unwrap();
        assert_eq!(proof.verify(&wrong_pub_key, svc_version, &peer_id), false);
    }
}
