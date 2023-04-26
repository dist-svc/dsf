#[cfg(feature = "std")]
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4, SocketAddrV6};

/// Internal IPv4 address type
pub type Ipv4 = [u8; 4];

/// Internal IPv6 address type
pub type Ipv6 = [u8; 16];

/// no_std compatible IPv4/6 address storage
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub enum Ip {
    V4(Ipv4),
    V6(Ipv6),
}
/// no_std compatible socket IPv4/6 address (IP and Port)
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub struct Address {
    pub ip: Ip,
    pub port: u16,
}

impl Address {
    pub fn new(ip: Ip, port: u16) -> Self {
        Self { ip, port }
    }
}

#[cfg(feature = "std")]
impl From<SocketAddr> for Address {
    fn from(s: SocketAddr) -> Self {
        match s {
            SocketAddr::V4(a) => Self::new(Ip::V4(a.ip().octets()), a.port()),
            SocketAddr::V6(a) => Self::new(Ip::V6(a.ip().octets()), a.port()),
        }
    }
}

#[cfg(feature = "std")]
impl From<Address> for SocketAddr {
    fn from(a: Address) -> Self {
        match &a.ip {
            Ip::V4(ip) => SocketAddr::V4(SocketAddrV4::new((*ip).into(), a.port)),
            Ip::V6(ip) => SocketAddr::V6(SocketAddrV6::new((*ip).into(), a.port, 0, 0)),
        }
    }
}

impl From<AddressV4> for Address {
    fn from(a: AddressV4) -> Self {
        Self::new(Ip::V4(a.ip), a.port)
    }
}

impl From<AddressV6> for Address {
    fn from(a: AddressV6) -> Self {
        Self::new(Ip::V6(a.ip), a.port)
    }
}

/// no_std compatible socket IPv4 address (IP and Port)
#[derive(Debug, Copy, Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub struct AddressV4 {
    pub ip: Ipv4,
    pub port: u16,
}

/// Default IPv4 broadcast address
pub const IPV4_BROADCAST: AddressV4 = AddressV4 {
    ip: [255, 255, 255, 255],
    port: 10100,
};

impl AddressV4 {
    pub fn new(ip: Ipv4, port: u16) -> Self {
        Self { ip, port }
    }
}

impl From<(u32, u16)> for AddressV4 {
    fn from(a: (u32, u16)) -> Self {
        Self::new(a.0.to_ne_bytes(), a.1)
    }
}

impl From<(Ipv4, u16)> for AddressV4 {
    fn from(a: (Ipv4, u16)) -> Self {
        Self::new(a.0, a.1)
    }
}

#[cfg(feature = "std")]
impl From<SocketAddrV4> for AddressV4 {
    fn from(a: SocketAddrV4) -> Self {
        Self::new(a.ip().octets(), a.port())
    }
}

#[cfg(feature = "std")]
impl Into<SocketAddrV4> for AddressV4 {
    fn into(self) -> SocketAddrV4 {
        SocketAddrV4::new(Ipv4Addr::from(self.ip), self.port)
    }
}

/// no_std compatible socket IPv6 address (IP and Port)
#[derive(Debug, Copy, Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub struct AddressV6 {
    pub ip: Ipv6,
    pub port: u16,
}

/// Default IPv6 broadcast address
pub const IPV6_BROADCAST: AddressV6 = AddressV6 {
    ip: [
        0xff, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00,
    ],
    port: 10100,
};

impl AddressV6 {
    pub fn new(ip: Ipv6, port: u16) -> Self {
        Self { ip, port }
    }
}

impl From<(Ipv6, u16)> for AddressV6 {
    fn from(a: (Ipv6, u16)) -> Self {
        Self::new(a.0, a.1)
    }
}

#[cfg(feature = "std")]
impl From<SocketAddrV6> for AddressV6 {
    fn from(a: SocketAddrV6) -> Self {
        Self::new(a.ip().octets(), a.port())
    }
}

#[cfg(feature = "std")]
impl Into<SocketAddrV6> for AddressV6 {
    fn into(self) -> SocketAddrV6 {
        SocketAddrV6::new(Ipv6Addr::from(self.ip), self.port, 0, 0)
    }
}
