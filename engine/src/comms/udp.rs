#[cfg(feature = "std")]
use std::net::SocketAddrV4;

use crate::comms::Comms;

/// [Comms] implementation for [std::net::UdpSocket]
#[cfg(feature = "std")]
impl Comms for std::net::UdpSocket {
    type Address = std::net::SocketAddr;

    type Error = std::io::Error;

    fn recv(&mut self, buff: &mut [u8]) -> Result<Option<(usize, Self::Address)>, Self::Error> {
        match self.recv_from(buff) {
            Ok(v) => Ok(Some(v)),
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => Ok(None),
            Err(e) => Err(e),
        }
    }

    fn send(&mut self, to: &Self::Address, data: &[u8]) -> Result<(), Self::Error> {
        self.send_to(data, to)?;
        Ok(())
    }

    fn broadcast(&mut self, data: &[u8]) -> Result<(), Self::Error> {
        use std::net::{Ipv4Addr, SocketAddr};

        // Local broadcast with default DSF addr
        let addr = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(255, 255, 255, 255), 10100));

        log::debug!("Broadcast {} bytes to: {}", data.len(), addr);

        self.send_to(data, addr)?;

        Ok(())
    }
}
