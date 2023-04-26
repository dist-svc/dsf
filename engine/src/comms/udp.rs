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

        let a = match self.local_addr()? {
            SocketAddr::V4(mut v4) => {
                v4.set_ip(Ipv4Addr::new(255, 255, 255, 255));
                v4
            }
            _ => unimplemented!(),
        };

        log::debug!("Broadcast {} bytes to: {}", data.len(), a);

        self.send_to(data, a)?;

        Ok(())
    }
}
