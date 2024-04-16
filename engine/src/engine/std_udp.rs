use core::fmt::Debug;

use dsf_core::{api::Application, prelude::Options};

use crate::{
    comms::Comms,
    engine::{Engine, EngineEvent},
    error::EngineError,
    store::Store,
};

/// A [std::net::UdpSocket] based engine for use with `std`
impl<A: Application, S: Store<Address = std::net::SocketAddr>, const N: usize>
    Engine<A, std::net::UdpSocket, S, N>
{
    /// Create a new [std::net::UdpSocket] based engine
    pub fn udp<Addr: std::net::ToSocketAddrs + Debug>(
        info: A::Info,
        opts: &[Options],
        addr: Addr,
        store: S,
    ) -> Result<Self, EngineError<std::io::Error, <S as Store>::Error>> {
        log::debug!("Connecting to socket: {:?}", addr);

        // Attempt to bind UDP socket
        let comms = std::net::UdpSocket::bind(addr).map_err(EngineError::Comms)?;

        // Enable broadcast and nonblocking polling
        comms.set_broadcast(true).map_err(EngineError::Comms)?;
        comms.set_nonblocking(true).map_err(EngineError::Comms)?;

        // Create engine instance
        Self::new(info, opts, comms, store)
    }

    /// Tick function to update engine and poll on socket
    pub fn tick(
        &mut self,
    ) -> Result<EngineEvent, EngineError<std::io::Error, <S as Store>::Error>> {
        let mut buff = [0u8; N];

        // Check for and handle received messages
        if let Some((n, a)) = Comms::recv(&mut self.comms, &mut buff).map_err(EngineError::Comms)? {
            log::debug!("Received {} bytes from {:?}", n, a);
            return self.handle(a, &mut buff[..n]);
        }

        // Update internal state
        self.update()
    }

    /// Resolve the local address of the engine
    pub fn addr(
        &mut self,
    ) -> Result<std::net::SocketAddr, EngineError<std::io::Error, <S as Store>::Error>> {
        let a = self.comms.local_addr().map_err(EngineError::Comms)?;
        Ok(a)
    }
}
