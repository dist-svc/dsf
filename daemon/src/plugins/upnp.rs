use std::net::{IpAddr, SocketAddr, SocketAddrV4};

use log::{info, warn};

use igd::aio::{search_gateway, Gateway};
use igd::{PortMappingProtocol, SearchOptions};

use dsf_core::types::*;
use tokio::select;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};

/// Commands that are handled by the MDNS actor
#[derive(Clone, Debug)]
pub enum UpnpCommand {
    Register(SocketAddr, Option<u16>),
    Deregister(IpAddr, u16),
}

/// Updates published by the MDNS actor
#[derive(Clone, Debug)]
pub enum UpnpUpdate {
    Empty,
    Registered(SocketAddr),
    Discovered(Vec<SocketAddr>),
    Deregistered,
}

#[derive(Clone, Debug, PartialEq)]
pub enum UpnpError {
    NoGateway,
    NoLocalIp,
    NoExternalIp,
    RegisterFailed,
    DeregisterFailed,
    UnsupportedAddress,
}

pub struct UpnpPlugin {
    id: Id,
    state: UpnpState,
}

pub enum UpnpState {
    Idle,
    Discovered(Gateway),
    Registered(Gateway, SocketAddr),
}

impl UpnpPlugin {
    /// Create a new uPnP plugin
    pub fn new(id: Id) -> Self {
        Self {
            id,
            state: UpnpState::Idle,
        }
    }

    pub async fn register(&mut self, local_addr: SocketAddr) -> Result<SocketAddr, UpnpError> {
        // Setup DSF specific uPnP config
        let name = format!("{}", ShortId::from(&self.id));
        let ext_port = None;
        info!("registering local address: {local_addr} for {name}");

        // Ensure we're using a V4 addrss
        let local_addr = match local_addr {
            SocketAddr::V4(v4) => v4,
            _ => {
                warn!("IPv6 addresses not supported");
                return Err(UpnpError::UnsupportedAddress);
            }
        };

        //TODO: we should probs run this for _each_ bind address (or local address)..?
        // But also, we only need one external connection...

        let options = SearchOptions::default();
        // TODO: only search for viable gateways for a specific binding
        // (probably needs to be integrated with net module..?)
        //options.bind_addr = SocketAddr::V4(local_addr);

        // Search for local gateway
        let gw = search_gateway(options)
            .await
            .map_err(|_e| UpnpError::NoGateway)?;

        // Update upnpn state
        self.state = UpnpState::Discovered(gw.clone());

        // Fetch external ip for the discovered gateway
        let external_ip = gw
            .get_external_ip()
            .await
            .map_err(|_e| UpnpError::NoExternalIp)?;
        info!("discovered public IP: {}", external_ip);

        // Request depends on port type
        let port = match ext_port {
            Some(port) => gw
                .add_port(PortMappingProtocol::UDP, port, local_addr, 3600, &name)
                .await
                .map(|_| port)
                .map_err(|_e| UpnpError::RegisterFailed)?,
            None => gw
                .add_any_port(PortMappingProtocol::UDP, local_addr, 3600, &name)
                .await
                .map_err(|_e| UpnpError::RegisterFailed)?,
        };

        info!(
            "Registration OK (service: {name} bind address: {local_addr} public address: {external_ip}:{port}",
        );

        // Update upnp state
        let r = SocketAddr::V4(SocketAddrV4::new(external_ip, port));
        self.state = UpnpState::Registered(gw.clone(), r.clone());

        // Return bound address
        Ok(r)
    }

    /// Deregister UPnP service
    pub async fn deregister(&mut self) -> Result<(), UpnpError> {
        let (gw, addr) = match &mut self.state {
            UpnpState::Registered(gw, addr) => (gw, addr),
            _ => {
                warn!("UPnP not registered");
                return Ok(());
            }
        };

        // Remove port on local address
        if let Err(e) = gw.remove_port(PortMappingProtocol::UDP, addr.port()).await {
            warn!("Failed to de-register port: {}", e);
            return Err(UpnpError::DeregisterFailed);
        }

        self.state = UpnpState::Idle;

        info!("De-registration OK");

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[tokio::test]
    #[ignore = "only runs in UPnP environments"]
    async fn test_upnp() {
        // TODO: implement this
    }
}
