use std::net::{IpAddr, SocketAddr, SocketAddrV4};

use log::{info, warn};

use igd::aio::search_gateway;
use igd::{PortMappingProtocol, SearchOptions};

use dsf_core::types::*;

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
    _id: Id,
}

impl UpnpPlugin {
    /// Create a new uPnP plugin
    pub fn new(id: Id) -> Self {
        Self { _id: id }
    }

    pub async fn register(
        &mut self,
        name: &str,
        local_addr: SocketAddr,
        ext_port: Option<u16>,
    ) -> Result<(), UpnpError> {
        info!("registering local address: {}", local_addr);

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

        let mut options = SearchOptions::default();
        options.bind_addr = SocketAddr::V4(local_addr);

        // Search for local gateway
        let gw = search_gateway(options)
            .await
            .map_err(|_e| UpnpError::NoGateway)?;

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
            "Registration OK (service: {} bind address: {} public address: {}:{}",
            name, local_addr, external_ip, port
        );
        let _r = SocketAddr::V4(SocketAddrV4::new(external_ip, port));

        Ok(())
    }

    pub async fn deregister(local_addr: IpAddr, ext_port: u16) -> Result<(), UpnpError> {
        let mut options = SearchOptions::default();
        options.bind_addr = SocketAddr::new(local_addr, 0);

        // Search for local gateway
        let gw = search_gateway(options)
            .await
            .map_err(|_e| UpnpError::NoGateway)?;

        // Remove port on local address
        if let Err(e) = gw.remove_port(PortMappingProtocol::UDP, ext_port).await {
            warn!("[UPNP] Failed to de-register port: {}", e);
            return Err(UpnpError::DeregisterFailed);
        }

        info!("[UPNP] De-registration OK");

        Ok(())
    }
}
