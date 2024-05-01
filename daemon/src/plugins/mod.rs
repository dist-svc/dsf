use core::net::SocketAddr;

pub mod address;

pub mod mdns;

pub mod upnp;

#[derive(Clone, Debug, PartialEq)]
pub enum PluginEvent {
    /// Detected a change in available addresses
    AddressChange(Vec<SocketAddr>),
}
