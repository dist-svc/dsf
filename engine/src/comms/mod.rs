use core::fmt::Debug;

//use crate::log::{Debug, trace, debug, info, warn, error};
//use crate::prelude::EpDescriptor;

//use super::{Engine, Store, EngineError, EngineEvent};

#[cfg(all(test, feature = "alloc"))]
pub mod mock;

pub mod udp;

/// Abstract communication interface trait
pub trait Comms {
    /// Address for directing packets
    type Address: Debug;

    /// Communication error type
    type Error: Debug;

    /// Receive data if available, returning None if no data is available
    fn recv(&mut self, buff: &mut [u8]) -> Result<Option<(usize, Self::Address)>, Self::Error>;

    /// Send data to the specified address
    fn send(&mut self, to: &Self::Address, data: &[u8]) -> Result<(), Self::Error>;

    /// Broadcast data
    fn broadcast(&mut self, data: &[u8]) -> Result<(), Self::Error>;
}
