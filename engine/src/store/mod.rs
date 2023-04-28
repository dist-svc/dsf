use core::convert::TryFrom;
use core::fmt::Debug;
use core::marker::PhantomData;

use byteorder::{ByteOrder, LittleEndian};

use dsf_core::crypto::{Crypto, PubKey as _};
use dsf_core::keys::{KeySource, Keys};
use dsf_core::prelude::*;
use dsf_core::types::{ImmutableData, MutableData, SIGNATURE_LEN};
use dsf_core::wire::Container;

#[cfg(feature = "std")]
mod mem_store;
#[cfg(feature = "std")]
pub use mem_store::MemoryStore;

#[cfg(feature = "sled")]
mod sled_store;
#[cfg(feature = "sled")]
pub use sled_store::SledStore;

bitflags::bitflags! {
    /// Features supported by a store interface
    pub struct StoreFlags: u16 {
        const KEYS  = 0b0000_0001;
        const SIGS  = 0b0000_0010;
        const PAGES = 0b0000_0100;

        const PEERS = 0b0000_1000;
        const SERVICES = 0b0001_0000;

        const ALL = Self::PAGES.bits() | Self::SIGS.bits() | Self::KEYS.bits() | Self::PEERS.bits() | Self::SERVICES.bits();
    }
}

pub trait Store: KeySource {
    const FEATURES: StoreFlags;

    /// Peer address type
    type Address: Clone + Debug + 'static;

    /// Storage error type
    type Error: Debug;

    /// Peer iterator type, for collecting subscribers etc.
    type Iter<'a>: Iterator<Item = (&'a Id, &'a Peer<Self::Address>)>
    where
        Self: 'a;

    /// Fetch keys associated with this service
    fn get_ident(&self) -> Result<Option<Keys>, Self::Error>;

    /// Set keys associated with this service
    fn set_ident(&mut self, keys: &Keys) -> Result<(), Self::Error>;

    /// Fetch previous object information
    fn get_last(&self) -> Result<Option<ObjectInfo>, Self::Error>;

    /// Update previous object information
    fn set_last(&mut self, info: &ObjectInfo) -> Result<(), Self::Error>;

    // Fetch peer information
    fn get_peer(&self, id: &Id) -> Result<Option<Peer<Self::Address>>, Self::Error>;

    // Update a specified peer
    fn update_peer<R: Debug, F: Fn(&mut Peer<Self::Address>) -> R>(
        &mut self,
        id: &Id,
        f: F,
    ) -> Result<R, Self::Error>;

    // Iterate through known peers
    fn peers<'a>(&'a self) -> Self::Iter<'a>;

    // Store a page
    fn store_page<T: ImmutableData>(
        &mut self,
        sig: &Signature,
        p: &Container<T>,
    ) -> Result<(), Self::Error>;

    // Fetch a stored page
    fn fetch_page<T: MutableData>(
        &mut self,
        sig: &Signature,
        buff: T,
    ) -> Result<Option<Container<T>>, Self::Error>;

    // Fetch service information
    fn get_service(&self, id: &Id) -> Result<Option<Service>, Self::Error>;

    // Update a specified service
    fn update_service<R: Debug, F: Fn(&mut Service) -> R>(
        &mut self,
        id: &Id,
        f: F,
    ) -> Result<R, Self::Error>;
}

#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub struct ObjectInfo {
    pub page_index: u16,
    pub block_index: u16,
    pub sig: Signature,
}

#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub struct Peer<Addr: Clone + Debug> {
    pub keys: Keys,                 // Key storage for the peer / service
    pub addr: Option<Addr>,         // Optional address for the peer / service
    pub subscriber: bool,           // Indicate whether this service is subscribed to us
    pub subscribed: SubscribeState, // Indicate whether we are subscribed to this service
}

impl<Addr: Clone + Debug> Default for Peer<Addr> {
    fn default() -> Self {
        Self {
            keys: Keys::default(),
            addr: None,
            subscriber: false,
            subscribed: SubscribeState::None,
        }
    }
}

#[derive(Copy, Clone, PartialEq, Debug)]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub enum SubscribeState {
    None,
    Subscribing(RequestId),
    Subscribed,
    Unsubscribing(RequestId),
}

impl<Addr: Clone + Debug> Peer<Addr> {
    pub fn subscribed(&self) -> bool {
        use SubscribeState::*;

        if let Subscribing(_) = self.subscribed {
            true
        } else if self.subscribed == Subscribed {
            true
        } else {
            false
        }
    }
}
