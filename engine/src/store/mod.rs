//! [Store] interface for [Engine] implementations
//!
//! This provides persistent storage to the engine to maintain
//! keys, history, and optionally peers and subscriptions.

use core::fmt::Debug;
use core::marker::PhantomData;

use dsf_core::{
    crypto::{Crypto, PubKey as _},
    keys::{KeySource, Keys},
    prelude::*,
    types::{ImmutableData, MutableData},
    wire::Container,
};

#[cfg(feature = "std")]
mod mem_store;
#[cfg(feature = "std")]
pub use mem_store::MemoryStore;

#[cfg(feature = "sqlite")]
mod sqlite_store;
#[cfg(feature = "sqlite")]
pub use sqlite_store::SqliteStore;

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

    // Fetch peer information
    fn get_peer(&self, id: &Id) -> Result<Option<Peer<Self::Address>>, Self::Error>;

    // Update a specified peer
    fn update_peer<R: Debug, F: Fn(&mut Peer<Self::Address>) -> R>(
        &mut self,
        id: &Id,
        f: F,
    ) -> Result<R, Self::Error>;

    // Iterate through known peers
    fn peers(&self) -> Self::Iter<'_>;

    // Store an object, updating last object information
    fn store_page<T: ImmutableData>(&mut self, p: &Container<T>) -> Result<(), Self::Error>;

    // Fetch a stored page
    fn fetch_page<T: MutableData>(
        &mut self,
        f: ObjectFilter,
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

#[derive(Clone, Debug, PartialEq)]
pub enum ObjectFilter {
    Latest,
    Sig(Signature),
    Index(u32),
}

#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub struct ObjectInfo {
    pub page_index: u32,
    pub block_index: u32,
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
        } else {
            self.subscribed == Subscribed
        }
    }
}
