//! API module defines the DSF remote API
//! This allows for limited capability devices to perform network operations via
//! a full-featured device

use encdec::DecodeOwned;

use crate::{
    base::{DataBody, PageBody},
    types::Id,
};

/// Application object used to describe a DSF application
pub trait Application {
    /// DSF Application ID
    const APPLICATION_ID: u16;

    /// Service information (page body) encoding
    type Info: PageBody + DecodeOwned<Output = Self::Info> + core::fmt::Debug;

    /// Service data (block body) encoding
    type Data: DataBody + DecodeOwned<Output = Self::Data> + core::fmt::Debug;

    /// Helper function for info matching to support discovery
    fn matches(_info: &Self::Info, _req: &[u8]) -> bool {
        false
    }
}

/// ServiceHandle objects are used to pass around instances of a service
#[derive(Debug, Clone, PartialEq)]
pub struct ServiceHandle {
    pub id: Id,
}

impl ServiceHandle {
    pub fn new(id: Id) -> Self {
        Self { id }
    }
}

/// Creation API used to create services
pub trait Create {
    type Options;
    type Error;

    /// Create a new service with the provided options
    fn create(&mut self, options: Self::Options) -> Result<ServiceHandle, Self::Error>;
}

/// Producer API trait used to register an existing service
pub trait Register {
    type Options;
    type Info;
    type Error;

    /// Register a service in the distributed database
    fn register(&mut self, options: Self::Options) -> Result<Self::Info, Self::Error>;
}

/// Locate API trait used to find an existing service
pub trait Locate {
    type Options;
    type Info;
    type Error;

    /// Locate a DIoT service in the distributed database
    /// This returns a future that will resolve to the desired service or an error
    fn locate(&mut self, options: Self::Options) -> Result<Self::Info, Self::Error>;
}

/// Publisher API trait used by publishers of service data
pub trait Publish {
    type Options;
    type Info;
    type Error;

    /// Publish service data
    fn publish(&mut self, options: Self::Options) -> Result<Self::Info, Self::Error>;
}

/// A boxed future stream to shorten method definitions
/// //pub type FutureStream<E> = impl Stream<Item=E>;

/// Subscriber API used by subscribers to service data
pub trait Subscribe {
    type Options;
    type Streamable;
    type Error;

    /// Locate a DIoT service in the distributed database
    /// This returns a future that will resolve to the desired service or an error
    fn subscribe(&mut self, options: Self::Options) -> Result<Self::Streamable, Self::Error>;
}
