//! Driver abstraction for generic client support

use std::time::Duration;

use dsf_rpc::{RequestKind, ResponseKind};

mod http;
pub use http::HttpDriver;

mod unix;
pub use unix::UnixDriver;

use crate::Error;

/// Drivers implement a classic request/response call
#[allow(async_fn_in_trait)]
pub trait Driver {
    // Execute a request and return a response
    async fn exec(&self, req: RequestKind, timeout: Duration) -> Result<ResponseKind, Error>;
}

/// [GenericDriver] wraps specific driver implementations,
/// switching based on `address`
pub enum GenericDriver {
    Http(HttpDriver),
    Unix(UnixDriver),
}

impl From<HttpDriver> for GenericDriver {
    fn from(value: HttpDriver) -> Self {
        Self::Http(value)
    }
}

impl From<UnixDriver> for GenericDriver {
    fn from(value: UnixDriver) -> Self {
        Self::Unix(value)
    }
}

impl Driver for GenericDriver {
    async fn exec(&self, req: RequestKind, timeout: Duration) -> Result<ResponseKind, Error> {
        match self {
            GenericDriver::Http(d) => d.exec(req, timeout).await,
            GenericDriver::Unix(d) => d.exec(req, timeout).await,
        }
    }
}

impl GenericDriver {
    /// Create a new [GenericDriver], switching based on address prefix
    pub async fn new(address: &str) -> Result<Self, Error> {
        let d = if address.starts_with("http://") {
            HttpDriver::new(address).await.map(|d| d.into())?
        } else if address.starts_with("unix://") {
            UnixDriver::new(address).await.map(|d| d.into())?
        } else {
            return Err(Error::Unknown);
        };

        Ok(d)
    }
}
