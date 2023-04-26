use std::time::Duration;

use async_trait::async_trait;

pub mod net;
pub use net::{Net, NetError, NetKind, NetMessage};

pub mod unix;
pub use unix::{Unix, UnixError, UnixMessage};

pub mod mock;

use dsf_core::prelude::*;

use crate::error::Error;

#[async_trait]
pub trait Connector {
    // Send a request and receive a response or error at some time in the future
    async fn request(
        &self,
        req_id: RequestId,
        target: Address,
        req: NetRequest,
        timeout: Duration,
    ) -> Result<NetResponse, Error>;

    // Send a response message
    async fn respond(
        &self,
        req_id: RequestId,
        target: Address,
        resp: NetResponse,
    ) -> Result<(), Error>;
}
