pub use dsf_core::error::Error as CoreError;

pub use crate::io::{NetError, UnixError};
pub use crate::store::StoreError;

#[derive(Debug, PartialEq)]
pub enum Error {
    Net(NetError),
    Unix(UnixError),
    Store(StoreError),
    Channel(futures::channel::mpsc::SendError),
    Core(CoreError),

    Timeout,
    Unknown,
    Unimplemented,
    NotFound,
    UnknownService,
    NoReplicasFound,
    NoPrivateKey,
    Closed,
}

impl From<NetError> for Error {
    fn from(e: NetError) -> Self {
        Self::Net(e)
    }
}

impl From<UnixError> for Error {
    fn from(e: UnixError) -> Self {
        Self::Unix(e)
    }
}

impl From<StoreError> for Error {
    fn from(e: StoreError) -> Self {
        Self::Store(e)
    }
}

impl From<futures::channel::mpsc::SendError> for Error {
    fn from(e: futures::channel::mpsc::SendError) -> Self {
        Self::Channel(e)
    }
}

impl From<CoreError> for Error {
    fn from(e: CoreError) -> Self {
        Self::Core(e)
    }
}

impl From<tokio::time::error::Error> for Error {
    fn from(_e: tokio::time::error::Error) -> Self {
        Self::Timeout
    }
}

impl From<tokio::task::JoinError> for Error {
    fn from(_e: tokio::task::JoinError) -> Self {
        Self::Unknown
    }
}
