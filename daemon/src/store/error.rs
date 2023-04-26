use diesel::r2d2::PoolError;
use diesel::result::Error as DieselError;
use diesel::ConnectionError;
use std::net::AddrParseError;
use strum::ParseError as StrumError;

#[derive(Debug, PartialEq)]
pub enum StoreError {
    Connection(ConnectionError),
    Pool,
    Diesel(DieselError),
    Strum(StrumError),
    B64(base64::DecodeError),
    Addr(AddrParseError),
    MissingSignature,
    MissingRawData,
    NotFound,
}

impl From<ConnectionError> for StoreError {
    fn from(e: ConnectionError) -> Self {
        Self::Connection(e)
    }
}

impl From<DieselError> for StoreError {
    fn from(e: DieselError) -> Self {
        Self::Diesel(e)
    }
}

impl From<base64::DecodeError> for StoreError {
    fn from(e: base64::DecodeError) -> Self {
        Self::B64(e)
    }
}

impl From<AddrParseError> for StoreError {
    fn from(e: AddrParseError) -> Self {
        Self::Addr(e)
    }
}

impl From<strum::ParseError> for StoreError {
    fn from(value: strum::ParseError) -> Self {
        Self::Strum(value)
    }
}
