use diesel::r2d2::PoolError;
use diesel::result::Error as DieselError;
use diesel::ConnectionError;
use dsf_core::helpers::ParseBytesError;
use std::net::AddrParseError;
use strum::ParseError as StrumError;

#[derive(Debug, PartialEq)]
pub enum StoreError {
    Connection(ConnectionError),
    Pool,
    Diesel(DieselError),
    Strum(StrumError),
    Parse(ParseBytesError),
    Addr(AddrParseError),
    MissingSignature,
    MissingRawData,
    NotFound,
    Decode,
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

impl From<ParseBytesError> for StoreError {
    fn from(e: ParseBytesError) -> Self {
        Self::Parse(e)
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
