use std::convert::Infallible;
use std::fs;
use std::io::{Error as IoError, ErrorKind as IoErrorKind};
use std::net::{SocketAddr, ToSocketAddrs};
use std::str::FromStr;
use std::time::Duration;

use humantime::Duration as HumanDuration;

pub fn try_parse_sock_addr(from: &str) -> Result<SocketAddr, IoError> {
    let mut addrs = from.to_socket_addrs()?;

    match addrs.next() {
        Some(a) => Ok(a),
        None => Err(IoError::new(
            IoErrorKind::Other,
            "no socket addresses found",
        )),
    }
}

pub fn try_load_file(from: &str) -> Result<Vec<u8>, IoError> {
    let data = fs::read(from)?;
    Ok(data)
}

pub fn try_parse_key_value(from: &str) -> Result<(String, String), IoError> {
    let split: Vec<_> = from.split(':').collect();
    if split.len() != 2 {
        return Err(IoError::new(
            IoErrorKind::Other,
            "key:value pair parsing failed",
        ));
    }

    Ok((split[0].to_owned(), split[1].to_owned()))
}

pub fn parse_duration(s: &str) -> Result<Duration, humantime::DurationError> {
    let hd = HumanDuration::from_str(s)?;
    Ok(hd.into())
}

pub fn data_from_str(s: &str) -> Result<Vec<u8>, Infallible> {
    Ok(s.as_bytes().to_vec())
}
