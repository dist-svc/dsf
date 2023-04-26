//! Base module provides a low-level structure for data encoding and decoding

#[cfg(feature = "alloc")]
use alloc::vec::Vec;

mod header;
pub use header::*;

use crate::options::Options;
use crate::types::{Id, ImmutableData};
use crate::Debug;

pub type Body = MaybeEncrypted;

pub use encdec::{Decode, Encode};

pub trait Message<'a>: Decode<'a> + Encode {
    const KIND: u16;
}

/// Marker trait for page body types
pub trait PageBody: Encode {}

impl PageBody for &[u8] {}

#[cfg(feature = "alloc")]
impl PageBody for Vec<u8> {}

impl PageBody for Id {}

impl PageBody for Empty {}

/// Marker trait for data body types
pub trait DataBody: Encode {}

impl DataBody for &[u8] {}

#[cfg(feature = "alloc")]
impl DataBody for Vec<u8> {}

impl DataBody for Empty {}

/// Empty object marker trait
#[derive(Clone, PartialEq, Debug, Encode, Decode)]
pub struct Empty;

/// Container for objects / collections that may be encrypted
#[derive(PartialEq, Clone, Debug)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub enum MaybeEncrypted<O: Encode = Vec<u8>, E: ImmutableData = Vec<u8>> {
    Cleartext(O),
    Encrypted(E),
    None,
}

impl PageBody for MaybeEncrypted {}

impl<O: Encode + Debug, E: ImmutableData> MaybeEncrypted<O, E> {
    pub fn cleartext(o: O) -> Self {
        Self::Cleartext(o)
    }

    pub fn encrypted(e: E) -> Self {
        Self::Encrypted(e)
    }
}

impl<O: Encode + Debug, E: ImmutableData + Debug> Encode for MaybeEncrypted<O, E> {
    type Error = <O as Encode>::Error;

    fn encode_len(&self) -> Result<usize, Self::Error> {
        let n = match self {
            Self::Encrypted(e) => e.as_ref().len(),
            Self::Cleartext(c) => c.encode_len()?,
            Self::None => 0,
        };
        Ok(n)
    }

    /// Encode a MaybeEncrypted object, writing data directly if encrypted or
    /// calling the inner .encode function for decrypted objects
    fn encode(&self, buff: &mut [u8]) -> Result<usize, Self::Error> {
        let n = match self {
            Self::Encrypted(e) if !e.as_ref().is_empty() => {
                let l = e.as_ref();
                buff[..l.len()].copy_from_slice(l);
                l.len()
            }
            Self::Cleartext(o) => o.encode(buff)?,
            _ => 0,
        };

        debug!("Encoded: {:02x?} ({} bytes)", self, n);

        Ok(n)
    }
}

impl<O: Encode + Debug, E: ImmutableData> Default for MaybeEncrypted<O, E> {
    fn default() -> Self {
        MaybeEncrypted::None
    }
}

impl<'a> MaybeEncrypted<&'a [Options], &'a [u8]> {
    pub fn to_vec(&self) -> MaybeEncrypted<Vec<Options>, Vec<u8>> {
        match self {
            Self::Encrypted(e) => MaybeEncrypted::Encrypted(e.to_vec()),
            Self::Cleartext(e) => MaybeEncrypted::Cleartext(e.to_vec()),
            Self::None => MaybeEncrypted::None,
        }
    }
}

#[cfg(feature = "alloc")]
impl From<Vec<u8>> for MaybeEncrypted<Vec<u8>, Vec<u8>> {
    fn from(o: Vec<u8>) -> Self {
        if !o.is_empty() {
            MaybeEncrypted::Cleartext(o)
        } else {
            MaybeEncrypted::None
        }
    }
}

impl From<Option<MaybeEncrypted>> for MaybeEncrypted {
    fn from(o: Option<MaybeEncrypted>) -> Self {
        match o {
            Some(b) => b,
            None => MaybeEncrypted::None,
        }
    }
}
