//! Types defines common data types for use in DSF
//!
//!

use core::marker::PhantomData;

#[cfg(feature = "std")]
pub use chrono::Duration;

mod array;
pub use array::*;

pub mod markers;
use markers::*;

pub mod kinds;
pub use self::kinds::*;

pub mod flags;
pub use self::flags::*;

pub mod datetime;
pub use self::datetime::DateTime;

pub mod address;
pub use self::address::{Address, AddressV4, AddressV6, Ip};

/// ImmutableData trait wraps AsRef<[u8]>
pub trait ImmutableData: AsRef<[u8]> + core::fmt::Debug {}

/// Generic impl of ImmutableData trait (since we don't have trait aliasing)
impl<T: AsRef<[u8]> + core::fmt::Debug> ImmutableData for T {}

/// MutableData trait, wraps AsMut<[u8]> and ImmutableData traits
pub trait MutableData: AsMut<[u8]> + ImmutableData {}

/// Generic impl of MutableData trait (since we don't have trait aliasing)
impl<T: AsMut<[u8]> + ImmutableData> MutableData for T {}

/// Queryable trait for name resolution services
pub trait Queryable: core::fmt::Debug {
    fn hash<H: CryptoHasher>(&self, h: &mut H) -> bool;
}

/// Automatic impl on references to Queryable types
impl<T: Queryable> Queryable for &T {
    fn hash<H: CryptoHasher>(&self, h: &mut H) -> bool {
        <T as Queryable>::hash(self, h)
    }
}

pub trait CryptoHasher {
    fn update(&mut self, buff: &[u8]);
}

pub const ID_LEN: usize = 32;
/// ID type
pub type Id = Array<IdTy, ID_LEN>;

#[derive(Copy, Clone, Debug, PartialEq)]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub struct IdTy {}

impl From<CryptoHash> for Id {
    fn from(h: CryptoHash) -> Self {
        Self(h.0, PhantomData)
    }
}

impl Queryable for Id {
    fn hash<H: CryptoHasher>(&self, state: &mut H) -> bool {
        state.update(&self.0);
        true
    }
}

impl Queryable for CryptoHash {
    fn hash<H: CryptoHasher>(&self, state: &mut H) -> bool {
        state.update(&self.0);
        true
    }
}

pub const REQUEST_ID_LEN: usize = 2;
/// Request ID type
pub type RequestId = u16;

pub const PUBLIC_KEY_LEN: usize = 32;
/// Public key type
pub type PublicKey = Array<PublicKeyTy, PUBLIC_KEY_LEN>;

pub const PRIVATE_KEY_LEN: usize = 64;
/// Private key type
pub type PrivateKey = Array<PrivateKeyTy, PRIVATE_KEY_LEN>;

pub const SIGNATURE_LEN: usize = 64;
/// Signature type
pub type Signature = Array<SignatureTy, SIGNATURE_LEN>;

pub const SECRET_KEY_LEN: usize = 32;
/// Secret key type
pub type SecretKey = Array<SecretKeyTy, SECRET_KEY_LEN>;

pub const SECRET_KEY_TAG_LEN: usize = 40;
/// Secret key encryption metadata (tag and nonce)
pub type SecretMeta = Array<SecretMetaTy, SECRET_KEY_TAG_LEN>;

pub const HASH_LEN: usize = 32;
/// Cryptographic hash value
pub type CryptoHash = Array<CryptoHashTy, HASH_LEN>;

pub type Data = crate::wire::Container;
