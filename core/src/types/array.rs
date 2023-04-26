use core::{
    cmp::{Ord, Ordering, PartialOrd},
    convert::TryFrom,
    fmt,
    hash::{Hash, Hasher},
    marker::PhantomData,
    ops::BitXor,
    ops::{Deref, DerefMut},
    str::FromStr,
};

use encdec::{Decode, Encode};

#[cfg(feature = "serde")]
use serde::{
    de::{self, Visitor},
    Deserializer, Serializer,
};

use crate::error::Error;

/// Basic const-generic array type to override display etc.
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub struct Array<K, const N: usize>(pub(super) [u8; N], pub(super) PhantomData<K>);

impl<K, const N: usize> Array<K, N> {
    /// Fetch array instance length
    pub const fn len() -> usize {
        N
    }

    /// Deref as array pointer (see [Array::deref] for slice)
    pub fn as_bytes(&self) -> &[u8; N] {
        &self.0
    }
}

impl<K, const N: usize> AsRef<[u8]> for Array<K, N> {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl<K, const N: usize> AsMut<[u8]> for Array<K, N> {
    fn as_mut(&mut self) -> &mut [u8] {
        &mut self.0
    }
}

impl<K, const N: usize> Deref for Array<K, N> {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        &self.0
    }
}

impl<K, const N: usize> DerefMut for Array<K, N> {
    fn deref_mut(&mut self) -> &mut [u8] {
        &mut self.0
    }
}

impl<K, const N: usize> Default for Array<K, N> {
    fn default() -> Self {
        Array([0u8; N], PhantomData)
    }
}

impl<K, const N: usize> Clone for Array<K, N> {
    fn clone(&self) -> Self {
        Self(self.0.clone(), PhantomData)
    }
}

impl<K, const N: usize> PartialEq for Array<K, N> {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl<K, const N: usize> Ord for Array<K, N> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.cmp(&other.0)
    }
}

impl<K, const N: usize> PartialOrd for Array<K, N> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<K, const N: usize> Encode for Array<K, N> {
    type Error = encdec::Error;

    fn encode_len(&self) -> Result<usize, Self::Error> {
        Ok(N)
    }

    fn encode(&self, buff: &mut [u8]) -> Result<usize, Self::Error> {
        if buff.len() < N {
            return Err(encdec::Error::Length);
        }

        buff[..N].copy_from_slice(&self.0);

        Ok(N)
    }
}

impl<'a, K, const N: usize> Decode<'a> for Array<K, N> {
    type Output = Self;

    type Error = encdec::Error;

    fn decode(buff: &'a [u8]) -> Result<(Self::Output, usize), Self::Error> {
        if buff.len() < N {
            return Err(encdec::Error::Length);
        }

        let mut d = [0u8; N];
        d.copy_from_slice(buff);

        Ok((Self(d, PhantomData), N))
    }
}

impl<K, const N: usize> TryFrom<&[u8]> for Array<K, N> {
    type Error = Error;

    fn try_from(data: &[u8]) -> Result<Self, Self::Error> {
        let mut a = [0u8; N];

        if data.len() != N {
            return Err(Error::BufferLength);
        }

        a.copy_from_slice(data);

        Ok(a.into())
    }
}

impl<K, const N: usize> From<[u8; N]> for Array<K, N> {
    fn from(data: [u8; N]) -> Self {
        Array(data, PhantomData)
    }
}

impl<K, const N: usize> From<&[u8; N]> for Array<K, N> {
    fn from(data: &[u8; N]) -> Self {
        let mut a = [0u8; N];

        a.copy_from_slice(data);

        a.into()
    }
}

impl<K, const N: usize> Into<[u8; N]> for Array<K, N> {
    fn into(self) -> [u8; N] {
        self.0
    }
}

impl<K, const N: usize> PartialEq<[u8; N]> for Array<K, N> {
    fn eq(&self, other: &[u8; N]) -> bool {
        self.0.as_ref() == other.as_ref()
    }
}

impl<K, const N: usize> Hash for Array<K, N> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.0.hash(state)
    }
}

impl<K, const N: usize> Eq for Array<K, N> {}

impl<K, const N: usize> BitXor for Array<K, N> {
    type Output = Array<K, N>;

    fn bitxor(self, rhs: Array<K, N>) -> Self::Output {
        let mut s = self;
        for i in 0..N {
            s[i] ^= rhs[i]
        }
        s
    }
}

impl<K, const N: usize> fmt::Display for Array<K, N> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let r: &[u8] = &self.0;
        let encoded = base64::encode_config(&r, base64::URL_SAFE);
        write!(f, "{}", encoded)?;
        Ok(())
    }
}

impl<K, const N: usize> fmt::Debug for Array<K, N> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let r: &[u8] = &self.0;
        let encoded = base64::encode_config(&r, base64::URL_SAFE);
        write!(f, "{}", encoded)?;
        Ok(())
    }
}

impl<K, const N: usize> fmt::UpperHex for Array<K, N> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        for i in 0..self.0.len() {
            if i == 0 {
                write!(f, "{:02X}", self.0[i])?;
            } else {
                write!(f, ":{:02X}", self.0[i])?;
            }
        }
        Ok(())
    }
}

impl<K, const N: usize> FromStr for Array<K, N> {
    type Err = base64::DecodeError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut data = [0u8; N];
        let _decoded = base64::decode_config_slice(s, base64::URL_SAFE, &mut data)?;
        // TODO: check decoded length

        Ok(data.into())
    }
}

#[cfg(feature = "serde")]
impl<K, const N: usize> serde::Serialize for Array<K, N> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.collect_str(self)
    }
}

#[cfg(feature = "serde")]
impl<'de, K, const N: usize> serde::Deserialize<'de> for Array<K, N> {
    fn deserialize<D>(deserializer: D) -> Result<Array<K, N>, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct B64Visitor<T>(PhantomData<T>);

        impl<'de, T: FromStr> Visitor<'de> for B64Visitor<T> {
            type Value = T;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a base64 encoded string")
            }

            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                T::from_str(value).map_err(|_e| de::Error::custom("decoding b64"))
            }
        }

        deserializer.deserialize_str(B64Visitor::<Array<K, N>>(PhantomData))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Copy, Clone, PartialEq, Debug)]
    struct SomeMarker;

    #[test]
    fn encode_decode_array32() {
        let a = Array::<SomeMarker, 32>([0u8; 32], PhantomData);

        let b = a.to_string();

        println!("B: {}", b);

        let c = Array::<SomeMarker, 32>::from_str(&b).unwrap();

        assert_eq!(a, c);
    }
}
