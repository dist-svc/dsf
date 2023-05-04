use core::convert::TryFrom;

#[cfg(feature = "alloc")]
use alloc::vec::Vec;

use crate::base::PageBody;
use crate::crypto::{Crypto, Hash as _, SecKey as _};
use crate::helpers::{parse_bytes_vec, print_bytes};
use crate::page::PageInfo;
use crate::types::*;

use crate::error::Error;
use crate::options::{Filters, Options, OptionsIter};

use super::builder::Init;
use super::header::WireHeader;
use super::{offsets, HEADER_LEN};

use super::Builder;

/// Container object provides base field accessors over an arbitrary (mutable or immutable) buffers
/// See <https://lab.whitequark.org/notes/2016-12-13/abstracting-over-mutability-in-rust/> for details
#[derive(Clone)]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub struct Container<T: ImmutableData = Vec<u8>> {
    /// Internal data buffer
    #[cfg_attr(feature = "defmt", defmt(Debug2Format))]
    pub(crate) buff: T,
    /// Length of object in container buffer
    pub(crate) len: usize,
    // Signals data / private options are currently decrypted
    pub(crate) decrypted: bool,
    // Signals container has been verified
    pub(crate) verified: bool,
}

/// Override `core::compare::PartialEq` to compare `.raw()` instead of `.buff`
impl<T: ImmutableData> PartialEq for Container<T> {
    fn eq(&self, other: &Self) -> bool {
        self.raw() == other.raw() && self.len == other.len && self.decrypted == other.decrypted
    }
}

/// Override `core::fmt::Debug` to show subfields
impl<T: ImmutableData> core::fmt::Debug for Container<T> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        let mut d = f.debug_struct("Container");

        d.field("id", &self.id()).field("header", &self.header());

        match self.encrypted() {
            true => d.field("body (encrypted)", &self.body_raw()),
            false => d.field("body (cleartext)", &self.body_raw()),
        };

        match self.encrypted() {
            true => d.field("private_opts", &self.private_options_raw()),
            false => d.field("private_opts", &self.private_options_iter()),
        };

        // TODO: there seems to be a fault in here which can lead to an infinite loop!?
        d.field("public_opts", &self.public_options_iter());
        //d.field("public_opts", &self.public_options_raw());

        d.field("tag", &self.tag())
            .field("sig", &self.signature())
            .field("len", &self.len())
            .field("decrypted", &self.decrypted)
            .field("verified", &self.verified)
            // TODO: work out how to make this optional / force this to format as hex?
            //.field("raw", &self.raw())
            .finish()
    }
}

/// Decode a container from the provided buffer
///
/// NOTE THIS IS A WIP / DOES NOT PERFORM ANY VALIDATION
impl<'a> encdec::Decode<'a> for Container<&'a [u8]> {
    type Output = Container<&'a [u8]>;
    type Error = Error;

    fn decode(buff: &'a [u8]) -> Result<(Self::Output, usize), Self::Error> {
        // Read header
        let header = WireHeader::new(buff);

        // Computed encoded object length
        let len = header.encoded_len();

        // Build container
        let c = Container {
            buff: &buff[..len],
            len,
            verified: false,
            decrypted: false,
        };

        Ok((c, len))
    }
}

/// Encode a container to the provided buffer
///
/// NOTE THIS IS A WIP / DOES NOT PERFORM ANY VALIDATION
impl<T: ImmutableData> encdec::Encode for Container<T> {
    type Error = Error;

    fn encode_len(&self) -> Result<usize, Self::Error> {
        Ok(self.raw().len())
    }

    fn encode(&self, buff: &mut [u8]) -> Result<usize, Self::Error> {
        // TODO: fail if encryption flag set but not encrypted, sig missing, etc...

        let raw = self.raw();

        if buff.len() < raw.len() {
            return Err(Error::BufferLength);
        }

        buff[..raw.len()].copy_from_slice(raw);

        Ok(raw.len())
    }
}

#[cfg(feature = "serde")]
impl<T: ImmutableData> serde::Serialize for Container<T> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        //serializer.serialize_bytes(self.raw())
        serializer.serialize_str(&print_bytes(self.raw()))
    }
}

#[cfg(feature = "serde")]
impl<'de: 'a, 'a> serde::Deserialize<'de> for Container {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct Visitor;

        impl<'de> serde::de::Visitor<'de> for Visitor {
            type Value = Container;

            fn expecting(&self, formatter: &mut core::fmt::Formatter) -> core::fmt::Result {
                formatter.write_str("raw container object bytes")
            }

            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                let buff = parse_bytes_vec(v)
                    .map_err(|_e| serde::de::Error::custom("byte string decoding failed"))?;

                Container::try_from(buff)
                    .map_err(|_e| serde::de::Error::custom("decoding container"))
            }

            fn visit_bytes<E>(self, v: &[u8]) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                let buff = parse_bytes_vec(v)
                    .map_err(|_e| serde::de::Error::custom("byte string decoding failed"))?;

                Container::try_from(buff)
                    .map(|c| c.to_owned())
                    .map_err(|_e| serde::de::Error::custom("decoding container"))
            }
        }

        deserializer.deserialize_bytes(Visitor)
    }
}

impl<'a> TryFrom<&'a [u8]> for Container<&'a [u8]> {
    // TODO: check basic container info
    type Error = ();

    fn try_from(buff: &'a [u8]) -> Result<Self, Self::Error> {
        let h = WireHeader::new(buff);
        let len = h.encoded_len();

        let c = Container {
            buff: &buff[..len],
            len,
            verified: false,
            decrypted: false,
        };

        Ok(c)
    }
}

impl TryFrom<Vec<u8>> for Container<Vec<u8>> {
    // TODO: check basic container info
    type Error = ();

    fn try_from(value: Vec<u8>) -> Result<Self, Self::Error> {
        let c = Container::from(value);
        Ok(c.0)
    }
}

impl<'a, T: ImmutableData> Container<T> {
    /// Create a new container object, providing field accessors over the provided buffer
    // TODO: this should validate the container on creation to avoid invalid containers ever existing
    pub fn from(buff: T) -> (Self, usize) {
        let len = buff.as_ref().len();
        let c = Container {
            buff,
            len,
            verified: false,
            decrypted: false,
        };
        let n = c.len();
        (c, n)
    }

    /// Convert to a Vec<u8> based owned container
    #[cfg(feature = "alloc")]
    pub fn to_owned(&self) -> Container<Vec<u8>> {
        let buff = self.raw().to_vec();
        let len = buff.len();
        Container {
            buff,
            len,
            decrypted: self.decrypted,
            verified: self.verified,
        }
    }

    /// Fetch wire header
    pub fn header(&self) -> WireHeader<&[u8]> {
        WireHeader {
            buff: &self.buff.as_ref()[..HEADER_LEN],
        }
    }

    /// Fetch object ID
    pub fn id_raw(&self) -> &[u8] {
        let data = self.buff.as_ref();

        &data[offsets::ID..offsets::BODY]
    }

    /// Fetch object ID
    pub fn id(&self) -> Id {
        Id::try_from(self.id_raw()).unwrap()
    }

    pub fn encrypted(&self) -> bool {
        self.header().flags().contains(Flags::ENCRYPTED) && !self.decrypted
    }

    /// Return the body of data
    pub fn body_raw(&self) -> &[u8] {
        let data = self.buff.as_ref();

        let s = self.header().data_len();
        &data[offsets::BODY..][..s]
    }

    /// Fetch page body type as appropriate
    pub fn body<B: PageBody>(&self) -> Result<B, Error> {
        todo!()
    }

    /// Return the private options section data, note this may be encrypted
    pub fn private_options_raw(&self) -> &[u8] {
        let data = self.buff.as_ref();

        let n = offsets::BODY + self.header().data_len();
        let s = self.header().private_options_len();
        &data[n..n + s]
    }

    /// Iterate over private options
    /// NOTE: ONLY VALID FOR DECRYPTED OBJECTS
    pub fn private_options_iter(&self) -> OptionsIter<&[u8]> {
        let data = self.buff.as_ref();
        let n = offsets::BODY + self.header().data_len();
        let s = self.header().private_options_len();

        OptionsIter::new(&data[n..n + s])
    }

    /// Return public options section data
    pub fn public_options_raw(&self) -> &[u8] {
        let h = self.header();
        let data = self.buff.as_ref();

        let tag_len =
            if h.flags().contains(Flags::ENCRYPTED) && !h.flags().contains(Flags::SYMMETRIC_MODE) {
                SECRET_KEY_TAG_LEN
            } else {
                0
            };

        let n = offsets::BODY + h.data_len() + h.private_options_len() + tag_len;
        let s = h.public_options_len();
        &data[n..n + s]
    }

    /// Ciphertext for encrypted objects (body + private options fields)
    pub fn cyphertext(&self) -> &[u8] {
        let data = self.buff.as_ref();

        let s = self.header().data_len() + self.header().private_options_len();

        &data[offsets::BODY..][..s]
    }

    /// Tag for secret key encryption
    pub fn tag_raw(&self) -> Option<&[u8]> {
        let h = self.header();
        let data = self.buff.as_ref();

        if !h.flags().contains(Flags::ENCRYPTED) || h.flags().contains(Flags::SYMMETRIC_MODE) {
            return None;
        }

        let n = HEADER_LEN + ID_LEN + h.data_len() + h.private_options_len();

        Some(&data[n..n + SECRET_KEY_TAG_LEN])
    }

    pub fn tag(&self) -> Option<SecretMeta> {
        self.tag_raw()
            .map(|d| SecretMeta::try_from(d).ok())
            .flatten()
    }

    /// Return the public options section data
    pub fn public_options_iter(&self) -> OptionsIter<&[u8]> {
        let data = self.buff.as_ref();
        let header = self.header();

        let tag_len = if header.flags().contains(Flags::ENCRYPTED)
            && !header.flags().contains(Flags::SYMMETRIC_MODE)
        {
            SECRET_KEY_TAG_LEN
        } else {
            0
        };

        let n = HEADER_LEN + ID_LEN + header.data_len() + header.private_options_len() + tag_len;
        let s = header.public_options_len();

        OptionsIter::new(&data[n..n + s])
    }

    /// Return the signed portion of the message for signing or verification
    pub fn signed(&self) -> &[u8] {
        let data = self.buff.as_ref();
        let n = self.len();

        &data[..n - SIGNATURE_LEN]
    }

    /// Return the signature portion of the message for verification
    pub fn signature_raw(&self) -> &[u8] {
        let data = self.buff.as_ref();

        let n = self.len() - SIGNATURE_LEN;
        let s = SIGNATURE_LEN;

        &data[n..n + s]
    }

    /// Fetch the message signature
    pub fn signature(&self) -> Signature {
        let r = self.signature_raw();
        Signature::try_from(r).unwrap()
    }

    /// Return the total length of the object (from the header)
    pub fn len(&self) -> usize {
        let header = self.header();
        let flags = header.flags();

        let tag_len = if flags.contains(Flags::ENCRYPTED) && !flags.contains(Flags::SYMMETRIC_MODE)
        {
            SECRET_KEY_TAG_LEN
        } else {
            0
        };

        HEADER_LEN
            + ID_LEN
            + header.data_len()
            + header.private_options_len()
            + tag_len
            + header.public_options_len()
            + SIGNATURE_LEN
    }

    /// Verify the contents of a given container
    /// This calls the provided verifier with the id, body, and signature and forwards the result to the caller
    pub fn verify<V, E>(&self, mut verifier: V) -> Result<bool, E>
    where
        V: FnMut(&Id, &Signature, &[u8]) -> Result<bool, E>,
    {
        let id: Id = self.id();
        let data = self.signed();
        let sig: Signature = self.signature();

        (verifier)(&id, &sig, data)
    }

    /// Fetch the raw data using internal header length
    pub fn raw(&self) -> &[u8] {
        let data = self.buff.as_ref();
        let len = self.len();

        &data[0..len]
    }

    /// Fetch page info from a container (filters body and options as required)
    pub fn info(&self) -> Result<PageInfo, Error> {
        let (kind, flags) = (self.header().kind(), self.header().flags());

        let info = if kind.is_page()
            && !flags.contains(Flags::SECONDARY)
            && !flags.contains(Flags::TERTIARY)
        {
            // Handle primary page parsing

            // Fetch public key from options
            let public_key: PublicKey = match self.public_options_iter().pub_key() {
                Some(pk) => pk,
                None => return Err(Error::NoPublicKey),
            };

            // Check public key and ID match
            let hash: Id = Id::from(Crypto::hash(&public_key).unwrap().as_bytes());
            if &hash != &self.id() {
                return Err(Error::KeyIdMismatch);
            }

            PageInfo::primary(public_key)
        } else if kind.is_page() && flags.contains(Flags::SECONDARY) {
            // Handle secondary page parsing

            let peer_id = self.public_options_iter().find_map(|o| match o {
                Options::PeerId(peer_id) => Some(peer_id.clone()),
                _ => None,
            });

            let peer_id = match peer_id {
                Some(id) => Ok(id),
                None => Err(Error::NoPeerId),
            }?;

            PageInfo::secondary(peer_id)
        } else if kind.is_page() && flags.contains(Flags::TERTIARY) {
            // Handle tertiary page parsing (requires access to page body)
            if self.encrypted() {
                return Err(Error::Encrypted);
            }

            let peer_id = self.public_options_iter().find_map(|o| match o {
                Options::PeerId(peer_id) => Some(peer_id.clone()),
                _ => None,
            });
            let peer_id = match peer_id {
                Some(id) => Ok(id),
                None => Err(Error::NoPeerId),
            }?;

            match PageKind::try_from(kind) {
                Ok(PageKind::ServiceLink) => {
                    let target_id = Id::try_from(self.body_raw())?;
                    PageInfo::service_link(target_id, peer_id)
                }
                Ok(PageKind::BlockLink) => {
                    let block_sig = Signature::try_from(self.body_raw())?;
                    PageInfo::block_link(block_sig, peer_id)
                }
                _ => return Err(Error::InvalidPageKind),
            }
        } else if kind.is_data() {
            PageInfo::Data(())
        } else {
            return Err(Error::InvalidPageKind);
        };

        Ok(info)
    }

    /// Check whether an object has expired
    #[cfg(feature = "std")]
    pub fn expired(&self) -> bool {
        use core::ops::Add;

        // Convert issued and expiry times
        let (issued, expiry): (Option<std::time::SystemTime>, Option<std::time::SystemTime>) = (
            self.public_options_iter().issued().map(|v| v.into()),
            self.public_options_iter().expiry().map(|v| v.into()),
        );

        // Compute validity
        match (issued, expiry) {
            // For fixed expiry, use this
            (_, Some(expiry)) => std::time::SystemTime::now() > expiry,
            // For no expiry, use 1h
            (Some(issued), None) => {
                std::time::SystemTime::now() > issued.add(std::time::Duration::from_secs(3600))
            }
            // Otherwise default non-expiring..?
            // TODO: should we even allow services _without_ valid time records?
            // how to approach this for non-timesync'd devices?
            _ => false,
        }
    }
}

impl<'a, T: MutableData> Container<T> {
    /// Create a new container builder
    pub fn builder(buff: T) -> Builder<Init, T> {
        Builder::new(buff)
    }

    pub fn cyphertext_mut(&mut self) -> &mut [u8] {
        let s = self.header().data_len() + self.header().private_options_len();
        let data = self.buff.as_mut();

        &mut data[offsets::BODY..][..s]
    }

    /// Decrypt private fields within an object (in place)
    pub fn decrypt(&mut self, sk: &SecretKey) -> Result<(), Error> {
        // TODO: skip if body + private options are empty...
        debug!("SK Decrypt body with key: {}", sk);

        // Check we're encrypted
        if !self.header().flags().contains(Flags::ENCRYPTED) || self.decrypted {
            debug!("Object already decrypted");
            return Err(Error::InvalidSignature);
        }

        // Extract tag
        let tag = match self.tag() {
            Some(t) => t,
            None => {
                debug!("Object does not contain tag");
                return Err(Error::InvalidSignature);
            }
        };

        // Perform decryption
        let c = self.cyphertext_mut();
        if let Err(_) = Crypto::sk_decrypt(sk, &tag, None, c) {
            debug!("Signature verification failed");
            return Err(Error::InvalidSignature);
        }

        self.decrypted = true;

        Ok(())
    }

    /// Decrypt a symmetric mode AEAD message
    pub fn sk_decrypt(&mut self, secret_key: &SecretKey) -> Result<(), Error> {
        let sig_index = {
            let h = self.header();
            offsets::BODY + h.data_len() + h.private_options_len() + h.public_options_len()
        };
        let sig = self.signature();

        debug!(
            "SK Verify/Decrypt (AEAD) with key: {} (Sig: {}, {} bytes)",
            secret_key, sig, sig_index
        );

        let buff = self.buff.as_mut();

        let (header, body) = buff[..sig_index].split_at_mut(HEADER_LEN + ID_LEN);

        if let Err(e) = Crypto::sk_decrypt(secret_key, &sig[..40], Some(header), body) {
            warn!("Failed AEAD decryption: {:?}", e);
            return Err(Error::CryptoError);
        }

        self.decrypted = true;

        return Ok(());
    }
}

impl<'a, T: ImmutableData> Container<T> {
    // Decrypt data and private options into the provided buffer
    pub fn decrypt_to<'b>(
        &self,
        sk: &SecretKey,
        buff: &'b mut [u8],
    ) -> Result<(&'b [u8], &'b [u8]), Error> {
        // Check we're encrypted
        if !self.header().flags().contains(Flags::ENCRYPTED) || self.decrypted {
            return Err(Error::InvalidSignature);
        }

        // Extract tag
        let tag = match self.tag() {
            Some(t) => t,
            None => return Err(Error::InvalidSignature),
        };

        // Perform decryption
        let c = self.cyphertext();
        buff[..c.len()].copy_from_slice(c);

        Crypto::sk_decrypt(sk, &tag, None, &mut buff[..c.len()])
            .map_err(|_e| Error::InvalidSignature)?;

        Ok((
            &buff[..self.header().data_len()],
            &buff[self.header().data_len()..][..self.header().private_options_len()],
        ))
    }
}

impl<'a, T: ImmutableData> AsRef<[u8]> for Container<T> {
    fn as_ref(&self) -> &[u8] {
        let n = self.len;
        &self.buff.as_ref()[..n]
    }
}

impl<T: MutableData> core::ops::Deref for Container<T> {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        let data = self.buff.as_ref();
        let len = self.len();

        &data[..len]
    }
}

impl<T: MutableData> core::ops::DerefMut for Container<T> {
    fn deref_mut(&mut self) -> &mut [u8] {
        let len = self.len();
        let data = self.buff.as_mut();

        &mut data[..len]
    }
}
