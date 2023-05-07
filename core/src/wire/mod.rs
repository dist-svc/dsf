//! Wire provides a container type to map byte data to fixed fields (and vice versa)
//! to support wire encoding and decoding.

#[cfg(feature = "alloc")]
use alloc::vec::Vec;

use pretty_hex::*;

use crate::crypto::{Crypto, Hash as _, PubKey as _};
use crate::error::Error;
use crate::options::Options;
use crate::types::*;

/// Header provides a low-cost header abstraction for encoding/decoding
pub mod header;

/// Builder provides methods to construct a container using a mutable buffer and base types
pub mod builder;
pub use builder::Builder;

/// Container provides methods to access underlying wire object fields
pub mod container;
pub use container::Container;

use crate::keys::{KeySource, Keys};

/// Header object length
pub const HEADER_LEN: usize = offsets::ID;

/// Offsets for fixed fields in the protocol header
mod offsets {
    pub const PROTO_VERSION: usize = 0;
    pub const APPLICATION_ID: usize = PROTO_VERSION + 1;
    pub const OBJECT_KIND: usize = APPLICATION_ID + 2;
    pub const FLAGS: usize = OBJECT_KIND + 2;
    pub const INDEX: usize = FLAGS + 2;
    pub const DATA_LEN: usize = INDEX + 3;
    pub const PRIVATE_OPTIONS_LEN: usize = DATA_LEN + 2;
    pub const PUBLIC_OPTIONS_LEN: usize = PRIVATE_OPTIONS_LEN + 2;
    pub const ID: usize = PUBLIC_OPTIONS_LEN + 2;
    pub const BODY: usize = ID + 32;
}

/// Helper for validating signatures in symmetric or asymmetric modes
fn validate<T: MutableData>(
    signing_id: &Id,
    keys: &Keys,
    container: &mut Container<T>,
) -> Result<bool, Error> {
    let header = container.header();
    let flags = header.flags();
    let kind = header.kind();

    // Attempt to use secret key mode if available
    let valid = if flags.contains(Flags::SYMMETRIC_MODE) {
        trace!("Using symmetric signing mode");

        // Ensure symmetric mode is only used for messages
        if !kind.is_message() {
            return Err(Error::UnsupportedSignatureMode);
        }

        // Check for matching symmetric key
        let sk = match &keys.sym_keys {
            Some(s) if flags.contains(Flags::SYMMETRIC_DIR) => &s.0,
            Some(s) => &s.1,
            None => {
                return Err(Error::NoSymmetricKeys);
            }
        };

        trace!("Decrypt/Verify(AEAD) with key: {}", sk);

        // Validate / decrypt object

        container.sk_decrypt(sk).map_err(|_e| Error::CryptoError)?;

        true

    // Otherwise use public key
    } else {
        trace!("Using asymmetric mode");

        // Check for matching public key
        let pub_key = match &keys.pub_key {
            Some(pk) => pk,
            None => return Err(Error::NoPublicKey),
        };

        // Check ID matches public key
        let h = Crypto::hash(pub_key).unwrap();
        if signing_id.as_bytes() != h.as_bytes() {
            error!(
                "Public key mismatch for object from {:?} ({})",
                signing_id, h
            );
            return Err(Error::KeyIdMismatch);
        }

        // Validate signature
        Crypto::pk_verify(pub_key, &container.signature(), container.signed())
            .map_err(|_e| Error::CryptoError)?
    };

    Ok(valid)
}

impl<'a, T: MutableData> Container<T> {
    /// Parses a data array into a base object using the pub_key and sec_key functions to locate keys for validation and decryption
    pub fn parse<K>(data: T, key_source: &K) -> Result<Container<T>, Error>
    where
        K: KeySource,
    {
        // Build container over buffer
        let (mut container, _n) = Container::from(data);

        trace!("Parsing object: {:02x?}", container.hex_dump());

        let (id, flags, _kind, index) = {
            let header = container.header();
            trace!("Parsed header: {:02x?}", header);

            (
                container.id(),
                header.flags(),
                header.kind(),
                header.index(),
            )
        };

        trace!("Parse container: {:?}", container);

        // Fetch signature for page
        let mut verified = false;
        let signature: Signature = container.signature();

        // Validate primary types immediately if pubkey is known
        let is_primary = !flags.contains(Flags::SECONDARY) && !flags.contains(Flags::TERTIARY);

        match (is_primary, key_source.keys(&id)) {
            (true, Some(keys)) if keys.pub_key.is_some() => {
                let pub_key = keys.pub_key.as_ref().unwrap();

                trace!(
                    "Early signature validate: {:02x?} using key: {:?}",
                    signature.as_ref(),
                    pub_key
                );

                // Perform verification
                verified = validate(&id, &keys, &mut container)?;

                // Stop processing if signature is invalid
                if !verified {
                    info!("Invalid signature with known pubkey");
                    return Err(Error::InvalidSignature);
                }
            }
            _ => {
                trace!("Skipping early signature validation, no keys loaded");
            }
        }

        trace!("Fetching public options");

        // Fetch public options
        let mut peer_id = None;
        let mut pub_key = None;
        let mut prev = None;

        for o in container.public_options_iter() {
            match o {
                Options::PeerId(v) => {
                    peer_id = Some(v.clone());
                }
                Options::PubKey(v) => {
                    pub_key = Some(v.clone());
                }
                Options::PrevSig(v) => {
                    prev = Some(v.clone());
                }
                _ => (),
            }
        }

        // Look for signing ID
        let signing_id: Id = match (!is_primary, &peer_id) {
            (false, _) => Ok(container.id()),
            (true, Some(id)) => Ok(id.clone()),
            _ => Err(Error::NoPeerId),
        }?;

        trace!(
            "Peer id: {:?} pub_key: {:?} prev: {:?} signing_id: {:?}",
            peer_id,
            pub_key,
            prev,
            signing_id
        );

        // Fetch public key from public options if available
        let keys: Option<Keys> = match (key_source.keys(&signing_id), &pub_key) {
            (Some(keys), _) if keys.pub_key.is_some() => Some(keys),
            (_, Some(key)) => Some(Keys::new(key.clone())),
            _ => {
                warn!(
                    "Missing public key for message: {:?} signing id: {:?}",
                    index, signing_id
                );
                None
            }
        };

        trace!("Re-validating object (keys: {:?})", keys);

        // Late validation for self-signed objects from unknown sources
        match (verified, keys) {
            (false, Some(keys)) => {
                // Check signature
                verified = validate(&signing_id, &keys, &mut container)?;

                // Stop processing on verification failure
                if !verified {
                    info!("Invalid signature for self-signed object from {:?}", id);
                    return Err(Error::InvalidSignature);
                }
            }
            (false, None) => {
                error!("No signature or key for object from {:?}", id);
                return Err(Error::NoSignature);
            }
            _ => (),
        }

        trace!("Parse OK! (verified: {:?})", verified);
        container.verified = verified;
        container.len = container.len();

        // Return verified container
        Ok(container)
    }
}

impl<T: ImmutableData> Container<T> {
    pub fn encode_pages(pages: &[Container<T>], buff: &mut [u8]) -> Result<usize, Error> {
        let mut i = 0;

        for p in pages {
            // TODO: is there any way to have an invalid (non-signed/verified) container here?
            // if so, handle this case
            let b = p.raw();

            // Check we have space
            if b.len() >= buff.len() - i {
                return Err(Error::BufferLength);
            }

            // Convert and encode, note these must be pre-signed / encrypted
            buff[i..][..b.len()].copy_from_slice(b);

            i += b.len();
        }

        Ok(i)
    }
}

impl Container {
    pub fn decode_pages<V>(buff: &[u8], key_source: &V) -> Result<Vec<Container>, Error>
    where
        V: KeySource,
    {
        let mut pages = vec![];
        let mut i = 0;

        // Last key used to cache the previous primary key to decode secondary pages published by a service in a single message.
        let mut last_key: Option<(Id, Keys)> = None;

        while i < buff.len() {
            // TODO: validate signatures against existing services!
            let c =
                match Container::parse((&buff[i..]).to_vec(), &key_source.cached(last_key.clone()))
                {
                    Ok(v) => v,
                    Err(e) => {
                        debug!("Error parsing base message: {:?}", e);
                        return Err(e);
                    }
                };

            i += c.len();

            // Cache key for next run
            if let Some(key) = c.info()?.pub_key() {
                last_key = Some((c.id().clone(), Keys::new(key)));
            }

            // Push page to parsed list
            pages.push(c);
        }

        Ok(pages)
    }
}

#[cfg(test)]
mod test {
    extern crate test;
    use test::Bencher;

    use super::*;

    use crate::{
        crypto::SecKey,
        keys::NullKeySource,
        prelude::{Body, Header},
    };

    fn setup() -> (Id, Keys) {
        #[cfg(feature = "simplelog")]
        let _ = simplelog::SimpleLogger::init(
            simplelog::LevelFilter::Trace,
            simplelog::Config::default(),
        );

        let (pub_key, pri_key) =
            Crypto::new_pk().expect("Error generating new public/private key pair");

        let id = Id::from(
            Crypto::hash(&pub_key)
                .expect("Error generating new ID")
                .as_bytes(),
        );

        let sec_key = Crypto::new_sk().expect("Error generating new secret key");
        (
            id,
            Keys {
                pub_key: Some(pub_key),
                pri_key: Some(pri_key),
                sec_key: Some(sec_key),
                sym_keys: None,
            },
        )
    }

    #[test]
    fn encode_decode_primary_page() {
        let (id, mut keys) = setup();
        keys.sec_key = None;

        let header = Header {
            kind: PageKind::Generic.into(),
            application_id: 10,
            index: 12,
            ..Default::default()
        };
        let data = vec![1, 2, 3, 4, 5, 6, 7];

        // Encode using builder
        let c = Builder::new(vec![0u8; 1024])
            .id(&id)
            .header(&header)
            .body(data)
            .unwrap()
            .private_options(&[])
            .unwrap()
            .public()
            .sign_pk(keys.pri_key.as_ref().unwrap())
            .expect("Error encoding page");

        // Decode into container
        let d = Container::parse(c.raw().to_vec(), &keys).expect("Error decoding page");

        // TODO: convert to pages and compare

        assert_eq!(c, d);
    }

    #[bench]
    fn bench_encode_primary(b: &mut Bencher) {
        let (id, mut keys) = setup();
        keys.sec_key = None;

        let header = Header {
            kind: PageKind::Generic.into(),
            application_id: 10,
            index: 12,
            ..Default::default()
        };
        let data = vec![1, 2, 3, 4, 5, 6, 7];

        b.iter(|| {
            // Encode using builder
            let _c = Builder::new([0u8; 1024])
                .id(&id)
                .header(&header)
                .body(&data)
                .unwrap()
                .private_options(&[])
                .unwrap()
                .public()
                .sign_pk(keys.pri_key.as_ref().unwrap())
                .expect("Error encoding page");
        });
    }

    #[test]
    fn encode_decode_secondary_page() {
        let (id, mut keys) = setup();
        keys.sec_key = None;

        let header = Header {
            kind: PageKind::Replica.into(),
            flags: Flags::SECONDARY,
            ..Default::default()
        };
        let data = vec![1, 2, 3, 4, 5, 6, 7];

        let encoded = Builder::new(vec![0u8; 1024])
            .id(&id)
            .header(&header)
            .body(Body::Cleartext(data))
            .unwrap()
            .private_options(&[])
            .unwrap()
            .public()
            .public_options(&[
                Options::peer_id(id.clone()),
                Options::pub_key(keys.pub_key.as_ref().unwrap().clone()),
            ])
            .unwrap()
            .sign_pk(keys.pri_key.as_ref().unwrap())
            .expect("Error encoding page");

        let decoded = Container::parse(encoded.raw().to_vec(), &keys)
            .expect("Error decoding page with known public key");

        assert_eq!(encoded, decoded);
        assert_eq!(encoded.raw(), decoded.raw());

        let decoded2 = Container::parse(encoded.raw().to_vec(), &NullKeySource)
            .expect("Error decoding page with unknown public key");

        assert_eq!(encoded, decoded2);
        assert_eq!(encoded.raw(), decoded.raw().to_vec());
    }

    #[test]
    fn encode_decode_tertiary_page() {
        let (id, mut keys) = setup();
        keys.sec_key = None;

        let header = Header {
            kind: PageKind::ServiceLink.into(),
            flags: Flags::TERTIARY,
            ..Default::default()
        };
        let data = id.to_vec();

        let encoded = Builder::new(vec![0u8; 1024])
            .id(&id)
            .header(&header)
            .body(Body::Cleartext(data))
            .unwrap()
            .private_options(&[])
            .unwrap()
            .public()
            .public_options(&[Options::peer_id(id.clone())])
            .unwrap()
            .sign_pk(keys.pri_key.as_ref().unwrap())
            .expect("Error encoding page");

        let decoded = Container::parse(encoded.raw().to_vec(), &keys)
            .expect("Error decoding page with known public key");

        assert_eq!(encoded, decoded);
        assert_eq!(encoded.raw(), decoded.raw().to_vec());
    }

    #[test]
    fn encode_decode_encrypted_page() {
        let (id, keys) = setup();

        let header = Header {
            kind: PageKind::Generic.into(),
            flags: Flags::ENCRYPTED,
            ..Default::default()
        };
        let data = vec![1, 2, 3, 4, 5, 6, 7];

        let encoded = Builder::new(vec![0u8; 1024])
            .id(&id)
            .header(&header)
            .body(Body::Cleartext(data.clone()))
            .unwrap()
            .private_options(&[])
            .unwrap()
            .encrypt(keys.sec_key.as_ref().unwrap())
            .unwrap()
            .public_options(&[Options::peer_id(id.clone())])
            .unwrap()
            .sign_pk(keys.pri_key.as_ref().unwrap())
            .expect("Error encoding page");

        let mut decoded =
            Container::parse(encoded.raw().to_vec(), &keys).expect("Error decoding page");
        assert_eq!(encoded, decoded);

        // Check we're encrypted
        assert_eq!(decoded.encrypted(), true);
        assert_ne!(decoded.body_raw(), &data);

        // Perform decryption
        decoded.decrypt(keys.sec_key.as_ref().unwrap()).unwrap();
        assert_eq!(decoded.body_raw(), &data);
    }

    #[test]
    fn encode_decode_encrypted_message() {
        let (id, keys) = setup();

        // Generate target keys
        let (pub_key, _pri_key) =
            Crypto::new_pk().expect("Error generating new public/private key pair");
        let keys = keys.derive_peer(pub_key).unwrap();

        let header = Header {
            kind: RequestKind::Hello.into(),
            flags: Flags::SYMMETRIC_MODE | Flags::ENCRYPTED,
            ..Default::default()
        };
        let data = vec![1, 2, 3, 4, 5, 6, 7];

        let encoded = Builder::new(vec![0u8; 1024])
            .id(&id)
            .header(&header)
            .body(Body::Cleartext(data.clone()))
            .unwrap()
            .private_options(&[])
            .unwrap()
            .public()
            .public_options(&[Options::peer_id(id.clone())])
            .unwrap()
            .encrypt_sk(keys.sym_keys.as_ref().map(|k| &k.1).unwrap())
            //.sign_pk(keys.pri_key.as_ref().unwrap())
            .expect("Error encoding message");

        let decoded = Container::parse(encoded.raw().to_vec(), &keys).expect("Error decoding page");

        assert_eq!(encoded.header(), decoded.header());

        // Check we're decrypted
        assert_eq!(decoded.encrypted(), false);
        assert_eq!(decoded.body_raw(), &data);
    }

    #[bench]
    fn bench_encode_primary_encrypted(b: &mut Bencher) {
        let (id, keys) = setup();

        let header = Header {
            kind: PageKind::Generic.into(),
            application_id: 10,
            index: 12,
            flags: Flags::ENCRYPTED,
            ..Default::default()
        };
        let data = vec![1, 2, 3, 4, 5, 6, 7];

        b.iter(|| {
            // Encode using builder
            let _c = Builder::new([0u8; 1024])
                .id(&id)
                .header(&header)
                .body(&data)
                .unwrap()
                .private_options(&[])
                .unwrap()
                .encrypt(keys.sec_key.as_ref().unwrap())
                .unwrap()
                .public_options(&[Options::peer_id(id.clone())])
                .unwrap()
                .sign_pk(keys.pri_key.as_ref().unwrap())
                .expect("Error encoding page");
        });
    }

    #[bench]
    fn bench_encode_message_signed_pk(b: &mut Bencher) {
        let (id, keys) = setup();

        let header = Header {
            kind: RequestKind::Hello.into(),
            application_id: 0,
            index: 12,
            ..Default::default()
        };
        let data = vec![1, 2, 3, 4, 5, 6, 7];

        b.iter(|| {
            // Encode using builder
            let _c = Builder::new([0u8; 1024])
                .id(&id)
                .header(&header)
                .body(&data)
                .unwrap()
                .private_options(&[])
                .unwrap()
                .public()
                .public_options(&[Options::peer_id(id.clone())])
                .unwrap()
                .sign_pk(keys.pri_key.as_ref().unwrap())
                .expect("Error encoding page");
        });
    }

    #[bench]
    fn bench_encode_message_encrypted_sk(b: &mut Bencher) {
        let (id, keys) = setup();

        let (pub_key, pri_key) =
            Crypto::new_pk().expect("Error generating new public/private key pair");
        let target = Keys {
            pub_key: Some(pub_key),
            pri_key: Some(pri_key),
            ..Default::default()
        };

        let target = target.derive_peer(keys.pub_key.unwrap().clone()).unwrap();

        let header = Header {
            kind: RequestKind::Hello.into(),
            application_id: 0,
            index: 12,
            flags: Flags::SYMMETRIC_MODE,
            ..Default::default()
        };
        let data = vec![1, 2, 3, 4, 5, 6, 7];

        b.iter(|| {
            // Encode using builder
            let _c = Builder::new([0u8; 1024])
                .id(&id)
                .header(&header)
                .body(&data)
                .unwrap()
                .private_options(&[])
                .unwrap()
                .public()
                .public_options(&[Options::peer_id(id.clone())])
                .unwrap()
                .encrypt_sk(target.sym_keys.as_ref().map(|k| &k.0).unwrap())
                .expect("Error encoding page");
        });
    }
}
