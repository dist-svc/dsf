//! Crypto module provides cryptographic interfaces and implementations for DSF
//!

use sodiumoxide::crypto::kx;
use sodiumoxide::crypto::sign;

use crate::types::Array32;
use crate::types::CryptoHasher;
use crate::types::SECRET_KEY_TAG_LEN;
use crate::types::{PrivateKey, PublicKey, SecretKey, Signature, SecretMeta};

use super::{PubKey, SecKey, Hash};

pub struct SodiumCrypto;

impl PubKey for SodiumCrypto {
    type Error = ();

    fn new_pk() -> Result<(PublicKey, PrivateKey), Self::Error> {
        let (pub_key, pri_key) = sign::gen_keypair();
        
        Ok((pub_key.0.into(), pri_key.0.into()))
    }

    fn pk_sign(private_key: &PrivateKey, data: &[u8]) -> Result<Signature, Self::Error> {
        // Parse key from provided slice
        let private_key = sign::SecretKey::from_slice(private_key).unwrap();
        // Generate signature
        let sig = sign::sign_detached(data, &private_key);
        // Return signature object
        Ok(sig.to_bytes().into())
    }

    fn pk_verify(public_key: &PublicKey, signature: &Signature, data: &[u8]) -> Result<bool, Self::Error> {
        // Parse key from provided slice
        let public_key = sign::PublicKey::from_slice(public_key).unwrap();
        // Parse signature from provided slice
        let sig = sign::Signature::new(signature.clone().into());
        // Verify signature (returns true or)
        Ok(sign::verify_detached(&sig, data, &public_key))
    }

    fn kx(pub_key: &PublicKey, pri_key: &PrivateKey, remote: &PublicKey) -> Result<(SecretKey, SecretKey), Self::Error> {
        // Convert to sodium signing keys
        let pri_key = sign::SecretKey::from_slice(pri_key).unwrap();
        let pub_key = sign::PublicKey::from_slice(pub_key).unwrap();
        let remote = sign::PublicKey::from_slice(remote).unwrap();

        // Convert signing keys to encryption keys
        let pri_key = sign::to_curve25519_sk(&pri_key).unwrap();
        let pub_key = sign::to_curve25519_pk(&pub_key).unwrap();
        let remote = sign::to_curve25519_pk(&remote).unwrap();

        // Convert encryption keys to kx
        let pri_key = kx::SecretKey::from_slice(&pri_key.0).unwrap();
        let pub_key = kx::PublicKey::from_slice(&pub_key.0).unwrap();
        let remote = kx::PublicKey::from_slice(&remote.0).unwrap();

        // Generate keys
        let k1 = kx::server_session_keys(&pub_key, &pri_key, &remote).unwrap();
        let k2 = kx::client_session_keys(&pub_key, &pri_key, &remote).unwrap();

        Ok((k1.0 .0.into(), k2.1 .0.into()))
    }
}


impl SecKey for SodiumCrypto {
    type Error = ();

    /// new_sk creates a new secret key for symmetric encryption and decryption
    fn new_sk() -> Result<SecretKey, Self::Error> {
        let key = sodiumoxide::crypto::aead::gen_key();
        Ok(key.0.into())
    }

    fn sk_encrypt(secret_key: &SecretKey, assoc: Option<&[u8]>, message: &mut [u8]) -> Result<SecretMeta, Self::Error> {
        use sodiumoxide::crypto::aead;

        let secret_key = aead::Key::from_slice(secret_key).unwrap();
        let nonce = aead::gen_nonce();

        // Perform in-place encryption
        let tag = aead::seal_detached(message, assoc, &nonce, &secret_key);

        // Generate encryption metadata
        let mut meta = [0u8; SECRET_KEY_TAG_LEN];
        (&mut meta[..aead::TAGBYTES]).copy_from_slice(&tag.0);
        (&mut meta[aead::TAGBYTES..]).copy_from_slice(&nonce.0);

        Ok(meta.into())
    }

    fn sk_decrypt(secret_key: &SecretKey, meta: &[u8], assoc: Option<&[u8]>, message: &mut [u8]) -> Result<(), Self::Error> {
        use sodiumoxide::crypto::aead;

        let secret_key = aead::Key::from_slice(secret_key).unwrap();

        // Parse encryption metadata
        let tag = aead::Tag::from_slice(&meta[..aead::TAGBYTES]).unwrap();
        let nonce = aead::Nonce::from_slice(&meta[aead::TAGBYTES..]).unwrap();

        // Perform in-place decryption
        aead::open_detached(message, assoc, &tag, &nonce, &secret_key)
    }

    fn sk_reencrypt(secret_key: &SecretKey, meta: &[u8], assoc: Option<&[u8]>, message: &mut [u8]) -> Result<SecretMeta, Self::Error> {
        use sodiumoxide::crypto::aead;

        let secret_key = aead::Key::from_slice(secret_key).unwrap();

        let t1 = aead::Tag::from_slice(&meta[..aead::TAGBYTES]).unwrap();
        let nonce = aead::Nonce::from_slice(&meta[aead::TAGBYTES..]).unwrap();

        // Perform in-place encryption
        let t2 = aead::seal_detached(message, assoc, &nonce, &secret_key);

        assert_eq!(t1, t2);

        // Generate encryption metadata
        let mut meta = [0u8; SECRET_KEY_TAG_LEN];
        (&mut meta[..aead::TAGBYTES]).copy_from_slice(&t2.0);
        (&mut meta[aead::TAGBYTES..]).copy_from_slice(&nonce.0);

        Ok(meta.into())
    }
}

impl Hash for SodiumCrypto {
    type Error = ();

    fn kdf(key: Array32) -> Result<Array32, ()> { 
        let mut derived: Array32 = Default::default();
        
        // We use a KDF here to ensure that knowing the hashed data cannot leak the app secret key
        let sk = sodiumoxide::crypto::kdf::Key(key.into());
        let _ = sodiumoxide::crypto::kdf::derive_from_key(&mut derived, DSF_NS_KDF_IDX, DSF_NS_KDF_CTX, &sk);
        
        Ok(derived)
    }
}


/// Blake2b KDF context, randomly generated
const DSF_NS_KDF_CTX: [u8; 8] = [208, 217, 2, 27, 15, 253, 70, 121];
/// KDF derived key index, must not be reused for any other purpose
const DSF_NS_KDF_IDX: u64 = 1;


impl CryptoHasher for sodiumoxide::crypto::hash::sha256::State {
    fn update(&mut self, buff: &[u8]) {
        self.update(buff)
    }
}

#[cfg(test)]
mod test {
    extern crate test;
    use test::Bencher;

    use crate::types::SECRET_KEY_TAG_LEN;
    use super::*;

    #[test]
    fn test_sk_tag_lengths() {
        use sodiumoxide::crypto::aead;

        assert_eq!(SECRET_KEY_TAG_LEN, aead::TAGBYTES + aead::NONCEBYTES);
    }

    #[test]
    fn test_pk_sign_verify() {
        let (public, private) = SodiumCrypto::new_pk().expect("Error generating public/private keypair");
        let mut data = vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9];

        let signature = SodiumCrypto::pk_sign(&private, &data).expect("Error generating signature");

        let valid = SodiumCrypto::pk_verify(&public, &signature, &data).expect("Error validating signature");
        assert_eq!(true, valid);

        data[3] = 100;
        let valid = SodiumCrypto::pk_verify(&public, &signature, &data).expect("Error validating signature");
        assert_eq!(false, valid);
    }

    #[test]
    fn test_pk_sym_sign_verify() {
        let (pub1, pri1) = SodiumCrypto::new_pk().expect("Error generating public/private keypair");
        let (pub2, pri2) = SodiumCrypto::new_pk().expect("Error generating public/private keypair");

        let mut data = vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9];

        let (s1, _) = SodiumCrypto::kx( &pub1, &pri1, &pub2).unwrap();
        let (_, s2) = SodiumCrypto::kx( &pub2, &pri2, &pub1).unwrap();

        assert_eq!(s1, s2);

        let signature = SodiumCrypto::sk_sign(&s1, &data).expect("Error generating signature");

        let valid = SodiumCrypto::sk_verify(&s2, &signature, &data).expect("Error validating signature");
        assert_eq!(true, valid);

        data[3] = 100;
        let valid = SodiumCrypto::sk_verify(&s2, &signature, &data).expect("Error validating signature");
        assert_eq!(false, valid);
    }

    #[test]
    fn test_sk_encrypt_decrypt() {
        let secret = SodiumCrypto::new_sk().expect("Error generating secret key");
        let data = vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9];
        let mut message = data.clone();

        let meta = SodiumCrypto::sk_encrypt(&secret, None, &mut message).expect("Error encrypting data");
        assert!(data != message);

        SodiumCrypto::sk_decrypt(&secret, &meta, None, &mut message).expect("Error decrypting data");
        assert_eq!(data, message);
    }

    #[bench]
    fn bench_pk_sign(b: &mut Bencher) {
        let (_public, private) = SodiumCrypto::new_pk().expect("Error generating public/private keypair");
        let data = [0xabu8; 256];

        b.iter(|| {
            let _sig = SodiumCrypto::pk_sign(&private, &data).expect("Error generating signature");
        })
    }

    #[bench]
    fn bench_pk_verify(b: &mut Bencher) {
        let (public, private) = SodiumCrypto::new_pk().expect("Error generating public/private keypair");
        let data = [0xabu8; 256];

        let signature = SodiumCrypto::pk_sign(&private, &data).expect("Error generating signature");

        b.iter(|| {
            let valid =
                SodiumCrypto::pk_verify(&public, &signature, &data).expect("Error validating signature");
            assert_eq!(true, valid);
        })
    }

    #[bench]
    fn bench_pk_sk_convert(b: &mut Bencher) {
        let (pub_key_a, pri_key_a) = SodiumCrypto::new_pk().expect("Error generating public/private keypair");
        let (pub_key_b, _pri_key_b) = SodiumCrypto::new_pk().expect("Error generating public/private keypair");

        b.iter(|| {
            let _ =
                SodiumCrypto::kx(&pub_key_a, &pri_key_a, &pub_key_b).expect("Error deriving secret keys");
        });
    }

    #[bench]
    fn bench_sk_sign(b: &mut Bencher) {
        let sec_key = SodiumCrypto::new_sk().expect("Error generating secret key");
        let data = [0xabu8; 256];

        b.iter(|| {
            let mut d = data.clone();
            let _sig = SodiumCrypto::sk_encrypt(&sec_key, None, &mut d).expect("Error signing data");
        });
    }

    #[bench]
    fn bench_sk_verify(b: &mut Bencher) {
        let sec_key = SodiumCrypto::new_sk().expect("Error generating secret key");
        let mut data = [0xabu8; 256];
        let sig = SodiumCrypto::sk_encrypt(&sec_key, None, &mut data).expect("Error signing data");

        b.iter(|| {
            let mut d = data.clone();
            let _v = SodiumCrypto::sk_decrypt(&sec_key, &sig, None, &mut d).expect("Error validating data");
        });
    }
}
