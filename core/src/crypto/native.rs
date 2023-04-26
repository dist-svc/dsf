use core::convert::{TryFrom, TryInto};
use core::ops::Deref;

use chacha20poly1305::aead::{AeadInPlace, NewAead};
use chacha20poly1305::ChaCha20Poly1305;
use ed25519_dalek::{Keypair, Signer};

use rand_core_0_6::{OsRng, RngCore as _};
use sha2::Digest;

use super::{Hash, PubKey, SecKey};
use crate::types::*;

pub struct RustCrypto;

/// Hacks to run two versions of rand_core because ed25519_dalek expects 0.5.x
struct RandHelper(OsRng);

impl rand_core_0_5::RngCore for RandHelper {
    fn next_u32(&mut self) -> u32 {
        self.0.next_u32()
    }

    fn next_u64(&mut self) -> u64 {
        self.0.next_u64()
    }

    fn fill_bytes(&mut self, dest: &mut [u8]) {
        self.0.fill_bytes(dest)
    }

    fn try_fill_bytes(&mut self, dest: &mut [u8]) -> Result<(), rand_core_0_5::Error> {
        match self.0.try_fill_bytes(dest) {
            Ok(()) => Ok(()),
            Err(e) => {
                let code = e
                    .code()
                    .unwrap_or(rand_core_0_5::Error::CUSTOM_START.try_into().unwrap());
                Err(rand_core_0_5::Error::from(code))
            }
        }
    }
}

impl rand_core_0_5::CryptoRng for RandHelper {}

impl PubKey for RustCrypto {
    type Error = ();

    fn new_pk() -> Result<(PublicKey, PrivateKey), Self::Error> {
        let keys = ed25519_dalek::Keypair::generate(&mut RandHelper(OsRng));

        let public_key = PublicKey::from(keys.public.to_bytes());

        // Our private keys contain both the public and private components
        let mut private_key = PrivateKey::default();
        private_key[..32].copy_from_slice(&keys.secret.to_bytes());
        private_key[32..].copy_from_slice(&keys.public.to_bytes());

        Ok((public_key, private_key))
    }

    fn pk_sign(private_key: &PrivateKey, data: &[u8]) -> Result<Signature, Self::Error> {
        // Regenerate keypair from private key
        let keys = Keypair::from_bytes(private_key).map_err(|_| ())?;

        // Perform sign operation
        let sig = keys.sign(data);

        // Return signature
        Ok(Signature::from(sig.to_bytes()))
    }

    fn pk_verify(
        public_key: &PublicKey,
        signature: &Signature,
        data: &[u8],
    ) -> Result<bool, Self::Error> {
        // Coerce public key and signature types
        let public_key = ed25519_dalek::PublicKey::from_bytes(public_key).map_err(|_e| ())?;
        let signature = ed25519_dalek::Signature::from_bytes(signature).map_err(|_e| ())?;

        // Perform verification
        match public_key.verify_strict(data, &signature) {
            Ok(_) => Ok(true),
            Err(_) => Ok(false),
        }
    }

    // TODO: replace static KX with actual DH exchange at protocol level
    // then remove this... required for now for libsodium compat.
    fn kx(
        pub_key: &PublicKey,
        pri_key: &PrivateKey,
        remote: &PublicKey,
    ) -> Result<(SecretKey, SecretKey), Self::Error> {
        // Parse initial keys
        let our_pri_key = ed25519_dalek::Keypair::from_bytes(&pri_key).unwrap();
        let their_pub_key = ed25519_dalek::PublicKey::from_bytes(&remote).unwrap();

        // Convert into kx form
        let our_pri_key = pri_ed26619_to_x25519(&our_pri_key.secret).unwrap();
        let their_pub_key = pub_ed26619_to_x25519(&their_pub_key).unwrap();

        let our_keys = crypto_kx::KeyPair::from(our_pri_key);

        // TODO: why bother doing this twice when we could... not?
        let k1 = our_keys.session_keys_to(&their_pub_key);
        let k2 = our_keys.session_keys_from(&their_pub_key);

        Ok((
            SecretKey::from(k1.rx.as_ref()),
            SecretKey::from(k2.tx.as_ref()),
        ))
    }
}

impl SecKey for RustCrypto {
    type Error = ();

    fn new_sk() -> Result<SecretKey, Self::Error> {
        let key = ChaCha20Poly1305::generate_key(&mut OsRng);

        let secret_key = SecretKey::try_from(key.deref()).map_err(|_| ())?;

        Ok(secret_key)
    }

    // TODO: When we can run this move to symmetric AEAD w/ header in place of symmetric signing...

    fn sk_encrypt(
        secret_key: &SecretKey,
        assoc: Option<&[u8]>,
        message: &mut [u8],
    ) -> Result<SecretMeta, Self::Error> {
        use chacha20poly1305::*;

        let secret_key = Key::from_slice(secret_key);
        let cipher = XChaCha20Poly1305::new(secret_key);

        let mut nonce = XNonce::default();
        OsRng.fill_bytes(&mut nonce);

        let tag = cipher
            .encrypt_in_place_detached(&nonce, assoc.unwrap_or(&[]), message)
            .map_err(|e| {
                error!("Failed to encrypt in place: {:?}", e);
                ()
            })?;

        let d = tag.deref();
        debug!("Tag length: {}", d.len());

        // Setup nonce and tag for decode
        let mut meta = SecretMeta::default();
        meta[..16].copy_from_slice(d);
        meta[16..][..24].copy_from_slice(&nonce);

        Ok(meta)
    }

    fn sk_decrypt(
        secret_key: &SecretKey,
        meta: &[u8],
        assoc: Option<&[u8]>,
        message: &mut [u8],
    ) -> Result<(), Self::Error> {
        use chacha20poly1305::*;

        let secret_key = Key::from_slice(secret_key);
        let cipher = XChaCha20Poly1305::new(secret_key);

        let tag = Tag::from_slice(&meta[..16]);
        let nonce = XNonce::from_slice(&meta[16..][..24]);

        cipher
            .decrypt_in_place_detached(&nonce, assoc.unwrap_or(&[]), message, &tag)
            .map_err(|_| ())?;

        Ok(())
    }

    fn sk_reencrypt(
        secret_key: &SecretKey,
        meta: &[u8],
        assoc: Option<&[u8]>,
        message: &mut [u8],
    ) -> Result<SecretMeta, Self::Error> {
        use chacha20poly1305::*;

        let secret_key = Key::from_slice(secret_key);
        let cipher = XChaCha20Poly1305::new(secret_key);

        let nonce = XNonce::from_slice(&meta[16..]);

        let tag = cipher
            .encrypt_in_place_detached(&nonce, assoc.unwrap_or(&[]), message)
            .map_err(|e| {
                error!("Failed to encrypt in place: {:?}", e);
                ()
            })?;

        let d = tag.deref();
        debug!("Tag length: {}", d.len());

        // Setup nonce and tag for decode
        let mut meta = SecretMeta::default();
        meta[..d.len()].copy_from_slice(d);
        meta[d.len()..][..nonce.len()].copy_from_slice(&nonce);

        Ok(meta)
    }
}

impl Hash for RustCrypto {
    type Error = ();

    // https://docs.rs/blake2/latest/blake2/struct.Blake2bMac.html
    // https://libsodium.gitbook.io/doc/key_derivation#key-derivation-with-libsodium-less-than-1.0.1
    fn kdf(key: &[u8]) -> Result<CryptoHash, ()> {
        use blake2::digest::{consts::U32, FixedOutput};

        let salt = DSF_NS_KDF_IDX.to_le_bytes();

        let mut inst =
            blake2::Blake2bMac::<U32>::new_with_salt_and_personal(&key, &salt, &DSF_NS_KDF_CTX)
                .map_err(|_| ())?;

        let derived = inst.finalize_fixed();

        Ok(CryptoHash::from(derived.as_ref()))
    }
}

/// Creates a curve25519 key from an ed25519 public key.
/// See: https://github.com/dalek-cryptography/x25519-dalek/issues/53
fn pub_ed26619_to_x25519(pk: &ed25519_dalek::PublicKey) -> Result<crypto_kx::PublicKey, ()> {
    use curve25519_dalek::edwards::CompressedEdwardsY;

    // Verify it's a valid public key
    if let Err(_e) = ed25519_dalek::PublicKey::from_bytes(pk.as_bytes()) {
        return Err(());
    }

    // PublicKey is a CompressedEdwardsY in dalek. So we decompress it to get the
    // EdwardsPoint which can then be used convert to the Montgomery Form.
    let cey = CompressedEdwardsY::from_slice(pk.as_bytes());
    let pub_key = match cey.decompress() {
        Some(ep) => ep.to_montgomery(),
        None => return Err(()),
    };

    Ok(crypto_kx::PublicKey::from(pub_key.0))
}

/// Creates a curve25519 key from an ed25519 private key.
// See: https://github.com/dalek-cryptography/x25519-dalek/issues/53
fn pri_ed26619_to_x25519(sk: &ed25519_dalek::SecretKey) -> Result<crypto_kx::SecretKey, ()> {
    // Verify we have a valid secret key
    if let Err(_e) = ed25519_dalek::SecretKey::from_bytes(sk.as_bytes()) {
        return Err(());
    }

    // hash secret
    let hash = sha2::Sha512::digest(&sk.as_bytes()[..32]);

    let mut output = [0u8; 32];
    output.copy_from_slice(&hash[..32]);

    // clamp result
    let secret = x25519_dalek::StaticSecret::from(output);

    Ok(crypto_kx::SecretKey::from(secret.to_bytes()))
}

/// KDF derived key index, must not be reused for any other purpose
const DSF_NS_KDF_IDX: u64 = 1;
/// Blake2b KDF context, randomly generated
const DSF_NS_KDF_CTX: [u8; 8] = [208, 217, 2, 27, 15, 253, 70, 121];

#[cfg(test)]
mod test {
    extern crate test;
    use test::Bencher;

    use super::*;
    use crate::types::SECRET_KEY_TAG_LEN;

    #[test]
    fn test_pk_sign_verify() {
        let (public, private) =
            RustCrypto::new_pk().expect("Error generating public/private keypair");
        let mut data = vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9];

        let signature = RustCrypto::pk_sign(&private, &data).expect("Error generating signature");

        let valid =
            RustCrypto::pk_verify(&public, &signature, &data).expect("Error validating signature");
        assert_eq!(true, valid);

        data[3] = 100;
        let valid =
            RustCrypto::pk_verify(&public, &signature, &data).expect("Error validating signature");
        assert_eq!(false, valid);
    }

    #[test]
    fn test_sk_encrypt_decrypt() {
        let secret = RustCrypto::new_sk().expect("Error generating secret key");
        let data = vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9];
        let mut message = data.clone();

        let meta =
            RustCrypto::sk_encrypt(&secret, None, &mut message).expect("Error encrypting data");
        assert!(data != message);

        RustCrypto::sk_decrypt(&secret, &meta, None, &mut message).expect("Error decrypting data");
        assert_eq!(data, message);
    }

    #[bench]
    fn bench_pk_sign(b: &mut Bencher) {
        let (_public, private) =
            RustCrypto::new_pk().expect("Error generating public/private keypair");
        let data = [0xabu8; 256];

        b.iter(|| {
            let _sig = RustCrypto::pk_sign(&private, &data).expect("Error generating signature");
        })
    }

    #[bench]
    fn bench_pk_verify(b: &mut Bencher) {
        let (public, private) =
            RustCrypto::new_pk().expect("Error generating public/private keypair");
        let data = [0xabu8; 256];

        let signature = RustCrypto::pk_sign(&private, &data).expect("Error generating signature");

        b.iter(|| {
            let valid = RustCrypto::pk_verify(&public, &signature, &data)
                .expect("Error validating signature");
            assert_eq!(true, valid);
        })
    }

    #[bench]
    fn bench_pk_sk_convert(b: &mut Bencher) {
        let (pub_key_a, pri_key_a) =
            RustCrypto::new_pk().expect("Error generating public/private keypair");
        let (pub_key_b, _pri_key_b) =
            RustCrypto::new_pk().expect("Error generating public/private keypair");

        b.iter(|| {
            let _ = RustCrypto::kx(&pub_key_a, &pri_key_a, &pub_key_b)
                .expect("Error deriving secret keys");
        });
    }

    #[bench]
    fn bench_sk_encrypt(b: &mut Bencher) {
        let sec_key = RustCrypto::new_sk().expect("Error generating secret key");
        let data = [0xabu8; 256];

        b.iter(|| {
            let mut d = data.clone();
            let _sig = RustCrypto::sk_encrypt(&sec_key, None, &mut d).expect("Error signing data");
        });
    }

    #[bench]
    fn bench_sk_decrypt(b: &mut Bencher) {
        let sec_key = RustCrypto::new_sk().expect("Error generating secret key");
        let mut data = [0xabu8; 256];
        let sig = RustCrypto::sk_encrypt(&sec_key, None, &mut data).expect("Error signing data");

        b.iter(|| {
            let mut d = data.clone();
            let _v = RustCrypto::sk_decrypt(&sec_key, &sig, None, &mut d)
                .expect("Error validating data");
        });
    }
}
