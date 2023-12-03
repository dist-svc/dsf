use crate::crypto::{Crypto, PubKey as _};
use crate::types::{Id, PrivateKey, PublicKey, SecretKey};

/// Key object stored and returned by a KeySource
#[derive(Clone, PartialEq, Debug)]
#[cfg_attr(feature = "clap", derive(clap::Parser))]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]

pub struct Keys {
    /// Service public key
    #[cfg_attr(feature = "clap", clap(long))]
    pub pub_key: Option<PublicKey>,

    /// Service private key
    #[cfg_attr(feature = "clap", clap(long))]
    pub pri_key: Option<PrivateKey>,

    /// Secret key for data encryption
    #[cfg_attr(feature = "clap", clap(long))]
    pub sec_key: Option<SecretKey>,

    /// Symmetric keys for p2p message signing / verification
    #[cfg_attr(feature = "clap", clap(skip))]
    pub sym_keys: Option<(SecretKey, SecretKey)>,
}

impl Default for Keys {
    fn default() -> Self {
        Self {
            pub_key: None,
            pri_key: None,
            sec_key: None,
            sym_keys: None,
        }
    }
}

impl Keys {
    pub fn new(pub_key: PublicKey) -> Self {
        Self {
            pub_key: Some(pub_key),
            pri_key: None,
            sec_key: None,
            sym_keys: None,
        }
    }

    pub fn with_pri_key(mut self, pri_key: PrivateKey) -> Self {
        self.pri_key = Some(pri_key);
        self
    }

    pub fn with_sec_key(mut self, sec_key: SecretKey) -> Self {
        self.sec_key = Some(sec_key);
        self
    }

    pub fn pub_key(&mut self) -> Option<PublicKey> {
        match (&self.pub_key, &self.pri_key) {
            // Return pub key directly if exists
            (Some(pub_key), _) => Some(pub_key.clone()),
            // Compute pub_key via pri_key if exists
            (_, Some(pri_key)) => {
                self.pub_key = Some(Crypto::get_public(pri_key));
                self.pub_key.clone()
            }
            // No pub_key available
            _ => None,
        }
    }

    /// Derive encryption keys for the specified peer
    pub fn derive_peer(&self, peer_pub_key: PublicKey) -> Result<Keys, ()> {
        // Derivation requires our public and private keys
        let (pub_key, pri_key) = match (&self.pub_key, &self.pri_key) {
            (Some(pub_key), Some(pri_key)) => (pub_key, pri_key),
            _ => return Err(()),
        };

        // Generate symmetric keys
        let sym_keys = Crypto::kx(pub_key, pri_key, &peer_pub_key)?;

        // Return generated key object for peer
        Ok(Keys {
            pub_key: Some(peer_pub_key),
            // TODO: this is -our- private key, shouldn't really be here / returned / available outside the object
            pri_key: self.pri_key.clone(),
            sec_key: None,
            sym_keys: Some(sym_keys),
        })
    }
}

pub trait KeySource: Sized {
    /// Find keys for a given service / peer ID
    fn keys(&self, id: &Id) -> Option<Keys>;

    /// Fetch public key
    fn pub_key(&self, id: &Id) -> Option<PublicKey> {
        self.keys(id).map(|k| k.pub_key).flatten()
    }

    /// Fetch private key
    fn pri_key(&self, id: &Id) -> Option<PrivateKey> {
        self.keys(id).map(|k| k.pri_key).flatten()
    }

    /// Fetch secret key
    fn sec_key(&self, id: &Id) -> Option<SecretKey> {
        self.keys(id).map(|k| k.sec_key).flatten()
    }

    /// Update keys for the specified ID (optional)
    fn update<F: FnMut(&mut Keys)>(&self, _id: &Id, _f: F) -> bool {
        false
    }

    /// Build cached keystore wrapper
    fn cached(&self, existing: Option<(Id, Keys)>) -> CachedKeySource<Self> {
        CachedKeySource {
            key_source: self,
            cached: existing,
        }
    }

    /// Build null keystore implementation
    fn null() -> NullKeySource {
        NullKeySource
    }
}

impl KeySource for Keys {
    fn keys(&self, _id: &Id) -> Option<Keys> {
        Some(self.clone())
    }
}

impl KeySource for Option<Keys> {
    fn keys(&self, _id: &Id) -> Option<Keys> {
        self.clone()
    }
}

impl KeySource for Option<SecretKey> {
    fn keys(&self, _id: &Id) -> Option<Keys> {
        self.as_ref().map(|v| Keys {
            sec_key: Some(v.clone()),
            ..Default::default()
        })
    }

    fn sec_key(&self, _id: &Id) -> Option<SecretKey> {
        self.as_ref().cloned()
    }
}

/// Wrapper to cache a KeySource with a value for immediate lookup
pub struct CachedKeySource<'a, K: KeySource + Sized> {
    key_source: &'a K,
    cached: Option<(Id, Keys)>,
}

impl<'a, K: KeySource + Sized> KeySource for CachedKeySource<'a, K> {
    fn keys(&self, id: &Id) -> Option<Keys> {
        if let Some(k) = self.key_source.keys(id) {
            return Some(k);
        }

        match &self.cached {
            Some(e) if &e.0 == id => Some(e.1.clone()),
            _ => None,
        }
    }
}

/// Null key source implementation contains no keys
pub struct NullKeySource;

impl KeySource for NullKeySource {
    fn keys(&self, _id: &Id) -> Option<Keys> {
        None
    }
}

#[cfg(feature = "std")]
impl KeySource for std::collections::HashMap<Id, Keys> {
    fn keys(&self, id: &Id) -> Option<Keys> {
        self.get(id).cloned()
    }
}
