use std::collections::HashMap;

use dsf_core::prelude::*;

use super::*;
use crate::log::Debug;

pub struct SledStore<Addr: Clone + Debug> {
    db: sled::Db,
    peers: HashMap<Id, Peer<Addr>>,
    services: HashMap<Id, Service>,
    _addr: PhantomData<Addr>,
}

impl<Addr: Clone + Debug> SledStore<Addr> {
    /// Create a new sled-backed store
    pub fn new(path: &str) -> Result<Self, sled::Error> {
        let db = sled::Config::default()
            .path(path)
            .cache_capacity(100_000_000)
            .flush_every_ms(Some(1_000))
            .open()?;

        let peers = HashMap::new();
        let services = HashMap::new();

        Ok(Self {
            db,
            peers,
            services,
            _addr: PhantomData,
        })
    }
}

const SLED_IDENT_KEY: &[u8] = b"ident";
const SLED_PAGE_KEY: &[u8] = b"page";
const SLED_LAST_KEY: &[u8] = b"last";

impl<Addr: Clone + Debug + 'static> Store for SledStore<Addr> {
    const FEATURES: StoreFlags = StoreFlags::ALL;

    type Address = Addr;

    type Error = sled::Error;

    type Iter<'a> = std::collections::hash_map::Iter<'a, Id, Peer<Addr>>;

    fn get_ident(&self) -> Result<Option<Keys>, Self::Error> {
        let ident = self.db.open_tree(SLED_IDENT_KEY)?;

        let mut keys = Keys::default();

        if let Some(pri_key) = ident.get("pri_key")? {
            let pri_key = PrivateKey::try_from(pri_key.as_ref()).unwrap();

            keys.pub_key = Some(Crypto::get_public(&pri_key));
            keys.pri_key = Some(pri_key);
        }

        if let Some(sec_key) = ident.get("sec_key")? {
            keys.sec_key = SecretKey::try_from(sec_key.as_ref()).ok();
        }

        match keys.pub_key.is_some() {
            true => Ok(Some(keys)),
            false => Ok(None),
        }
    }

    fn set_ident(&mut self, keys: &Keys) -> Result<(), Self::Error> {
        let ident = self.db.open_tree(SLED_IDENT_KEY)?;

        if let Some(pri_key) = keys.pri_key.as_deref() {
            ident.insert("pri_key", pri_key)?;
        }

        if let Some(sec_key) = keys.sec_key.as_deref() {
            ident.insert("sec_key", sec_key)?;
        }

        Ok(())
    }

    fn get_last(&self) -> Result<Option<ObjectInfo>, Self::Error> {
        match self.db.get(SLED_LAST_KEY)? {
            Some(k) => {
                let _d = k.as_ref();

                Ok(Some(ObjectInfo {
                    page_index: LittleEndian::read_u32(&k[0..]),
                    block_index: LittleEndian::read_u32(&k[4..]),
                    sig: Signature::try_from(&k[8..][..SIGNATURE_LEN]).unwrap(),
                }))
            }
            None => Ok(None),
        }
    }

    fn set_last(&mut self, info: &ObjectInfo) -> Result<(), Self::Error> {
        let mut d = [0u8; 4 + 4 + SIGNATURE_LEN];

        LittleEndian::write_u32(&mut d[0..], info.page_index);
        LittleEndian::write_u32(&mut d[4..], info.block_index);
        d[8..].copy_from_slice(&info.sig);

        self.db.insert(SLED_LAST_KEY, d.as_slice())?;

        Ok(())
    }

    fn get_peer(&self, id: &Id) -> Result<Option<Peer<Addr>>, Self::Error> {
        let p = self.peers.get(id);
        Ok(p.map(|p| p.clone()))
    }

    fn peers<'a>(&'a self) -> Self::Iter<'a> {
        self.peers.iter()
    }

    fn update_peer<R: Debug, F: Fn(&mut Peer<Addr>) -> R>(
        &mut self,
        id: &Id,
        f: F,
    ) -> Result<R, Self::Error> {
        let p = self.peers.entry(id.clone()).or_default();
        Ok(f(p))
    }

    fn store_page<T: ImmutableData>(
        &mut self,
        sig: &Signature,
        p: &Container<T>,
    ) -> Result<(), Self::Error> {
        let pages = self.db.open_tree(SLED_PAGE_KEY)?;

        pages.insert(sig, p.raw())?;

        Ok(())
    }

    fn fetch_page<T: MutableData>(
        &mut self,
        sig: &Signature,
        mut buff: T,
    ) -> Result<Option<Container<T>>, Self::Error> {
        let pages = self.db.open_tree(SLED_PAGE_KEY)?;

        match pages.get(sig)? {
            Some(p) => {
                let b = buff.as_mut();
                let p = p.as_ref();

                b[..p.len()].copy_from_slice(p);

                let (c, _n) = Container::from(buff);
                Ok(Some(c))
            }
            None => Ok(None),
        }
    }

    // Fetch service information
    fn get_service(&self, id: &Id) -> Result<Option<Service>, Self::Error> {
        let s = self.services.get(id);
        Ok(s.map(|s| s.clone()))
    }

    // Update a specified service
    fn update_service<R: Debug, F: Fn(&mut Service) -> R>(
        &mut self,
        id: &Id,
        f: F,
    ) -> Result<R, Self::Error> {
        let s = self.services.entry(id.clone()).or_default();
        Ok(f(s))
    }
}

impl<Addr: Clone + Debug> KeySource for SledStore<Addr> {
    fn keys(&self, id: &Id) -> Option<Keys> {
        self.peers.get(id).map(|p| p.keys.clone())
    }
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;

    use dsf_core::{
        crypto::{Crypto, Hash, PubKey, SecKey},
        prelude::*,
    };
    use tempfile::tempdir;

    use super::*;

    #[test]
    fn sled_store_ident() {
        let f = tempdir().unwrap();

        let mut store = SledStore::<SocketAddr>::new(f.path().to_str().unwrap()).unwrap();

        let (pub_key, pri_key) = Crypto::new_pk().unwrap();
        let sec_key = Crypto::new_sk().unwrap();
        let keys = Keys {
            pub_key: Some(pub_key),
            pri_key: Some(pri_key),
            sec_key: Some(sec_key),
            sym_keys: None,
        };

        store.set_ident(&keys).unwrap();

        let keys2 = store.get_ident().unwrap().unwrap();

        assert_eq!(keys, keys2);
    }

    #[test]
    fn sled_store_peer() {
        let f = tempdir().unwrap();

        let mut store = SledStore::<SocketAddr>::new(f.path().to_str().unwrap()).unwrap();

        let (pub_key, pri_key) = Crypto::new_pk().unwrap();
        let sec_key = Crypto::new_sk().unwrap();
        let id: Id = Crypto::hash(&pub_key).unwrap().into();
        let keys = Keys {
            pub_key: Some(pub_key),
            pri_key: Some(pri_key),
            sec_key: Some(sec_key),
            sym_keys: None,
        };

        store
            .update_peer(&id, |p| {
                p.keys = keys.clone();
                p.addr = Some(SocketAddr::from(([127, 0, 0, 1], 1234)));
            })
            .unwrap();

        let peer = store.get_peer(&id).unwrap().unwrap();

        assert_eq!(peer.keys, keys);
        assert_eq!(peer.addr, Some(SocketAddr::from(([127, 0, 0, 1], 1234))));
        assert_eq!(peer.subscriber, false);
        assert_eq!(peer.subscribed, SubscribeState::None);

        store
            .update_peer(&id, |p| {
                p.subscriber = true;
                p.subscribed = SubscribeState::Subscribed;
            })
            .unwrap();

        let peer = store.get_peer(&id).unwrap().unwrap();

        assert_eq!(peer.subscriber, true);
        assert_eq!(peer.subscribed, SubscribeState::Subscribed);
    }

    #[test]
    fn sled_store_history() {
        let f = tempdir().unwrap();

        let mut store = SledStore::<SocketAddr>::new(f.path().to_str().unwrap()).unwrap();

        let last = store.get_last().unwrap();
        assert_eq!(last, None);

        let (_pub_key, pri_key) = Crypto::new_pk().unwrap();
        let sig = Crypto::pk_sign(&pri_key, &[0xaa, 0xbb, 0xcc]).unwrap();

        let info = ObjectInfo {
            page_index: 1234,
            block_index: 5678,
            sig,
        };
        store.set_last(&info).unwrap();

        let last = store.get_last().unwrap().unwrap();

        assert_eq!(last, info);
    }

    #[test]
    fn sled_store_page() {
        let f = tempdir().unwrap();

        let mut store = SledStore::<SocketAddr>::new(f.path().to_str().unwrap()).unwrap();

        let mut s = ServiceBuilder::<Vec<u8>>::generic()
            .body(vec![0xaa, 0xbb, 0xcc])
            .build()
            .unwrap();

        let mut buff = vec![0u8; 1024];
        let (_n, p) = s.publish_primary(Default::default(), &mut buff).unwrap();

        store.store_page(&p.signature(), &p).unwrap();

        let mut buff = vec![0u8; 1024];
        let p1 = store
            .fetch_page(&p.signature(), &mut buff)
            .unwrap()
            .unwrap();

        assert_eq!(p1.header(), p.header());
        assert_eq!(p1.signature(), p.signature());
        assert_eq!(p1.body_raw(), p.body_raw());
        assert_eq!(p1.public_options_raw(), p.public_options_raw());
        assert_eq!(p1.private_options_raw(), p.private_options_raw());
    }
}
