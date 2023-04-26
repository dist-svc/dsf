use std::collections::{hash_map::Iter, HashMap};

use dsf_core::prelude::*;

use super::*;

pub struct MemoryStore<Addr: Clone + Debug = std::net::SocketAddr> {
    pub(crate) our_keys: Option<Keys>,
    pub(crate) last_sig: Option<ObjectInfo>,
    pub(crate) peers: HashMap<Id, Peer<Addr>>,
    pub(crate) pages: HashMap<Signature, Container>,
}

impl<Addr: Clone + Debug> MemoryStore<Addr> {
    pub fn new() -> Self {
        Self {
            our_keys: None,
            last_sig: None,
            peers: HashMap::new(),
            pages: HashMap::new(),
        }
    }
}

impl<Addr: Clone + Debug + 'static> Store for MemoryStore<Addr> {
    const FEATURES: StoreFlags = StoreFlags::ALL;

    type Address = Addr;
    type Error = core::convert::Infallible;
    type Iter<'a> = Iter<'a, Id, Peer<Addr>>;

    fn get_ident(&self) -> Result<Option<Keys>, Self::Error> {
        Ok(self.our_keys.clone())
    }

    fn set_ident(&mut self, keys: &Keys) -> Result<(), Self::Error> {
        self.our_keys = Some(keys.clone());
        Ok(())
    }

    /// Fetch previous object information
    fn get_last(&self) -> Result<Option<ObjectInfo>, Self::Error> {
        Ok(self.last_sig.clone())
    }

    /// Update previous object information
    fn set_last(&mut self, info: &ObjectInfo) -> Result<(), Self::Error> {
        self.last_sig = Some(info.clone());
        Ok(())
    }

    fn get_peer(&self, id: &Id) -> Result<Option<Peer<Self::Address>>, Self::Error> {
        let p = self.peers.get(id);
        Ok(p.map(|p| p.clone()))
    }

    fn peers<'a>(&'a self) -> Self::Iter<'a> {
        self.peers.iter()
    }

    fn update_peer<R: Debug, F: Fn(&mut Peer<Self::Address>) -> R>(
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
        self.pages.insert(sig.clone(), p.to_owned());
        Ok(())
    }

    fn fetch_page<T: MutableData>(
        &mut self,
        sig: &Signature,
        mut buff: T,
    ) -> Result<Option<Container<T>>, Self::Error> {
        match self.pages.get(sig) {
            Some(p) => {
                let b = buff.as_mut();
                b[..p.len()].copy_from_slice(p.raw());

                let (c, _n) = Container::from(buff);
                Ok(Some(c))
            }
            None => Ok(None),
        }
    }
}

impl<'a, Addr: Clone + Debug + 'static> IntoIterator for &'a MemoryStore<Addr> {
    type Item = (&'a Id, &'a Peer<Addr>);

    type IntoIter = Iter<'a, Id, Peer<Addr>>;

    fn into_iter(self) -> Self::IntoIter {
        self.peers.iter()
    }
}

impl<Addr: Clone + Debug> KeySource for MemoryStore<Addr> {
    fn keys(&self, id: &Id) -> Option<Keys> {
        self.peers.get(id).map(|p| p.keys.clone())
    }
}
