//! Alloc / in-memory data store
//!
//! Real-world service must persist key information and published objects,
//! this should only be used for testing.

use std::collections::{hash_map::Iter, HashMap};

use dsf_core::{prelude::*, types::BaseKind};

use super::*;

pub struct MemoryStore<Addr: Clone + Debug = std::net::SocketAddr> {
    pub(crate) our_keys: Option<Keys>,
    pub(crate) last_sig: Option<Signature>,
    last_page: u32,
    last_block: u32,
    pub(crate) peers: HashMap<Id, Peer<Addr>>,
    pub(crate) pages: HashMap<Signature, Container>,
    pub(crate) page_idx: HashMap<u32, Signature>,
    pub(crate) services: HashMap<Id, Service>,
}

impl<Addr: Clone + Debug> MemoryStore<Addr> {
    pub fn new() -> Self {
        Self {
            our_keys: None,
            last_sig: None,
            last_page: 0,
            last_block: 0,
            peers: HashMap::new(),
            pages: HashMap::new(),
            page_idx: HashMap::new(),
            services: HashMap::new(),
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
        let i = match self.last_sig.as_ref() {
            Some(s) => Some(ObjectInfo {
                page_index: self.last_page,
                block_index: self.last_block,
                sig: s.clone(),
            }),
            _ => None,
        };

        Ok(i)
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

    fn store_page<T: ImmutableData>(&mut self, p: &Container<T>) -> Result<(), Self::Error> {
        let header = p.header();

        // Store page
        self.pages.insert(p.signature(), p.to_owned());
        self.page_idx.insert(p.header().index(), p.signature());

        // Update last page information
        self.last_sig = Some(p.signature());
        match header.kind().base_kind {
            BaseKind::Page => self.last_page = header.index(),
            BaseKind::Block => self.last_block = header.index(),
            _ => (),
        }

        Ok(())
    }

    fn fetch_page<T: MutableData>(
        &mut self,
        f: ObjectFilter,
        mut buff: T,
    ) -> Result<Option<Container<T>>, Self::Error> {
        let p = match (f, &self.last_sig) {
            (ObjectFilter::Latest, Some(s)) => self.pages.get(s),
            (ObjectFilter::Sig(ref s), _) => self.pages.get(s),
            (ObjectFilter::Index(n), _) => {
                self.page_idx.get(&n).map(|s| self.pages.get(s)).flatten()
            }
            _ => return Ok(None),
        };

        match p {
            Some(p) => {
                let b = buff.as_mut();
                b[..p.len()].copy_from_slice(p.raw());

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
