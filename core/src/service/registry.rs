use core::ops::Add;

use crate::base::PageBody;
use crate::options::Options;

use crate::crypto::{Crypto, Hash as _};
use crate::error::Error;
use crate::prelude::Header;
use crate::types::{DateTime, Flags, Id, Kind, MutableData, PageKind, Queryable, Signature};
use crate::wire::{Builder, Container};

use super::Service;

pub trait Registry {
    /// Generate ID for registry lookup
    fn resolve(&self, q: impl Queryable) -> Result<Id, Error>;

    /// Generates a tertiary page for the provided service ID and options
    fn publish_tertiary<Q: Queryable, T: MutableData>(
        &mut self,
        link: TertiaryLink,
        opts: TertiaryOptions,
        q: Q,
        buff: T,
    ) -> Result<(usize, Container<T>), Error>;

    /// Generates a tertiary page for the provided service ID and options
    fn publish_tertiary_buff<const N: usize, Q: Queryable>(
        &mut self,
        link: TertiaryLink,
        opts: TertiaryOptions,
        q: Q,
    ) -> Result<(usize, Container<[u8; N]>), Error> {
        let buff = [0u8; N];
        self.publish_tertiary(link, opts, q, buff)
    }
}

#[derive(Clone, PartialEq, Debug)]
pub enum TertiaryLink {
    Service(Id),
    Block(Signature),
}

impl From<Id> for TertiaryLink {
    fn from(id: Id) -> Self {
        Self::Service(id)
    }
}

impl From<Signature> for TertiaryLink {
    fn from(sig: Signature) -> Self {
        Self::Block(sig)
    }
}

/// Tertiary page configuration options
#[derive(Clone, PartialEq, Debug)]
pub struct TertiaryOptions {
    pub index: u16,
    pub issued: DateTime,
    pub expiry: DateTime,
}

#[cfg(feature = "std")]
impl Default for TertiaryOptions {
    /// Create a tertiary page with default 1 day expiry
    fn default() -> Self {
        let now = std::time::SystemTime::now();

        Self {
            index: 0,
            issued: now.into(),
            expiry: now
                .add(core::time::Duration::from_secs(24 * 60 * 60))
                .into(),
        }
    }
}

impl<B: PageBody> Registry for Service<B> {
    /// Resolve an ID for a given hash
    fn resolve(&self, q: impl Queryable) -> Result<Id, Error> {
        // Generate ID for page lookup using this registry
        match Crypto::hash_tid(self.id(), &self.keys(), q) {
            Ok(tid) => Ok(Id::from(tid.as_bytes())),
            Err(_) => Err(Error::CryptoError),
        }
    }

    fn publish_tertiary<Q: Queryable, T: MutableData>(
        &mut self,
        link: TertiaryLink,
        opts: TertiaryOptions,
        q: Q,
        buff: T,
    ) -> Result<(usize, Container<T>), Error> {
        // Generate TID
        let tid = match Crypto::hash_tid(self.id(), &self.keys(), q) {
            Ok(tid) => Id::from(tid.as_bytes()),
            Err(_) => return Err(Error::CryptoError),
        };

        // Setup flags
        let mut flags = Flags::TERTIARY;
        if self.encrypted {
            flags |= Flags::ENCRYPTED;
        }

        let (kind, body): (_, &[u8]) = match &link {
            TertiaryLink::Service(id) => (PageKind::ServiceLink, &id),
            TertiaryLink::Block(sig) => (PageKind::BlockLink, &sig),
        };

        // Setup header
        let header = Header {
            kind: Kind::page(kind as u16),
            index: 0,
            flags,
            ..Default::default()
        };

        // TODO: should service link be in private options..?
        let b = Builder::new(buff)
            .header(&header)
            .id(&tid)
            .body(body)?
            .private_options(&[])?;

        // Apply internal encryption if enabled
        let b = self.encrypt(b)?;

        let b = b.public_options(&[
            Options::peer_id(self.id()),
            Options::issued(opts.issued),
            Options::expiry(opts.expiry),
        ])?;

        // Sign generated object
        let c = self.sign(b)?;

        Ok((c.len(), c))
    }
}

#[cfg(test)]
mod test {
    use crate::base::Empty;
    use crate::options::{Filters, Options};
    use crate::{prelude::*, service::Publisher};

    use super::*;

    fn registry_publish(mut r: Service) {
        // Build target service
        let opt_name = "something";
        let mut c = ServiceBuilder::<Empty>::generic()
            .public_options(vec![Options::name(opt_name)])
            .build()
            .unwrap();

        let (_n, _c) = c.publish_primary_buff(Default::default()).unwrap();

        // Generate page for name entry
        let (_n, p1) = Registry::publish_tertiary_buff::<512, _>(
            &mut r,
            c.id().into(),
            TertiaryOptions::default(),
            &Options::name(opt_name),
        )
        .unwrap();

        println!("Tertiary page: {:02x?}", p1);

        // Lookup TID for name
        let tid_name = Registry::resolve(&r, &Options::name(opt_name)).unwrap();
        assert_eq!(&p1.id(), &tid_name);

        // Check link to registry
        let opts: Vec<_> = p1.public_options_iter().collect();
        let pid = Filters::peer_id(&opts.iter()).unwrap();
        assert_eq!(pid, r.id());

        // Check link to service
        #[cfg(todo)]
        {
            let pi = match p1.info() {
                PageInfo::Tertiary(t) => Some(t),
                _ => None,
            }
            .unwrap();
            assert_eq!(pi.target_id, c.id());
        }
    }

    #[test]
    fn registry_publish_public() {
        // Build registry service
        let r = ServiceBuilder::ns("test.com").build().unwrap();

        // Test publishing
        registry_publish(r);
    }

    #[test]
    fn registry_publish_private() {
        // Build registry service
        let r = ServiceBuilder::ns("test.com").encrypt().build().unwrap();

        // Test publishing
        registry_publish(r);
    }
}
