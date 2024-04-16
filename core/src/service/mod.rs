//! This module provides a DSF Service implementation.
//!
//! `Publisher`, `Subscriber`, and `Net` traits provide functionality for publishing services,
//! subscribing to services, and sending messages respectively.

use crate::base::{MaybeEncrypted, PageBody};
use crate::crypto::{Crypto, Hash as _, PubKey as _, SecKey};
use crate::error::Error;
use crate::options::Options;
use crate::types::*;

#[cfg(feature = "alloc")]
use alloc::vec::Vec;

mod kinds;
pub use kinds::*;
// Service extensions
mod publisher;
pub use publisher::{DataOptions, PrimaryOptions, Publisher, SecondaryData, SecondaryOptions};

mod subscriber;
pub use subscriber::Subscriber;

mod registry;
pub use registry::{Registry, TertiaryData, TertiaryLink, TertiaryOptions};

pub(crate) mod net;
pub use net::Net;

mod builder;
pub use builder::ServiceBuilder;

use crate::keys::Keys;

/// Generic Service Type.
/// This provides the basis for all services in DSF.
///
/// Services should be constructed using the ServiceBuilder type
#[derive(PartialEq, Debug, Clone)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
#[cfg_attr(feature = "diesel", derive(diesel::Queryable))]
pub struct Service<B: PageBody = Vec<u8>> {
    id: Id,

    application_id: u16,
    kind: PageKind,

    /// Current service version (index of last primary page)
    version: u32,

    /// Index / counter of linked published objects (primary pages + data blocks)
    index: u32,

    body: MaybeEncrypted<B>,

    public_options: Vec<Options>,
    private_options: MaybeEncrypted<Vec<Options>>,

    public_key: PublicKey,
    private_key: Option<PrivateKey>,
    secret_key: Option<SecretKey>,

    encrypted: bool,

    last_sig: Option<Signature>,
}

impl<B: PageBody> Default for Service<B> {
    /// Create a default / blank Service for further initialisation.
    fn default() -> Self {
        // Generate service keys
        let (public_key, private_key) = Crypto::new_pk().unwrap();
        let secret_key = Crypto::new_sk().unwrap();

        // Generate service ID from public key
        let id = Crypto::hash(&public_key).unwrap();

        // Create service object
        Service {
            id: Id::from(id.as_bytes()),
            application_id: 0,
            kind: PageKind::Generic,
            version: 0,
            index: 0,
            body: MaybeEncrypted::None,
            public_options: vec![],
            private_options: MaybeEncrypted::None,
            public_key,
            private_key: Some(private_key),
            encrypted: false,
            secret_key: Some(secret_key),
            last_sig: None,
        }
    }
}

pub struct ServiceOpts<B: PageBody> {
    /// Service ID
    pub id: Option<Id>,

    /// Service kind
    pub kind: PageKind,

    /// Application ID
    pub application_id: u16,

    /// Service private key
    pub private_key: Option<PrivateKey>,

    /// Service secret key
    pub secret_key: Option<SecretKey>,

    /// Page body
    pub body: Option<B>,

    /// Public options
    pub public_options: Vec<Options>,

    /// Private options
    pub private_options: Vec<Options>,

    /// Service publishing history
    pub history: History,

    /// Encrypted flag
    pub encrypted: bool,
}

impl<B: PageBody> Default for ServiceOpts<B> {
    fn default() -> Self {
        Self {
            id: None,
            kind: PageKind::Generic,
            application_id: 0,
            private_key: None,
            secret_key: None,
            body: None,
            public_options: vec![],
            private_options: vec![],
            history: Default::default(),
            encrypted: false,
        }
    }
}

impl<B: PageBody> ServiceOpts<B> {
    /// Setup a peer service.
    /// This is equivalent to .kind(Kind::Peer)
    pub fn peer() -> Self {
        Self {
            kind: PageKind::Peer,
            ..Default::default()
        }
    }

    /// Setup a generic service.
    /// This is equivalent to .kind(Kind::Generic)
    pub fn generic() -> Self {
        Self {
            kind: PageKind::Generic,
            ..Default::default()
        }
    }

    /// Setup a name service with the specified prefix
    pub fn ns(prefix: &str) -> Self {
        let mut s = Self {
            // TODO: fix up kinds, seems to be conflation between DB and DSF pages?
            // maybe this should be flags for pri | sec | ter + enum for variant?
            kind: PageKind::Name,
            ..Default::default()
        };

        s.private_options = vec![Options::name(prefix)];

        s
    }
}

impl<B: PageBody> Service<B> {
    /// Create a new service using the provided options.
    ///
    /// This will reuse credentials if provided, or generate a new identity and keys if not.
    ///
    /// To _load_ an existing service from a service page,
    /// see [Service::load]
    pub fn new(opts: ServiceOpts<B>) -> Result<Self, Error> {
        // Setup service keys
        let (public_key, private_key) = match opts.private_key {
            // If we have a private key, regenerate the matching public key
            Some(private_key) => {
                let public_key = Crypto::get_public(&private_key);
                (public_key, private_key)
            }
            // Otherwise, create a new keypair
            None => Crypto::new_pk().unwrap(),
        };

        // Setup service ID
        let id: Id = Crypto::hash(&public_key).unwrap().into();
        match &opts.id {
            Some(v) if v != &id => return Err(Error::KeyIdMismatch),
            _ => (),
        };

        // Setup secret / encryption keys
        let secret_key = match opts.secret_key {
            Some(sk) => sk,
            None => Crypto::new_sk().unwrap(),
        };

        // Setup service body
        let body = match opts.body {
            Some(b) => MaybeEncrypted::Cleartext(b),
            None => MaybeEncrypted::None,
        };

        // Return service object
        Ok(Service {
            id: opts.id.unwrap(),
            application_id: opts.application_id,
            kind: opts.kind,
            version: opts.history.last_page,
            index: opts.history.last_data.max(opts.history.last_page),
            body,
            public_options: opts.public_options,
            private_options: MaybeEncrypted::Cleartext(opts.private_options),
            public_key,
            private_key: Some(private_key),
            encrypted: opts.encrypted,
            secret_key: Some(secret_key),
            last_sig: None,
        })
    }

    pub fn id(&self) -> Id {
        self.id.clone()
    }

    /// Update a service, allows in-place editing of descriptors and options
    pub fn update<U>(&mut self, update_fn: U) -> Result<(), Error>
    where
        U: Fn(&mut MaybeEncrypted<B>, &mut Vec<Options>, &mut MaybeEncrypted<Vec<Options>>),
    {
        if self.private_key().is_none() {
            return Err(Error::NoPrivateKey);
        }

        update_fn(
            &mut self.body,
            &mut self.public_options,
            &mut self.private_options,
        );

        Ok(())
    }

    pub fn is_origin(&self) -> bool {
        match (&self.private_key, &self.encrypted, &self.secret_key) {
            (Some(_), false, _) => true,
            (Some(_), true, Some(_)) => true,
            _ => false,
        }
    }

    pub fn app_id(&self) -> u16 {
        self.application_id
    }

    pub fn version(&self) -> u32 {
        self.version
    }

    pub fn index(&self) -> u32 {
        self.index
    }

    pub fn kind(&self) -> PageKind {
        self.kind
    }

    pub fn body(&self) -> &MaybeEncrypted<B> {
        &self.body
    }

    pub fn public_options(&self) -> &[Options] {
        &self.public_options
    }

    pub fn encrypted(&self) -> bool {
        self.encrypted
    }

    pub fn public_key(&self) -> PublicKey {
        self.public_key.clone()
    }

    pub fn private_key(&self) -> Option<PrivateKey> {
        self.private_key.clone()
    }

    pub fn secret_key(&self) -> Option<SecretKey> {
        self.secret_key.clone()
    }

    pub fn set_private_key(&mut self, key: Option<PrivateKey>) {
        self.private_key = key;
    }

    pub fn set_secret_key(&mut self, key: Option<SecretKey>) {
        self.secret_key = key;
    }

    pub fn set_last(&mut self, index: u32, sig: Signature) {
        self.index = index;
        self.last_sig = Some(sig);
    }

    pub fn keys(&self) -> Keys {
        Keys {
            pub_key: Some(self.public_key.clone()),
            pri_key: self.private_key.as_ref().cloned(),
            sec_key: self.secret_key.as_ref().cloned(),
            sym_keys: None,
        }
    }
}

#[cfg(test)]
mod test {

    use std::net::{Ipv4Addr, SocketAddrV4};
    use std::vec::Vec;

    use pretty_assertions::assert_eq;

    use crate::service::publisher::{DataOptions, Publisher, SecondaryOptions};
    use crate::service::subscriber::Subscriber;
    use crate::wire::Container;

    use super::*;

    #[test]
    fn test_service() {
        //let _ = simplelog::SimpleLogger::init(simplelog::LevelFilter::Debug, simplelog::Config::default());

        let socket = SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), 8080);

        println!("Creating new service");
        let service_builder = ServiceBuilder::<Vec<u8>>::default()
            .kind(PageKind::Generic)
            .public_options(vec![Options::name("Test Service")])
            .private_options(vec![Options::address_v4(socket)])
            .encrypt();
        let mut service = service_builder.build().unwrap();
        let keys = service.keys();

        println!("Generating and encoding service page");
        let (n, page1) = service
            .publish_primary_buff(Default::default())
            .expect("Error creating page");

        debug!("page1: {:?}", page1);

        let pp1 = Container::parse(page1.raw().to_vec(), &keys).unwrap();

        debug!("pp1: {:?}", pp1);

        assert_eq!(service.version, 0, "initial service version");
        assert_eq!(service.index, 1, "service object index");

        println!("Encoded service to {} bytes", n);

        println!("Decoding service page");
        let s = service.clone();

        let base2 =
            Container::parse(page1.raw().to_vec(), &keys).expect("Error parsing service page");
        assert_eq!(n, base2.len());
        assert_eq!(pp1, base2);

        println!("Generating service replica");
        let mut replica =
            Service::<Vec<u8>>::load(&base2).expect("Error generating service replica");
        assert_eq!(replica.version, 0);

        println!("Updating service");
        service
            .update(|_body, public_options, _private_options| {
                public_options.push(Options::kind("Test Kind"));
            })
            .expect("Error updating service");

        println!("Generating updated page");
        let (_n, page3) = service
            .publish_primary_buff(Default::default())
            .expect("Error publishing primary page");

        assert_eq!(service.version, 1, "publish primary page updates version");
        assert_eq!(service.index, 2, "publish primary page updates index");

        let pp3 = Container::parse(page3.raw().to_vec(), &keys).unwrap();

        println!("Applying updated page to replica");
        replica
            .apply_primary(&pp3)
            .expect("Error updating service replica");
        assert_eq!(replica.version, 1);

        println!("Generating a secondary page");
        let secondary_options = SecondaryOptions::default();
        let (_n, secondary) = service
            .publish_secondary_buff(&s.id(), secondary_options)
            .expect("Error publishing secondary page");

        println!("Validating secondary page");
        // Convert secondary container to page
        let b = Container::parse(secondary.raw().to_vec(), &s.keys()).unwrap();
        service
            .validate_secondary(&b)
            .expect("Error validating secondary page against publisher");

        println!("Generating a data object");
        let data_options = DataOptions::<'static, &[u8]>::default();
        let (_n, data) = service
            .publish_data_buff(data_options)
            .expect("Error publishing data object");

        println!("Validating data object");
        let b = Container::parse(data.raw().to_vec(), &s.keys()).unwrap();
        replica
            .validate_data(&b)
            .expect("Error validating data against replica");
    }
}
