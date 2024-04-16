use core::ops::Add;

use encdec::{Decode, DecodeOwned, Encode};

use crate::{
    base::{DataBody, Header, MaybeEncrypted, PageBody},
    error::Error,
    options::Options,
    service::Service,
    types::*,
    wire::{
        builder::{Encrypt, SetPublicOptions},
        Builder, Container,
    },
};

/// Publisher trait allows services to generate primary, data, and secondary pages
/// as well as to encode (and sign and optionally encrypt) generated pages
pub trait Publisher<const N: usize = 512> {
    /// Generates a primary page to publish for the given service and encodes it into the provided buffer
    fn publish_primary<T: MutableData>(
        &mut self,
        options: PrimaryOptions,
        buff: T,
    ) -> Result<(usize, Container<T>), Error>;

    // Helper to publish primary page using fixed sized buffer
    fn publish_primary_buff(
        &mut self,
        options: PrimaryOptions,
    ) -> Result<(usize, Container<[u8; N]>), Error> {
        let buff = [0u8; N];
        let (n, c) = self.publish_primary(options, buff)?;
        Ok((n, c))
    }

    /// Create a data object for publishing with the provided options and encodes it into the provided buffer
    fn publish_data<B: DataBody, T: MutableData>(
        &mut self,
        options: DataOptions<B>,
        buff: T,
    ) -> Result<(usize, Container<T>), Error>;

    // Helper to publish data block using fixed size buffer
    fn publish_data_buff<B: DataBody>(
        &mut self,
        options: DataOptions<B>,
    ) -> Result<(usize, Container<[u8; N]>), Error> {
        let buff = [0u8; N];
        let (n, c) = self.publish_data(options, buff)?;
        Ok((n, c))
    }

    /// Create a secondary page for publishing with the provided options and encodes it into the provided buffer
    fn publish_secondary<T: MutableData>(
        &mut self,
        id: &Id,
        options: SecondaryOptions,
        buff: T,
    ) -> Result<(usize, Container<T>), Error>;

    /// Helper to publish secondary page fixed size buffer
    fn publish_secondary_buff(
        &mut self,
        id: &Id,
        options: SecondaryOptions,
    ) -> Result<(usize, Container<[u8; N]>), Error> {
        let buff = [0u8; N];
        let (n, c) = self.publish_secondary(id, options, buff)?;
        Ok((n, c))
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct PrimaryOptions {
    /// Page publish time
    pub issued: Option<DateTime>,

    /// Page expiry time
    pub expiry: Option<DateTime>,
}

impl Default for PrimaryOptions {
    fn default() -> Self {
        Self {
            issued: default_issued(),
            expiry: default_expiry(),
        }
    }
}

fn default_issued() -> Option<DateTime> {
    #[cfg(feature = "std")]
    return Some(std::time::SystemTime::now().into());

    #[cfg(not(feature = "std"))]
    return None;
}

fn default_expiry() -> Option<DateTime> {
    #[cfg(feature = "std")]
    return Some(
        std::time::SystemTime::now()
            .add(std::time::Duration::from_secs(24 * 60 * 60))
            .into(),
    );

    #[cfg(not(feature = "std"))]
    return None;
}

#[derive(Clone)]
pub struct SecondaryOptions<'a, Body: DataBody = &'a [u8]> {
    /// Application ID of primary service
    pub application_id: u16,

    /// Page object kind
    pub page_kind: Kind,

    /// Page version
    /// This is monotonically increased for any successive publishing of the same page
    pub version: u32,

    /// Page body
    pub body: Option<Body>,

    /// Page publish time
    pub issued: Option<DateTime>,

    /// Page expiry time
    pub expiry: Option<DateTime>,

    /// Public options attached to the page
    pub public_options: &'a [Options],

    /// Private options attached to the page
    pub private_options: &'a [Options],
}

impl<'a> Default for SecondaryOptions<'a> {
    fn default() -> Self {
        Self {
            application_id: 0,
            page_kind: PageKind::Generic.into(),
            version: 0,
            body: None,
            issued: default_issued(),
            expiry: default_expiry(),
            public_options: &[],
            private_options: &[],
        }
    }
}

/// Secondary data block, published by peers when replicating
#[derive(Clone, PartialEq, Debug, Encode, DecodeOwned)]
pub struct SecondaryData {
    /// List of TIDs for registry linking
    pub sig: Signature,
}

impl DataBody for SecondaryData {}

#[derive(Clone, Debug)]
pub struct DataOptions<'a, Body: DataBody = &'a [u8]> {
    /// Data object kind
    pub data_kind: Kind,

    /// Data object body
    pub body: Option<Body>,

    /// Data publish time
    pub issued: Option<DateTime>,

    /// Public options attached to the data object
    pub public_options: &'a [Options],

    /// Private options attached to the data object
    pub private_options: &'a [Options],

    /// Do not attach last signature to object
    pub no_last_sig: bool,
}

impl<'a, Body: DataBody> Default for DataOptions<'a, Body> {
    fn default() -> Self {
        Self {
            data_kind: Kind::data(false, DataKind::Generic as u8),
            body: None,
            issued: default_issued(),
            public_options: &[],
            private_options: &[],
            no_last_sig: false,
        }
    }
}

impl<B> Publisher for Service<B>
where
    B: PageBody,
    <B as Encode>::Error: core::fmt::Debug,
{
    /// Publish generates a page to publishing for the given service.
    fn publish_primary<T: MutableData>(
        &mut self,
        options: PrimaryOptions,
        buff: T,
    ) -> Result<(usize, Container<T>), Error> {
        let mut flags = Flags::default();
        if self.encrypted {
            flags |= Flags::ENCRYPTED;
        }

        debug!("Primary options: {:?}", options);

        // Fetch index for next published object
        let index = self.index;

        // Setup header
        let header = Header {
            application_id: self.application_id,
            kind: self.kind.into(),
            index,
            flags,
            ..Default::default()
        };

        debug!("Header: {:?}", header);

        let private_opts = match &self.private_options {
            MaybeEncrypted::Cleartext(o) => &o[..],
            MaybeEncrypted::None => &[],
            _ => return Err(Error::CryptoError),
        };

        // Build object
        let b = Builder::new(buff).header(&header).id(&self.id());

        let b = match &self.body {
            MaybeEncrypted::Cleartext(body) => b.body(body).map_err(|e| {
                error!("Failed to encode body: {:?}", e);
                Error::EncodeFailed
            })?,
            MaybeEncrypted::Encrypted(_) => {
                // TODO: we can't publish if we don't have the secret keys anyway
                // so we _could_ just write this directly...
                error!("Cannot encode encrypted body");
                return Err(Error::EncodeFailed);
            }
            MaybeEncrypted::None => b.no_body(),
        };

        let b = b.private_options(private_opts.iter())?;

        // Apply internal encryption if enabled
        let mut b = self.encrypt(b)?;

        // Generate and append public options
        b = b.public_options([Options::pub_key(self.public_key.clone())].iter())?;

        // Attach last sig if available
        if let Some(last) = &self.last_sig {
            b = b.public_options([Options::prev_sig(last)].iter())?;
        }

        // Generate and append public options

        // Attach issued if provided
        if let Some(iss) = options.issued {
            b = b.public_options([Options::issued(iss)].iter())?;
        }
        // Attach expiry if provided
        if let Some(exp) = options.expiry {
            b = b.public_options([Options::expiry(exp)].iter())?;
        }

        // Then finally attach public options
        // TODO: filter / update issued and expiry if included?
        let b = b.public_options(self.public_options.iter())?;

        // Sign generated object
        let c = self.sign(b)?;

        // Update current version and next index
        self.version = index;
        self.index = index.wrapping_add(1);

        // Return container and encode
        Ok((c.len(), c))
    }

    /// Secondary generates a secondary page using this service to be attached to / stored at the provided service ID
    fn publish_secondary<T: MutableData>(
        &mut self,
        id: &Id,
        options: SecondaryOptions,
        buff: T,
    ) -> Result<(usize, Container<T>), Error> {
        // Set secondary page flags
        let mut flags = Flags::SECONDARY;
        if self.encrypted {
            flags |= Flags::ENCRYPTED;
        }

        // Check we are publishing a page
        assert!(options.page_kind.is_page());

        // Setup header
        let header = Header {
            application_id: self.application_id,
            kind: options.page_kind,
            flags,
            index: options.version,
            ..Default::default()
        };

        // Build object
        let b = Builder::new(buff).header(&header).id(id);

        let b = match options.body {
            Some(body) => b.body(body)?,
            None => b.with_body(|_b| Ok(0))?,
        };

        let b = b.private_options(options.private_options.iter())?;

        // Apply internal encryption if enabled
        let b = self.encrypt(b)?;

        // Generate and append public options
        let mut b = b.public_options([Options::peer_id(self.id.clone())].iter())?;

        // Attach issued if provided
        if let Some(iss) = options.issued {
            b = b.public_options([Options::issued(iss)].iter())?;
        }
        // Attach expiry if provided
        if let Some(exp) = options.expiry {
            b = b.public_options([Options::expiry(exp)].iter())?;
        }
        // Attach last sig if available
        if let Some(last) = &self.last_sig {
            b = b.public_options([Options::prev_sig(last)].iter())?;
        }
        // Then finally attach public options
        let b = b.public_options(options.public_options.iter())?;

        // Sign generated object
        let c = self.sign(b)?;

        Ok((c.len(), c))
    }

    fn publish_data<D: DataBody, T: MutableData>(
        &mut self,
        options: DataOptions<D>,
        buff: T,
    ) -> Result<(usize, Container<T>), Error> {
        let mut flags = Flags::default();
        if self.encrypted {
            flags |= Flags::ENCRYPTED;
        }

        let index = self.index;

        let header = Header {
            application_id: self.application_id,
            kind: options.data_kind,
            flags,
            index,
            ..Default::default()
        };

        // Build object
        let b = Builder::new(buff).header(&header).id(&self.id());

        let b = match options.body {
            Some(body) => b.body(body).map_err(|e| {
                error!("Failed to encode data body: {:?}", e);
                Error::EncodeFailed
            })?,
            None => b.with_body(|_b| Ok(0))?,
        };

        let b = b.private_options(options.private_options)?;

        // Apply internal encryption if enabled
        let mut b = self.encrypt(b)?;

        // Generate and append public options

        // Attach issued if provided
        if let Some(iss) = options.issued {
            b = b.public_options(&[Options::issued(iss)])?;
        }

        // Attach last sig if available
        if let Some(last) = &self.last_sig {
            b = b.public_options(&[Options::prev_sig(last)])?;
        }

        // Attach public options
        let b = b.public_options(options.public_options)?;

        // Sign generated object
        let c = self.sign(b)?;

        // Update index
        self.index = self.index.wrapping_add(1);

        // Return container and encoded length
        Ok((c.len(), c))
    }
}

impl<B: PageBody> Service<B> {
    /// Encrypt the data and private options in the provided container builder
    pub(super) fn encrypt<T: MutableData>(
        &self,
        b: Builder<Encrypt, T>,
    ) -> Result<Builder<SetPublicOptions, T>, Error> {
        // Apply internal encryption if enabled
        let b = match (self.encrypted, &self.secret_key) {
            (true, Some(sk)) => b.encrypt(sk)?,
            (false, _) => b.public(),
            _ => todo!(),
        };

        Ok(b)
    }

    /// Sign and finalise a container builder
    pub(super) fn sign<T: MutableData>(
        &mut self,
        b: Builder<SetPublicOptions, T>,
    ) -> Result<Container<T>, Error> {
        // Sign generated object
        let c = match &self.private_key {
            Some(pk) => b.sign_pk(pk)?,
            None => {
                error!("No public key for object signing");
                return Err(Error::NoPrivateKey);
            }
        };

        // Update last signature
        self.last_sig = Some(c.signature());

        // Return signed container
        Ok(c)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{options::Filters, prelude::*};

    fn init_service() -> Service {
        ServiceBuilder::default()
            .kind(PageKind::Generic)
            .public_options(vec![Options::name("Test Service")])
            .encrypt()
            .build()
            .expect("Failed to create service")
    }

    #[test]
    fn test_publish_primary() {
        let mut svc = init_service();

        let opts = PrimaryOptions {
            ..Default::default()
        };

        // Publish first page
        let (_n, p) = svc
            .publish_primary_buff(opts.clone())
            .expect("Failed to publish primary page");
        assert_eq!(p.header().index(), 0);

        // Publish second page
        let (_n, p) = svc
            .publish_primary_buff(opts.clone())
            .expect("Failed to publish primary page");
        assert_eq!(p.header().index(), 1);
    }

    #[test]
    fn test_publish_data() {
        let mut svc = init_service();

        let opts = PrimaryOptions {
            ..Default::default()
        };

        // Generate primary page for linking
        let (_n, p) = svc
            .publish_primary_buff(opts)
            .expect("Failed to publish primary page");
        assert_eq!(svc.last_sig, Some(p.signature()));

        let body: &[u8] = &[0x00, 0x11, 0x22, 0x33];
        let opts = DataOptions {
            body: Some(body),
            ..Default::default()
        };

        // Publish first data object
        let (_n, d1) = svc
            .publish_data_buff(opts.clone())
            .expect("Failed to publish data object");
        assert_eq!(d1.header().index(), 1);
        assert_eq!(d1.public_options_iter().prev_sig(), Some(p.signature()));
        assert_eq!(svc.last_sig, Some(d1.signature()));

        // Publish second data object
        let (_n, d2) = svc
            .publish_data_buff(opts.clone())
            .expect("Failed to publish data object");

        assert_eq!(d2.header().index(), 2);
        assert_eq!(d2.public_options_iter().prev_sig(), Some(d1.signature()));
        assert_eq!(svc.last_sig, Some(d2.signature()));
    }
}
