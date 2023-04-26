use core::fmt::Debug;
use core::marker::PhantomData;

use encdec::{Encode, EncodeExt};
use log::trace;
use pretty_hex::*;

use crate::base::Header;
use crate::crypto::{Crypto, PubKey as _, SecKey as _};
use crate::error::Error;
use crate::options::Options;
use crate::types::*;

use super::container::Container;
use super::header::WireHeader;
use super::{offsets, HEADER_LEN};

/// Init state, no data set
pub struct Init;

/// SetBody state, has header and ID
pub struct SetBody;

/// SetPrivateOptions state, has Body and previous (SetBody)
pub struct SetPrivateOptions;

/// Encrypt state, has body and private options
pub struct Encrypt;

/// SetPublicOptions state, has PrivateOptions and previous (SetPrivateOptions)
pub struct SetPublicOptions;

/// Sign state, has PublicOptions and previous (SetPublicOptions)
pub struct Sign;

/// Internal trait to support encoding of optionally encrypted objects in a generic buffer
pub trait EncodeEncrypted {
    fn encode<B: MutableData>(
        &self,
        buf: B,
        secret_key: Option<&SecretKey>,
    ) -> Result<usize, Error>;
}

/// Builder provides a low-level wire protocol builder object.
/// This is generic over buffer types and uses type-state mutation to ensure created objects are valid
pub struct Builder<S, T: MutableData> {
    /// internal data buffer
    buf: T,
    /// Current index count
    n: usize,
    /// Local index count
    c: usize,
    /// Encrypted flag
    encrypted: bool,

    _s: PhantomData<S>,
}

// Implementations that are always available
impl<S, T: MutableData> Builder<S, T> {
    /// Set the object id
    pub fn id(mut self, id: &Id) -> Self {
        let d = self.buf.as_mut();

        d[HEADER_LEN..HEADER_LEN + ID_LEN].clone_from_slice(id);

        self
    }

    /// Fetch a mutable instance of the object header
    pub fn header_mut(&mut self) -> WireHeader<&mut [u8]> {
        WireHeader::new(&mut self.buf.as_mut()[..HEADER_LEN])
    }

    /// Fetch a mutable instance of the object header
    pub fn header_ref(&self) -> WireHeader<&[u8]> {
        WireHeader::new(&self.buf.as_ref()[..HEADER_LEN])
    }

    /// Fetch the header bytes (including ID)
    pub fn header_raw(&self) -> &[u8] {
        &self.buf.as_ref()[..HEADER_LEN + ID_LEN]
    }
}

impl<T: MutableData> Builder<Init, T> {
    /// Create a new base builder object
    pub fn new(buf: T) -> Self {
        Builder {
            buf,
            n: offsets::BODY,
            c: 0,
            encrypted: false,
            _s: PhantomData,
        }
    }

    /// Set the object header.
    /// Note that length fields will be overwritten by actual lengths
    pub fn header(mut self, header: &Header) -> Self {
        trace!("Set header: {:02?}", header);

        self.header_mut().encode(header);
        self.header_mut().set_data_len(0);
        self.header_mut().set_private_options_len(0);
        self.header_mut().set_public_options_len(0);

        self
    }

    /// Add body data, mutating the state of the builder
    pub fn body<B: Encode>(
        mut self,
        body: B,
    ) -> Result<Builder<SetPrivateOptions, T>, <B as Encode>::Error> {
        let b = self.buf.as_mut();

        self.n = offsets::BODY;

        let n = body.encode(&mut b[self.n..])?;
        self.n += n;

        self.header_mut().set_data_len(n);

        trace!("Add {} byte body: {:02x?}, new index: {}", n, body, self.n);

        Ok(Builder {
            buf: self.buf,
            n: self.n,
            c: 0,
            encrypted: false,
            _s: PhantomData,
        })
    }

    pub fn with_body(
        mut self,
        f: impl Fn(&mut [u8]) -> Result<usize, Error>,
    ) -> Result<Builder<SetPrivateOptions, T>, Error> {
        let b = self.buf.as_mut();
        self.n = offsets::BODY;

        trace!(
            "Writing body, available bytes: {}",
            b.len() - HEADER_LEN - SIGNATURE_LEN
        );

        let n = f(&mut b[offsets::BODY..])?;
        self.n += n;

        self.header_mut().set_data_len(n);

        trace!("Add {} byte body, new index: {}", n, self.n);

        Ok(Builder {
            buf: self.buf,
            n: self.n,
            c: 0,
            encrypted: false,
            _s: PhantomData,
        })
    }

    pub fn no_body(self) -> Builder<SetPrivateOptions, T> {
        Builder {
            buf: self.buf,
            n: self.n,
            c: 0,
            encrypted: false,
            _s: PhantomData,
        }
    }

    pub fn encrypted(
        self,
        body: &[u8],
        private_options: &[u8],
        tag: &[u8],
    ) -> Result<Builder<SetPublicOptions, T>, Error> {
        self.body::<&[u8]>(body)
            .unwrap()
            .private_options_raw(private_options)
            .unwrap()
            .tag(tag)
    }
}

impl<T: MutableData> Builder<SetPrivateOptions, T> {
    /// Encode private options
    /// This must be done in one pass as the entire options block is encrypted
    pub fn private_options<'a, C: IntoIterator<Item = &'a Options> + Debug>(
        mut self,
        options: C,
    ) -> Result<Builder<Encrypt, T>, Error> {
        let b = self.buf.as_mut();

        let n = Options::encode_iter(options.into_iter(), &mut b[self.n..])?;
        self.n += n;

        trace!("Encoded private options: {:02x?}", &b[self.n - n..][..n]);

        let p = self.header_mut().private_options_offset();
        let l = self.n - p;
        self.header_mut().set_private_options_len(l);

        trace!("Add private options {} bytes, new index: {}", n, self.n);

        Ok(Builder {
            buf: self.buf,
            n: self.n,
            c: 0,
            encrypted: false,
            _s: PhantomData,
        })
    }

    /// Write raw (encrypted) private options
    /// This must be done in one pass as the entire body + private options block is encrypted
    pub fn private_options_raw(mut self, options: &[u8]) -> Result<Builder<Encrypt, T>, Error> {
        let b = self.buf.as_mut();
        let o = options.as_ref();

        b[self.n..][..o.len()].copy_from_slice(o);
        self.n += o.len();

        self.header_mut().set_private_options_len(o.len());

        trace!(
            "Add raw private options, {} bytes, new index: {}",
            o.len(),
            self.n
        );

        Ok(Builder {
            buf: self.buf,
            n: self.n,
            c: 0,
            encrypted: true,
            _s: PhantomData,
        })
    }
}

impl<T: MutableData> Builder<Encrypt, T> {
    /// Encrypt private data and options
    /// This must be done in one pass as the entire data/options block is encrypted
    pub fn encrypt(
        mut self,
        secret_key: &SecretKey,
    ) -> Result<Builder<SetPublicOptions, T>, Error> {
        // TODO: skip if body + private options are empty...

        debug!("SK body encrypt with key: {}", secret_key);

        // Calculate area to be encrypted
        let o = HEADER_LEN + ID_LEN;
        let l = self.header_ref().data_len() + self.header_ref().private_options_len();

        let b = self.buf.as_mut();

        let block = &mut b[o..o + l];
        trace!("Encrypting block: {:?}", block.hex_dump());

        // Perform encryption
        let tag = Crypto::sk_encrypt(secret_key, None, block).unwrap();

        trace!("Encrypted block: {:?}", block.hex_dump());
        trace!("Encryption tag: {:?}", tag.hex_dump());

        // Attach tag to object
        b[self.n..][..SECRET_KEY_TAG_LEN].copy_from_slice(&tag);
        self.n += SECRET_KEY_TAG_LEN;

        trace!(
            "Encrypted {} bytes at offset {}, new index: {}",
            l,
            o,
            self.n
        );

        Ok(Builder {
            buf: self.buf,
            n: self.n,
            c: 0,
            encrypted: true,
            _s: PhantomData,
        })
    }

    /// Re-encode private data and options, using existing encryption tag
    /// This must be done in one pass as the entire data/options block is encrypted
    pub fn re_encrypt<C: ImmutableData>(
        mut self,
        secret_key: &SecretKey,
        tag: C,
    ) -> Result<Builder<SetPublicOptions, T>, Error> {
        // Calculate area to be encrypted
        let o = HEADER_LEN + ID_LEN;
        let l = self.header_ref().data_len() + self.header_ref().private_options_len();

        let b = self.buf.as_mut();

        // Perform encryption
        Crypto::sk_reencrypt(secret_key, tag.as_ref(), None, &mut b[o..o + l]).unwrap();

        // Attach tag to object
        b[self.n..][..SECRET_KEY_TAG_LEN].copy_from_slice(&tag.as_ref());
        self.n += SECRET_KEY_TAG_LEN;

        trace!(
            "Re-encrypted {} bytes at offset {} with tag: {:02x?}, new index: {}",
            l,
            o,
            tag.as_ref(),
            self.n
        );

        Ok(Builder {
            buf: self.buf,
            n: self.n,
            c: 0,
            encrypted: true,
            _s: PhantomData,
        })
    }

    /// Attach tag for already encrypted data
    pub fn tag<C: ImmutableData>(mut self, tag: C) -> Result<Builder<SetPublicOptions, T>, Error> {
        // Calculate area to be encrypted
        let o = HEADER_LEN
            + ID_LEN
            + self.header_ref().data_len()
            + self.header_ref().private_options_len();

        let b = self.buf.as_mut();

        // Attach tag to object
        b[o..][..SECRET_KEY_TAG_LEN].copy_from_slice(tag.as_ref());
        self.n = o + SECRET_KEY_TAG_LEN;

        trace!("Added tag: {:02x?}, new index: {}", tag.as_ref(), self.n);

        Ok(Builder {
            buf: self.buf,
            n: self.n,
            c: 0,
            encrypted: true,
            _s: PhantomData,
        })
    }

    pub fn public(self) -> Builder<SetPublicOptions, T> {
        trace!("Set object type to public, index: {}", self.n);

        Builder {
            buf: self.buf,
            n: self.n,
            c: 0,
            encrypted: false,
            _s: PhantomData,
        }
    }
}

impl<T: MutableData> Builder<SetPublicOptions, T> {
    /// Encode a list of public options
    pub fn public_options<'a, C: IntoIterator<Item = &'a Options> + Debug>(
        mut self,
        options: C,
    ) -> Result<Builder<SetPublicOptions, T>, Error> {
        let b = self.buf.as_mut();

        let n = Options::encode_iter(options.into_iter(), &mut b[self.n..])?;
        self.n += n;
        self.c += n;
        let c = self.c;

        self.header_mut().set_public_options_len(c);

        trace!("Add public options {} bytes, new index: {}", n, self.n);

        Ok(self)
    }

    /// Add a single public option
    pub fn public_option(&mut self, option: &Options) -> Result<(), Error> {
        let b = self.buf.as_mut();

        let n = option.encode(&mut b[self.n..])?;
        self.n += n;
        self.c += n;
        let c = self.c;

        self.header_mut().set_public_options_len(c);

        trace!(
            "Add public option: {:?}, {} bytes, new index: {}",
            option,
            n,
            self.n
        );

        Ok(())
    }

    // Sign the builder object, returning a new signed container
    pub fn sign_pk(mut self, signing_key: &PrivateKey) -> Result<Container<T>, Error> {
        let b = self.buf.as_mut();

        // Generate signature
        let sig = Crypto::pk_sign(signing_key, &b[..self.n]).unwrap();

        trace!(
            "Sign {} byte object, new index: {}",
            self.n,
            self.n + SIGNATURE_LEN
        );

        // Write to object
        (&mut b[self.n..self.n + SIGNATURE_LEN]).copy_from_slice(&sig);
        self.n += SIGNATURE_LEN;

        trace!("Created object: {:?}", PrettyHex::hex_dump(&self));

        // Return base object
        Ok(Container {
            buff: self.buf,
            len: self.n,
            verified: true,
            decrypted: false,
        })
    }

    pub fn encrypt_sk(mut self, secret_key: &SecretKey) -> Result<Container<T>, Error> {
        debug!(
            "SK Sign/Encrypt (AEAD) with key: {} ({} bytes)",
            secret_key, self.n
        );

        let buf = self.buf.as_mut();

        let (header, body) = buf[..self.n].split_at_mut(HEADER_LEN + ID_LEN);
        let tag = Crypto::sk_encrypt(secret_key, Some(header), body).unwrap();

        debug!("MAC: {}", tag);

        buf[self.n..][..tag.len()].copy_from_slice(&tag);
        self.n += SIGNATURE_LEN;

        Ok(Container {
            buff: self.buf,
            len: self.n,
            verified: true,
            decrypted: false,
        })
    }

    // Provide an existing signature to the builder object
    pub fn sign_raw(mut self, sig: &Signature) -> Result<Container<T>, Error> {
        let b = self.buf.as_mut();

        (&mut b[self.n..self.n + SIGNATURE_LEN]).copy_from_slice(&sig);
        self.n += SIGNATURE_LEN;

        // Return base object
        Ok(Container {
            buff: self.buf,
            len: self.n,
            verified: true,
            decrypted: false,
        })
    }
}

impl<S, T: MutableData> AsRef<[u8]> for Builder<S, T> {
    fn as_ref(&self) -> &[u8] {
        let n = self.n;
        &self.buf.as_ref()[..n]
    }
}
