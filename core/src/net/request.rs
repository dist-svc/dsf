use core::convert::TryFrom;
use core::fmt::Debug;
use core::marker::PhantomData;
use core::ops::Deref;

use encdec::{Decode, DecodeExt, Encode};

#[cfg(feature = "alloc")]
use alloc::vec::Vec;

use super::Common;
use crate::{
    base::Message,
    error::Error,
    keys::KeySource,
    options::{Filters, Options},
    types::*,
    wire::{Builder, Container},
};

#[derive(Clone, Debug)]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub struct Request<D = RequestBody> {
    pub common: Common,
    pub data: D,
}

impl Deref for Request {
    type Target = Common;

    fn deref(&self) -> &Common {
        &self.common
    }
}

/// Builder for request objects
pub struct RequestBuilder2<'a> {
    _p: PhantomData<&'a ()>,
}

impl<'a> RequestBuilder2<'a> {
    /// Create a new request builder
    pub fn new() -> Self {
        Self { _p: PhantomData }
    }
}

pub trait RequestBuilder {}

impl<S, T: MutableData> RequestBuilder for Builder<S, T> {}

#[derive(Clone, PartialEq, Debug, strum::Display)]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub enum RequestBody {
    Hello,
    Ping,
    FindNode(Id),
    FindValue(Id),
    Store(Id, Vec<Container>),

    Locate(Id),
    Subscribe(Id),
    Unsubscribe(Id),
    Query(Id),
    PushData(Id, Vec<Container>),

    Register(Id, Vec<Container>),
    Unregister(Id),
    Discover(Vec<u8>, Vec<Options>),
}

#[derive(Debug, Encode, Decode)]
pub struct Hello;

impl<'a> Message<'a> for Hello {
    const KIND: u16 = RequestKind::Hello as u16;
}

#[derive(Debug, Encode, Decode)]
pub struct Ping;

impl<'a> Message<'a> for Ping {
    const KIND: u16 = RequestKind::Ping as u16;
}

#[derive(Debug, Encode, Decode)]
pub struct FindNode(pub Id);

impl<'a> Message<'a> for FindNode {
    const KIND: u16 = RequestKind::FindNodes as u16;
}

#[derive(Debug, Encode, Decode)]
pub struct FindValue(pub Id);

impl<'a> Message<'a> for FindValue {
    const KIND: u16 = RequestKind::FindValues as u16;
}

/// Generic helper for messages containing lists of containers
#[derive(Debug)]
pub struct DataContainer<
    'a,
    C: Iterator<Item = Container<&'a [u8]>> + Clone + Debug,
    Ty: Copy + Debug,
>(pub Id, C, PhantomData<&'a Ty>);

impl<'a, C, Ty> encdec::Decode<'a> for DataContainer<'a, C, Ty>
where
    C: Iterator<Item = Container<&'a [u8]>> + Clone + Debug,
    Ty: Copy + Debug,
{
    type Output = (Id, encdec::decode::DecodeIter<'a, Container<&'a [u8]>>);

    type Error = Error;

    fn decode(buff: &'a [u8]) -> Result<(Self::Output, usize), Self::Error> {
        let mut index = 0;

        // First, read index
        let (id, n) = Id::decode(&buff[index..])?;
        index += n;

        // Then, return object with page iter
        let c = Container::decode_iter(&buff[index..]);

        Ok(((id, c), buff.len()))
    }
}

impl<'a, C, Ty> encdec::Encode for DataContainer<'a, C, Ty>
where
    C: Iterator<Item = Container<&'a [u8]>> + Clone + Debug,
    Ty: Copy + Debug,
{
    type Error = Error;

    fn encode_len(&self) -> Result<usize, Self::Error> {
        let mut index = ID_LEN;

        for c in self.1.clone() {
            index += c.raw().len();
        }

        Ok(index)
    }

    fn encode(&self, buff: &mut [u8]) -> Result<usize, Self::Error> {
        let mut index = 0;

        // First, write ID
        index += self.0.encode(&mut buff[index..])?;

        // Then, containers
        for c in self.1.clone() {
            let b = c.raw();
            buff[index..][..b.len()].copy_from_slice(b);
            index += b.len();
        }

        Ok(index)
    }
}

/// Store message type
pub type Store<'a, C> = DataContainer<'a, C, StoreTy>;

/// Store marker type
#[derive(Copy, Clone, PartialEq, Debug)]
pub struct StoreTy;

/// Message impl for store type
impl<'a, C: Iterator<Item = Container<&'a [u8]>> + Clone + Debug> Message<'a> for Store<'a, C> {
    const KIND: u16 = RequestKind::Store as u16;
}

#[derive(Debug, Encode, Decode)]
pub struct Locate(pub Id);

impl<'a> Message<'a> for Locate {
    const KIND: u16 = RequestKind::Locate as u16;
}

#[derive(Debug, Encode, Decode)]
pub struct Subscribe(pub Id);

impl<'a> Message<'a> for Subscribe {
    const KIND: u16 = RequestKind::Subscribe as u16;
}

#[derive(Debug, Encode, Decode)]
pub struct Unsubscribe(pub Id);

impl<'a> Message<'a> for Unsubscribe {
    const KIND: u16 = RequestKind::Unsubscribe as u16;
}

#[derive(Debug, Encode, Decode)]
pub struct Query(pub Id);

impl<'a> Message<'a> for Query {
    const KIND: u16 = RequestKind::Query as u16;
}

/// Store message type
pub type PushData<'a, C> = DataContainer<'a, C, PushDataTy>;

/// PushData marker type
#[derive(Copy, Clone, PartialEq, Debug)]
pub struct PushDataTy;

impl<'a, C: Iterator<Item = Container<&'a [u8]>> + Clone + Debug> Message<'a> for PushData<'a, C> {
    const KIND: u16 = RequestKind::PushData as u16;
}

/// Register message type
pub type Register<'a, C> = DataContainer<'a, C, RegisterTy>;

/// Register marker type
#[derive(Copy, Clone, PartialEq, Debug)]
pub struct RegisterTy;

/// Register [`Message`] implementation
impl<'a, C: Iterator<Item = Container<&'a [u8]>> + Clone + Debug> Message<'a> for Register<'a, C> {
    const KIND: u16 = RequestKind::Register as u16;
}

#[derive(Debug, Encode, Decode)]
pub struct Unregister(pub Id);

impl<'a> Message<'a> for Unregister {
    const KIND: u16 = RequestKind::Unregister as u16;
}

/// Discover messages -also- use options fields for matching
// TODO: Unclear how to package body and -options- in Encode/Decode
#[derive(Debug)]
pub struct Discover<'a>(pub &'a [u8]);

impl<'a> Message<'a> for Discover<'a> {
    const KIND: u16 = RequestKind::Discover as u16;
}

impl<'a> Decode<'a> for Discover<'a> {
    type Output = Self;
    type Error = Error;

    fn decode(buff: &'a [u8]) -> Result<(Self::Output, usize), Self::Error> {
        Ok((Self(buff), buff.len()))
    }
}

impl<'a> Encode for Discover<'a> {
    type Error = Error;

    fn encode_len(&self) -> Result<usize, Self::Error> {
        Ok(self.0.len())
    }

    fn encode(&self, buff: &mut [u8]) -> Result<usize, Self::Error> {
        let len = self.0.len();

        if buff.len() < self.0.len() {
            return Err(Error::BufferLength);
        }

        buff[..len].copy_from_slice(self.0);

        Ok(len)
    }
}

/// Convert request kind containers to protocol message enumerations
impl From<&RequestBody> for RequestKind {
    fn from(r: &RequestBody) -> Self {
        match r {
            RequestBody::Hello => RequestKind::Hello,
            RequestBody::Ping => RequestKind::Ping,
            RequestBody::FindNode(_) => RequestKind::FindNodes,
            RequestBody::FindValue(_) => RequestKind::FindValues,
            RequestBody::Store(_, _) => RequestKind::Store,
            RequestBody::Locate(_) => RequestKind::Locate,
            RequestBody::Subscribe(_) => RequestKind::Subscribe,
            RequestBody::Unsubscribe(_) => RequestKind::Unsubscribe,
            RequestBody::Query(_) => RequestKind::Query,
            RequestBody::PushData(_, _) => RequestKind::PushData,
            RequestBody::Register(_, _) => RequestKind::Register,
            RequestBody::Unregister(_) => RequestKind::Unregister,
            RequestBody::Discover(_, _) => RequestKind::Discover,
        }
    }
}

impl<D> Request<D> {
    pub fn new(from: Id, request_id: u16, data: D, flags: Flags) -> Self {
        let common = Common {
            from,
            id: request_id,
            flags: flags | Flags::SYMMETRIC_DIR,
            public_key: None,
            remote_address: None,
        };
        Request { common, data }
    }

    pub fn flags(&mut self) -> &mut Flags {
        &mut self.common.flags
    }

    pub fn set_public_key(&mut self, pk: PublicKey) {
        self.common.public_key = Some(pk);
    }

    pub fn with_public_key(mut self, pk: PublicKey) -> Self {
        self.common.public_key = Some(pk);
        self
    }
}

impl PartialEq for Request {
    fn eq(&self, b: &Self) -> bool {
        self.from == b.from && self.flags == b.flags && self.data == b.data
    }
}

impl Request {
    pub fn convert<T: ImmutableData, K: KeySource>(
        base: Container<T>,
        key_source: &K,
    ) -> Result<Request, Error> {
        let header = base.header();

        if base.encrypted() {
            error!("Attempted to convert encrypted container to request");
            return Err(Error::CryptoError);
        }

        let body = base.body_raw();

        let remote_address = None;
        let public_options: Vec<_> = base.public_options_iter().collect();

        let public_key = Filters::pub_key(&public_options.iter());
        //let _private_options = base.private_options().to_vec();

        let kind = match RequestKind::try_from(header.kind()) {
            Ok(k) => k,
            Err(_) => return Err(Error::InvalidRequestKind),
        };

        let data = match kind {
            RequestKind::Hello => RequestBody::Hello,
            RequestKind::Ping => RequestBody::Ping,
            RequestKind::FindNodes => {
                let mut id = Id::default();
                id.copy_from_slice(&body[0..ID_LEN]);
                RequestBody::FindNode(id)
            }
            RequestKind::FindValues => {
                let mut id = Id::default();
                id.copy_from_slice(&body[0..ID_LEN]);
                RequestBody::FindValue(id)
            }
            RequestKind::Subscribe => {
                let mut id = Id::default();
                id.copy_from_slice(&body[0..ID_LEN]);
                RequestBody::Subscribe(id)
            }
            RequestKind::Unsubscribe => {
                let mut id = Id::default();
                id.copy_from_slice(&body[0..ID_LEN]);
                RequestBody::Unsubscribe(id)
            }
            RequestKind::Query => {
                let mut id = Id::default();
                id.copy_from_slice(&body[0..ID_LEN]);
                RequestBody::Query(id)
            }
            RequestKind::Locate => {
                let mut id = Id::default();
                id.copy_from_slice(&body[0..ID_LEN]);
                RequestBody::Locate(id)
            }
            RequestKind::Store => {
                let mut id = Id::default();
                id.copy_from_slice(&body[0..ID_LEN]);

                // Perhaps i should not fetch pages until later..?
                // And also sign them earlier..?
                let pages = Container::decode_pages(&body[ID_LEN..], key_source)?;

                RequestBody::Store(id, pages)
            }
            RequestKind::PushData => {
                let mut id = Id::default();
                id.copy_from_slice(&body[0..ID_LEN]);

                let pages = Container::decode_pages(&body[ID_LEN..], key_source)?;

                RequestBody::PushData(id, pages)
            }
            RequestKind::Register => {
                let mut id = Id::default();
                id.copy_from_slice(&body[0..ID_LEN]);

                let pages = Container::decode_pages(&body[ID_LEN..], key_source)?;

                RequestBody::Register(id, pages)
            }
            RequestKind::Unregister => {
                let mut id = Id::default();
                id.copy_from_slice(&body[0..ID_LEN]);

                RequestBody::Unregister(id)
            }
            RequestKind::Discover => {
                // TODO: pass through discover options
                RequestBody::Discover(body.to_vec(), public_options)
            }
        };

        // TODO: fetch message specific options
        //let remote_address = Base::filter_address_option(&mut public_options);

        let common = Common {
            from: base.id(),
            id: header.index(),
            flags: header.flags(),
            public_key,
            remote_address,
        };
        Ok(Request { common, data })
    }
}
