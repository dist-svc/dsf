use core::fmt::Display;
use core::str;
use core::str::FromStr;

use encdec::Decode;

use super::{OptionString, Options, OPTION_HEADER_LEN};
use crate::{
    helpers::ParseBytesError,
    types::{Address, DateTime, Id, ImmutableData, PublicKey, Signature},
};

/// Iterator for decoding options from the provided buffer
pub struct OptionsIter<T> {
    index: usize,
    buff: T,
}

impl<T: ImmutableData> core::fmt::Debug for OptionsIter<T> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        let i = OptionsIter::new(&self.buff);
        f.debug_list().entries(i).finish()
    }
}

impl<T: ImmutableData + Clone> Clone for OptionsIter<T> {
    fn clone(&self) -> Self {
        Self {
            index: 0,
            buff: self.buff.clone(),
        }
    }
}

impl<T> OptionsIter<T>
where
    T: AsRef<[u8]>,
{
    /// Create a new options
    pub fn new(buff: T) -> Self {
        Self { index: 0, buff }
    }
}

impl<T> Iterator for OptionsIter<T>
where
    T: AsRef<[u8]>,
{
    type Item = Options;

    fn next(&mut self) -> Option<Options> {
        // Fetch remaining data
        let rem = &self.buff.as_ref()[self.index..];

        // Short circuit if we're too short
        if rem.len() < OPTION_HEADER_LEN {
            return None;
        }

        let (o, n) = match Options::decode(rem) {
            Ok(v) => v,
            Err(e) => {
                error!("Option parsing error: {:?}", e);
                return None;
            }
        };

        self.index += n;

        Some(o)
    }
}

/// Filter helpers for option iterators
pub trait Filters {
    fn pub_key(&self) -> Option<PublicKey>;
    fn peer_id(&self) -> Option<Id>;
    fn issued(&self) -> Option<DateTime>;
    fn expiry(&self) -> Option<DateTime>;
    fn prev_sig(&self) -> Option<Signature>;
    fn address(&self) -> Option<Address>;
    fn name(&self) -> Option<OptionString>;
    fn index(&self) -> Option<u32>;
}

/// Filter implementation for [`OptionsIter`]
impl<T: AsRef<[u8]>> Filters for OptionsIter<T> {
    fn pub_key(&self) -> Option<PublicKey> {
        let mut s = OptionsIter {
            index: 0,
            buff: self.buff.as_ref(),
        };
        s.find_map(|o| match o {
            Options::PubKey(pk) => Some(pk.clone()),
            _ => None,
        })
    }

    fn peer_id(&self) -> Option<Id> {
        let mut s = OptionsIter {
            index: 0,
            buff: self.buff.as_ref(),
        };
        s.find_map(|o| match o {
            Options::PeerId(peer_id) => Some(peer_id.clone()),
            _ => None,
        })
    }

    fn issued(&self) -> Option<DateTime> {
        let mut s = OptionsIter {
            index: 0,
            buff: self.buff.as_ref(),
        };
        s.find_map(|o| match o {
            Options::Issued(t) => Some(t),
            _ => None,
        })
    }

    fn expiry(&self) -> Option<DateTime> {
        let mut s = OptionsIter {
            index: 0,
            buff: self.buff.as_ref(),
        };
        s.find_map(|o| match o {
            Options::Expiry(t) => Some(t),
            _ => None,
        })
    }

    fn prev_sig(&self) -> Option<Signature> {
        let mut s = OptionsIter {
            index: 0,
            buff: self.buff.as_ref(),
        };
        s.find_map(|o| match o {
            Options::PrevSig(s) => Some(s.clone()),
            _ => None,
        })
    }

    fn name(&self) -> Option<OptionString> {
        let mut s = OptionsIter {
            index: 0,
            buff: self.buff.as_ref(),
        };
        s.find_map(|o| match o {
            Options::Name(name) => Some(name.clone()),
            _ => None,
        })
    }

    fn address(&self) -> Option<Address> {
        let mut s = OptionsIter {
            index: 0,
            buff: self.buff.as_ref(),
        };
        s.find_map(|o| match o {
            Options::IPv4(addr) => Some((addr).into()),
            Options::IPv6(addr) => Some((addr).into()),
            _ => None,
        })
    }

    fn index(&self) -> Option<u32> {
        let mut s = OptionsIter {
            index: 0,
            buff: self.buff.as_ref(),
        };

        s.find_map(|o| match o {
            Options::Index(v) => Some(v),
            _ => None,
        })
    }
}

/// [`Filters`] implementation for types implementing Iterator over Options
impl<'a, T: Iterator<Item = &'a Options> + Clone> Filters for T {
    fn pub_key(&self) -> Option<PublicKey> {
        self.clone().find_map(|o| match o {
            Options::PubKey(pk) => Some(pk.clone()),
            _ => None,
        })
    }

    fn peer_id(&self) -> Option<Id> {
        self.clone().find_map(|o| match o {
            Options::PeerId(peer_id) => Some(peer_id.clone()),
            _ => None,
        })
    }

    fn issued(&self) -> Option<DateTime> {
        self.clone().find_map(|o| match o {
            Options::Issued(t) => Some(*t),
            _ => None,
        })
    }

    fn expiry(&self) -> Option<DateTime> {
        self.clone().find_map(|o| match o {
            Options::Expiry(t) => Some(*t),
            _ => None,
        })
    }

    fn prev_sig(&self) -> Option<Signature> {
        self.clone().find_map(|o| match o {
            Options::PrevSig(s) => Some(s.clone()),
            _ => None,
        })
    }

    fn name(&self) -> Option<OptionString> {
        self.clone().find_map(|o| match o {
            Options::Name(name) => Some(name.clone()),
            _ => None,
        })
    }

    fn address(&self) -> Option<Address> {
        self.clone().find_map(|o| match o {
            Options::IPv4(addr) => Some((*addr).into()),
            Options::IPv6(addr) => Some((*addr).into()),
            _ => None,
        })
    }

    fn index(&self) -> Option<u32> {
        self.clone().find_map(|o| match o {
            Options::Index(v) => Some(*v),
            _ => None,
        })
    }
}

#[derive(Debug, Clone)]
#[cfg_attr(feature = "thiserror", derive(thiserror::Error))]
pub enum OptionsParseError {
    #[cfg_attr(feature = "thiserror", error("Invalid format (expected key:value)"))]
    InvalidFormat,

    #[cfg_attr(
        feature = "thiserror",
        error("String encode/decode not supported for this option kind")
    )]
    Unsupported,

    #[cfg_attr(feature = "thiserror", error("byte string decode error: {0}"))]
    Parse(ParseBytesError),
}

impl FromStr for Options {
    type Err = OptionsParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        use OptionsParseError::*;

        let mut p = s.split(':');
        let (prefix, data) = match (p.next(), p.next()) {
            (Some(p), Some(d)) => (p, d),
            _ => return Err(InvalidFormat),
        };

        let o = match prefix {
            "pub_key" => Options::pub_key(PublicKey::from_str(data).map_err(Parse)?),
            "name" => Options::name(data),
            "kind" => Options::kind(data),
            "building" => Options::building(data),
            "room" => Options::room(data),
            _ => return Err(Unsupported),
        };

        Ok(o)
    }
}

impl Display for Options {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Options::PubKey(o) => {
                write!(f, "pub_key: ",)?;
                o.fmt(f)?;
            }
            Options::PrevSig(o) => {
                write!(f, "prev_sig: ")?;
                o.fmt(f)?;
            }
            Options::Name(o) => write!(f, "name: {}", o)?,
            Options::Kind(o) => write!(f, "kind: {}", o)?,
            Options::Building(o) => write!(f, "building: {}", o)?,
            Options::Room(o) => write!(f, "room: {}", o)?,
            Options::Issued(t) => write!(f, "issued: {}", t)?,
            Options::Expiry(t) => write!(f, "expiry: {}", t)?,
            Options::Index(t) => write!(f, "index: {}", t)?,
            _ => write!(f, "{:?}", self)?,
        }

        Ok(())
    }
}

#[cfg(test)]
mod test {
    #[test]
    fn test_options_parsing() {}
}
