//! Header is a high level representation of the protocol header used in all DSF objects

use byteorder::{ByteOrder, LittleEndian};
use encdec::{DecodeOwned, Encode};

use crate::{
    error::Error,
    types::{Flags, Kind, PageKind},
    wire::HEADER_LEN,
};

/// Header encodes information for a given page in the database.
///
/// Wire encoding and decoding exists in [`crate::wire::WireHeader`]
#[derive(Clone, PartialEq, Debug)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub struct Header {
    /// Protocol version
    pub protocol_version: u8,

    // Application ID
    pub application_id: u16,

    /// Object kind (Page, Block, Request, Response, etc.)
    pub kind: Kind,

    /// Object flags
    pub flags: Flags,

    /// Index of the object
    /// - index in the published chain for primary pages / data blocks (enables simple sorting and requests)
    /// - the Request ID for request / response messages
    /// - index of the corresponding data block for tertiary pages
    pub index: u32,

    /// Length of object data
    pub data_len: u16,

    /// Length of encoded private options
    pub private_options_len: u16,

    /// Length of encoded public options
    pub public_options_len: u16,
}

impl Default for Header {
    /// Create a default object header
    fn default() -> Self {
        Self {
            protocol_version: 0,
            application_id: 0,
            kind: PageKind::Generic.into(),
            flags: Flags::default(),
            index: 0,
            data_len: 0,
            private_options_len: 0,
            public_options_len: 0,
        }
    }
}

impl Header {
    pub fn new(application_id: u16, kind: Kind, index: u32, flags: Flags) -> Header {
        Header {
            protocol_version: 0,
            application_id,
            kind,
            flags,
            index,
            data_len: 0,
            private_options_len: 0,
            public_options_len: 0,
        }
    }

    pub fn protocol_version(&self) -> u8 {
        self.protocol_version
    }

    pub fn application_id(&self) -> u16 {
        self.application_id
    }

    pub fn kind(&self) -> Kind {
        self.kind
    }

    pub fn flags(&self) -> Flags {
        self.flags
    }

    pub fn index(&self) -> u32 {
        self.index
    }
}

/// [Encode] impl for [Header] type, used for byte-wise encoding
impl Encode for Header {
    type Error = Error;

    fn encode_len(&self) -> Result<usize, Self::Error> {
        Ok(HEADER_LEN)
    }

    fn encode(&self, buff: &mut [u8]) -> Result<usize, Self::Error> {
        // Check buffer length is long enough for a header object
        if buff.len() < HEADER_LEN {
            return Err(Error::BufferLength);
        }

        let mut n = 0;

        buff[0] = self.protocol_version;
        n += 1;

        LittleEndian::write_u16(&mut buff[n..], self.application_id);
        n += 2;

        n += self.kind.encode(&mut buff[n..])?;

        LittleEndian::write_u32(&mut buff[n..], self.index);
        n += 4;

        LittleEndian::write_u16(&mut buff[n..], self.flags.bits());
        n += 2;

        LittleEndian::write_u16(&mut buff[n..], self.data_len);
        n += 2;

        LittleEndian::write_u16(&mut buff[n..], self.private_options_len);
        n += 2;

        LittleEndian::write_u16(&mut buff[n..], self.public_options_len);
        n += 2;

        Ok(n)
    }
}

/// [DecodeOwned] impl for [Header] type, used for byte-wise decoding
impl DecodeOwned for Header {
    type Output = Header;

    type Error = Error;

    fn decode_owned(buff: &[u8]) -> Result<(Self::Output, usize), Self::Error> {
        // Check buffer length is long enough for a header object
        if buff.len() < HEADER_LEN {
            return Err(Error::BufferLength);
        }

        let mut n = 0;

        let protocol_version = buff[0];
        n += 1;

        let application_id = LittleEndian::read_u16(&buff[n..]);
        n += 2;

        let (kind, m) = Kind::decode_owned(&buff[n..])?;
        n += m;

        let index = LittleEndian::read_u32(&buff[n..]);
        n += 4;

        let f = LittleEndian::read_u16(&buff[n..]);
        let flags = Flags::from_bits_truncate(f);
        n += 2;

        let data_len = LittleEndian::read_u16(&buff[n..]);
        n += 2;

        let private_options_len = LittleEndian::read_u16(&buff[n..]);
        n += 2;

        let public_options_len = LittleEndian::read_u16(&buff[n..]);
        n += 2;

        Ok((
            Self {
                protocol_version,
                application_id,
                kind,
                flags,
                index,
                data_len,
                private_options_len,
                public_options_len,
            },
            n,
        ))
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn header_encode_decode() {
        let h = Header {
            protocol_version: 2,
            application_id: 0x3456,
            kind: Kind::data(true, 0x15),
            index: 0x12345678,
            flags: Flags::CONSTRAINED,
            data_len: 0x0123,
            private_options_len: 0x4321,
            public_options_len: 0x7654,
        };

        let mut buff = [0u8; 16];
        let n = h.encode(&mut buff).unwrap();
        assert_eq!(n, 16);

        let expected = [
            0x02,
            0x56,
            0x34,
            0b0111_0101,
            0x78,
            0x56,
            0x34,
            0x12,
            0x80,
            0x00,
            0x23,
            0x01,
            0x21,
            0x43,
            0x54,
            0x76,
        ];
        assert_eq!(
            buff, expected,
            "mismatch (actual: {:02x?}, expected: {:02x?}",
            buff, expected
        );
    }
}
