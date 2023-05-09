use byteorder::{ByteOrder, LittleEndian};
use encdec::Decode;

use super::{offsets, HEADER_LEN, SECRET_KEY_TAG_LEN};
use crate::base::Header;
use crate::types::{Flags, ImmutableData, Kind, MutableData, ID_LEN, SIGNATURE_LEN};

/// Header generic over arbitrary storage for wire encoding
// TODO: decide what to do with the high / low level impls
pub struct WireHeader<T: ImmutableData> {
    pub(crate) buff: T,
}

impl<T: ImmutableData> PartialEq for WireHeader<T> {
    fn eq(&self, other: &Self) -> bool {
        self.buff.as_ref() == other.buff.as_ref()
    }
}

impl<T: ImmutableData> From<&WireHeader<T>> for Header {
    /// Build a base::Header object from a WireHeader
    fn from(wh: &WireHeader<T>) -> Header {
        Header::new(wh.application_id(), wh.kind(), wh.index(), wh.flags())
    }
}

impl<T: ImmutableData> WireHeader<T> {
    /// Create a new header object
    pub fn new(buff: T) -> Self {
        Self { buff }
    }

    pub fn protocol_version(&self) -> u8 {
        self.buff.as_ref()[offsets::PROTO_VERSION]
    }

    pub fn application_id(&self) -> u16 {
        LittleEndian::read_u16(&self.buff.as_ref()[offsets::APPLICATION_ID..])
    }

    pub fn kind(&self) -> Kind {
        // TODO: should pass error here, but aiming to deprecate WireKind completely
        let (kind, _) = Kind::decode(&self.buff.as_ref()[offsets::OBJECT_KIND..]).unwrap();
        kind
    }

    pub fn flags(&self) -> Flags {
        let raw = LittleEndian::read_u16(&self.buff.as_ref()[offsets::FLAGS..]);
        Flags::from_bits_truncate(raw)
    }

    pub fn index(&self) -> u32 {
        LittleEndian::read_u32(&self.buff.as_ref()[offsets::INDEX..])
    }

    pub fn data_len(&self) -> usize {
        LittleEndian::read_u16(&self.buff.as_ref()[offsets::DATA_LEN..]) as usize
    }

    pub fn private_options_len(&self) -> usize {
        LittleEndian::read_u16(&self.buff.as_ref()[offsets::PRIVATE_OPTIONS_LEN..]) as usize
    }

    pub fn public_options_len(&self) -> usize {
        LittleEndian::read_u16(&self.buff.as_ref()[offsets::PUBLIC_OPTIONS_LEN..]) as usize
    }

    pub fn data_offset(&self) -> usize {
        offsets::BODY
    }

    pub fn private_options_offset(&self) -> usize {
        self.data_offset() + self.data_len()
    }

    pub fn tag_offset(&self) -> usize {
        self.private_options_offset() + self.private_options_len()
    }

    pub fn public_options_offset(&self) -> usize {
        let mut o = self.private_options_offset() + self.private_options_len();

        if self.flags().contains(Flags::ENCRYPTED) {
            o += SECRET_KEY_TAG_LEN
        };

        o
    }

    pub fn signature_offset(&self) -> usize {
        self.public_options_offset() + self.public_options_len()
    }

    pub fn encoded_len(&self) -> usize {
        let flags = self.flags();

        let tag_len = if flags.contains(Flags::ENCRYPTED) && !flags.contains(Flags::SYMMETRIC_MODE)
        {
            SECRET_KEY_TAG_LEN
        } else {
            0
        };

        HEADER_LEN
            + ID_LEN
            + self.data_len()
            + self.private_options_len()
            + tag_len
            + self.public_options_len()
            + SIGNATURE_LEN
    }
}

impl<T: ImmutableData> core::fmt::Debug for WireHeader<T> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> Result<(), core::fmt::Error> {
        f.debug_struct("WireHeader")
            .field("protocol_version", &self.protocol_version())
            .field("application_id", &self.application_id())
            .field("kind", &self.kind())
            .field("flags", &self.flags())
            .field("index", &self.index())
            .field("data_len", &self.data_len())
            .field("private_options_len", &self.private_options_len())
            .field("public_options_len", &self.public_options_len())
            .finish()
    }
}

impl<T: MutableData> WireHeader<T> {
    /// Write a base::Header
    pub fn encode(&mut self, h: &Header) {
        self.set_protocol_version(h.protocol_version());
        self.set_application_id(h.application_id());
        self.set_kind(h.kind());
        self.set_flags(h.flags());
        self.set_index(h.index());
    }

    /// Set the protocol version
    pub fn set_protocol_version(&mut self, version: u8) {
        self.buff.as_mut()[offsets::PROTO_VERSION] = version;
    }

    /// Set the application ID
    pub fn set_application_id(&mut self, application_id: u16) {
        LittleEndian::write_u16(
            &mut self.buff.as_mut()[offsets::APPLICATION_ID..],
            application_id,
        )
    }

    /// Set object flags
    pub fn set_flags(&mut self, flags: Flags) {
        LittleEndian::write_u16(&mut self.buff.as_mut()[offsets::FLAGS..], flags.bits())
    }

    /// Set the object kind
    pub fn set_kind(&mut self, kind: Kind) {
        self.buff.as_mut()[offsets::OBJECT_KIND] = kind.into();
    }

    /// Set object index
    pub fn set_index(&mut self, index: u32) {
        let d = &mut self.buff.as_mut()[offsets::INDEX..];

        LittleEndian::write_u32(&mut self.buff.as_mut()[offsets::INDEX..], index);
    }

    /// Set the body field length
    pub fn set_data_len(&mut self, data_len: usize) {
        LittleEndian::write_u16(
            &mut self.buff.as_mut()[offsets::DATA_LEN..],
            data_len as u16,
        )
    }

    /// Set the private options field length
    pub fn set_private_options_len(&mut self, private_options_len: usize) {
        LittleEndian::write_u16(
            &mut self.buff.as_mut()[offsets::PRIVATE_OPTIONS_LEN..],
            private_options_len as u16,
        )
    }

    /// Set the public options field length
    pub fn set_public_options_len(&mut self, public_options_len: usize) {
        LittleEndian::write_u16(
            &mut self.buff.as_mut()[offsets::PUBLIC_OPTIONS_LEN..],
            public_options_len as u16,
        )
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::wire::HEADER_LEN;

    use crate::base::Header;
    use crate::types::PageKind;

    #[test]
    fn test_wire_header_size() {
        assert_eq!(HEADER_LEN, 16);
    }

    #[test]
    fn test_encode_wire_header() {
        // Create high level header
        let h = Header::new(0, PageKind::Generic.into(), 1124356, Flags::SECONDARY);

        // Create new wire header
        let mut h1 = WireHeader::new([0u8; HEADER_LEN]);

        // Encode high-level onto wire
        h1.encode(&h);

        // Parse high-level from wire
        let h2 = Header::from(&h1);

        // Check original / decoded match
        assert_eq!(h, h2);
    }
}
