//! Header is a high level representation of the protocol header used in all DSF objects

use crate::types::{Flags, Kind};

/// Header encodes information for a given page in the database.
///
/// Wire encoding and decoding exists in [`crate::wire::WireHeader`]
#[derive(Clone, PartialEq, Debug)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub struct Header {
    /// Protocol version
    pub protocol_version: u16,

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
}

impl Default for Header {
    fn default() -> Self {
        Self {
            protocol_version: 0,
            application_id: 0,
            kind: Kind::from_bytes([0, 0]),
            flags: Flags::default(),
            index: 0,
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
        }
    }

    pub fn protocol_version(&self) -> u16 {
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
