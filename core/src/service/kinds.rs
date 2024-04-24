// TODO: work out how to avoid owning well, everything ideally, so this works without alloc
#[cfg(feature = "alloc")]
use alloc::{string::String, vec::Vec};

use crate::types::Signature;

#[derive(PartialEq, Debug, Clone)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
pub struct Generic {
    pub name: String,
    pub addresses: Vec<String>,
    //pub meta: HashMap<String, String>,
}

#[derive(PartialEq, Debug, Clone)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
pub struct Unknown {
    pub body: Vec<u8>,
}

/// Service history information
#[derive(PartialEq, Debug, Clone)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
#[derive(Default)]
pub struct History {
    /// Last page index
    pub last_page: u32,
    /// Last data index
    pub last_data: u32,
    /// Last object signature
    pub last_sig: Option<Signature>,
}
