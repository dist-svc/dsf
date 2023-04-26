use crate::types::{Id, PublicKey, Signature};

/// Information about a type of page
#[derive(Debug, PartialEq, Clone)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub enum PageInfo {
    Primary(Primary),
    Secondary(Secondary),
    Data(()),
    ServiceLink(ServiceLink),
    BlockLink(BlockLink),
}

impl PageInfo {
    pub fn primary(pub_key: PublicKey) -> Self {
        PageInfo::Primary(Primary { pub_key })
    }

    pub fn secondary(peer_id: Id) -> Self {
        PageInfo::Secondary(Secondary { peer_id })
    }

    pub fn service_link(target_id: Id, peer_id: Id) -> Self {
        PageInfo::ServiceLink(ServiceLink { target_id, peer_id })
    }

    pub fn block_link(block_sig: Signature, peer_id: Id) -> Self {
        PageInfo::BlockLink(BlockLink { block_sig, peer_id })
    }

    pub fn is_primary(&self) -> bool {
        match self {
            PageInfo::Primary(_) => true,
            _ => false,
        }
    }

    pub fn is_secondary(&self) -> bool {
        match self {
            PageInfo::Secondary(_) => true,
            _ => false,
        }
    }

    pub fn pub_key(&self) -> Option<PublicKey> {
        match self {
            PageInfo::Primary(p) => Some(p.pub_key.clone()),
            _ => None,
        }
    }

    pub fn peer_id(&self) -> Option<Id> {
        match self {
            PageInfo::Secondary(s) => Some(s.peer_id.clone()),
            _ => None,
        }
    }
}

#[derive(Debug, PartialEq, Clone)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub struct Primary {
    pub pub_key: PublicKey,
}

#[derive(Debug, PartialEq, Clone)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub struct Secondary {
    pub peer_id: Id,
}

#[derive(Debug, PartialEq, Clone)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub struct ServiceLink {
    pub target_id: Id,
    pub peer_id: Id,
}

#[derive(Debug, PartialEq, Clone)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub struct BlockLink {
    pub block_sig: Signature,
    pub peer_id: Id,
}
