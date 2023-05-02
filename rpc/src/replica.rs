use std::time::SystemTime;

use serde::{Deserialize, Serialize};

use dsf_core::prelude::*;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ReplicaInfo {
    //pub service_id: Id,
    pub page_id: Id,
    pub peer_id: Id,

    pub version: u32,

    pub issued: SystemTime,
    pub updated: SystemTime,
    pub expiry: Option<SystemTime>,
    pub active: bool,
}
