use std::time::SystemTime;

use dsf_core::prelude::*;
pub use dsf_rpc::peer::{PeerAddress, PeerInfo, PeerState};

bitflags::bitflags!(
    pub struct PeerFlags: u16 {
        const SYMMETRIC_AVAILABLE = (1 << 0);
        const SYMMETRIC_ENABLED = (1 << 1);
        const CONSTRAINED = (1 << 2);
    }
);

/// Peer object used to store peer related state and information
#[derive(Clone, Debug, PartialEq)]
pub struct Peer {
    pub(crate) info: PeerInfo,
    pub(crate) flags: PeerFlags,
}

impl Peer {
    pub fn id(&self) -> Id {
        self.info.id.clone()
    }

    pub fn address(&self) -> Address {
        self.info.address().clone()
    }

    pub fn state(&self) -> PeerState {
        self.info.state().clone()
    }

    pub fn seen(&self) -> Option<SystemTime> {
        self.info.seen().clone()
    }

    pub fn info(&self) -> PeerInfo {
        self.info.clone()
    }

    pub fn pub_key(&self) -> Option<PublicKey> {
        match self.info.state() {
            PeerState::Known(pk) => Some(pk.clone()),
            _ => None,
        }
    }
}

#[cfg(disabled)]
impl fmt::Display for PeerInfo {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if f.sign_plus() {
            write!(f, "id: {}", self.id)?;
        } else {
            write!(f, ", {}", self.id)?;
        }

        if f.sign_plus() {
            write!(f, "\n  - address: {}", self.address())?;
        } else {
            write!(f, "{}", self.address())?;
        }

        if f.sign_plus() {
            write!(f, "\n  - state: {}", self.state)?;
        } else {
            write!(f, ", {}", self.state)?;
        }

        if let Some(seen) = self.seen {
            let dt: chrono::DateTime<chrono::Local> = chrono::DateTime::from(seen);
            let ht = chrono_humanize::HumanTime::from(dt);

            if f.sign_plus() {
                write!(f, "\n  - last seen: {}", ht)?;
            } else {
                write!(f, ", {}", ht)?;
            }
        }

        if f.sign_plus() {
            write!(f, "\n  - sent: {}, received: {}", self.sent, self.received)?;
        } else {
            write!(f, ", {}, {}", self.sent, self.received)?;
        }

        Ok(())
    }
}

#[cfg(disabled)]
impl fmt::Display for PeerState {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if f.sign_plus() {
            write!(f, "state: ")?;
        }

        let s = match self {
            PeerState::Unknown => "unknown".red(),
            PeerState::Known(_) => "known  ".green(),
        };

        write!(f, "{}", s)?;

        Ok(())
    }
}
