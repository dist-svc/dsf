use std::fmt::{Display, Formatter, Result};
use std::net::SocketAddr;

use colored::Colorize;

use crate::{DataInfo, PeerInfo, ServiceInfo};
use dsf_core::base::Body;

#[cfg(nope)]
impl Display for PeerAddress {
    fn fmt(&self, f: &mut Formatter) -> Result {}
}

impl Display for PeerInfo {
    fn fmt(&self, f: &mut Formatter) -> Result {
        if f.sign_plus() {
            write!(f, "id: {}", self.id)?;
        } else {
            write!(f, ", {}", self.id)?;
        }

        if f.sign_plus() {
            write!(f, "\n  - address: {}", SocketAddr::from(*self.address()))?;
        } else {
            write!(f, "{}", SocketAddr::from(*self.address()))?;
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

impl Display for DataInfo {
    fn fmt(&self, f: &mut Formatter) -> Result {
        if f.sign_plus() {
            write!(f, "index: {}", self.index)?;
        } else {
            write!(f, "{}", self.index)?;
        }

        if f.sign_plus() {
            write!(f, "\n  - service id: {}", self.service)?;
        } else {
            write!(f, "{}", self.service)?;
        }

        let body = match &self.body {
            Body::Cleartext(v) => format!("{:?}", v).green(),
            Body::Encrypted(_) => "Encrypted".to_string().red(),
            Body::None => "None".to_string().blue(),
        };

        if f.sign_plus() {
            write!(f, "\n  - body: {}", body)?;
        } else {
            write!(f, "{}", body)?;
        }

        let parent = match &self.previous {
            Some(p) => format!("{}", p).green(),
            None => "None".to_string().red(),
        };

        if f.sign_plus() {
            write!(f, "\n  - parent: {}", parent)?;
        } else {
            write!(f, "{}", parent)?;
        }

        if f.sign_plus() {
            write!(f, "\n  - signature: {}", self.signature)?;
        } else {
            write!(f, "{}", self.signature)?;
        }

        Ok(())
    }
}

impl Display for ServiceInfo {
    fn fmt(&self, f: &mut Formatter) -> Result {
        if f.sign_plus() {
            write!(f, "id: {}", self.id)?;
        } else {
            write!(f, "{}", self.id)?;
        }

        if f.sign_plus() {
            write!(f, "\n  - index: {}", self.index)?;
        } else {
            write!(f, "{}", self.index)?;
        }

        if f.sign_plus() {
            write!(f, "\n  - state: {}", self.state)?;
        } else {
            write!(f, ", {}", self.state)?;
        }

        if f.sign_plus() {
            write!(f, "\n  - public key: {}", self.public_key)?;
        } else {
            write!(f, ", {}", self.public_key)?;
        }

        if let Some(sk) = &self.secret_key {
            if f.sign_plus() {
                write!(f, "\n  - secret key: {}", sk.to_string().dimmed())?;
            } else {
                write!(f, ", {}", sk)?;
            }
        }

        if let Some(updated) = self.last_updated {
            let dt: chrono::DateTime<chrono::Local> = chrono::DateTime::from(updated);
            let ht = chrono_humanize::HumanTime::from(dt);

            if f.sign_plus() {
                write!(f, "\n  - last updated: {}", ht)?;
            } else {
                write!(f, ", {}", ht)?;
            }
        }

        if f.sign_plus() {
            write!(f, "\n  - replicas: {}", self.replicas)?;
        } else {
            write!(f, ", {}", self.replicas)?;
        }

        Ok(())
    }
}
