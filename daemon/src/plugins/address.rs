//! Local address discovery plugin
//! Allows the daemon to perform local address discovery

use std::{net::IpAddr, os::unix::net::SocketAddr, time::Duration};

use log::info;

use get_if_addrs::get_if_addrs;

/// AddressActor provides local address lookup / update services
pub struct AddressPlugin {
    known: Vec<IpAddr>,
}

impl AddressPlugin {
    /// Create a new address plugin
    pub fn new() -> Self {
        Self { known: vec![] }
    }

    /// Update known addresses
    pub async fn update(&mut self) -> Result<Vec<IpAddr>, std::io::Error> {
        // Fetch locateable addresses
        let res = get_addrs().await?;

        // Find new addresses
        let mut added = vec![];
        for i in &res {
            if !self.known.contains(i) {
                added.push(*i);
                self.known.push(*i);
            }
        }

        // Find removed addresses
        let mut removed = vec![];
        for i in 0..self.known.len() {
            let a = &self.known[i];

            if !res.contains(a) {
                removed.push(*a);
                self.known.remove(i);
            }
        }

        info!("Discovered: {:?} Removed: {:?}", added, removed);

        Ok(self.known.clone())
    }
}

/// Fetch known addresses
async fn get_addrs() -> Result<Vec<IpAddr>, std::io::Error> {
    let a = get_if_addrs()?;

    let a = a
        .iter()
        .map(|v| v.ip())
        .filter(|v| !v.is_loopback())
        .collect();

    Ok(a)
}

#[cfg(test)]
mod test {
    use super::*;

    #[tokio::test]
    async fn test_get_addrs() {
        let a = get_addrs()
            .await
            .expect("Failed to fetch interface addresses");
        println!("addresses: {a:?}");
    }
}
