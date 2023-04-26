use std::net::SocketAddr;
use std::str::FromStr;

use log::error;

use chrono::NaiveDateTime;
use diesel::prelude::*;

use dsf_core::prelude::*;
use dsf_rpc::{PeerAddress, PeerInfo, PeerState};

use super::{from_dt, to_dt, Store, StoreError};

const KNOWN: &str = "known";
const UNKNOWN: &str = "unknown";

const IMPLICIT: &str = "implicit";
const EXPLICIT: &str = "explicit";

type PeerFields = (
    String,
    String,
    Option<String>,
    String,
    String,
    Option<NaiveDateTime>,
    i32,
    i32,
    bool,
);

use crate::store::schema::peers::dsl::*;

impl Store {
    // Store an item with it's associated page
    pub fn save_peer(&self, info: &PeerInfo) -> Result<(), StoreError> {
        let (s, k) = match &info.state {
            PeerState::Known(k) => (
                state.eq(KNOWN.to_string()),
                Some(public_key.eq(k.to_string())),
            ),
            PeerState::Unknown => (state.eq(UNKNOWN.to_string()), None),
        };

        let am = match info.address {
            PeerAddress::Implicit(_) => IMPLICIT.to_string(),
            PeerAddress::Explicit(_) => EXPLICIT.to_string(),
        };

        let seen = info.seen.map(|v| last_seen.eq(to_dt(v)));

        let values = (
            peer_id.eq(info.id.to_string()),
            s,
            k,
            address.eq(SocketAddr::from(info.address().clone()).to_string()),
            address_mode.eq(am),
            seen,
            sent.eq(info.sent as i32),
            received.eq(info.received as i32),
            blocked.eq(info.blocked),
        );

        let r = peers
            .filter(peer_id.eq(info.id.to_string()))
            .select(peer_id)
            .load::<String>(&self.pool.get().unwrap())?;

        if r.len() != 0 {
            diesel::update(peers)
                .filter(peer_id.eq(info.id.to_string()))
                .set(values)
                .execute(&self.pool.get().unwrap())?;
        } else {
            diesel::insert_into(peers)
                .values(values)
                .execute(&self.pool.get().unwrap())?;
        }

        Ok(())
    }

    // Find an item or items
    pub fn find_peer(&self, id: &Id) -> Result<Option<PeerInfo>, StoreError> {
        let results = peers
            .filter(peer_id.eq(id.to_string()))
            .select((
                peer_id,
                state,
                public_key,
                address,
                address_mode,
                last_seen,
                sent,
                received,
                blocked,
            ))
            .load::<PeerFields>(&self.pool.get().unwrap())?;

        let p: Vec<PeerInfo> = results
            .iter()
            .filter_map(|v| match Self::parse_peer(v) {
                Ok(i) => Some(i),
                Err(e) => {
                    error!("Error parsing peer: {:?}", e);
                    None
                }
            })
            .collect();

        if p.len() != 1 {
            return Ok(None);
        }

        Ok(p.get(0).map(|v| v.clone()))
    }

    // Load all items
    pub fn load_peers(&self) -> Result<Vec<PeerInfo>, StoreError> {
        let results = peers
            .select((
                peer_id,
                state,
                public_key,
                address,
                address_mode,
                last_seen,
                sent,
                received,
                blocked,
            ))
            .load::<PeerFields>(&self.pool.get().unwrap())?;

        let p = results
            .iter()
            .filter_map(|v| match Self::parse_peer(v) {
                Ok(i) => Some(i),
                Err(e) => {
                    error!("Error parsing peer: {:?}", e);
                    None
                }
            })
            .collect();

        Ok(p)
    }

    pub fn delete_peer(&self, peer: &PeerInfo) -> Result<(), StoreError> {
        diesel::delete(peers)
            .filter(peer_id.eq(peer.id.to_string()))
            .execute(&self.pool.get().unwrap())?;

        Ok(())
    }

    fn parse_peer(v: &PeerFields) -> Result<PeerInfo, StoreError> {
        let (r_id, r_state, r_pk, r_address, r_address_mode, r_seen, r_sent, r_recv, r_blocked) = v;

        let s_state = match (r_state.as_ref(), &r_pk) {
            (KNOWN, Some(k)) => PeerState::Known(PublicKey::from_str(k)?),
            (UNKNOWN, _) => PeerState::Unknown,
            _ => unreachable!(),
        };

        let s_addr = match r_address_mode.as_ref() {
            IMPLICIT => PeerAddress::Implicit(SocketAddr::from_str(r_address)?.into()),
            EXPLICIT => PeerAddress::Explicit(SocketAddr::from_str(r_address)?.into()),
            _ => unreachable!(),
        };

        let p = PeerInfo {
            id: Id::from_str(r_id)?,
            state: s_state,
            address: s_addr,
            index: 0,

            seen: r_seen.as_ref().map(|v| from_dt(v)),

            sent: *r_sent as u64,
            received: *r_recv as u64,
            blocked: *r_blocked,
        };

        Ok(p)
    }
}

#[cfg(test)]
mod test {
    use std::net::SocketAddr;
    use std::time::SystemTime;

    extern crate tracing_subscriber;
    use tracing_subscriber::{filter::LevelFilter, FmtSubscriber};

    use super::Store;

    use dsf_core::{
        crypto::{Crypto, Hash, PubKey, SecKey},
        types::Id,
    };
    use dsf_rpc::{PeerAddress, PeerInfo, PeerState};

    #[test]
    fn store_peer_inst() {
        let _ = FmtSubscriber::builder()
            .with_max_level(LevelFilter::DEBUG)
            .try_init();

        let store = Store::new("/tmp/dsf-test-2.db").expect("Error opening store");

        store.drop_tables().unwrap();

        store.create_tables().unwrap();

        let (public_key, _private_key) = Crypto::new_pk().unwrap();
        let _secret_key = Crypto::new_sk().unwrap();
        let id: Id = Crypto::hash(&public_key).unwrap().into();

        let mut p = PeerInfo {
            id,
            index: 0,
            address: PeerAddress::Explicit("127.0.0.1:8080".parse::<SocketAddr>().unwrap().into()),
            state: PeerState::Known(public_key),
            seen: Some(SystemTime::now()),
            sent: 14,
            received: 12,
            blocked: false,
        };

        // Check no matching service exists
        assert_eq!(None, store.find_peer(&p.id).unwrap());

        // Store service
        store.save_peer(&p).unwrap();
        assert_eq!(Some(&p), store.find_peer(&p.id).unwrap().as_ref());

        // update service
        p.sent = 16;
        store.save_peer(&p).unwrap();
        assert_eq!(Some(&p), store.find_peer(&p.id).unwrap().as_ref());

        // Delete service
        store.delete_peer(&p).unwrap();
        assert_eq!(None, store.find_peer(&p.id).unwrap());
    }
}
