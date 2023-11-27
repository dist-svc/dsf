use std::net::SocketAddr;
use std::str::FromStr;

use log::error;

use chrono::NaiveDateTime;
use diesel::{connection::LoadConnection, prelude::*, sqlite::Sqlite};

use dsf_core::{prelude::*, types::ShortId};
use dsf_rpc::{PeerAddress, PeerFlags, PeerInfo, PeerState};

use super::{from_dt, to_dt, Backend, Store, StoreError};

const KNOWN: &str = "known";
const UNKNOWN: &str = "unknown";

const IMPLICIT: &str = "implicit";
const EXPLICIT: &str = "explicit";

type PeerFields = (
    String,
    i32,
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

impl<B: Backend> Store<B> {
    // Store an item with it's associated page
    pub fn save_peer(&self, info: &PeerInfo) -> Result<(), StoreError> {
        self.with(|conn| save_peer(conn, info))
    }

    // Find an item or items
    pub fn find_peer(&self, id: &Id) -> Result<Option<PeerInfo>, StoreError> {
        self.with(|conn| find_peer(conn, id))
    }

    // Load all items
    pub fn load_peers(&self) -> Result<Vec<PeerInfo>, StoreError> {
        self.with(|conn| load_peers(conn))
    }

    pub fn delete_peer(&self, id: &Id) -> Result<(), StoreError> {
        self.with(|conn| delete_peer(conn, id))
    }
}

// Store an item with it's associated page
fn save_peer<C: Connection<Backend = Sqlite> + LoadConnection>(
    conn: &mut C,
    info: &PeerInfo,
) -> Result<(), StoreError> {
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
        peer_index.eq(info.index as i32),
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
        .load::<String>(conn)?;

    if r.len() != 0 {
        diesel::update(peers)
            .filter(peer_id.eq(info.id.to_string()))
            .set(values)
            .execute(conn)?;
    } else {
        diesel::insert_into(peers).values(values).execute(conn)?;
    }

    Ok(())
}

// Find an item or items
fn find_peer<C: Connection<Backend = Sqlite> + LoadConnection>(
    conn: &mut C,
    id: &Id,
) -> Result<Option<PeerInfo>, StoreError> {
    let results = peers
        .filter(peer_id.eq(id.to_string()))
        .select((
            peer_id,
            peer_index,
            state,
            public_key,
            address,
            address_mode,
            last_seen,
            sent,
            received,
            blocked,
        ))
        .load::<PeerFields>(conn)?;

    let p: Vec<PeerInfo> = results
        .iter()
        .filter_map(|v| match parse_peer(v) {
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
fn load_peers<C: Connection<Backend = Sqlite> + LoadConnection>(
    conn: &mut C,
) -> Result<Vec<PeerInfo>, StoreError> {
    let results = peers
        .select((
            peer_id,
            peer_index,
            state,
            public_key,
            address,
            address_mode,
            last_seen,
            sent,
            received,
            blocked,
        ))
        .load::<PeerFields>(conn)?;

    let p = results
        .iter()
        .filter_map(|v| match parse_peer(v) {
            Ok(i) => Some(i),
            Err(e) => {
                error!("Error parsing peer: {:?}", e);
                None
            }
        })
        .collect();

    Ok(p)
}

fn delete_peer<C: Connection<Backend = Sqlite> + LoadConnection>(
    conn: &mut C,
    id: &Id,
) -> Result<(), StoreError> {
    diesel::delete(peers)
        .filter(peer_id.eq(id.to_string()))
        .execute(conn)?;

    Ok(())
}

fn parse_peer(v: &PeerFields) -> Result<PeerInfo, StoreError> {
    let (
        r_id,
        r_index,
        r_state,
        r_pk,
        r_address,
        r_address_mode,
        r_seen,
        r_sent,
        r_recv,
        r_blocked,
    ) = v;

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

    let id = Id::from_str(r_id)?;
    let short_id = ShortId::from(&id);

    let p = PeerInfo {
        id,
        short_id,
        index: *r_index as usize,
        state: s_state,
        address: s_addr,
        // TODO(low): persist flags if these become relevant?
        flags: PeerFlags::empty(),

        seen: r_seen.as_ref().map(|v| from_dt(v)),

        sent: *r_sent as u64,
        received: *r_recv as u64,
        blocked: *r_blocked,
    };

    Ok(p)
}

#[cfg(test)]
mod test {
    use std::net::SocketAddr;
    use std::time::SystemTime;

    extern crate tracing_subscriber;
    use tempdir::TempDir;
    use tracing_subscriber::{filter::LevelFilter, FmtSubscriber};

    use super::Store;

    use dsf_core::{
        crypto::{Crypto, Hash, PubKey, SecKey},
        types::{Id, ShortId},
    };
    use dsf_rpc::{PeerAddress, PeerFlags, PeerInfo, PeerState};

    fn store_peer_inst() {
        let _ = FmtSubscriber::builder()
            .with_max_level(LevelFilter::DEBUG)
            .try_init();

        let d = TempDir::new("dsf-db").unwrap();
        let d = d.path().to_str().unwrap().to_string();

        let store = Store::new_rc(&format!("{d}/dsf-test-peer.db"), Default::default())
            .expect("Error opening store");

        store.drop_tables().unwrap();

        store.create_tables().unwrap();

        let (public_key, _private_key) = Crypto::new_pk().unwrap();
        let _secret_key = Crypto::new_sk().unwrap();
        let id: Id = Crypto::hash(&public_key).unwrap().into();

        let mut p = PeerInfo {
            id: id.clone(),
            short_id: ShortId::from(&id),
            index: 123,
            address: PeerAddress::Explicit("127.0.0.1:8080".parse::<SocketAddr>().unwrap().into()),
            flags: PeerFlags::empty(),
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
        store.delete_peer(&p.id).unwrap();
        assert_eq!(None, store.find_peer(&p.id).unwrap());
    }
}
