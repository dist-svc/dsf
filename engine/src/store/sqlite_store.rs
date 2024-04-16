//! sqlite backed engine storage

use core::str::FromStr;
use std::collections::{hash_map::Iter, HashMap};

use diesel::{
    prelude::*,
    r2d2::{ConnectionManager, Pool},
    sql_query, RunQueryDsl, SqliteConnection,
};
use dsf_core::{prelude::*, types::BaseKind};
use log::debug;

use super::*;

pub struct SqliteStore<Addr: Clone + Debug = std::net::SocketAddr> {
    pool: Pool<ConnectionManager<SqliteConnection>>,
    peers: HashMap<Id, Peer<Addr>>,
    services: HashMap<Id, Service>,
    keys: Keys,
    _addr: PhantomData<Addr>,
}

#[derive(Debug)]
#[cfg_attr(feature = "thiserror", derive(thiserror::Error))]
pub enum SqliteError {
    #[cfg_attr(feature = "thiserror", error("Connection error {0}"))]
    Connect(diesel::result::ConnectionError),
    #[cfg_attr(feature = "thiserror", error("Database error {0}"))]
    Diesel(diesel::result::Error),
    #[cfg_attr(feature = "thiserror", error("Parser error"))]
    Parse,
    #[cfg_attr(feature = "thiserror", error("Pool error"))]
    R2d2,
}

impl From<diesel::result::Error> for SqliteError {
    fn from(v: diesel::result::Error) -> Self {
        Self::Diesel(v)
    }
}

table! {
    ident (pri_key) {
        pri_key -> Text,
        sec_key -> Text,
    }
}

table! {
    peers (peer_id) {
        peer_id -> Text,
        pub_key -> Nullable<Text>,
        sec_key -> Nullable<Text>,
        address -> Nullable<Text>,

        subscriber -> Bool,
        subscribed -> Bool,
    }
}

table! {
    objects (signature) {
        object_index -> Integer,
        kind -> Text,
        signature -> Text,
        data -> Blob,
    }
}

impl<Addr: Clone + Debug + 'static> SqliteStore<Addr> {
    pub fn new(path: &str) -> Result<Self, SqliteError> {
        debug!("Connecting to store: {}", path);
        let mgr = ConnectionManager::new(path);
        let pool = Pool::new(mgr).map_err(|_e| SqliteError::R2d2)?;

        let mut s = Self {
            pool,
            keys: Keys::default(),
            peers: HashMap::new(),
            services: HashMap::new(),
            _addr: PhantomData,
        };

        if let Err(e) = s.create_tables() {
            println!("Failed to create tables: {:?}", e);
        }

        if let Some(ident) = s.get_ident()? {
            s.keys = ident;
        }

        Ok(s)
    }

    fn create_tables(&mut self) -> Result<(), SqliteError> {
        let mut conn = self.pool.get().map_err(|_| SqliteError::R2d2)?;

        let _ = sql_query(
            "CREATE TABLE ident (
                pri_key TEXT NOT NULL UNIQUE PRIMARY KEY,
                sec_key TEXT NOT NULL
            );",
        )
        .execute(&mut conn);

        let _ = sql_query(
            "CREATE TABLE peers (
                peer_id TEXT NOT NULL UNIQUE PRIMARY KEY,
                pub_key TEXT NOT NULL,
                sec_key TEXT,
                address TEXT,
                subscriber BOOLEAN NOT NULL,
                subscribed BOOLEAN NOT NULL
            );",
        )
        .execute(&mut conn);

        let _ = sql_query(
            "CREATE INDEX peer_id ON peers (peer_id);",
        )
        .execute(&mut conn);

        let _ = sql_query(
            "CREATE TABLE objects (
                signature TEXT NOT NULL UNIQUE PRIMARY KEY,
                object_index INTEGER NOT NULL,
                kind TEXT NOT NULL,
                data BLOB NOT NULL
            );",
        )
        .execute(&mut conn);

        let _ = sql_query(
            "CREATE INDEX objects_sig ON objects (signature);",
        )
        .execute(&mut conn);

        let _ = sql_query(
            "CREATE INDEX objects_idx ON objects (object_index);",
        )
        .execute(&mut conn);

        Ok(())
    }
}

trait Storable: ToString + FromStr {}

impl<Addr: Clone + Debug + 'static> Store for SqliteStore<Addr> {
    const FEATURES: StoreFlags = StoreFlags::ALL;

    type Address = Addr;
    type Error = SqliteError;
    type Iter<'a> = Iter<'a, Id, Peer<Addr>>;

    fn get_ident(&self) -> Result<Option<Keys>, Self::Error> {
        use self::ident::dsl::*;

        let mut conn = self.pool.get().map_err(|_| SqliteError::R2d2)?;

        let v = ident
            .select((pri_key, sec_key))
            .limit(1)
            .load::<(String, String)>(&mut conn)
            .map_err(SqliteError::Diesel)?;

        if v.len() != 1 {
            return Ok(None);
        }

        let e = &v[0];

        let private_key = match PrivateKey::from_str(&e.0) {
            Ok(v) => v,
            Err(_) => return Ok(None),
        };

        let public_key = Crypto::get_public(&private_key);

        let keys = Keys {
            pub_key: Some(public_key),
            pri_key: Some(private_key),
            sec_key: SecretKey::from_str(&e.1).ok(),
            sym_keys: None,
        };

        Ok(Some(keys))
    }

    fn set_ident(&mut self, keys: &Keys) -> Result<(), Self::Error> {
        use self::ident::dsl::*;

        let values = (
            pri_key.eq(keys.pri_key.as_ref().unwrap().to_string()),
            sec_key.eq(keys.sec_key.as_ref().unwrap().to_string()),
        );

        diesel::insert_into(ident)
            .values(&values)
            .on_conflict(pri_key)
            .do_nothing()
            .execute(&mut self.pool.get().unwrap())?;

        self.keys = keys.clone();

        Ok(())
    }

    /// Fetch previous object information
    fn get_last(&self) -> Result<Option<ObjectInfo>, Self::Error> {
        use self::objects::dsl::*;

        let mut conn = self.pool.get().map_err(|_| SqliteError::R2d2)?;

        let v = objects
            .select((signature, object_index))
            .filter(kind.eq(BaseKind::Page.to_string()))
            .order_by(object_index.desc())
            .limit(1)
            .load::<(String, i32)>(&mut conn)
            .map_err(SqliteError::Diesel)?;

        let mut sig = None;

        let last_page = if v.len() == 1 {
            sig = Some(Signature::from_str(&v[0].0).unwrap());
            v[0].1
        } else {
            0
        };

        let v = objects
            .select((signature, object_index))
            .filter(kind.eq(BaseKind::Data.to_string()))
            .order_by(object_index.desc())
            .limit(1)
            .load::<(String, i32)>(&mut conn)
            .map_err(SqliteError::Diesel)?;

        let last_block = if v.len() == 1 {
            if v[0].1 > last_page {
                sig = Some(Signature::from_str(&v[0].0).unwrap());
            }

            v[0].1
        } else {
            0
        };

        let last_sig = match sig {
            Some(v) => v,
            None => return Ok(None),
        };

        Ok(Some(ObjectInfo {
            page_index: last_page as u32,
            block_index: last_block as u32,
            sig: last_sig,
        }))
    }

    fn get_peer(&self, id: &Id) -> Result<Option<Peer<Self::Address>>, Self::Error> {
        let p = self.peers.get(id);
        Ok(p.cloned())
    }

    fn peers(&self) -> Self::Iter<'_> {
        self.peers.iter()
    }

    fn update_peer<R: Debug, F: Fn(&mut Peer<Self::Address>) -> R>(
        &mut self,
        id: &Id,
        f: F,
    ) -> Result<R, Self::Error> {
        let p = self.peers.entry(id.clone()).or_default();
        Ok(f(p))
    }

    fn store_page<T: ImmutableData>(&mut self, p: &Container<T>) -> Result<(), Self::Error> {
        use self::objects::dsl::*;

        let values = (
            object_index.eq(p.header().index() as i32),
            kind.eq(p.header().kind().base().to_string()),
            signature.eq(p.signature().to_string()),
            data.eq(p.raw()),
        );

        diesel::insert_into(objects)
            .values(&values)
            .execute(&mut self.pool.get().unwrap())?;

        Ok(())
    }

    fn fetch_page<T: MutableData>(
        &mut self,
        f: ObjectFilter,
        mut buff: T,
    ) -> Result<Option<Container<T>>, Self::Error> {
        use self::objects::dsl::*;

        let mut conn = self.pool.get().map_err(|_| SqliteError::R2d2)?;

        let q = objects
            .select((signature, object_index, kind, data))
            .limit(1);

        let v = match f {
            ObjectFilter::Latest => q
                .order_by(object_index.desc())
                .load::<(String, i32, String, Vec<u8>)>(&mut conn),
            ObjectFilter::Index(n) => {
                q.filter(object_index.eq(n as i32))
                    .load::<(String, i32, String, Vec<u8>)>(&mut conn)
            }
            ObjectFilter::Sig(s) => {
                q.filter(signature.eq(s.to_string()))
                    .load::<(String, i32, String, Vec<u8>)>(&mut conn)
            }
        }
        .map_err(SqliteError::Diesel)?;

        if v.len() != 1 {
            return Ok(None);
        }

        let v = &v[0];

        let b = buff.as_mut();
        b[..v.3.len()].copy_from_slice(&v.3);

        let c = match Container::parse(buff, &self.keys) {
            Ok(v) => v,
            Err(e) => {
                println!("Failed to parse container: {:?}", e);
                return Err(SqliteError::Parse);
            }
        };

        Ok(Some(c))
    }

    // Fetch service information
    fn get_service(&self, id: &Id) -> Result<Option<Service>, Self::Error> {
        let s = self.services.get(id);
        Ok(s.cloned())
    }

    // Update a specified service
    fn update_service<R: Debug, F: Fn(&mut Service) -> R>(
        &mut self,
        id: &Id,
        f: F,
    ) -> Result<R, Self::Error> {
        let s = self.services.entry(id.clone()).or_default();
        Ok(f(s))
    }
}

impl<'a, Addr: Clone + Debug + 'static> IntoIterator for &'a SqliteStore<Addr> {
    type Item = (&'a Id, &'a Peer<Addr>);

    type IntoIter = Iter<'a, Id, Peer<Addr>>;

    fn into_iter(self) -> Self::IntoIter {
        self.peers.iter()
    }
}

impl<Addr: Clone + Debug> KeySource for SqliteStore<Addr> {
    fn keys(&self, id: &Id) -> Option<Keys> {
        self.peers.get(id).map(|p| p.keys.clone())
    }
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;

    use dsf_core::{
        crypto::{Crypto, PubKey, SecKey},
        prelude::*,
    };
    use tempfile::NamedTempFile;

    use super::*;

    #[test]
    fn sqlite_store_ident() {
        let f = NamedTempFile::new().unwrap();

        let mut store = SqliteStore::<SocketAddr>::new(f.path().to_str().unwrap()).unwrap();

        let (pub_key, pri_key) = Crypto::new_pk().unwrap();
        let sec_key = Crypto::new_sk().unwrap();
        let keys = Keys {
            pub_key: Some(pub_key),
            pri_key: Some(pri_key),
            sec_key: Some(sec_key),
            sym_keys: None,
        };

        store.set_ident(&keys).unwrap();

        let keys2 = store.get_ident().unwrap().unwrap();

        assert_eq!(keys, keys2);
    }

    #[test]
    fn sqlite_store_page() {
        let f = NamedTempFile::new().unwrap();

        let mut store = SqliteStore::<SocketAddr>::new(f.path().to_str().unwrap()).unwrap();

        let mut s = ServiceBuilder::<Vec<u8>>::generic()
            .body(vec![0xaa, 0xbb, 0xcc])
            .build()
            .unwrap();

        // Build page
        let mut buff = vec![0u8; 1024];
        let (_n, p) = s.publish_primary(Default::default(), &mut buff).unwrap();

        // Write to store
        store.store_page(&p).unwrap();

        // Retrieve from store
        let mut buff = vec![0u8; 1024];
        let p1 = store
            .fetch_page(ObjectFilter::Sig(p.signature()), &mut buff)
            .unwrap()
            .unwrap();

        // Check page matches
        assert_eq!(p1.header(), p.header());
        assert_eq!(p1.signature(), p.signature());
        assert_eq!(p1.body_raw(), p.body_raw());
        assert_eq!(p1.public_options_raw(), p.public_options_raw());
        assert_eq!(p1.private_options_raw(), p.private_options_raw());

        // Check last page info is updated
        let i = store.get_last().unwrap().unwrap();
        assert_eq!(i.page_index, p1.header().index());
        assert_eq!(i.sig, p1.signature());
    }
}
