use std::str::FromStr;
use std::time::SystemTime;

use dsf_core::types::ImmutableData;
use dsf_core::wire::Container;
use dsf_rpc::{PeerInfo, ServiceInfo};
use log::{debug, error, warn};

use diesel::dsl::sql_query;
use diesel::prelude::*;
use diesel::r2d2::{ConnectionManager, Pool};
use diesel::sqlite::SqliteConnection;

use chrono::{DateTime, Local, NaiveDateTime, TimeZone};

use dsf_core::prelude::*;
use dsf_core::service::Subscriber;

pub mod data;
pub mod error;
pub mod peers;
pub mod schema;
pub mod services;

pub mod page;

pub use error::StoreError;

#[derive(Clone)]
pub struct Store {
    pool: Pool<ConnectionManager<SqliteConnection>>,
}

fn to_dt(s: SystemTime) -> NaiveDateTime {
    DateTime::<Local>::from(s).naive_utc()
}

fn from_dt(n: &NaiveDateTime) -> SystemTime {
    let l = chrono::offset::Local;
    let dt = l.from_utc_datetime(n);
    dt.into()
}

#[async_trait::async_trait]
pub trait DataStore {
    /// Store a peer, updating the object if existing
    fn peer_update(&self, info: &PeerInfo) -> Result<(), StoreError> {
        todo!()
    }

    /// Fetch a peer by ID
    fn peer_get(&self, id: &Id) -> Result<PeerInfo, StoreError> {
        todo!()
    }

    /// Delete a peer by ID
    fn peer_del(&self, id: &Id) -> Result<(), StoreError> {
        todo!()
    }

    /// Store a service, updating the object if existing
    fn service_update(&self, info: &ServiceInfo) -> Result<(), StoreError> {
        todo!()
    }

    /// Fetch a service by ID
    fn service_get(&self, id: &Id) -> Result<ServiceInfo, StoreError> {
        todo!()
    }

    /// Delete a service by ID
    fn service_del(&self, id: &Id) -> Result<(), StoreError> {
        todo!()
    }

    /// Store an object, linked to a service ID and signature
    fn object_put<T: ImmutableData>(
        &self,
        id: &Id,
        sig: &Signature,
        page: Container<T>,
    ) -> Result<(), StoreError> {
        todo!()
    }

    /// Fetch an object by signature
    fn object_get(&self, sig: &Signature) -> Result<Container, StoreError> {
        todo!()
    }

    /// Delete an object by signature
    fn object_del(&self, sig: &Signature) -> Result<(), StoreError> {
        todo!()
    }
}

impl Store {
    /// Create or connect to a store with the provided filename
    pub fn new(path: &str) -> Result<Self, StoreError> {
        debug!("Connecting to store: {}", path);

        // Setup connection manager / pool
        let _conn = SqliteConnection::establish(path)?;

        let mgr = ConnectionManager::new(path);
        let pool = Pool::new(mgr).unwrap();

        // Create object
        let s = Self { pool };

        // Ensure tables exist
        let _ = s.create_tables();

        Ok(s)
    }

    /// Initialise the store
    ///
    /// This is called automatically in the `new` function
    pub fn create_tables(&self) -> Result<(), StoreError> {
        sql_query(
            "CREATE TABLE services (
            service_id TEXT NOT NULL UNIQUE PRIMARY KEY, 
            service_index TEGER NOT NULL, 
            state TEXT NOT NULL, 

            public_key TEXT NOT NULL, 
            private_key TEXT, 
            secret_key TEXT, 

            primary_page BLOB, 
            replica_page BLOB, 
            
            last_updated TEXT, 
            subscribers INTEGER NOT NULL, 
            replicas INTEGER NOT NULL,
            original BOOLEAN NOT NULL,
            subscribed BOOLEAN NOT NULL
        );",
        )
        .execute(&self.pool.get().unwrap())?;

        sql_query(
            "CREATE TABLE peers (
            peer_id TEXT NOT NULL UNIQUE PRIMARY KEY, 
            peer_index INTEGER, 
            state TEXT NOT NULL, 

            public_key TEXT, 
            address TEXT, 
            address_mode TEXT, 
            last_seen TEXT, 
            
            sent INTEGER NOT NULL, 
            received INTEGER NOT NULL, 
            blocked BOOLEAN NOT NULL
        );",
        )
        .execute(&self.pool.get().unwrap())?;

        sql_query(
            "CREATE TABLE data (
            signature TEXT NOT NULL UNIQUE PRIMARY KEY, 
            service_id TEXT NOT NULL,

            object_index INTEGER NOT NULL,
            body_kind TEXT NOT NULL,
            body_value BLOB,

            previous TEXT
        );",
        )
        .execute(&self.pool.get().unwrap())?;

        sql_query(
            "CREATE TABLE object (
            service_id TEXT NOT NULL,

            raw_data BLOB NOT NULL,

            previous TEXT,
            signature TEXT NOT NULL PRIMARY KEY
        );",
        )
        .execute(&self.pool.get().unwrap())?;

        sql_query(
            "CREATE TABLE identity (
            service_id TEXT NOT NULL PRIMARY KEY,

            public_key TEXT NOT NULL,
            private_key TEXT NOT NULL,
            secret_key TEXT,
            
            last_page TEXT NOT NULL
        );",
        )
        .execute(&self.pool.get().unwrap())?;

        Ok(())
    }

    pub fn drop_tables(&self) -> Result<(), StoreError> {
        sql_query("DROP TABLE IF EXISTS services;").execute(&self.pool.get().unwrap())?;

        sql_query("DROP TABLE IF EXISTS peers;").execute(&self.pool.get().unwrap())?;

        sql_query("DROP TABLE IF EXISTS data;").execute(&self.pool.get().unwrap())?;

        sql_query("DROP TABLE IF EXISTS object;").execute(&self.pool.get().unwrap())?;

        sql_query("DROP TABLE IF EXISTS identity;").execute(&self.pool.get().unwrap())?;

        Ok(())
    }

    pub fn load_peer_service(&self) -> Result<Option<Service>, StoreError> {
        use crate::store::schema::identity::dsl::*;

        // Find service id and last page
        let results = identity
            .select((service_id, public_key, private_key, secret_key, last_page))
            .load::<(String, String, String, Option<String>, String)>(&self.pool.get().unwrap())?;

        if results.len() != 1 {
            return Ok(None);
        }

        let (_s_id, s_pub_key, s_pri_key, s_sec_key, page_sig) = &results[0];

        let sig = Signature::from_str(&page_sig).unwrap();
        let pub_key = PublicKey::from_str(&s_pub_key).unwrap();
        let keys = Keys::new(pub_key);

        // Load page
        let page = match self.load_page(&sig, &keys)? {
            Some(v) => v,
            None => return Ok(None),
        };

        // Generate service
        let mut service = Service::load(&page).unwrap();

        service.set_private_key(Some(PrivateKey::from_str(s_pri_key).unwrap()));
        let sec_key = s_sec_key.as_ref().map(|v| SecretKey::from_str(&v).unwrap());
        service.set_secret_key(sec_key);

        Ok(Some(service))
    }

    pub fn set_peer_service<T: ImmutableData>(
        &self,
        service: &Service,
        page: &Container<T>,
    ) -> Result<(), StoreError> {
        use crate::store::schema::identity::dsl::*;

        let pub_key = public_key.eq(service.public_key().to_string());
        let pri_key = service.private_key().map(|v| private_key.eq(v.to_string()));
        let sec_key = service.secret_key().map(|v| secret_key.eq(v.to_string()));
        let sig = last_page.eq(page.signature().to_string());

        let p_sig = page.signature();

        let keys = Keys::new(service.public_key());

        // Ensure the page has been written
        if self.load_page(&p_sig, &keys)?.is_none() {
            self.save_page(page)?;
        }

        // Setup identity values
        let values = (
            service_id.eq(service.id().to_string()),
            pub_key,
            pri_key,
            sec_key,
            sig,
        );

        // Check if the identity already exists
        let results = identity
            .select(service_id)
            .load::<String>(&self.pool.get().unwrap())?;

        // Create or update
        if results.len() != 0 {
            diesel::update(identity)
                .set(values)
                .execute(&self.pool.get().unwrap())?;
        } else {
            diesel::insert_into(identity)
                .values(values)
                .execute(&self.pool.get().unwrap())?;
        }

        Ok(())
    }
}
