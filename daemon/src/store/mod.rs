use std::str::FromStr;
use std::time::{Duration, SystemTime};

use diesel::connection::SimpleConnection;
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

pub mod error;
pub mod schema;
pub use error::StoreError;

pub mod object;
pub mod peers;
pub mod services;

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
    fn peer_update(&self, _info: &PeerInfo) -> Result<(), StoreError> {
        todo!()
    }

    /// Fetch a peer by ID
    fn peer_get(&self, _id: &Id) -> Result<PeerInfo, StoreError> {
        todo!()
    }

    /// Delete a peer by ID
    fn peer_del(&self, _id: &Id) -> Result<(), StoreError> {
        todo!()
    }

    /// Store a service, updating the object if existing
    fn service_update(&self, _info: &ServiceInfo) -> Result<(), StoreError> {
        todo!()
    }

    /// Fetch a service by ID
    fn service_get(&self, _id: &Id) -> Result<ServiceInfo, StoreError> {
        todo!()
    }

    /// Delete a service by ID
    fn service_del(&self, _id: &Id) -> Result<(), StoreError> {
        todo!()
    }

    /// Store an object, linked to a service ID and signature
    fn object_put<T: ImmutableData>(
        &self,
        _id: &Id,
        _sig: &Signature,
        _page: Container<T>,
    ) -> Result<(), StoreError> {
        todo!()
    }

    /// Fetch an object by signature
    fn object_get(&self, _sig: &Signature) -> Result<Container, StoreError> {
        todo!()
    }

    /// Delete an object by signature
    fn object_del(&self, _sig: &Signature) -> Result<(), StoreError> {
        todo!()
    }
}

/// SQLite3 database connection options
///

#[derive(Clone, PartialEq, Debug)]
pub struct StoreOptions {
    pub enable_wal: bool,
    pub enable_foreign_keys: bool,
    pub busy_timeout: Option<Duration>,
}

impl Default for StoreOptions {
    fn default() -> Self {
        Self {
            enable_wal: true,
            enable_foreign_keys: true,
            busy_timeout: Some(Duration::from_millis(500)),
        }
    }
}

/// Connection customisation for [StoreOptions]
///
/// Enables Write Ahead Logging (WAL) with busy timeouts
///
/// see: <https://stackoverflow.com/questions/57123453/how-to-use-diesel-with-sqlite-connections-and-avoid-database-is-locked-type-of>
impl diesel::r2d2::CustomizeConnection<SqliteConnection, diesel::r2d2::Error> for StoreOptions {
    fn on_acquire(&self, conn: &mut SqliteConnection) -> Result<(), diesel::r2d2::Error> {
        (|| {
            if self.enable_wal {
                conn.batch_execute("PRAGMA journal_mode = WAL; PRAGMA synchronous = NORMAL;")?;
            }
            if self.enable_foreign_keys {
                conn.batch_execute("PRAGMA foreign_keys = ON;")?;
            }
            if let Some(d) = self.busy_timeout {
                conn.batch_execute(&format!("PRAGMA busy_timeout = {};", d.as_millis()))?;
            }
            Ok(())
        })()
        .map_err(diesel::r2d2::Error::QueryError)
    }
}

impl Store {
    /// Create or connect to a store with the provided filename
    pub fn new(path: &str, opts: StoreOptions) -> Result<Self, StoreError> {
        debug!("Connecting to store: {}", path);

        // TODO: check path exists?

        // Setup connection manager / pool
        let mgr = ConnectionManager::new(path);
        let pool = Pool::builder()
            .connection_customizer(Box::new(opts))
            .build(mgr)
            .unwrap();

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
        let mut conn = self.pool.get().unwrap();

        sql_query(
            "CREATE TABLE IF NOT EXISTS services (
            service_id TEXT NOT NULL UNIQUE PRIMARY KEY,
            short_id TEXT NOT NULL UNIQUE,
            service_index INTEGER NOT NULL,
            kind TEXT NOT NULL, 
            state TEXT NOT NULL, 

            public_key TEXT NOT NULL, 
            private_key TEXT, 
            secret_key TEXT, 

            primary_page BLOB, 
            replica_page BLOB, 
            
            last_updated TEXT, 
            subscribers INTEGER NOT NULL, 
            replicas INTEGER NOT NULL,
            flags INTEGER NOT NULL
        );",
        )
        .execute(&mut conn)?;

        sql_query(
            "CREATE TABLE IF NOT EXISTS peers (
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
        .execute(&mut conn)?;

        sql_query(
            "CREATE TABLE IF NOT EXISTS object (
            service_id TEXT NOT NULL,
            object_index INTEGER NOT NULL,

            raw_data BLOB NOT NULL,

            previous TEXT,
            signature TEXT NOT NULL PRIMARY KEY
        );",
        )
        .execute(&mut conn)?;

        sql_query(
            "CREATE TABLE IF NOT EXISTS identity (
            service_id TEXT NOT NULL PRIMARY KEY,

            public_key TEXT NOT NULL,
            private_key TEXT NOT NULL,
            secret_key TEXT,
            
            last_page TEXT NOT NULL
        );",
        )
        .execute(&mut conn)?;

        Ok(())
    }

    pub fn drop_tables(&self) -> Result<(), StoreError> {
        let mut conn = self.pool.get().unwrap();

        sql_query("DROP TABLE IF EXISTS services;").execute(&mut conn)?;

        sql_query("DROP TABLE IF EXISTS peers;").execute(&mut conn)?;

        sql_query("DROP TABLE IF EXISTS data;").execute(&mut conn)?;

        sql_query("DROP TABLE IF EXISTS object;").execute(&mut conn)?;

        sql_query("DROP TABLE IF EXISTS identity;").execute(&mut conn)?;

        Ok(())
    }

    pub fn load_peer_service(&self) -> Result<Option<Service>, StoreError> {
        use crate::store::schema::identity::dsl::*;

        // Find service id and last page
        let results = identity
            .select((service_id, public_key, private_key, secret_key, last_page))
            .load::<(String, String, String, Option<String>, String)>(
                &mut self.pool.get().unwrap(),
            )?;

        if results.len() != 1 {
            return Ok(None);
        }

        let (s_id, s_pub_key, s_pri_key, s_sec_key, page_sig) = &results[0];

        let id = Id::from_str(&s_id).unwrap();
        let sig = Signature::from_str(&page_sig).unwrap();
        let pub_key = PublicKey::from_str(&s_pub_key).unwrap();
        let keys = Keys::new(pub_key);

        // Load page
        let page = match self.load_object(&id, &sig, &keys)? {
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

        let mut conn = self.pool.get().unwrap();

        let pub_key = public_key.eq(service.public_key().to_string());
        let pri_key = service.private_key().map(|v| private_key.eq(v.to_string()));
        let sec_key = service.secret_key().map(|v| secret_key.eq(v.to_string()));
        let sig = last_page.eq(page.signature().to_string());

        let p_sig = page.signature();

        let keys = Keys::new(service.public_key());

        // Ensure the page has been written
        if self.load_object(&service.id(), &p_sig, &keys)?.is_none() {
            self.save_object(page)?;
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
        let results = identity.select(service_id).load::<String>(&mut conn)?;

        // Create or update
        if results.len() != 0 {
            diesel::update(identity).set(values).execute(&mut conn)?;
        } else {
            diesel::insert_into(identity)
                .values(values)
                .execute(&mut conn)?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempdir::TempDir;
    use test::Bencher;

    /// Helper to setup the store for reading / writing
    fn setup_store(store: &mut Store) -> Service {
        // Ensure tables are configured
        store.drop_tables().unwrap();
        store.create_tables().unwrap();

        // Create new service
        let mut s = Service::<Vec<u8>>::default();
        let keys = s.keys();

        let (_n, page) = s
            .publish_primary_buff(Default::default())
            .expect("Error creating page");
        let sig = page.signature();

        // Check no matching service exists
        assert_eq!(None, store.load_object(&s.id(), &sig, &keys).unwrap());

        // Store service page
        store.save_object(&page).unwrap();
        assert_eq!(
            Some(&page.to_owned()),
            store.load_object(&s.id(), &sig, &keys).unwrap().as_ref()
        );

        // Return service instance
        s
    }

    /// Helper to publish a data object and write this to the store
    fn test_write_data(store: &mut Store, s: &mut Service) {
        // Build new data object
        let (_n, data) = s.publish_data_buff::<Vec<u8>>(Default::default()).unwrap();

        // Store data object
        store.save_object(&data).unwrap();

        // Load data object
        assert_eq!(
            Some(&data.to_owned()),
            store
                .load_object(&s.id(), &data.signature(), &s.keys())
                .unwrap()
                .as_ref()
        );
    }

    /// Benchmark in-memory database
    #[bench]
    fn bench_db_mem(b: &mut Bencher) {
        let mut store = Store::new("sqlite://:memory:?cache=shared", Default::default())
            .expect("Error opening store");

        let mut svc = setup_store(&mut store);

        b.iter(|| {
            test_write_data(&mut store, &mut svc);
        })
    }

    /// Benchmark file based database without WAL
    #[bench]
    fn bench_db_file_nowal(b: &mut Bencher) {
        let d = TempDir::new("dsf-db").unwrap();
        let d = d.path().to_str().unwrap().to_string();

        let mut store = Store::new(
            &format!("{d}/sqlite-nowal.db"),
            StoreOptions {
                enable_wal: false,
                ..Default::default()
            },
        )
        .expect("Error opening store");

        let mut svc = setup_store(&mut store);

        b.iter(|| {
            test_write_data(&mut store, &mut svc);
        })
    }

    /// Benchmark file based database with WAL
    #[bench]
    fn bench_db_file_wal(b: &mut Bencher) {
        let d = TempDir::new("dsf-db").unwrap();
        let d = d.path().to_str().unwrap().to_string();

        let mut store = Store::new(&format!("{d}/sqlite-wal.db"), Default::default())
            .expect("Error opening store");

        let mut svc = setup_store(&mut store);

        b.iter(|| {
            test_write_data(&mut store, &mut svc);
        })
    }
}
