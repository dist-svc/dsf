use std::cell::RefCell;
use std::str::FromStr;
use std::time::{Duration, SystemTime};

use diesel::connection::{LoadConnection, SimpleConnection};
use dsf_core::types::ImmutableData;
use dsf_core::wire::Container;
use dsf_rpc::{PeerInfo, ServiceInfo};
use log::{debug, error, warn};

use diesel::dsl::sql_query;
use diesel::prelude::*;
use diesel::r2d2::{ConnectionManager, Pool, PooledConnection};
use diesel::sqlite::{Sqlite, SqliteConnection};

use chrono::{DateTime, Local, NaiveDateTime, TimeZone};

use dsf_core::prelude::*;
use dsf_core::service::Subscriber;

pub mod error;
pub mod schema;
pub use error::StoreError;
use tokio::select;
use tokio::sync::oneshot;
use tokio::sync::{
    mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
    oneshot::Sender as OneshotSender,
};

pub mod auth;
pub mod ident;
pub mod object;
pub mod peers;
pub mod services;

/// Store wraps a backend database connection to provide typed interfaces to the database
#[derive(Clone)]
pub struct Store<B: Backend = Pool<ConnectionManager<SqliteConnection>>> {
    b: B,
}

pub type RefCellStore = Store<RefCell<SqliteConnection>>;

pub type PoolStore = Store<Pool<ConnectionManager<SqliteConnection>>>;

/// Backend abstraction to implement database operations over different connection types

pub trait Backend {
    type Conn: Connection<Backend = Sqlite> + LoadConnection;

    /// Execute an function using the provided connection
    fn with<R>(
        &self,
        f: impl FnMut(&mut Self::Conn) -> Result<R, StoreError>,
    ) -> Result<R, StoreError>;
}

pub trait BackendConnection: Connection<Backend = Sqlite> + LoadConnection {}

impl<C: Connection<Backend = Sqlite> + LoadConnection> BackendConnection for C {}

/// [Backend] implementation for pooled connections
impl Backend for Pool<ConnectionManager<SqliteConnection>> {
    type Conn = PooledConnection<ConnectionManager<SqliteConnection>>;

    fn with<R>(
        &self,
        mut f: impl FnMut(&mut Self::Conn) -> Result<R, StoreError>,
    ) -> Result<R, StoreError> {
        let mut c = self.get().map_err(|_| StoreError::Acquire)?;
        f(&mut c)
    }
}

/// [Backend] implementation for unshared (RefCell-based) connections
impl Backend for RefCell<SqliteConnection> {
    type Conn = SqliteConnection;

    fn with<R>(
        &self,
        mut f: impl FnMut(&mut Self::Conn) -> Result<R, StoreError>,
    ) -> Result<R, StoreError> {
        let mut c = self.try_borrow_mut().map_err(|_| StoreError::Acquire)?;
        f(&mut c)
    }
}

/// [Backend] implementation for [Store] to avoid direct access
impl<B: Backend> Backend for Store<B> {
    type Conn = <B as Backend>::Conn;

    fn with<R>(
        &self,
        f: impl FnMut(&mut Self::Conn) -> Result<R, StoreError>,
    ) -> Result<R, StoreError> {
        <B as Backend>::with(&self.b, f)
    }
}

fn to_dt(s: SystemTime) -> NaiveDateTime {
    DateTime::<Local>::from(s).naive_utc()
}

fn from_dt(n: &NaiveDateTime) -> SystemTime {
    let l = chrono::offset::Local;
    let dt = l.from_utc_datetime(n);
    dt.into()
}

/// SQLite3 database connection options
///
#[derive(Clone, PartialEq, Debug)]
pub struct StoreOptions {
    pub enable_wal: bool,
    pub synchronous: bool,
    pub enable_foreign_keys: bool,
    pub busy_timeout: Option<Duration>,
}

impl Default for StoreOptions {
    fn default() -> Self {
        Self {
            enable_wal: true,
            synchronous: false,
            enable_foreign_keys: true,
            busy_timeout: Some(Duration::from_millis(5_000)),
        }
    }
}

impl StoreOptions {
    /// Apply Sqlite connection options
    pub fn apply<C: Connection<Backend = Sqlite>>(&self, conn: &mut C) -> diesel::QueryResult<()> {
        if self.enable_wal {
            conn.batch_execute("PRAGMA journal_mode = WAL;")?;
        }
        if self.synchronous {
            conn.batch_execute("PRAGMA synchronous = NORMAL;")?;
        } else {
            conn.batch_execute("PRAGMA synchronous = OFF;")?;
        }
        if self.enable_foreign_keys {
            conn.batch_execute("PRAGMA foreign_keys = ON;")?;
        }
        if let Some(d) = self.busy_timeout {
            conn.batch_execute(&format!("PRAGMA busy_timeout = {};", d.as_millis()))?;
        }
        Ok(())
    }
}

/// Connection customisation for r2d2 pool via [StoreOptions]
///
/// Enables Write Ahead Logging (WAL) with busy timeouts
///
/// see: <https://stackoverflow.com/questions/57123453/how-to-use-diesel-with-sqlite-connections-and-avoid-database-is-locked-type-of>
impl diesel::r2d2::CustomizeConnection<SqliteConnection, diesel::r2d2::Error> for StoreOptions {
    fn on_acquire(&self, conn: &mut SqliteConnection) -> Result<(), diesel::r2d2::Error> {
        (|| {
            self.apply(conn)?;
            Ok(())
        })()
        .map_err(diesel::r2d2::Error::QueryError)
    }
}

impl Store<Pool<ConnectionManager<SqliteConnection>>> {
    /// Create a new r2d2 pooled [PoolStore] and connect to the specified database
    pub fn new_pooled(path: &str, opts: StoreOptions) -> Result<Self, StoreError> {
        debug!("Connecting to store: {}", path);

        // Setup connection manager / pool
        let mgr = ConnectionManager::new(path);
        let pool = Pool::builder()
            .connection_customizer(Box::new(opts))
            .build(mgr)
            .unwrap();

        // TODO(low): check connection is okay
        let _c = pool.get().map_err(|_| StoreError::Acquire)?;

        // Create object
        let s = Self { b: pool };

        // Ensure tables exist
        let _ = s.b.with(schema::create_tables);

        Ok(s)
    }
}

impl Store<RefCell<SqliteConnection>> {
    /// Create a new singleton [RefCellStore] and connect to the specified database
    pub fn new_rc(path: &str, opts: StoreOptions) -> Result<Self, StoreError> {
        debug!("Connecting to store: {}", path);

        // Setup connection
        let mut conn = SqliteConnection::establish(path)?;

        // Configure
        opts.apply(&mut conn)?;

        // Create object
        let s = Self {
            b: RefCell::new(conn),
        };

        // Ensure tables exist
        let _ = s.b.with(schema::create_tables);

        Ok(s)
    }
}

impl<B: Backend> Store<B> {
    pub fn create_tables(&self) -> Result<(), StoreError> {
        self.with(schema::create_tables)
    }

    pub fn drop_tables(&self) -> Result<(), StoreError> {
        self.with(schema::drop_tables)
    }
}

#[cfg(test)]
mod tests {
    use super::{object::save_objects, *};
    use rand::random;
    use tempdir::TempDir;
    use test::Bencher;

    /// Helper to setup the store for reading / writing
    fn setup_store<B: Backend>(store: &mut Store<B>) -> Service {
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
    fn test_write_data<B: Backend>(store: &mut Store<B>, s: &mut Service) {
        // Build new data object
        let (_n, data) = s.publish_data_buff::<Vec<u8>>(Default::default()).unwrap();

        // Store data object
        store.save_object(&data).unwrap();

        // Load data object
        #[cfg(nope)]
        assert_eq!(
            Some(&data.to_owned()),
            store
                .load_object(&s.id(), &data.signature(), &s.keys())
                .unwrap()
                .as_ref()
        );
    }

    /// Benchmark in-memory database reads
    #[bench]
    fn bench_db_write_mem(b: &mut Bencher) {
        let mut store =
            Store::new_rc("sqlite://:memory:", Default::default()).expect("Error opening store");

        let mut svc = setup_store(&mut store);

        b.iter(|| {
            test_write_data(&mut store, &mut svc);
        })
    }

    /// Benchmark in-memory database
    #[bench]
    fn bench_db_read_mem(b: &mut Bencher) {
        let mut store =
            Store::new_rc("sqlite://:memory:", Default::default()).expect("Error opening store");

        let mut svc = setup_store(&mut store);

        let data: Vec<_> = (0..100)
            .map(|_| {
                svc.publish_data_buff::<Vec<u8>>(Default::default())
                    .unwrap()
                    .1
            })
            .collect();

        store.with(|conn| save_objects(conn, &data[..])).unwrap();

        b.iter(|| {
            let d: &Container<_> = &data[random::<usize>() % data.len()];
            store
                .load_object(&svc.id(), &d.signature(), &svc.keys())
                .unwrap();
        })
    }

    /// Benchmark in-memory database w/ batched writes
    #[bench]
    fn bench_db_write_mem_batch(b: &mut Bencher) {
        let mut store =
            Store::new_rc("sqlite://:memory:", Default::default()).expect("Error opening store");

        let mut svc = setup_store(&mut store);

        b.iter(|| {
            let data: Vec<_> = (0..100)
                .map(|_| {
                    svc.publish_data_buff::<Vec<u8>>(Default::default())
                        .unwrap()
                        .1
                })
                .collect();

            store.with(|conn| save_objects(conn, &data[..])).unwrap();
        })
    }

    /// Benchmark file based database write without WAL
    #[bench]
    fn bench_db_write_file_nowal(b: &mut Bencher) {
        let d = TempDir::new("dsf-db").unwrap();
        let d = d.path().to_str().unwrap().to_string();

        let mut store = Store::new_rc(
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

    #[bench]
    fn bench_db_read_file(b: &mut Bencher) {
        let d = TempDir::new("dsf-db").unwrap();
        let d = d.path().to_str().unwrap().to_string();

        let mut store = Store::new_rc(
            &format!("{d}/sqlite-nowal.db"),
            StoreOptions {
                enable_wal: false,
                ..Default::default()
            },
        )
        .expect("Error opening store");

        let mut svc = setup_store(&mut store);

        let data: Vec<_> = (0..100)
            .map(|_| {
                svc.publish_data_buff::<Vec<u8>>(Default::default())
                    .unwrap()
                    .1
            })
            .collect();

        store.with(|conn| save_objects(conn, &data[..])).unwrap();

        b.iter(|| {
            let d: &Container<_> = &data[random::<usize>() % data.len()];
            store
                .load_object(&svc.id(), &d.signature(), &svc.keys())
                .unwrap();
        })
    }

    /// Benchmark file based database with WAL
    #[bench]
    fn bench_db_write_file_wal(b: &mut Bencher) {
        let d = TempDir::new("dsf-db").unwrap();
        let d = d.path().to_str().unwrap().to_string();

        let mut store = Store::new_rc(
            &format!("{d}/sqlite-wal.db"),
            StoreOptions {
                enable_wal: true,
                ..Default::default()
            },
        )
        .expect("Error opening store");

        let mut svc = setup_store(&mut store);

        b.iter(|| {
            test_write_data(&mut store, &mut svc);
        })
    }
}
