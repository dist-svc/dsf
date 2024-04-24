use diesel::prelude::*;
use diesel::sqlite::Sqlite;
use diesel::*;

use super::StoreError;

table! {
    services (service_id) {
        service_id -> Text,
        service_index -> Integer,
        kind -> Text,
        state -> Text,

        public_key -> Text,
        private_key -> Nullable<Text>,
        secret_key -> Nullable<Text>,

        primary_page -> Nullable<Text>,
        replica_page -> Nullable<Text>,

        last_updated -> Nullable<Timestamp>,

        subscribers -> Integer,
        replicas -> Integer,
        flags -> Integer,
    }
}

table! {
    peers (peer_id) {
        peer_id -> Text,
        peer_index -> Integer,
        state -> Text,
        public_key -> Nullable<Text>,

        address -> Text,
        address_mode -> Text,

        last_seen -> Nullable<Timestamp>,

        sent -> Integer,
        received -> Integer,
        blocked -> Bool,
    }
}

table! {
    peer_addresses (peer_id) {
        peer_id -> Text,

        address -> Text,
        address_mode -> Text,

        last_used -> Nullable<Timestamp>,
    }
}

table! {
    subscriptions (service_id, peer_id) {
        service_id -> Text,
        peer_id -> Text,

        last_updated -> Nullable<Timestamp>,
        expiry -> Nullable<Timestamp>,
    }
}

table! {
    subscribers (service_id, peer_id) {
        service_id -> Text,
        peer_id -> Text,

        last_updated -> Nullable<Timestamp>,
        expiry -> Nullable<Timestamp>,
    }
}

table! {
    object (signature) {
        service_id -> Text,
        object_index -> Integer,

        raw_data -> Blob,

        previous -> Nullable<Text>,
        signature -> Text,
    }
}

table! {
    identity (service_id) {
        service_id -> Text,

        public_key -> Text,
        private_key -> Text,
        secret_key -> Nullable<Text>,

        last_page -> Text,
    }
}

/// Initialise database tables
///
/// This is called automatically in the `new` function
pub(super) fn create_tables<C: Connection<Backend = Sqlite>>(
    conn: &mut C,
) -> Result<(), StoreError> {
    sql_query(
        "CREATE TABLE IF NOT EXISTS services (
        service_id TEXT NOT NULL UNIQUE PRIMARY KEY,
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
    .execute(conn)?;

    sql_query("CREATE INDEX IF NOT EXISTS service_id_idx ON services(service_id);")
        .execute(conn)?;

    sql_query(
        "CREATE TABLE IF NOT EXISTS peers (
        peer_id TEXT NOT NULL UNIQUE PRIMARY KEY,
        peer_index INTEGER NOT NULL,
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
    .execute(conn)?;

    sql_query("CREATE INDEX IF NOT EXISTS peer_id_idx ON peers(peer_id);").execute(conn)?;

    sql_query(
        "CREATE TABLE IF NOT EXISTS object (
        service_id TEXT NOT NULL,
        object_index INTEGER NOT NULL,

        raw_data BLOB NOT NULL,

        previous TEXT,
        signature TEXT NOT NULL PRIMARY KEY
    );",
    )
    .execute(conn)?;

    sql_query("CREATE INDEX IF NOT EXISTS object_sig_idx ON object(signature);").execute(conn)?;
    sql_query("CREATE INDEX IF NOT EXISTS object_svc_idx ON object(service_id);").execute(conn)?;
    sql_query("CREATE INDEX IF NOT EXISTS object_idx_idx ON object(object_index);").execute(conn)?;

    sql_query(
        "CREATE TABLE IF NOT EXISTS identity (
        service_id TEXT NOT NULL PRIMARY KEY,

        public_key TEXT NOT NULL,
        private_key TEXT NOT NULL,
        secret_key TEXT,
        
        last_page TEXT NOT NULL
    );",
    )
    .execute(conn)?;

    Ok(())
}

/// Drop database tables
pub(super) fn drop_tables<C: Connection<Backend = Sqlite>>(conn: &mut C) -> Result<(), StoreError> {
    sql_query("DROP TABLE IF EXISTS services;").execute(conn)?;

    sql_query("DROP TABLE IF EXISTS peers;").execute(conn)?;

    sql_query("DROP TABLE IF EXISTS data;").execute(conn)?;

    sql_query("DROP TABLE IF EXISTS object;").execute(conn)?;

    sql_query("DROP TABLE IF EXISTS identity;").execute(conn)?;

    Ok(())
}
