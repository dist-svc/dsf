use std::str::FromStr;
use std::time::{Duration, SystemTime};

use diesel::connection::{LoadConnection, SimpleConnection};
use dsf_core::types::ImmutableData;
use dsf_core::wire::Container;
use dsf_rpc::{PeerInfo, ServiceInfo};
use log::{debug, error, warn};

use diesel::dsl::sql_query;
use diesel::r2d2::{ConnectionManager, Pool};
use diesel::sqlite::{Sqlite, SqliteConnection};
use diesel::{prelude::*, result};

use chrono::{DateTime, Local, NaiveDateTime, TimeZone};

use dsf_core::prelude::*;
use dsf_core::service::Subscriber;

use crate::store::object::ObjectIdentifier;

use super::{
    object::{load_object, save_object},
    Backend, Store, StoreError,
};

impl<B: Backend> Store<B> {
    pub fn load_service_auths(&self, service_id: &Id) -> Result<Vec<(Id, String)>, StoreError> {
        self.with(|conn| load_service_auths(conn, service_id))
    }

    pub fn create_update_service_auth(
        &self,
        service_id: &Id,
        peer_id: &Id,
        role: &str,
    ) -> Result<(), StoreError> {
        self.with(|conn| create_update_service_auth(conn, service_id, peer_id, role))
    }

    pub fn delete_service_auth(&self, service_id: &Id, peer_id: &Id) -> Result<(), StoreError> {
        self.with(|conn| delete_service_auth(conn, service_id, peer_id))
    }
}

pub fn load_service_auths<C: Connection<Backend = Sqlite> + LoadConnection>(
    conn: &mut C,
    id: &Id,
) -> Result<Vec<(Id, String)>, StoreError> {
    use crate::store::schema::auth::dsl::*;

    // Find service id and last page
    let results = auth
        .filter(service_id.eq(id.to_string()))
        .select((service_id, peer_id, role))
        .load::<(String, String, String)>(conn)?
        .iter()
        .map(|r| (Id::from_str(&r.1).unwrap(), (&r.2).to_string()))
        .collect();

    Ok(results)
}

pub fn create_update_service_auth<C: Connection<Backend = Sqlite> + LoadConnection>(
    conn: &mut C,
    s_id: &Id,
    p_id: &Id,
    r: &str,
) -> Result<(), StoreError> {
    use crate::store::schema::auth::dsl::*;

    // Find service id and last page
    let result = auth
        .filter(service_id.eq(s_id.to_string()))
        .filter(peer_id.eq(p_id.to_string()))
        .select(role)
        .load::<String>(conn)?;

    if !result.is_empty() {
        diesel::update(auth)
            .filter(service_id.eq(s_id.to_string()))
            .filter(peer_id.eq(p_id.to_string()))
            .set(role.eq(r.to_string()))
            .execute(conn)?;
    } else {
        diesel::insert_into(auth)
            .values((
                service_id.eq(s_id.to_string()),
                peer_id.eq(p_id.to_string()),
                role.eq(r),
            ))
            .execute(conn)?;
    }

    Ok(())
}

fn delete_service_auth<C: Connection<Backend = Sqlite> + LoadConnection>(
    conn: &mut C,
    s_id: &Id,
    p_id: &Id,
) -> Result<(), StoreError> {
    use crate::store::schema::auth::dsl::*;

    diesel::delete(auth)
        .filter(service_id.eq(s_id.to_string()))
        .filter(peer_id.eq(p_id.to_string()))
        .execute(conn)?;

    Ok(())
}
