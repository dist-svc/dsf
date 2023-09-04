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

use super::{Store, StoreError};

impl Store {
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
