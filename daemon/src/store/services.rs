use std::str::FromStr;

use log::{debug, error, info, trace, warn};

use diesel::prelude::*;

use chrono::NaiveDateTime;

use dsf_core::prelude::*;
use dsf_rpc::{ServiceInfo, ServiceState};

use super::{from_dt, to_dt, Store, StoreError};

type ServiceFields = (
    String,
    i32,
    String,
    String,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<NaiveDateTime>,
    i32,
    i32,
    bool,
    bool,
);

impl Store {
    // Store an item
    pub fn save_service(&self, info: &ServiceInfo) -> Result<(), StoreError> {
        use crate::store::schema::services::dsl::*;

        let pri_key = info
            .private_key
            .as_ref()
            .map(|v| private_key.eq(v.to_string()));
        let sec_key = info
            .secret_key
            .as_ref()
            .map(|v| secret_key.eq(v.to_string()));
        let up = info.last_updated.map(|v| last_updated.eq(to_dt(v)));

        let pp = info
            .primary_page
            .as_ref()
            .map(|v| primary_page.eq(v.to_string()));
        let rp = info
            .replica_page
            .as_ref()
            .map(|v| replica_page.eq(v.to_string()));

        let values = (
            service_id.eq(info.id.to_string()),
            service_index.eq(info.index as i32),
            state.eq(info.state.to_string()),
            public_key.eq(info.public_key.to_string()),
            pri_key,
            sec_key,
            pp,
            rp,
            up,
            subscribers.eq(info.subscribers as i32),
            replicas.eq(info.replicas as i32),
            original.eq(info.origin),
            subscribed.eq(info.subscribed),
        );

        let r = services
            .filter(service_id.eq(info.id.to_string()))
            .select(service_index)
            .load::<i32>(&self.pool.get().unwrap())?;

        if r.len() != 0 {
            diesel::update(services)
                .filter(service_id.eq(info.id.to_string()))
                .set(values)
                .execute(&self.pool.get().unwrap())?;
        } else {
            diesel::insert_into(services)
                .values(values)
                .execute(&self.pool.get().unwrap())?;
        }

        Ok(())
    }

    // Find an item or items
    pub fn find_service(&self, id: &Id) -> Result<Option<ServiceInfo>, StoreError> {
        use crate::store::schema::services::dsl::*;

        let results = services
            .filter(service_id.eq(id.to_string()))
            .select((
                service_id,
                service_index,
                state,
                public_key,
                private_key,
                secret_key,
                primary_page,
                replica_page,
                last_updated,
                subscribers,
                replicas,
                original,
                subscribed,
            ))
            .load::<ServiceFields>(&self.pool.get().unwrap())?;

        let mut v = vec![];
        for r in &results {
            v.push(Self::parse_service(r)?);
        }

        Ok(v.get(0).map(|v| v.clone()))
    }

    // Load all items
    pub fn load_services(&self) -> Result<Vec<ServiceInfo>, StoreError> {
        use crate::store::schema::services::dsl::*;

        let results = services
            .select((
                service_id,
                service_index,
                state,
                public_key,
                private_key,
                secret_key,
                primary_page,
                replica_page,
                last_updated,
                subscribers,
                replicas,
                original,
                subscribed,
            ))
            .load::<ServiceFields>(&self.pool.get().unwrap())?;

        let mut v = vec![];
        for r in &results {
            v.push(Self::parse_service(r)?);
        }

        Ok(v)
    }

    pub fn delete_service(&self, info: &ServiceInfo) -> Result<(), StoreError> {
        use crate::store::schema::services::dsl::*;

        diesel::delete(services)
            .filter(service_id.eq(info.id.to_string()))
            .execute(&self.pool.get().unwrap())?;

        Ok(())
    }

    fn parse_service(v: &ServiceFields) -> Result<ServiceInfo, StoreError> {
        let (
            r_id,
            r_index,
            r_state,
            r_pub_key,
            r_pri_key,
            r_sec_key,
            r_pp,
            r_rp,
            r_upd,
            r_subs,
            r_reps,
            r_original,
            r_subscribed,
        ) = v;

        let s = ServiceInfo {
            id: Id::from_str(r_id)?,
            index: *r_index as usize,
            state: ServiceState::from_str(r_state)?,

            primary_page: r_pp.as_ref().map(|v| Signature::from_str(&v).unwrap()),
            replica_page: r_rp.as_ref().map(|v| Signature::from_str(&v).unwrap()),

            public_key: PublicKey::from_str(r_pub_key)?,
            private_key: r_pri_key
                .as_ref()
                .map(|v| PrivateKey::from_str(&v).unwrap()),
            secret_key: r_sec_key.as_ref().map(|v| SecretKey::from_str(&v).unwrap()),

            last_updated: r_upd.as_ref().map(|v| from_dt(v)),

            subscribers: *r_subs as usize,
            replicas: *r_reps as usize,
            origin: *r_original,
            subscribed: *r_subscribed,
        };

        Ok(s)
    }
}

#[cfg(test)]
mod test {
    use std::time::SystemTime;

    extern crate tracing_subscriber;
    use tracing_subscriber::{filter::LevelFilter, FmtSubscriber};

    use super::Store;

    use dsf_core::crypto::{Crypto, Hash, PubKey, SecKey};
    use dsf_core::{base::Body, types::Id};
    use dsf_rpc::{ServiceInfo, ServiceState};

    #[test]
    fn store_service_info() {
        let _ = FmtSubscriber::builder()
            .with_max_level(LevelFilter::DEBUG)
            .try_init();

        let store = Store::new("/tmp/dsf-test-1.db").expect("Error opening store");

        store.drop_tables().unwrap();

        store.create_tables().unwrap();

        let (public_key, private_key) = Crypto::new_pk().unwrap();
        let secret_key = Crypto::new_sk().unwrap();
        let id: Id = Crypto::hash(&public_key).unwrap().into();
        let sig = Crypto::pk_sign(&private_key, &id).unwrap();

        let mut s = ServiceInfo {
            id,
            index: 10,
            state: ServiceState::Registered,
            public_key,
            private_key: Some(private_key),
            secret_key: Some(secret_key),
            primary_page: Some(sig.clone()),
            replica_page: Some(sig),
            last_updated: Some(SystemTime::now()),
            subscribers: 14,
            replicas: 12,
            origin: true,
            subscribed: false,
        };

        // Check no matching service exists
        assert_eq!(None, store.find_service(&s.id).unwrap());

        // Store service
        store.save_service(&s).unwrap();
        assert_eq!(Some(&s), store.find_service(&s.id).unwrap().as_ref());

        // update service
        s.subscribers = 0;
        store.save_service(&s).unwrap();
        assert_eq!(Some(&s), store.find_service(&s.id).unwrap().as_ref());

        // Delete service
        store.delete_service(&s).unwrap();
        assert_eq!(None, store.find_service(&s.id).unwrap());
    }
}
