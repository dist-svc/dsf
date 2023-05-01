use diesel::prelude::*;

use dsf_core::{
    keys::KeySource, options::Filters, prelude::*, types::ImmutableData, wire::Container,
};
use log::error;

use super::{Store, StoreError, schema::object};

use crate::store::schema::object::dsl::*;

pub type PageFields = (String, Vec<u8>, Option<String>, String);

impl Store {
    // Store an item
    pub fn save_object<T: ImmutableData>(&self, page: &Container<T>) -> Result<(), StoreError> {
        // TODO: is it possible to have an invalid container here?
        // constructors _should_ make this impossible, but, needs to be checked.
        let sig = signature.eq(page.signature().to_string());
        let idx = object_index.eq(page.header().index() as i32);
        let raw = raw_data.eq(page.raw());

        let prev = page
            .public_options_iter()
            .prev_sig()
            .as_ref()
            .map(|v| previous.eq(v.to_string()));
        let values = (service_id.eq(page.id().to_string()), idx, raw, prev, sig.clone());

        let r = object
            .filter(sig.clone())
            .select(service_id)
            .load::<String>(&self.pool.get().unwrap())?;

        if r.len() != 0 {
            diesel::update(object)
                .filter(sig)
                .set(values)
                .execute(&self.pool.get().unwrap())?;
        } else {
            diesel::insert_into(object)
                .values(values)
                .execute(&self.pool.get().unwrap())?;
        }

        Ok(())
    }

    // Find an item or items
    pub fn find_objects<K: KeySource>(
        &self,
        id: &Id,
        key_source: &K,
    ) -> Result<Vec<Container>, StoreError> {
        let results = object
            .filter(service_id.eq(id.to_string()))
            .select((service_id, raw_data, previous, signature))
            .load::<PageFields>(&self.pool.get().unwrap())?;

        let mut objects = Vec::with_capacity(results.len());

        for r in results {
            let (_r_id, r_raw, _r_previous, r_signature) = &r;

            let v = match Container::parse(r_raw.clone(), key_source) {
                Ok(v) => v,
                Err(e) => {
                    error!("Failed to decode object {}: {:?}", r_signature, e);
                    continue;
                }
            };

            objects.push(v);
        }

        Ok(objects)
    }

    // Delete an item
    pub fn delete_object(&self, sig: &Signature) -> Result<(), StoreError> {
        diesel::delete(object)
            .filter(signature.eq(sig.to_string()))
            .execute(&self.pool.get().unwrap())?;

        Ok(())
    }

    pub fn load_object<K: KeySource>(
        &self,
        sig: &Signature,
        key_source: &K,
    ) -> Result<Option<Container>, StoreError> {
        let results = object
            .filter(signature.eq(sig.to_string()))
            .select((service_id, raw_data, previous, signature))
            .load::<PageFields>(&self.pool.get().unwrap())?;

        if results.len() == 0 {
            return Ok(None);
        }

        let (_r_id, r_raw, _r_previous, _r_signature) = &results[0];

        let c = Container::parse(r_raw.to_vec(), key_source)
            .map_err(|_e| StoreError::Decode)?;

        Ok(Some(c))
    }
}

#[cfg(test)]
mod test {
    extern crate tracing_subscriber;
    use std::convert::TryFrom;

    use tracing_subscriber::{filter::LevelFilter, FmtSubscriber};

    use super::Store;

    use dsf_core::service::{Publisher, Service};

    #[test]
    fn store_page_inst() {
        let _ = FmtSubscriber::builder()
            .with_max_level(LevelFilter::DEBUG)
            .try_init();

        let store = Store::new("/tmp/dsf-test-4.db").expect("Error opening store");

        store.drop_tables().unwrap();
        store.create_tables().unwrap();

        let mut s = Service::<Vec<u8>>::default();
        let keys = s.keys();

        let (_n, page) = s
            .publish_primary_buff(Default::default())
            .expect("Error creating page");
        let sig = page.signature();

        // Check no matching service exists
        assert_eq!(None, store.load_object(&sig, &keys).unwrap());

        // Store data
        store.save_object(&page).unwrap();
        assert_eq!(
            Some(&page.to_owned()),
            store.load_object(&sig, &keys).unwrap().as_ref()
        );
        assert_eq!(
            vec![page.to_owned()],
            store.find_objects(&s.id(), &keys).unwrap()
        );

        // Delete data
        store.delete_object(&sig).unwrap();
        assert_eq!(None, store.load_object(&sig, &keys).unwrap());
    }
}
