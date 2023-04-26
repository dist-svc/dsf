use std::str::FromStr;

use diesel::prelude::*;

use dsf_core::prelude::*;
use dsf_rpc::DataInfo;

use super::{Store, StoreError};

use crate::store::schema::data::dsl::*;

pub type DataFields = (String, i32, String, Option<Vec<u8>>, Option<String>, String);

const BODY_NONE: &str = "none";
const BODY_CLEARTEXT: &str = "cleartext";
const BODY_ENCRYPTED: &str = "encrypted";

impl Store {
    pub fn save_data(&self, info: &DataInfo) -> Result<(), StoreError> {
        let (bk, bv) = match &info.body {
            Body::None => (BODY_NONE.to_string(), None),
            Body::Cleartext(v) => (BODY_CLEARTEXT.to_string(), Some(body_value.eq(v.clone()))),
            Body::Encrypted(v) => (BODY_ENCRYPTED.to_string(), Some(body_value.eq(v.clone()))),
        };

        let p = info.previous.as_ref().map(|v| previous.eq(v.to_string()));

        let values = (
            service_id.eq(info.service.to_string()),
            object_index.eq(info.index as i32),
            body_kind.eq(bk),
            bv,
            p,
            signature.eq(info.signature.to_string()),
        );

        let r = data
            .filter(signature.eq(info.signature.to_string()))
            .select(service_id)
            .load::<String>(&self.pool.get().unwrap())?;

        if r.len() != 0 {
            diesel::update(data)
                .filter(signature.eq(info.signature.to_string()))
                .set(values)
                .execute(&self.pool.get().unwrap())?;
        } else {
            diesel::insert_into(data)
                .values(values)
                .execute(&self.pool.get().unwrap())?;
        }

        Ok(())
    }

    // Find an item or items
    pub fn find_data(&self, id: &Id) -> Result<Vec<DataInfo>, StoreError> {
        let results = data
            .filter(service_id.eq(id.to_string()))
            .select((
                service_id,
                object_index,
                body_kind,
                body_value,
                previous,
                signature,
            ))
            .load::<DataFields>(&self.pool.get().unwrap())?;

        let mut v = vec![];

        for r in &results {
            v.push(Self::parse_data(r)?);
        }

        Ok(v)
    }

    pub fn delete_data(&self, sig: &Signature) -> Result<(), StoreError> {
        diesel::delete(data)
            .filter(signature.eq(sig.to_string()))
            .execute(&self.pool.get().unwrap())?;

        Ok(())
    }

    pub fn load_data(&self, sig: &Signature) -> Result<Option<DataInfo>, StoreError> {
        let results = data
            .filter(signature.eq(sig.to_string()))
            .select((
                service_id,
                object_index,
                body_kind,
                body_value,
                previous,
                signature,
            ))
            .load::<DataFields>(&self.pool.get().unwrap())?;

        if results.len() == 0 {
            return Ok(None);
        }

        let p = Self::parse_data(&results[0])?;

        Ok(Some(p))
    }

    fn parse_data(v: &DataFields) -> Result<DataInfo, StoreError> {
        let (r_id, r_index, r_body_kind, r_body, r_previous, r_signature) = v;

        let body = match (r_body_kind.as_ref(), r_body) {
            (BODY_NONE, _) => Body::None,
            (BODY_CLEARTEXT, Some(b)) => Body::Cleartext(b.to_vec()),
            (BODY_ENCRYPTED, Some(b)) => Body::Encrypted(b.to_vec()),
            _ => unreachable!(),
        };

        let d = DataInfo {
            service: Id::from_str(r_id)?,
            index: *r_index as u16,
            body,
            previous: r_previous
                .as_ref()
                .map(|v| Signature::from_str(&v).unwrap()),
            signature: Signature::from_str(r_signature)?,
        };

        Ok(d)
    }
}

#[cfg(test)]
mod test {
    extern crate tracing_subscriber;
    use tracing_subscriber::{filter::LevelFilter, FmtSubscriber};

    use super::Store;

    use dsf_core::crypto::{Crypto, Hash, PubKey, SecKey};
    use dsf_core::{base::Body, types::Id};
    use dsf_rpc::DataInfo;

    #[test]
    fn store_data_inst() {
        let _ = FmtSubscriber::builder()
            .with_max_level(LevelFilter::DEBUG)
            .try_init();

        let store = Store::new("/tmp/dsf-test-3.db").expect("Error opening store");

        store.drop_tables().unwrap();
        store.create_tables().unwrap();

        let (public_key, private_key) = Crypto::new_pk().unwrap();
        let _secret_key = Crypto::new_sk().unwrap();
        let id: Id = Crypto::hash(&public_key).unwrap().into();
        let sig = Crypto::pk_sign(&private_key, &[0xaa, 0xaa, 0xaa, 0xaa]).unwrap();

        let d = DataInfo {
            service: id.clone(),
            index: 10,
            body: Body::Cleartext(vec![0xaa, 0xbb, 0xcc, 0xdd]),

            previous: Some(sig.clone()),
            signature: sig,
        };

        // Check no matching service exists
        assert_eq!(0, store.find_data(&id).unwrap().len());

        // Store data
        store.save_data(&d).unwrap();
        //assert_eq!(Some(&d), store.load::<DataInfo>(&d.signature).unwrap().as_ref());
        assert_eq!(vec![d.clone()], store.find_data(&id).unwrap());

        // Delete data
        store.delete_data(&d.signature).unwrap();
        assert_eq!(0, store.find_data(&id).unwrap().len());
    }
}
