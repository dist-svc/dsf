use crate::sync::{Arc, Mutex};
use std::convert::TryFrom;

use dsf_core::prelude::MaybeEncrypted;
use log::{debug, error, info, trace, warn};

use dsf_core::types::{Id, ImmutableData, Signature};
use dsf_core::{keys::Keys, wire::Container};

pub use dsf_rpc::data::DataInfo;
use dsf_rpc::{PageBounds, TimeBounds};

use crate::error::Error;
use crate::store::Store;

pub struct DataManager {
    store: Store,
}

pub struct DataInst {
    pub info: DataInfo,
    pub page: Container,
}

impl DataManager {
    pub fn new(store: Store) -> Self {
        Self { store }
    }

    /// Fetch data for a given service
    // TODO: add paging?
    pub fn fetch_data(
        &self,
        service_id: &Id,
        page_bounds: &PageBounds,
        _time_bounds: &TimeBounds,
    ) -> Result<Vec<DataInst>, Error> {
        // Load service info
        let service = match self.store.find_service(service_id)? {
            Some(s) => s,
            None => return Err(Error::NotFound),
        };
        let keys = Keys {
            pub_key: Some(service.public_key.clone()),
            sec_key: service.secret_key.clone(),
            ..Default::default()
        };

        // TODO: apply offset to query
        let offset = page_bounds.offset.unwrap_or(0);
        let count = page_bounds.count.unwrap_or(3);

        // TODO: filter by time bounds (where possible)

        // Load data from store
        // TODO: filter (and sort?) via db rather than in post
        let mut data = self.store.find_objects(service_id, &keys, offset, count)?;

        debug!("Retrieved {} objects: {:?}", data.len(), data);

        // Generate info objects for data
        let mut results = Vec::with_capacity(data.len());
        for d in data.drain(..) {
            let i = DataInfo::from_block(&d, &keys)?;
            results.push(DataInst { info: i, page: d })
        }

        Ok(results)
    }

    pub fn get_object(&self, service_id: &Id, sig: &Signature) -> Result<Option<DataInst>, Error> {
        // Load service info
        let service = match self.store.find_service(service_id)? {
            Some(s) => s,
            None => return Err(Error::NotFound),
        };
        let keys = Keys {
            pub_key: Some(service.public_key.clone()),
            sec_key: service.secret_key.clone(),
            ..Default::default()
        };

        let object = match self.store.load_object(sig, &keys)? {
            Some(v) => v,
            None => return Ok(None),
        };

        let i = DataInfo::from_block(&object, &keys)?;

        Ok(Some(DataInst {
            info: i,
            page: object,
        }))
    }

    /// Store data for a given service
    pub fn store_data<T: ImmutableData>(&self, page: &Container<T>) -> Result<(), Error> {
        // Store page object
        #[cfg(feature = "store")]
        self.store.save_object(page)?;

        Ok(())
    }
}
