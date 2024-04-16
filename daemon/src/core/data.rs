use crate::core::store::DataStore;
use crate::error::StoreError;
use crate::store::object::ObjectIdentifier;
use crate::sync::{Arc, Mutex};
use std::convert::TryFrom;

use dsf_core::prelude::MaybeEncrypted;
use log::{debug, error, info, trace, warn};

use dsf_core::types::{Id, ImmutableData, Signature};
use dsf_core::{keys::Keys, wire::Container};

pub use dsf_rpc::data::DataInfo;
use dsf_rpc::{PageBounds, ServiceIdentifier, TimeBounds};

use crate::{
    core::{store::AsyncStore, Core},
    error::Error,
};

pub struct DataInst {
    pub info: DataInfo,
    pub page: Container,
}

impl Core {
    /// List data for a given service
    pub async fn list_data(
        &self,
        ident: &ServiceIdentifier,
        page_bounds: &PageBounds,
        _time_bounds: &TimeBounds,
    ) -> Result<Vec<(DataInfo, Container)>, Error> {
        // Get service info for object decoding
        let service = match self.service_get(ident).await {
            Some(s) => s,
            None => return Err(Error::NotFound),
        };
        let keys = Keys {
            pub_key: Some(service.public_key.clone()),
            sec_key: service.secret_key.clone(),
            ..Default::default()
        };

        // TODO(low): filter by time bounds (where possible)

        // Load data from store
        let mut data = self
            .store
            .object_find(&service.id, &keys, page_bounds)
            .await?;

        debug!("Retrieved {} objects: {:?}", data.len(), data);

        // Generate info objects for data
        let mut results = Vec::with_capacity(data.len());
        for d in data.drain(..) {
            let i = DataInfo::from_block(&d, &keys)?;
            results.push((i, d))
        }

        Ok(results)
    }

    /// Fetch a single object for the specified service
    pub async fn get_object<F: Into<ObjectIdentifier>>(
        &self,
        service_id: &ServiceIdentifier,
        f: F,
    ) -> Result<(DataInfo, Container), Error> {
        // Fetch service info for object decoding
        let service = match self.service_get(service_id).await {
            Some(s) => s,
            None => return Err(Error::NotFound),
        };
        let keys = Keys {
            pub_key: Some(service.public_key.clone()),
            sec_key: service.secret_key.clone(),
            ..Default::default()
        };

        // TODO(low): check whether object exists in local cache
        // and return early if found

        // Fetch object from backing store
        let object = match self.store.object_get(&service.id, f.into(), &keys).await {
            Ok(v) => v,
            Err(StoreError::NotFound) => return Err(Error::NotFound),
            Err(e) => return Err(Error::Store(e)),
        };

        let i = DataInfo::from_block(&object, &keys)?;

        Ok((i, object))
    }

    /// Store data for a given service
    pub async fn store_data(&self, service_id: &Id, pages: Vec<Container>) -> Result<(), Error> {
        // Fetch service info for object verification
        let service = match self.service_get(&ServiceIdentifier::from(service_id)).await {
            Some(s) => s,
            None => return Err(Error::NotFound),
        };
        let _keys = Keys {
            pub_key: Some(service.public_key.clone()),
            sec_key: service.secret_key.clone(),
            ..Default::default()
        };

        // TODO: verify and decode/encode objects as required for storage

        // TODO(low): Add data to local cache

        // TODO(med): update associated service information

        // Start data store operation
        // This uses a task to dispatch the operation without blocking
        // and limiting overall throughput here
        let s = self.store.clone();

        #[cfg(feature = "store")]
        tokio::task::spawn(async move {
            debug!("Start async store task");

            for p in pages {
                match s.object_put(p).await {
                    Ok(_) => {
                        //TODO (low): signal object is allowed to be dropped from cache
                    }
                    Err(e) => {
                        error!("Failed to write object to store: {e:?}");
                    }
                }
            }

            debug!("Store operation complete");
        });

        Ok(())
    }
}
