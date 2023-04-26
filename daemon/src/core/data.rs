use crate::sync::{Arc, Mutex};
use std::convert::TryFrom;

use log::{debug, error, info, trace};

use dsf_core::types::{Id, ImmutableData};
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

impl From<Container> for DataInst {
    fn from(page: Container) -> Self {
        let info = DataInfo::try_from(&page).unwrap();
        DataInst { info, page }
    }
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
        let keys = Keys::new(service.public_key.clone());

        // Load data info
        let mut info: Vec<DataInfo> = self.store.find_data(service_id)?;
        info.sort_by(|a, b| a.index.partial_cmp(&b.index).unwrap());
        info.reverse();

        // TODO: filter by time bounds (where possible)

        // TODO: apply offset

        let offset = page_bounds.offset.unwrap_or(0);
        let limit = page_bounds.count.unwrap_or(10);
        let limit = info.len().min(offset + limit);

        debug!("Loaded data info for service: {}", service_id);

        // Load associated raw pages
        let mut data = Vec::with_capacity(info.len());
        for i in info.drain(offset..limit) {
            trace!("Fetching raw page for: {}", i.signature);

            let p = self.store.load_page(&i.signature, &keys)?;
            data.push(DataInst {
                info: i,
                page: p.unwrap(),
            })
        }

        Ok(data)
    }

    /// Store data for a given service
    pub fn store_data<T: ImmutableData>(
        &self,
        info: &DataInfo,
        page: &Container<T>,
    ) -> Result<(), Error> {
        // Store data object
        #[cfg(feature = "store")]
        self.store.save_data(info)?;

        // Store backing raw object
        #[cfg(feature = "store")]
        self.store.save_page(page)?;

        Ok(())
    }
}
