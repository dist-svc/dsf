use std::{str::FromStr, time::Duration};

use reqwest::{Client, Url};
use tracing::{debug, error};

use dsf_rpc::{RequestKind, ResponseKind};

use super::Driver;
use crate::Error;

pub struct HttpDriver {
    address: Url,
    client: Client,
}

impl HttpDriver {
    /// Create a new unix driver
    pub async fn new(address: &str) -> Result<Self, Error> {
        debug!("HTTP client (address: {})", address);

        let address = match Url::from_str(address) {
            Ok(v) => v,
            Err(e) => {
                error!("Failed to parse address: {:?}", e);
                return Err(Error::Socket);
            }
        };

        let client = Client::new();

        Ok(Self { address, client })
    }
}

impl Driver for HttpDriver {
    async fn exec(&self, req: RequestKind, timeout: Duration) -> Result<ResponseKind, Error> {
        debug!("HTTP req: {:?}", req);

        let target = match self.address.join("/exec") {
            Ok(v) => v,
            Err(e) => {
                error!("Failed to build address: {:?}", e);
                return Err(Error::Socket);
            }
        };

        let res = match self
            .client
            .post(target)
            .timeout(timeout)
            .json(&req)
            .send()
            .await
        {
            Ok(v) => v,
            Err(e) => {
                error!("Request failed: {:?}", e);
                return Err(Error::Socket);
            }
        };

        let resp: ResponseKind = match res.json().await {
            Ok(v) => v,
            Err(e) => {
                error!("Error fetching response: {e:?}");
                return Err(Error::Socket);
            }
        };

        Ok(resp)
    }
}
