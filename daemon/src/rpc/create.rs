//! Create operation, create a new service and optionally register this in the database

use std::convert::TryFrom;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::SystemTime;

use futures::channel::mpsc;
use futures::prelude::*;

use log::{debug, error, info, warn};
use tracing::{span, Level};

use dsf_core::options::Options;
use dsf_core::prelude::*;
use dsf_core::service::Publisher;

use dsf_rpc::{self as rpc, CreateOptions, RegisterOptions, ServiceIdentifier};

use crate::daemon::{net::NetIf, Dsf};
use crate::error::Error;

use crate::core::peers::Peer;
use crate::core::services::ServiceState;
use crate::core::services::*;

use super::ops::*;

pub enum CreateState {
    Init,
    Pending,
    Done,
    Error,
}

#[async_trait::async_trait]
pub trait CreateService {
    /// Create a new service
    async fn service_create(&self, options: CreateOptions) -> Result<ServiceInfo, DsfError>;
}

#[async_trait::async_trait]
impl<T: Engine> CreateService for T {
    async fn service_create(&self, options: CreateOptions) -> Result<ServiceInfo, DsfError> {
        info!("Creating service: {:?}", options);
        let mut sb = ServiceBuilder::generic();

        if let Some(kind) = options.page_kind {
            sb = sb.kind(kind);
        }

        // Attach a body if provided
        if let Some(body) = &options.body {
            sb = sb.body(body.clone());
        } else {
            sb = sb.body(vec![]);
        }

        // Append supplied public and private options
        sb = sb.public_options(options.public_options.clone());
        sb = sb.private_options(options.private_options.clone());

        // Append addresses as private options
        sb = sb.private_options(
            options
                .addresses
                .iter()
                .map(|v| Options::address(v.clone()))
                .collect(),
        );

        // TODO: append metadata
        for _m in &options.metadata {
            //TODO
        }

        // If the service is not public, encrypt the object
        if !options.public {
            sb = sb.encrypt();
        }

        debug!("Generating service");
        let mut service = sb.build().unwrap();
        let id = service.id();

        debug!("Generating service page");
        // TODO: revisit this
        let buff = vec![0u8; 1024];
        let (_n, primary_page) = service.publish_primary(Default::default(), buff).unwrap();

        // Add service to local store
        debug!("Storing service information");
        let mut info = self.svc_create(service, primary_page.clone()).await?;

        let pages = vec![primary_page];

        // TODO: option of generating replica information here / prior to registration

        // Register via DHT if enabled
        if options.register {
            // Register service page in DHT
            match self.dht_put(id.clone(), pages).await {
                Ok(_) => match self
                    .svc_update(
                        id.clone(),
                        Box::new(|_svc, state| {
                            *state = ServiceState::Registered;
                            Ok(Res::Ok)
                        }),
                    )
                    .await
                {
                    Ok(_) => info.state = ServiceState::Registered,
                    Err(e) => {
                        error!("Failed to update service state: {:?}", e);
                    }
                },
                Err(e) => {
                    error!("Failed to store service page in DHT: {:?}", e);
                    // TODO: return error? attach to info? check from client?
                }
            }
        }

        Ok(info)
    }
}
