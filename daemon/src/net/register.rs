use std::{
    ops::Add,
    time::{Duration, SystemTime},
};

use tracing::{debug, error, info, trace, warn};

use dsf_core::{
    net::{self, Common, Request, RequestBody, Response, ResponseBody},
    prelude::*,
    types::ShortId,
};
use dsf_rpc::{
    LocateOptions, PeerAddress, PeerFlags, PeerInfo, PeerState, QosPriority, RegisterOptions,
    ServiceFlags, ServiceIdentifier, ServiceState, SubscribeOptions, SubscriptionInfo,
    SubscriptionKind,
};

use crate::{
    core::AsyncCore,
    error::Error,
    rpc::{locate::ServiceRegistry as _, register::RegisterService, subscribe::PubSub, Engine},
    store::object::ObjectIdentifier,
};

/// Register a service from pages
pub(super) async fn register<T: Engine>(
    engine: T,
    core: AsyncCore,
    id: &Id,
    flags: Flags,
    pages: Vec<Container>,
) -> Result<ResponseBody, Error> {
    // TODO(low): determine whether we should allow this service to be
    // registered by this peer

    // Add to local service registry
    core.service_register(id.clone(), pages).await?;

    // TODO(low): forward pages to subscribers if we have any

    // Return early for standard registrations
    if !flags.contains(Flags::CONSTRAINED) {
        info!("Register request for service: {:#} complete", id);
        return Ok(ResponseBody::Status(net::Status::Ok));
    }

    // DELEGATION: Execute network registration
    // TODO(low): check peer criteria before allowing delegation
    // (eg. should only be allowed for devices with locally scoped addresses)
    info!("starting delegated registration for {id:#}");

    let resp = match engine
        .service_register(RegisterOptions {
            service: ServiceIdentifier::id(id.clone()),
            no_replica: false,
        })
        .await
    {
        Ok(v) => {
            info!("Registration complete: {v:?}");
            net::ResponseBody::Status(net::Status::Ok)
        }
        Err(e) => {
            error!("Registration failed: {:?}", e);
            net::ResponseBody::Status(net::Status::Failed)
        }
    };

    Ok(resp)
}
