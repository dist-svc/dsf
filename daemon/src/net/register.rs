use std::{time::{SystemTime, Duration}, ops::Add};

use tracing::{trace, debug, info, warn, error};

use dsf_core::{prelude::*, net::{self, Request, RequestBody, Response, ResponseBody, Common}, types::ShortId};
use dsf_rpc::{PeerInfo, PeerFlags, PeerAddress, PeerState, SubscriptionKind, SubscriptionInfo, QosPriority, RegisterOptions, ServiceIdentifier, LocateOptions, SubscribeOptions, ServiceState, ServiceFlags};

use crate::{error::Error, rpc::{Engine, register::RegisterService, locate::ServiceRegistry as _, subscribe::PubSub}, core::AsyncCore, store::object::ObjectIdentifier};


/// Register a service from pages
pub(super) async fn register<T: Engine + 'static>(engine: T, core: AsyncCore, id: &Id, flags: Flags, pages: Vec<Container>) -> Result<ResponseBody, Error> {
    
    // TODO(low): determine whether we should allow this service to be 
    // registered by this peer

    // Add to local service registry
    core.service_register(id.clone(), pages).await?;

    // TODO(low): forward pages to subscribers if we have any

    // Return early for standard registrations
    if !flags.contains(Flags::CONSTRAINED) {
        info!("Register request for service: {:#} complete", id);
        return Ok(ResponseBody::Status(net::Status::Ok))
    }

    // DELEGATION: Execute network registration
    // TODO(low): check peer criteria before allowing delegation
    // (eg. should only be allowed for devices with locally scoped addresses)
    info!("starting delegated registration for {id:#}");

    let resp = match engine.service_register(RegisterOptions {
        service: ServiceIdentifier::id(id.clone()),
        no_replica: false,
    }).await {
        Ok(v) => {
            info!("Registration complete: {v:?}");
            net::ResponseBody::Status(net::Status::Ok)
        },
        Err(e) => {
            error!("Registration failed: {:?}", e);
            net::ResponseBody::Status(net::Status::Failed)
        }
    };

    Ok(resp)
}
