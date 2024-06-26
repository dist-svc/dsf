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
pub(super) async fn subscribe<T: Engine>(
    engine: T,
    mut core: AsyncCore,
    from: PeerInfo,
    service_id: &Id,
    flags: Flags,
) -> Result<ResponseBody, Error> {
    // Fetch service information, checking service exists / is known
    let service = match core.service_get(service_id.clone()).await {
        Ok(s) if s.primary_page.is_some() => s,
        _ => {
            // Only known services can be subscribed
            error!("no service found (id: {})", service_id);
            return Ok(ResponseBody::Status(net::Status::InvalidRequest));
        }
    };

    // Only allow subscriptions to known and public services
    // TODO(low): check peer criteria before allowing delegation
    // (eg. should only be allowed for devices with locally scoped addresses)
    if service.state != ServiceState::Registered
        && service.state != ServiceState::Subscribed
        && !flags.contains(Flags::CONSTRAINED)
    {
        error!(
            "Peer {from} attempted subscription to non-origin service: {service_id} (state: {})",
            service.state
        );
        return Ok(ResponseBody::Status(net::Status::InvalidRequest));
    }

    // Fetch primary page for service
    // TODO(med): should this fail out if we don't have a primary page?
    let pages = {
        match core
            .object_get(&(service_id).into(), &service.primary_page.unwrap())
            .await
        {
            Ok((_i, p)) => vec![p.clone()],
            Err(_) => vec![],
        }
    };

    // TODO: verify this is coming from an active upstream subscriber

    // TODO: also update peer subscription information here
    core.subscriber_create_or_update(SubscriptionInfo {
        service_id: service_id.clone(),
        kind: SubscriptionKind::Peer(from.id.clone()),
        updated: Some(SystemTime::now()),
        expiry: Some(SystemTime::now().add(Duration::from_secs(3600))),
        qos: match flags.contains(Flags::QOS_PRIO_LATENCY) {
            true => QosPriority::Latency,
            false => QosPriority::None,
        },
    })
    .await?;

    // Return early for normal subscribe requests
    if !flags.contains(Flags::CONSTRAINED) {
        info!("Subscribe request complete");
        return Ok(ResponseBody::ValuesFound(service_id.clone(), pages));
    }

    // DELEGATION: initiate subscription to upstream replicas
    let resp = match engine
        .subscribe(SubscribeOptions {
            service: ServiceIdentifier::id(service_id.clone()),
        })
        .await
    {
        Ok(_v) => {
            info!("Delegated subscription ok!");
            net::ResponseBody::Status(net::Status::Ok)
        }
        Err(e) => {
            error!("Delegated subscription failed: {:?}", e);
            net::ResponseBody::Status(net::Status::Failed)
        }
    };

    // TODO(low): attach delegation information to subscriptions
    // so these can be cleaned up when no longer required

    Ok(resp)
}
