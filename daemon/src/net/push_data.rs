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

/// Push service data
pub(super) async fn push_data<T: Engine>(
    engine: T,
    mut core: AsyncCore,
    id: &Id,
    flags: Flags,
    data: Vec<Container>,
) -> Result<ResponseBody, Error> {
    // Find matching service and make sure we're subscribed
    let service_info = match core.service_get(id.clone()).await {
        Ok(s) => s,
        Err(_e) => {
            // Only known services can be registered
            error!("no service found (id: {})", id);
            return Ok(ResponseBody::Status(net::Status::InvalidRequest));
        }
    };

    // Check we're subscribed to the service otherwise ignore the data push
    if service_info.state != ServiceState::Subscribed
        && !service_info.flags.contains(ServiceFlags::SUBSCRIBED)
    {
        warn!("Data push for non-subscribed service: {id:#}");
        return Ok(ResponseBody::Status(net::Status::InvalidRequest));
    }

    // TODO: validate incoming data prior to processing
    #[cfg(nyet)]
    if let Err(e) = self.services().validate_pages(&id, &data) {
        error!("Invalid data for service: {} ({:?})", id, e);
        return Ok(ResponseBody::Status(net::Status::InvalidRequest));
    }

    // Register or update service if a primary page is provided
    // TODO: improve behaviour for multiple page push
    if let Some(primary_page) = data
        .iter()
        .find(|p| p.header().kind().is_page() && !p.header().flags().contains(Flags::SECONDARY))
    {
        if let Err(e) = core
            .service_register(id.clone(), vec![primary_page.clone()])
            .await
        {
            error!("Failed to update service {id}: {e:?}");
        }
    }

    // Store pages and data, spawning a task to ensure data throughput
    // does not depend on database performance
    // TODO(med): this should only need to be data objects as page should be stored in service_register
    {
        let core = core.clone();
        let id = id.clone();
        let data = data.clone();
        tokio::task::spawn(async move {
            if let Err(e) = core.data_store(&id, data.clone()).await {
                error!("Failed to store data: {e:?}");
            }
        });
    }

    // Generate peer list for data push
    // TODO(med): we should be cleverer about this to avoid
    // loops etc. (and prolly add a TTL if it can be repeated?)
    let subscribers = core.subscriber_list(id.clone()).await?;
    if !subscribers.is_empty() {
        // Generate data push message for subscribers
        let req = RequestBody::PushData(id.clone(), data);

        let mut peer_subs = Vec::new();
        for s in subscribers {
            let peer_id = match s.kind {
                SubscriptionKind::Peer(id) => id,
                // TODO(high): include RPC peers in data push
                _ => continue,
            };
            match core.peer_get(peer_id).await {
                Ok(p) => peer_subs.push(p),
                _ => (),
            }
        }

        info!("Sending data push message to: {:?}", peer_subs);

        // Issue data push requests
        // TODO(low): we should probably wire the return here to send a delayed PublishInfo to the requester?
        match engine.net_req(req, peer_subs).await {
            Ok(_) => info!("Data push complete"),
            Err(e) => warn!("Data push error: {:?}", e),
        }
    }

    Ok(ResponseBody::Status(net::Status::Ok))
}
