
use std::{time::{SystemTime, Duration}, ops::Add};

use tracing::{trace, debug, info, warn, error};

use dsf_core::{prelude::*, net::{self, Request, RequestBody, Response, ResponseBody, Common}, types::ShortId};
use dsf_rpc::{PeerInfo, PeerFlags, PeerAddress, PeerState, SubscriptionKind, SubscriptionInfo, QosPriority, RegisterOptions, ServiceIdentifier, LocateOptions, SubscribeOptions, ServiceState, ServiceFlags};

use crate::{error::Error, rpc::{Engine, register::RegisterService, locate::ServiceRegistry as _, subscribe::PubSub}, core::AsyncCore, store::object::ObjectIdentifier};

mod push_data;
use push_data::push_data;

mod register;
use register::register;

mod subscribe;
use subscribe::subscribe;

/// Async network handler abstraction
#[allow(async_fn_in_trait)]
pub trait Net {
    /// Handle incoming network requests, returning a network response
    async fn handle_net(&mut self, req: Request) -> Response;
}

impl<T: Engine> Net for T {
    async fn handle_net(&mut self, req: Request) -> Response {
        todo!()
    }
}

/// Handle DSF type requests
pub(crate) async fn handle_dsf_req<T: Engine + 'static>(engine: T, mut core: AsyncCore, from: PeerInfo, req: RequestBody, flags: Flags) -> Result<ResponseBody, Error> {
    
    match req {
        RequestBody::Hello => Ok(ResponseBody::Status(net::Status::Ok)),
        RequestBody::Subscribe(service_id) => {
            info!(
                "Subscribe request from peer: {from} for service: {service_id}"
            );
            subscribe(engine, core, from, &service_id, flags).await
        }
        RequestBody::Unsubscribe(service_id) => {
            info!(
                "Unsubscribe request from: {:#} for service: {:#}",
                from, service_id
            );

            core
                .subscriber_remove(service_id.clone(), SubscriptionKind::Peer(from.id.clone()))
                .await?;

            Ok(ResponseBody::Status(net::Status::Ok))
        }
        RequestBody::Locate(service_id) => {
            info!(
                "Delegated locate request from: {:#} for service: {:#}",
                from.id, service_id
            );

            // TODO(low): check peer criteria before allowing delegation
            // (eg. should only be allowed for devices with locally scoped addresses)

            // Perform search for service
            let resp = match engine.service_locate(LocateOptions {
                id: service_id.clone(),
                local_only: false,
                no_persist: false,
            }).await {
                Ok(ref v) => {
                    if let Some(p) = &v.page {
                        info!("Locate ok (index: {})", p.header().index());
                        ResponseBody::ValuesFound(service_id, vec![p.to_owned()])
                    } else {
                        error!("Locate failed, no page found");
                        ResponseBody::Status(net::Status::Failed)
                    }
                }
                Err(e) => {
                    error!("Locate failed: {:?}", e);
                    ResponseBody::Status(net::Status::Failed)
                }
            };

            // TODO(low): attach delegation information to discovered services
            // so these can be cleaned up when no longer required

            Ok(resp)
        },
        RequestBody::Query(id, index) => {
            info!("Data query from: {:#} for service: {:#}", from, id);

            // TODO: Check we have a replica of this service
            let _service = match core.service_get(id.clone()).await {
                Ok(s) => s,
                Err(_) => {
                    // Only known services can be queried
                    error!("no service found (id: {})", id);
                    return Ok(ResponseBody::Status(net::Status::InvalidRequest));
                }
            };

            // Fetch data for service
            let q = match index {
                Some(n) => ObjectIdentifier::Index(n),
                None => ObjectIdentifier::Latest,
            };
            let r = match core.object_get(&id, q.clone()).await {
                Ok(Some(i)) => Ok(ResponseBody::PullData(id, vec![i])),
                Ok(None) => Ok(ResponseBody::NoResult),
                Err(e) => {
                    error!("Failed to fetch object {:?}: {:?}", q, e);
                    Err(e.into())
                }
            };

            info!("Data query complete");

            r
        }
        RequestBody::Register(id, pages) => {
            info!("Register request from: {:#} for service: {:#}", from, id);
            register(engine, core, &id, flags, pages).await
        }
        RequestBody::Unregister(id) => {
            info!("Unegister request from: {} for service: {}", from, id);
            // TODO: determine whether we should allow this service to be unregistered
            todo!("unregister not yet implemented")
        }
        RequestBody::PushData(id, data) => {
            info!("Data push from peer: {from:#} for service: {id:#}");
            push_data(engine, core, &id, flags, data).await
        }
        _ => {
            error!("Unhandled DSF request: {req:?} (flags: {flags:?})");
            Err(Error::Unimplemented)
        },
    }
}


/// DELEGATION: Handle raw page or data objects
pub(crate) async fn handle_net_raw<T: Engine + 'static>(engine: T, core: AsyncCore, id: &Id, container: Container) -> Result<ResponseBody, Error> {
    let header = container.header();

    debug!(
        "Handling raw {id:#} object from service: {:?}",
        header.kind()
    );

    // Look for matching service
    let service_info = match core.service_get(id.clone()).await {
        Ok(info) => info,
        Err(_) => {
            warn!("Unknown service: {id:#}");
            return Ok(ResponseBody::Status(net::Status::InvalidRequest));
        },
    };

    // Check we're subscribed to the service otherwise ignore the data push
    if service_info.state != ServiceState::Subscribed
            && !service_info.flags.contains(ServiceFlags::SUBSCRIBED) {
        warn!("Data push for non-subscribed service: {id:#}");
        return Ok(ResponseBody::Status(net::Status::InvalidRequest));
    }
            
    // Update local service
    match header.kind() {
        Kind::Page { .. } => {
            debug!(
                "Receive service page {:#} index {}",
                id,
                container.header().index()
            );
            
            register(engine, core, &id, header.flags(), vec![container.to_owned()]).await
        }
        Kind::Data { .. } => {
            debug!(
                "Receive service data {:#} index {}",
                id,
                container.header().index()
            );

            push_data(engine, core, &id, header.flags(), vec![container.to_owned()]).await
        }
        _ => Ok(ResponseBody::Status(net::Status::InvalidRequest)),
    }
}

/// Generic peer-related handling common to all incoming messages
pub(crate) async fn handle_base(our_id: &Id, mut core: AsyncCore, peer_id: &Id, address: &Address, common: &Common) -> Option<PeerInfo> {
    trace!(
        "[DSF ({:?})] Handling base message from: {:?} address: {:?} public_key: {:?}",
        our_id,
        peer_id,
        address,
        common.public_key
    );

    // Skip RX of messages / loops
    // TODO(low): may need to this to check tunnels for STUN or equivalents... a problem for later
    if peer_id == our_id {
        warn!("handle_base called for self...");
        return None;
    }

    let mut peer_flags = PeerFlags::empty();
    if common.flags.contains(Flags::CONSTRAINED) {
        peer_flags |= PeerFlags::CONSTRAINED;
    }
    if common.flags.contains(Flags::NO_PERSIST) {
        peer_flags |= PeerFlags::TRANSIENT;
    }

    // Find or create (and push) peer
    let peer = core.peer_create_or_update(PeerInfo {
            id: peer_id.clone(),
            short_id: ShortId::from(peer_id),
            // Attach keys
            state: common
                .public_key
                .clone()
                .map(PeerState::Known)
                .unwrap_or(PeerState::Unknown),
            // Set address
            address: match common.remote_address {
                Some(a) => PeerAddress::Explicit(a),
                None => PeerAddress::Implicit(address.clone()),
            },
            flags: peer_flags,
            seen: Some(SystemTime::now()),
            received: 1,
            index: 0,
            sent: 0,
            blocked: false,
        })
        .await;

    // Handle failure crating peer instance
    let peer = match peer {
        Ok(p) => p,
        Err(e) => {
            error!("Failed to create or update peer: {e:?}");
            return None;
        }
    };
    
    trace!(
        "[DSF ({:?})] Peer id: {:?} state: {:?} seen: {:?}",
        our_id,
        peer_id,
        peer.state(),
        peer.seen()
    );

    // TODO: update key caching?

    // Return peer object
    Some(peer)
}

