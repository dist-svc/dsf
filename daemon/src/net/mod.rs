
use std::{time::{SystemTime, Duration}, ops::Add};

use tracing::{trace, debug, info, warn, error};

use dsf_core::{prelude::*, net::{self, Request, RequestBody, Response, ResponseBody, Common}, types::ShortId};
use dsf_rpc::{PeerInfo, PeerFlags, PeerAddress, PeerState, SubscriptionKind, SubscriptionInfo, QosPriority, RegisterOptions, ServiceIdentifier, LocateOptions, SubscribeOptions, ServiceState, ServiceFlags};

use crate::{error::Error, rpc::{Engine, register::RegisterService, locate::ServiceRegistry as _, subscribe::PubSub}, core::AsyncCore, store::object::ObjectIdentifier};

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
            if service.state != ServiceState::Registered && service.state != ServiceState::Subscribed && !flags.contains(Flags::CONSTRAINED) {
                error!("Peer {from} attempted subscription to non-origin service: {service_id} (state: {})", service.state);
                return Ok(ResponseBody::Status(net::Status::InvalidRequest));
            }

            // Fetch primary page for service
            let pages = {
                match core
                    .object_get(&service_id, &service.primary_page.unwrap())
                    .await
                    .unwrap()
                {
                    Some(p) => vec![p.clone()],
                    None => vec![],
                }
            };

            // TODO: verify this is coming from an active upstream subscriber

            // TODO: also update peer subscription information here
            core
                .subscriber_create_or_update(SubscriptionInfo {
                    service_id: service_id.clone(),
                    kind: SubscriptionKind::Peer(from.id.clone()),
                    updated: Some(SystemTime::now()),
                    expiry: Some(SystemTime::now().add(Duration::from_secs(3600))),
                    qos: match flags.contains(Flags::QOS_PRIO_LATENCY) {
                        true => QosPriority::Latency,
                        false => QosPriority::None,
                    }
                })
                .await?;

            // Return early for normal subscribe requests
            if !flags.contains(Flags::CONSTRAINED) {
                info!("Subscribe request complete");
                return Ok(ResponseBody::ValuesFound(service_id, pages))
            }

            // DELEGATION: initiate subscription to upstream replicas
            let resp = match engine.subscribe(SubscribeOptions {
                service: ServiceIdentifier::id(service_id.clone()),
            }).await {
                Ok(_v) => {
                    info!("Delegated subscription ok!");
                    net::ResponseBody::Status(net::Status::Ok)
                },
                Err(e) => {
                    error!("Delegated subscription failed: {:?}", e);
                    net::ResponseBody::Status(net::Status::Failed)
                }
            };

            // TODO(low): attach delegation information to subscriptions
            // so these can be cleaned up when no longer required

            Ok(resp)
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

async fn register<T: Engine + 'static>(engine: T, core: AsyncCore, id: &Id, flags: Flags, pages: Vec<Container>) -> Result<ResponseBody, Error> {
    
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

async fn push_data<T: Engine + 'static>(engine: T, mut core: AsyncCore, id: &Id, flags: Flags, data: Vec<Container>) -> Result<ResponseBody, Error> {

    // Find matching service and make sure we're subscribed
    // TODO(med): make sure we're subscribed or ignore pushes / respond with unsubscribe
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
            && !service_info.flags.contains(ServiceFlags::SUBSCRIBED) {
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
    if let Some(primary_page) = data.iter().find(|p| {
        p.header().kind().is_page() && !p.header().flags().contains(Flags::SECONDARY)
    }) {
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

    // Generate data push message for subscribers
    let req = RequestBody::PushData(id.clone(), data);

    // Generate peer list for data push
    // TODO(med): we should be cleverer about this to avoid
    // loops etc. (and prolly add a TTL if it can be repeated?)
    let subscribers = core.subscriber_list(id.clone()).await?;
    let mut peer_subs = Vec::new();
    for s in subscribers {
        let peer_id = match s.kind {
            SubscriptionKind::Peer(id) => id,
            // TODO: include RPC peers in data push
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
    tokio::task::spawn(async move {
        match engine.net_req(req, peer_subs).await {
            Ok(_) => info!("Data push complete"),
            Err(e) => warn!("Data push error: {:?}", e),
        }
    });

    Ok(ResponseBody::Status(net::Status::Ok))
}

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


/// Generic handling for all incoming messages
pub(crate) async fn handle_base<T: Engine>(engine: T, peer_id: &Id, address: &Address, common: &Common) -> Option<PeerInfo> {
    trace!(
        "[DSF ({:?})] Handling base message from: {:?} address: {:?} public_key: {:?}",
        engine.id(),
        peer_id,
        address,
        common.public_key
    );

    // Skip RX of messages / loops
    // TODO(low): may need to this to check tunnels for STUN or equivalents... a problem for later
    if *peer_id == engine.id() {
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
    let peer = engine.peer_create_update(PeerInfo {
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
        engine.id(),
        peer_id,
        peer.state(),
        peer.seen()
    );

    // TODO: update key caching?

    // Return peer object
    Some(peer)
}
