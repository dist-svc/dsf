//! Registry register and search operations for interactions with name services
//!
//!

use std::convert::TryFrom;

use dsf_core::wire::Container;
use futures::{future, Future, FutureExt};
use log::{debug, error, info, warn};
use serde::{Deserialize, Serialize};

use dsf_core::error::Error as CoreError;
use dsf_core::options::{self, Filters};
use dsf_core::prelude::{DsfError, Options, PageInfo};
use dsf_core::service::Registry;
use dsf_core::types::{CryptoHash, Flags, Id, PageKind};

use dsf_rpc::{
    LocateOptions, NsRegisterInfo, NsRegisterOptions, NsSearchOptions, Response, ServiceIdentifier,
};

use crate::daemon::Dsf;
use crate::error::Error;
use crate::rpc::locate::ServiceRegistry;
use crate::rpc::ops::Res;

use super::ops::{Engine, OpKind, RpcKind};

#[async_trait::async_trait]
pub trait NameService {
    /// Search for a service or block by hashed value
    async fn ns_search(&self, opts: NsSearchOptions) -> Result<Vec<Container>, DsfError>;

    /// Register a service by name
    async fn ns_register(&self, opts: NsRegisterOptions) -> Result<NsRegisterInfo, DsfError>;
}

#[async_trait::async_trait]
impl<T: Engine> NameService for T {
    /// Search for a matching service using the provided (or relevant) nameserver
    async fn ns_search(&self, opts: NsSearchOptions) -> Result<Vec<Container>, DsfError> {
        debug!("Locating nameservice for search: {:?}", opts);

        // Resolve nameserver using provided options
        let ns = self.service_resolve(opts.ns).await;

        // TODO: support lookups by prefix

        // Check we found a viable name service
        let ns = match ns {
            Ok(ns) => ns,
            Err(_e) => {
                error!("No matching nameservice found");
                return Err(DsfError::NotFound);
            }
        };

        // Generate search query
        let lookup = match (opts.name, opts.hash) {
            (Some(n), _) => ns.resolve(&Options::name(&n))?,
            (_, Some(h)) => {
                todo!("Hash based searching not yet implemented");
            }
            _ => {
                todo!("Search requires hash or name argument");
            }
        };

        info!("NS query for {} via {}", lookup, ns.id());

        // Issue query for tertiary pages
        let tertiary_pages = match self.dht_search(lookup).await {
            Ok(p) => p,
            Err(e) => {
                error!("DHT lookup failed: {:?}", e);
                return Err(e.into());
            }
        };

        debug!("Located tertiary pages: {:?}", tertiary_pages);

        // TODO: should we reduce the response pages here?
        // Possible to have > 1 service link pages where target services are different
        // Only ever 1 data link page at a given location
        // (except collisions... what're the odds?)

        // Collapse resolved pages
        let mut resolved = vec![];

        for p in tertiary_pages {
            // Check page is of tertiary kind
            let mut r = match p.info() {
                // Fetch information for linked service
                Ok(PageInfo::ServiceLink(s)) if s.peer_id == ns.id() => {
                    // Lookup service pages
                    let pages = match self.dht_search(s.target_id.clone()).await {
                        Ok(p) => p,
                        Err(e) => {
                            error!("Failed to find pages for service {}: {:?}", s.target_id, e);
                            continue;
                        }
                    };

                    // Update service using pages
                    let i = match self
                        .service_register(s.target_id.clone(), pages.clone())
                        .await
                    {
                        Ok(i) => i,
                        Err(e) => {
                            error!("Failed to register new service {}: {:?}", s.target_id, e);
                            continue;
                        }
                    };

                    debug!("Located service: {:?}", i);

                    pages
                }
                // Fetch linked block
                Ok(PageInfo::BlockLink(b)) if b.peer_id == ns.id() => {
                    // Lookup data blocks
                    todo!()

                    // TODO: Check discovered block name matches link ID
                    // (ie. make sure a tertiary page cannot link to a block that is not named to match)
                    // TODO: are there cases in which this will not work?
                }
                // TODO: log rejection of pages at wrong ID?
                _ => continue,
            };

            // TODO: check response matches query

            // Store response object
            resolved.append(&mut r);
        }

        // Return pages / blocks for resolved service
        // TODO: could we separate these better?
        Ok(resolved)
    }

    async fn ns_register(&self, opts: NsRegisterOptions) -> Result<NsRegisterInfo, DsfError> {
        debug!("Locating nameserver for register: {:?}", opts);

        // Resolve nameserver using provided options
        let ns = self.service_resolve(opts.ns.clone()).await;

        // TODO: support lookups by prefix
        // Check we found a viable name service
        let ns = match ns {
            Ok(ns) => ns,
            Err(_e) => {
                error!("No matching name service found");
                return Err(DsfError::NotFound);
            }
        };

        // Ensure this _is_ a name service
        if ns.kind() != PageKind::Name {
            error!("Service {} not a name service ({:?})", ns.id(), ns);
            return Err(DsfError::Unknown);
        }

        // Check we can use this for publishing
        if !ns.is_origin() {
            error!("Publishing to remote name services not implemented");
            return Err(DsfError::Unimplemented);
        }

        // Lookup prefix for NS
        let prefix = ns.public_options().iter().find_map(|o| match o {
            Options::Name(n) => Some(n.to_string()),
            _ => None,
        });

        debug!("Locating target for register: {:?}", opts);

        // Lookup service for registering
        let t = match self.service_resolve(opts.target.into()).await {
            Ok(s) => s,
            Err(e) => {
                error!("No matching target service found: {:?}", e);
                return Err(DsfError::NotFound);
            }
        };

        info!(
            "Registering service: {} via ns: {} ({:?}) ",
            t.id(),
            ns.id(),
            prefix
        );

        let (name, hashes) = (opts.name.clone(), opts.hash.clone());

        // Generate pages for registration
        let pages = self
            .service_update(
                ns.id(),
                Box::new(move |s| {
                    let mut pages = vec![];

                    // Create page for name if provided
                    if let Some(n) = &name {
                        if let Some((_n, p)) = s
                            .publish_tertiary_buff::<256, _>(
                                t.id().into(),
                                Default::default(),
                                &Options::name(&n),
                            )
                            .ok()
                        {
                            pages.push(p.to_owned());
                        }
                    }

                    // Create pages for provided hashes
                    for h in &hashes {
                        if let Some((_n, p)) = s
                            .publish_tertiary_buff::<256, _>(
                                t.id().into(),
                                Default::default(),
                                h.clone(),
                            )
                            .ok()
                        {
                            pages.push(p.to_owned());
                        }
                    }

                    Ok(Res::Pages(pages))
                }),
            )
            .await?;

        let pages = match pages {
            Res::Pages(p) => p,
            _ => unreachable!(),
        };

        // Publish pages to database
        for p in pages {
            // TODO: handle no peers case, return list of updated pages perhaps?
            if let Err(e) = self.dht_put(p.id(), vec![p]).await {
                warn!("Failed to publish pages to DHT: {:?}", e);
            }
        }

        // TODO: return result
        let i = NsRegisterInfo {
            ns: ns.id(),
            prefix,
            name: opts.name,
            hashes: opts.hash,
        };

        Ok(i)
    }
}

#[cfg(test)]
mod test {
    use std::collections::hash_map::Entry;
    use std::collections::{HashMap, VecDeque};
    use std::convert::{TryFrom, TryInto};
    use std::sync::{Arc, Mutex};

    use dsf_core::options::{Filters, Options};
    use dsf_core::page::ServiceLink;
    use dsf_rpc::ServiceInfo;
    use futures::future;

    use super::*;
    use dsf_core::prelude::*;

    struct MockEngine {
        inner: Arc<Mutex<Inner>>,
    }

    struct Inner {
        pub ns: Service,
        pub target: Service,
        pub expect: VecDeque<Expect>,
    }

    type Expect =
        Box<dyn Fn(OpKind, &mut Service, &mut Service) -> Result<Res, CoreError> + Send + 'static>;

    impl MockEngine {
        pub fn setup() -> (Self, Id, Id) {
            let _ = simplelog::SimpleLogger::init(
                simplelog::LevelFilter::Debug,
                simplelog::Config::default(),
            );

            let ns = ServiceBuilder::ns("test.com").build().unwrap();
            let target = ServiceBuilder::default()
                .public_options(vec![Options::name("something")])
                .build()
                .unwrap();

            let inner = Inner {
                ns: ns.clone(),
                target: target.clone(),
                expect: VecDeque::new(),
            };

            let e = MockEngine {
                inner: Arc::new(Mutex::new(inner)),
            };

            (e, ns.id(), target.id())
        }

        pub fn expect(&self, ops: Vec<Expect>) {
            let mut e = self.inner.lock().unwrap();
            e.expect = ops.into();
        }

        pub fn with<R, F: Fn(&mut Service, &mut Service) -> R>(&self, f: F) -> R {
            let mut i = self.inner.lock().unwrap();
            let Inner {
                ref mut ns,
                ref mut target,
                ..
            } = *i;

            f(ns, target)
        }
    }

    #[async_trait::async_trait]
    impl Engine for MockEngine {
        async fn exec(&self, op: OpKind) -> Result<Res, CoreError> {
            let mut i = self.inner.lock().unwrap();

            let Inner {
                ref mut ns,
                ref mut target,
                ref mut expect,
            } = *i;

            debug!("Exec op: {:?}", op);

            match expect.pop_front() {
                Some(f) => f(op, ns, target),
                None => panic!("No remaining expectations"),
            }
        }
    }

    #[tokio::test]
    async fn test_register() {
        let (e, ns_id, target_id) = MockEngine::setup();

        e.expect(vec![
            // Lookup NS
            Box::new(|op, ns, _t| match op {
                OpKind::ServiceResolve(ServiceIdentifier { id, .. }) if id == Some(ns.id()) => {
                    Ok(Res::Service(ns.clone()))
                }
                _ => panic!("Unexpected operation: {:?}, expected get {}", op, ns.id()),
            }),
            // Lookup target
            Box::new(|op, _ns, t| match op {
                OpKind::ServiceResolve(ServiceIdentifier { id, .. }) if id == Some(t.id()) => {
                    Ok(Res::Service(t.clone()))
                }
                _ => panic!("Unexpected operation: {:?}, expected get {}", op, t.id()),
            }),
            // Attempt NS registration
            Box::new(|op, ns, _t| match op {
                OpKind::ServiceUpdate(id, f) if id == ns.id() => f(ns),
                _ => panic!(
                    "Unexpected operation: {:?}, expected update {}",
                    op,
                    ns.id()
                ),
            }),
            // Publish pages to DHT
            Box::new(|op, ns, t| {
                match op {
                    OpKind::DhtPut(_id, pages) => {
                        // Check tertiary page info
                        let p = &pages[0];
                        let n = t.public_options().iter().name().unwrap();

                        assert_eq!(p.id(), ns.resolve(&Options::name(&n)).unwrap());
                        assert_eq!(
                            p.info(),
                            Ok(PageInfo::ServiceLink(ServiceLink {
                                target_id: t.id(),
                                peer_id: ns.id()
                            }))
                        );

                        Ok(Res::Ids(vec![]))
                    }
                    _ => panic!(
                        "Unexpected operation: {:?}, expected update {}",
                        op,
                        ns.id()
                    ),
                }
            }),
        ]);

        let _r = e
            .ns_register(NsRegisterOptions {
                ns: ServiceIdentifier::id(ns_id),
                target: target_id,
                name: Some("something".to_string()),
                hash: vec![],
            })
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_search() {
        let (e, ns_id, target_id) = MockEngine::setup();

        let target_info = ServiceInfo::from(&e.inner.lock().unwrap().target);

        // Pre-generate registration page
        let (name, primary, tertiary) = e.with(|ns, t| {
            let (_n, primary) = t.publish_primary_buff(Default::default()).unwrap();

            let name = t.public_options().iter().name().unwrap();
            let (_, tertiary) = ns
                .publish_tertiary_buff::<256, _>(
                    t.id().into(),
                    Default::default(),
                    Options::name(&name),
                )
                .unwrap();

            (name, primary.to_owned(), tertiary.to_owned())
        });
        let p = primary.clone();

        e.expect(vec![
            // Lookup NS
            Box::new(|op, ns, _t| match op {
                OpKind::ServiceResolve(ServiceIdentifier { id, .. }) if id == Some(ns.id()) => {
                    Ok(Res::Service(ns.clone()))
                }
                _ => panic!("Unexpected operation: {:?}, expected get {}", op, ns.id()),
            }),
            // Lookup tertiary pages in dht
            Box::new(move |op, ns, _t| match op {
                OpKind::DhtSearch(_id) => Ok(Res::Pages(vec![tertiary.clone()])),
                _ => panic!(
                    "Unexpected operation: {:?}, expected DhtSearch for tertiary page{}",
                    op,
                    ns.id()
                ),
            }),
            // Lookup primary pages for linked service
            Box::new(move |op, ns, _t| match op {
                OpKind::DhtSearch(id) if id == target_id => Ok(Res::Pages(vec![primary.clone()])),
                _ => panic!(
                    "Unexpected operation: {:?}, expected DhtSearch for primary page {}",
                    op,
                    ns.id()
                ),
            }),
            // Register newly discovered service
            Box::new(move |op, _ns, t| match op {
                OpKind::ServiceCreate(id, p) if id == t.id() => {
                    Ok(Res::ServiceInfo(target_info.clone()))
                }
                _ => panic!(
                    "Unexpected operation: {:?}, expected ServiceCreate {}",
                    op,
                    t.id()
                ),
            }),
        ]);

        let r = e
            .ns_search(NsSearchOptions {
                ns: ServiceIdentifier::id(ns_id),
                name: Some(name.to_string()),
                hash: None,
            })
            .await
            .unwrap();

        // Returns pages for located service
        assert_eq!(&r, &[p]);
    }
}
