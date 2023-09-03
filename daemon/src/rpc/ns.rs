//! Registry register and search operations for interactions with name services
//!
//!

use std::time::Duration;
use std::{convert::TryFrom, time::Instant};

use futures::{future, Future, FutureExt};
use serde::{Deserialize, Serialize};
use tracing::{debug, error, info, instrument, span, warn, Level};

use dsf_core::{
    base::Empty,
    error::Error as CoreError,
    options::{self, Filters},
    prelude::{DsfError, Options, PageInfo},
    service::{
        DataOptions, Publisher, Registry, ServiceBuilder, TertiaryData, TertiaryLink,
        TertiaryOptions,
    },
    types::{CryptoHash, DataKind, DateTime, Flags, Id, PageKind},
    wire::Container,
};
use dsf_rpc::{
    DataInfo, DhtInfo, LocateInfo, LocateOptions, NsCreateOptions, NsRegisterInfo,
    NsRegisterOptions, NsSearchInfo, NsSearchOptions, Response, ServiceIdentifier, ServiceInfo,
};

use crate::daemon::Dsf;
use crate::error::Error;
use crate::rpc::locate::ServiceRegistry;
use crate::rpc::ops::Res;

use super::ops::{Engine, OpKind};

/// [NameService] trait provides NS operations over an [Engine] implementation
pub trait NameService {
    /// Create a new name service
    async fn ns_create(&self, opts: NsCreateOptions) -> Result<ServiceInfo, DsfError>;

    /// Search for a service or block by hashed value
    async fn ns_search(&self, opts: NsSearchOptions) -> Result<NsSearchInfo, DsfError>;

    /// Register a service by name
    async fn ns_register(&self, opts: NsRegisterOptions) -> Result<NsRegisterInfo, DsfError>;
}

impl<T: Engine> NameService for T {
    /// Create a new name service
    #[instrument(skip(self))]
    async fn ns_create(&self, opts: NsCreateOptions) -> Result<ServiceInfo, DsfError> {
        debug!("Creating new nameservice (opts: {:?})", opts);

        // Create nameservice instance
        let mut sb = ServiceBuilder::<Vec<u8>>::ns(&opts.name);
        sb = sb.kind(PageKind::Name);

        // If the service is not public, encrypt the object
        if !opts.public {
            sb = sb.encrypt();
        }

        debug!("Generating service");
        let mut service = match sb.build() {
            Ok(v) => v,
            Err(e) => {
                error!("Failed to build name service: {:?}", e);
                return Err(e);
            }
        };

        // Generate primary page for NS
        debug!("Generating service page");
        // TODO: revisit this
        let buff = vec![0u8; 1024];
        let (_n, primary_page) = service.publish_primary(Default::default(), buff).unwrap();

        // Persist service and page to local store
        debug!("Storing service instance");
        let id = service.id();
        match self.svc_create(service, primary_page.clone()).await {
            Ok(v) => v,
            Err(e) => {
                error!("Failed to create service: {:?}", e);
                return Err(e);
            }
        };

        // Register in DHT
        debug!("Registering service in DHT");
        if let Err(e) = self.dht_put(id.clone(), vec![primary_page]).await {
            error!("Failed to register service: {:?}", e);
        }

        debug!("Created NameService {}", id);

        let info = self.svc_get(id).await?;

        Ok(info)
    }

    /// Search for a matching service using the provided (or relevant) nameserver
    #[instrument(skip(self))]
    async fn ns_search(&self, opts: NsSearchOptions) -> Result<NsSearchInfo, DsfError> {
        debug!("Locating nameservice");

        let t1 = Instant::now();

        // Resolve nameserver using provided options
        let ns = self.svc_resolve(opts.ns).await;

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
        let lookup = match (&opts.name, &opts.options, &opts.hash) {
            (Some(n), _, _) => ns.resolve(&Options::name(&n))?,
            (_, Some(o), _) => ns.resolve(o)?,
            (_, _, Some(h)) => ns.resolve(h)?,
            _ => {
                error!("Search requires name, option, or hash argument");
                return Err(DsfError::InvalidOption);
            }
        };

        info!("NS query for {} via {}", lookup, ns.id());

        // Issue query for tertiary pages
        let (mut tertiary_pages, search_info) = match self.dht_search(lookup.clone()).await {
            Ok(p) => p,
            Err(e) => {
                error!("DHT lookup failed: {:?}", e);
                return Err(e.into());
            }
        };

        debug!("Located {} tertiary pages", tertiary_pages.len());

        // TODO: should we reduce the response pages here?
        // Possible to have > 1 service link pages where target services are different
        // Only ever 1 data link page at a given location
        // (except collisions... what're the odds?)

        // Collapse resolved pages
        let mut resolved = vec![];

        for p in &mut tertiary_pages {
            // Decrypt responses if required
            // TODO: check we have the keys for encrypted NS' before starting this
            if p.encrypted() {
                if let Some(sk) = ns.secret_key() {
                    if let Err(e) = p.decrypt(&sk) {
                        error!("Failed to decrypt tertiary page: {:?}", e);
                        continue;
                    }
                } else {
                    warn!("Received encrypted tertiary page with no decryption key");
                    continue;
                }
            }

            let info = p.info();
            debug!("Using info: {:?}", info);

            // Check page is of tertiary kind
            match info {
                // Fetch information for linked service
                Ok(PageInfo::ServiceLink(s)) if s.peer_id == ns.id() => {
                    // Check whether service can be locally resolved
                    let i = match self
                        .service_locate(LocateOptions {
                            id: s.target_id.clone(),
                            local_only: false,
                            no_persist: opts.no_persist,
                        })
                        .await
                    {
                        Ok(i) => i,
                        Err(e) => {
                            error!("Failed to locate service {}: {:?}", s.target_id, e);
                            continue;
                        }
                    };

                    debug!("Located service: {:?}", i);

                    resolved.push(i);
                }
                // Fetch linked block
                // TODO: remove this function for now
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

            // TODO: verify services matches request filters
            // _if_ this is possible with opaque hashes..?
        }

        let elapsed = Instant::now().duration_since(t1);
        info!(
            "Search complete after {} ms: {:?}",
            elapsed.as_millis(),
            resolved
        );

        // Return resolved service information
        Ok(NsSearchInfo {
            ns: ns.id(),
            hash: lookup,
            matches: resolved,
            info: dsf_rpc::DhtInfo {
                depth: search_info.depth,
                elapsed,
            },
        })
    }

    /// Register a service with the provided name service
    #[instrument(skip(self))]
    async fn ns_register(&self, opts: NsRegisterOptions) -> Result<NsRegisterInfo, DsfError> {
        debug!("Locating nameservice");

        let t1 = Instant::now();

        // Resolve nameserver using provided options
        let ns = self.svc_resolve(opts.ns.clone()).await;

        // TODO: support lookups by prefix
        // Check we found a viable name service
        let ns = match ns {
            Ok(ns) => ns,
            Err(_e) => {
                error!("No matching nameservice found");
                return Err(DsfError::NotFound);
            }
        };

        debug!("Using nameservice: {}", ns.id());

        // Ensure this _is_ a name service
        if ns.kind() != PageKind::Name {
            error!("Service {} not a nameservice ({:?})", ns.id(), ns);
            return Err(DsfError::Unknown);
        }

        // Check we can use this for publishing
        if !ns.is_origin() {
            error!("Publishing to remote name services not yet supported");
            return Err(DsfError::Unimplemented);
        }

        // Lookup prefix for NS
        let prefix = ns.public_options().iter().find_map(|o| match o {
            Options::Name(n) => Some(n.to_string()),
            _ => None,
        });

        debug!("Locating target service for register operation: {:?}", opts);

        // Lookup service to be registered
        // TODO: fallback / use DHT if service is not known locally?
        let target = match self.svc_resolve(opts.target.into()).await {
            Ok(s) => s,
            Err(e) => {
                error!("No matching target service found: {:?}", e);
                return Err(DsfError::NotFound);
            }
        };

        info!(
            "Registering service: {} via ns: {} ({:?}) ",
            target.id(),
            ns.id(),
            prefix
        );

        // Attach target options to NS object
        let mut private_options = opts.options.clone();

        // Generate TIDs from provided options and hashes
        let mut tids = vec![];

        // Name if provided
        // TODO: check name is not duplicated
        if let Some(n) = opts.name.as_ref().map(Options::name) {
            private_options.push(n.clone());
            tids.push(ns.resolve(n)?);
        }

        // Generic TIDs using options
        for o in &opts.options {
            tids.push(ns.resolve(o)?);
        }

        // Application-specific TIDs via pre-hashed values
        for h in &opts.hashes {
            tids.push(ns.resolve(h)?);
        }

        private_options.push(Options::peer_id(target.id()));

        // TODO: setup issued / expiry times to be consistent
        let issued = DateTime::now();
        let expiry = issued + Duration::from_secs(60 * 60);

        // Generate name service data block
        let body = Some(TertiaryData { tids: tids.clone() });
        let res = self
            .svc_update(
                ns.id(),
                Box::new(move |s, _| {
                    // First, create a data block for the new registration
                    let (_, d) = s.publish_data_buff::<TertiaryData>(DataOptions {
                        data_kind: DataKind::Name.into(),
                        body: body.clone(),
                        private_options: &private_options,
                        public_options: &[Options::expiry(expiry.clone())],
                        ..Default::default()
                    })?;

                    Ok(Res::Pages(vec![d.to_owned()], None))
                }),
            )
            .await?;
        let data = match res {
            Res::Pages(p, _) if p.len() == 1 => p[0].clone(),
            _ => unreachable!(),
        };

        // Store data block
        debug!("Storing NS data: {:#}", data.signature());
        self.object_put(data.clone()).await?;

        // TODO: Lookup subscribers and distribute update
        // self.net_req(req, peers)

        // Generate tertiary pages
        let mut pages = vec![];
        for t in &tids {
            let r = ns.publish_tertiary_buff::<512>(
                TertiaryLink::Service(target.id()),
                TertiaryOptions {
                    index: data.header().index(),
                    issued: issued,
                    expiry: expiry,
                },
                t.clone(),
            );

            match r {
                Ok((_n, p)) => pages.push(p.to_owned()),
                Err(e) => {
                    error!("Failed to generate tertiary page: {:?}", e);
                }
            }
        }

        // Publish pages to database
        let mut info = vec![];
        for p in pages {
            // TODO: handle no peers case, return list of updated pages perhaps?
            let put_t1 = Instant::now();
            match self.dht_put(p.id(), vec![p]).await {
                Ok((_, i)) => info.push(DhtInfo {
                    depth: i.depth,
                    elapsed: Instant::now().duration_since(put_t1),
                }),
                Err(e) => {
                    warn!("Failed to publish pages to DHT: {:?}", e);
                }
            }
        }

        // TODO: return result
        let info = NsRegisterInfo {
            ns: ns.id(),
            prefix,
            name: opts.name,
            tids,
            info,
        };

        let elapsed = Instant::now().duration_since(t1);
        info!(
            "Register complete after {} ms: {:?}",
            elapsed.as_millis(),
            info
        );

        Ok(info)
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
    use dsf_rpc::{ServiceFlags, ServiceInfo, ServiceState};
    use futures::future;

    use crate::rpc::SearchInfo;

    use super::*;
    use dsf_core::prelude::*;

    struct MockEngine {
        inner: Arc<Mutex<Inner>>,
        id: Id,
    }

    impl core::fmt::Debug for MockEngine {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("MockEngine").finish()
        }
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
                id: ns.id(),
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
        fn id(&self) -> Id {
            self.id.clone()
        }

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
                None => panic!("No remaining expectations for op: {:?}", op),
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
                OpKind::ServiceUpdate(id, f) if id == ns.id() => {
                    let mut state = ServiceState::Created;
                    f(ns, &mut state)
                }
                _ => panic!(
                    "Unexpected operation: {:?}, expected update {}",
                    op,
                    ns.id()
                ),
            }),
            // Store NS data block
            Box::new(|op, ns, _t| {
                match op {
                    OpKind::ObjectPut(object) => {
                        // TODO: check object contains expected NS information
                        Ok(Res::Sig(object.signature()))
                    }
                    _ => panic!(
                        "Unexpected operation: {:?}, expected object put {}",
                        op,
                        ns.id()
                    ),
                }
            }),
            // TODO: distribute updates to subscribers
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
                hashes: vec![],
                options: vec![],
            })
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_search() {
        let (e, ns_id, target_id) = MockEngine::setup();

        let mut target_info = ServiceInfo::from(&e.inner.lock().unwrap().target);

        // Pre-generate registration page
        let (name, primary, tertiary) = e.with(|ns, t| {
            let (_n, primary) = t.publish_primary_buff(Default::default()).unwrap();

            let name = t.public_options().iter().name().unwrap();
            let tid = ns.resolve(&Options::name(&name)).unwrap();
            let (_, tertiary) = ns
                .publish_tertiary_buff::<256>(t.id().into(), Default::default(), tid)
                .unwrap();

            (name, primary.to_owned(), tertiary.to_owned())
        });
        let t = target_id.clone();
        let p = primary.clone();
        target_info.primary_page = Some(p.signature());

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
                OpKind::DhtSearch(_id) => Ok(Res::Pages(
                    vec![tertiary.clone()],
                    Some(SearchInfo::default()),
                )),
                _ => panic!(
                    "Unexpected operation: {:?}, expected DhtSearch for tertiary page{}",
                    op,
                    ns.id()
                ),
            }),
            // Lookup linked service locally
            Box::new(move |op, ns, _t| match op {
                OpKind::ServiceGet(_id) => Err(DsfError::NotFound),
                _ => panic!(
                    "Unexpected operation: {:?}, expected DhtSearch for primary page {}",
                    op,
                    ns.id()
                ),
            }),
            // Lookup primary pages for linked service
            Box::new(move |op, ns, _t| match op {
                OpKind::DhtSearch(id) if id == t => Ok(Res::Pages(
                    vec![primary.clone()],
                    Some(SearchInfo::default()),
                )),
                _ => panic!(
                    "Unexpected operation: {:?}, expected DhtSearch for primary page {}",
                    op,
                    ns.id()
                ),
            }),
            // Register newly discovered service
            Box::new(move |op, _ns, t| match op {
                OpKind::ServiceRegister(id, _p) if id == t.id() => {
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
                options: None,
                no_persist: false,
            })
            .await
            .unwrap();

        // Returns pages for located service
        assert_eq!(
            &r.matches,
            &[LocateInfo {
                id: target_id,
                flags: ServiceFlags::empty(),
                updated: true,
                page_version: 0,
                page: Some(p)
            }]
        );
    }
}
