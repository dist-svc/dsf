//! Registry register and search operations for interactions with name services
//!
//!

use std::convert::TryFrom;
use std::time::Duration;

use dsf_core::base::Empty;
use dsf_core::wire::Container;
use futures::{future, Future, FutureExt};
use log::{debug, error, info, warn};
use serde::{Deserialize, Serialize};

use dsf_core::error::Error as CoreError;
use dsf_core::options::{self, Filters};
use dsf_core::prelude::{DsfError, Options, PageInfo};
use dsf_core::service::{Registry, ServiceBuilder, Publisher, DataOptions, TertiaryData, TertiaryLink, TertiaryOptions};
use dsf_core::types::{CryptoHash, Flags, Id, PageKind, DateTime};

use dsf_rpc::{
    LocateOptions, NsRegisterInfo, NsRegisterOptions, NsSearchOptions, Response, ServiceIdentifier, ServiceInfo, NsCreateOptions, DataInfo, LocateInfo,
};

use crate::daemon::Dsf;
use crate::error::Error;
use crate::rpc::locate::ServiceRegistry;
use crate::rpc::ops::Res;

use super::ops::{Engine, OpKind, RpcKind};

#[async_trait::async_trait]
pub trait NameService {
    /// Create a new name service
    async fn ns_create(&self, opts: NsCreateOptions) -> Result<ServiceInfo, DsfError>;

    /// Search for a service or block by hashed value
    async fn ns_search(&self, opts: NsSearchOptions) -> Result<Vec<LocateInfo>, DsfError>;

    /// Register a service by name
    async fn ns_register(&self, opts: NsRegisterOptions) -> Result<NsRegisterInfo, DsfError>;
}

#[async_trait::async_trait]
impl<T: Engine> NameService for T {

    /// Create a new name service
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
                return Err(e)
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
        match self.service_create(service, primary_page.clone()).await {
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

        let info = self.service_get(id).await?;

        Ok(info)
    }

    /// Search for a matching service using the provided (or relevant) nameserver
    async fn ns_search(&self, opts: NsSearchOptions) -> Result<Vec<LocateInfo>, DsfError> {
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
        let mut tertiary_pages = match self.dht_search(lookup).await {
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
                    let i = match self.service_locate(LocateOptions{
                        id: s.target_id.clone(),
                        local_only: false,
                    }).await {
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

        // Return resolved service information
        Ok(resolved)
    }

    async fn ns_register(&self, opts: NsRegisterOptions) -> Result<NsRegisterInfo, DsfError> {
        debug!("Locating nameservice for register: {:?}", opts);

        // Resolve nameserver using provided options
        let ns = self.service_resolve(opts.ns.clone()).await;

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
        let target = match self.service_resolve(opts.target.into()).await {
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

        // Generate TIDs from provided options and hashes
        let mut tids = vec![];
        if let Some(n) = &opts.name {
            tids.push(ns.resolve(&Options::name(n))?);
        }
        for h in &opts.hashes {
            tids.push(ns.resolve(h)?);
        }

        // TODO: setup issued / expiry times to be consistent
        let issued = DateTime::now();
        let expiry = issued + Duration::from_secs(60 * 60);        

        // Generate name service data block
        let body = Some(TertiaryData{
            tids: tids.clone(),
        });
        let res = self
            .service_update(
                ns.id(),
                Box::new(move |s| {
                    // First, create a data block for the new registration
                    let (_, d) = s.publish_data_buff::<TertiaryData>(
                        DataOptions{
                            data_kind: PageKind::Name.into(),
                            body: body.clone(),
                            issued: Some(issued.clone()),
                            public_options: &[
                                Options::expiry(expiry.clone())
                            ],
                            ..Default::default()
                        }
                    )?;

                    Ok(Res::Pages(vec![d.to_owned()]))
                }),
            )
            .await?;
        let data = match res {
            Res::Pages(p) if p.len() == 1 => p[0].clone(),
            _ => unreachable!(),
        };

        // Store data block

        // TODO: remove info, doesn't provide any particular utility
        let info = DataInfo::try_from(&data).unwrap();
        self.object_put(info.clone(), data.clone()).await?;

        // TODO: Lookup subscribers and distribute update
        // self.net_req(req, peers)

        // Generate tertiary pages
        let mut pages = vec![];
        for t in tids {
            let r = ns.publish_tertiary_buff::<256>(
                TertiaryLink::Service(target.id()),
                TertiaryOptions{
                    index: info.index,
                    issued: issued,
                    expiry: expiry,
                },
                t,
            );

            match r {
                Ok((_n, p)) => pages.push(p.to_owned()),
                Err(e) => {
                    error!("Failed to generate tertiary page: {:?}", e);
                }
            }
        }

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
            hashes: opts.hashes,
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
                OpKind::ServiceUpdate(id, f) if id == ns.id() => f(ns),
                _ => panic!(
                    "Unexpected operation: {:?}, expected update {}",
                    op,
                    ns.id()
                ),
            }),
            // Store NS data block
            Box::new(|op, ns, t| {
                match op {
                    OpKind::ObjectPut(info, _object) => {
                        // TODO: check object contains expected NS information
                        Ok(Res::Sig(info.signature))
                    },
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
                .publish_tertiary_buff::<256>(
                    t.id().into(),
                    Default::default(),
                    tid,
                )
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
                OpKind::DhtSearch(_id) => Ok(Res::Pages(vec![tertiary.clone()])),
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
                OpKind::DhtSearch(id) if id == t => Ok(Res::Pages(vec![primary.clone()])),
                _ => panic!(
                    "Unexpected operation: {:?}, expected DhtSearch for primary page {}",
                    op,
                    ns.id()
                ),
            }),
            // Register newly discovered service
            Box::new(move |op, _ns, t| match op {
                OpKind::ServiceRegister(id, p) if id == t.id() => {
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
        assert_eq!(&r, &[LocateInfo{
            id: target_id, origin: true, updated: true, page_version: 0, page: Some(p) }]);
    }
}
