use std::collections::hash_map::{Entry, RandomState};
use std::collections::HashMap;
use std::iter::FromIterator;
use std::num;

use dsf_core::options::Filters;
use dsf_core::wire::Container;
use kad::prelude::*;

use futures::channel::mpsc;

use dsf_core::net::{RequestBody, ResponseBody};
use dsf_core::prelude::*;
use dsf_core::types::{Data, Id, RequestId};
use log::error;

use super::{net::NetIf, Dsf};

use crate::core::peers::{Peer, PeerAddress, PeerFlags};
use crate::error::Error;
use crate::rpc::Engine;

/// Adaptor to convert between DSF and DHT requests/responses
#[derive(Clone)]
pub struct DhtAdaptor {
    dht_sink: mpsc::Sender<DsfDhtMessage>,
}

pub struct DsfDhtMessage {
    pub(crate) target: DhtEntry<Id, Peer>,
    pub(crate) req: DhtRequest<Id, Data>,
    pub(crate) resp_sink: mpsc::Sender<DhtResponse<Id, Peer, Data>>,
}

impl<Net> Dsf<Net>
where
    Dsf<Net>: NetIf<Interface = Net>,
{
    /// Handle a DHT request message
    pub(crate) fn handle_dht_req(
        &mut self,
        from: Id,
        peer: Peer,
        req: DhtRequest<Id, Data>,
    ) -> Result<DhtResponse<Id, Peer, Data>, Error> {
        // Map peer to existing DHT entry
        // TODO: resolve this rather than creating a new instance
        // (or, use only the index and rely on external storage etc.?)
        let entry = DhtEntry::new(from.into(), peer);

        // Pass to DHT
        match self.dht_mut().handle_req(&entry, &req) {
            Ok(resp) => Ok(resp),
            Err(e) => {
                error!("Error handling DHT request {:?}: {:?}", req, e);
                Err(Error::Unknown)
            }
        }
    }

    pub(crate) fn is_dht_req(msg: &NetRequest) -> bool {
        match msg.data {
            RequestBody::Ping
            | RequestBody::Store(_, _)
            | RequestBody::FindValue(_)
            | RequestBody::FindNode(_) => true,
            _ => false,
        }
    }

    pub(crate) fn is_dht_resp(msg: &NetResponse) -> bool {
        match msg.data {
            ResponseBody::NoResult
            | ResponseBody::NodesFound(_, _)
            | ResponseBody::ValuesFound(_, _) => true,
            _ => false,
        }
    }

    pub(crate) fn dht_to_net_request(req: DhtRequest<Id, Data>) -> NetRequestBody {
        match req {
            DhtRequest::Ping => RequestBody::Ping,
            DhtRequest::FindNode(id) => RequestBody::FindNode(Id::from(id.clone())),
            DhtRequest::FindValue(id) => RequestBody::FindValue(Id::from(id.clone())),
            DhtRequest::Store(id, values) => {
                RequestBody::Store(Id::from(id.clone()), values.to_vec())
            }
        }
    }

    pub(crate) fn dht_to_net_response(resp: DhtResponse<Id, Peer, Data>) -> NetResponseBody {
        match resp {
            DhtResponse::NodesFound(id, nodes) => {
                let nodes = nodes
                    .iter()
                    .filter_map(|n| {
                        // Drop unseen or nodes without keys from responses
                        // TODO: is this the desired behaviour?
                        if n.info().pub_key().is_none() || n.info().seen().is_none() {
                            None
                        } else {
                            Some((
                                Id::from(n.id().clone()),
                                n.info().address(),
                                n.info().pub_key().unwrap(),
                            ))
                        }
                    })
                    .collect();

                ResponseBody::NodesFound(Id::from(id.clone()), nodes)
            }
            DhtResponse::ValuesFound(id, values) => {
                ResponseBody::ValuesFound(Id::from(id.clone()), values.to_vec())
            }
            DhtResponse::NoResult => ResponseBody::NoResult,
        }
    }

    pub(crate) fn net_to_dht_request(req: &NetRequestBody) -> Option<DhtRequest<Id, Data>> {
        match req {
            RequestBody::Ping => Some(DhtRequest::Ping),
            RequestBody::FindNode(id) => Some(DhtRequest::FindNode(Id::into(id.clone()))),
            RequestBody::FindValue(id) => Some(DhtRequest::FindValue(Id::into(id.clone()))),
            RequestBody::Store(id, values) => {
                Some(DhtRequest::Store(Id::into(id.clone()), values.to_vec()))
            }
            _ => None,
        }
    }

    pub(crate) async fn net_to_dht_response<E: Engine>(
        e: &E,
        resp: &NetResponseBody,
    ) -> Option<DhtResponse<Id, Peer, Data>> {
        // TODO: fix peers:new here peers:new
        match resp {
            ResponseBody::NodesFound(id, nodes) => {
                let mut dht_nodes = Vec::with_capacity(nodes.len());

                for (id, addr, key) in nodes {
                    // Add peer to local tracking
                    let node = e
                        .peer_create_update(
                            id.clone(),
                            PeerAddress::Implicit(addr.clone()),
                            Some(key.clone()),
                            PeerFlags::empty(),
                        )
                        .await
                        .unwrap();

                    dht_nodes.push((id.clone(), node).into());
                }

                Some(DhtResponse::NodesFound(Id::into(id.clone()), dht_nodes))
            }
            ResponseBody::ValuesFound(id, values) => Some(DhtResponse::ValuesFound(
                Id::into(id.clone()),
                values.to_vec(),
            )),
            ResponseBody::NoResult => Some(DhtResponse::NoResult),
            _ => None,
        }
    }
}

/// Reducer function reduces pages stored in the database
pub(crate) fn dht_reducer(id: &Id, pages: &[Container]) -> Vec<Container> {
    // Build sorted array for filtering
    let mut ordered: Vec<_> = pages.iter().collect();
    ordered.sort_by_key(|p| p.header().index());

    let mut filtered = vec![];

    // Select the latest primary page

    let index = 0;
    let mut primary = None;

    for c in &ordered {
        let h = c.header();
        match c.info() {
            Ok(PageInfo::Primary(_)) if &c.id() == id && h.index() >= index => {
                primary = Some(c.to_owned())
            }
            _ => (),
        }
    }

    if let Some(pri) = primary {
        filtered.push(pri.clone());
    }

    // Reduce secondary pages by peer_id and version (via a hashmap to get only the latest value)
    // These must be public so `.info()` works here
    let secondaries = ordered.iter().filter_map(|c| match c.info() {
        Ok(PageInfo::Secondary(s)) if &c.id() == id && !c.expired() => {
            Some((s.peer_id, c.to_owned()))
        }
        _ => None,
    });
    let mut map = HashMap::<Id, Container, _>::new();
    for (peer, c) in secondaries {
        match map.entry(peer) {
            Entry::Occupied(mut o) if o.get().header().index() < c.header().index() => {
                o.insert(c.clone());
            }
            Entry::Occupied(_) => (),
            Entry::Vacant(v) => {
                v.insert(c.clone());
            }
        }
    }

    filtered.extend(map.drain().map(|(_k, v)| v.clone()));

    // Reduce tertiary pages
    // These may be public or private, so must be handled without knowledge of the internals

    // For public registries we can reduce by linked service (only keep the latest tertiary page for a given linked service)
    let mut public_tertiaries = HashMap::<Id, &Container>::new();
    for c in ordered.iter() {
        if c.expired() {
            continue;
        }

        if let Ok(PageInfo::ServiceLink(s)) = c.info() {
            match public_tertiaries.entry(s.target_id) {
                Entry::Occupied(mut e) if e.get().header().index() < c.header().index() => {
                    e.insert(c);
                }
                Entry::Vacant(e) => {
                    e.insert(c);
                }
                _ => (),
            }
        }
    }

    println!("Public tertiaries: {:?}", public_tertiaries);

    // For private registries we just have to take the latest subset of pages
    // TODO: any better approach to this? some form of deterministic one-way fn for
    let private_tertiaries = ordered.iter().filter(|c| {
        let h = c.header();
        h.kind().is_page() && h.flags().contains(Flags::TERTIARY | Flags::ENCRYPTED) && !c.expired()
    });

    println!("Private tertiaries: {:?}", private_tertiaries);

    // TODO: should we reduce per-ns?
    // (it is _very improbable_ that hash collisions result in more than one NS attempting to use the same TID)

    // Then we need to deduplicate by index
    let mut tertiaries = HashMap::<u32, &Container>::new();
    for (_k, c) in public_tertiaries {
        tertiaries.insert(c.header().index(), c);
    }
    for c in private_tertiaries {
        tertiaries.insert(c.header().index(), c);
    }

    // And finally sort by index and limit to the latest N indicies
    let mut tertiaries: Vec<&Container> = tertiaries.iter().map(|(_k, v)| *v).collect();
    tertiaries.sort_by_key(|c| c.header().index());

    let num_tertiaries = tertiaries.len().min(16);
    let tertiaries = tertiaries.drain(..num_tertiaries).map(|c| c.clone());

    filtered.extend(tertiaries);

    // TODO: should we be checking page sigs here or can we depend on these being validated earlier?
    // pretty sure it should be earlier...

    // TODO: expire pages
    // Secondary and tertiary pages _must_ contain issued / expiry times
    // Primary pages are somewhat more difficult as constrained devices may not have times...
    // eviction will need to be tracked based on when they are stored

    filtered
}

#[cfg(test)]
mod test {
    use std::{
        ops::Add,
        time::{Duration, SystemTime},
    };

    use super::*;
    use dsf_core::{
        prelude::*,
        service::{Registry, TertiaryOptions},
        types::DateTime,
    };

    fn setup() -> Service {
        ServiceBuilder::generic()
            .build()
            .expect("Failed to build service")
    }

    #[test]
    fn test_reduce_primary() {
        let mut svc = setup();

        let (_, p1) = svc.publish_primary_buff(Default::default()).unwrap();
        let (_, p2) = svc.publish_primary_buff(Default::default()).unwrap();

        let r = dht_reducer(&svc.id(), &[p1.to_owned(), p2.to_owned()]);
        assert_eq!(r, vec![p2.to_owned()]);
    }

    #[test]
    fn test_reduce_secondary() {
        let mut svc = setup();
        let mut peer1 = setup();
        let mut peer2 = setup();

        let (_, svc_page) = svc.publish_primary_buff(Default::default()).unwrap();

        let mut version = 0;

        let (_, p1a) = peer1
            .publish_secondary_buff(
                &svc.id(),
                SecondaryOptions {
                    version,
                    ..Default::default()
                },
            )
            .unwrap();
        version += 1;

        let (_, p1b) = peer1
            .publish_secondary_buff(
                &svc.id(),
                SecondaryOptions {
                    version,
                    ..Default::default()
                },
            )
            .unwrap();
        version += 1;

        let (_, p2a) = peer2
            .publish_secondary_buff(
                &svc.id(),
                SecondaryOptions {
                    version,
                    ..Default::default()
                },
            )
            .unwrap();
        version += 1;

        let (_, p2b) = peer2
            .publish_secondary_buff(
                &svc.id(),
                SecondaryOptions {
                    version,
                    ..Default::default()
                },
            )
            .unwrap();

        let pages = vec![
            svc_page.to_owned(),
            p1a.to_owned(),
            p1b.to_owned(),
            p2a.to_owned(),
            p2b.to_owned(),
        ];

        let mut r = dht_reducer(&svc.id(), &pages);

        let mut e = vec![svc_page.to_owned(), p1b.to_owned(), p2b.to_owned()];

        r.sort_by_key(|p| p.signature());
        e.sort_by_key(|p| p.signature());

        assert_eq!(r, e);
    }

    #[test]
    fn test_reduce_tertiary_public() {
        let ns1: Service<Vec<u8>> = ServiceBuilder::ns("test-ns").build().unwrap();
        let _ns2 = setup();

        let target1 = Id::from(rand::random::<[u8; 32]>());
        let target2 = Id::from(rand::random::<[u8; 32]>());

        let name = Options::name("test-name");
        let tid = ns1.resolve(&name).unwrap();

        let mut tertiary_opts = TertiaryOptions {
            index: 0,
            issued: DateTime::now(),
            expiry: DateTime::now().add(Duration::from_secs(60 * 60)),
        };

        // Generate two pages
        let (_, t1) = ns1
            .publish_tertiary_buff::<512>(
                target1.clone().into(),
                tertiary_opts.clone(),
                tid.clone(),
            )
            .unwrap();

        tertiary_opts.index += 1;
        let (_, t2) = ns1
            .publish_tertiary_buff::<512>(
                target2.clone().into(),
                tertiary_opts.clone(),
                tid.clone(),
            )
            .unwrap();

        tertiary_opts.index += 1;
        let (_, t2a) = ns1
            .publish_tertiary_buff::<512>(
                target2.clone().into(),
                tertiary_opts.clone(),
                tid.clone(),
            )
            .unwrap();

        let pages = vec![t1.to_owned(), t2.to_owned(), t2a.to_owned()];

        // Reduce should leave only the _later_ tertiary page for a given target
        let mut r = dht_reducer(&tid, &pages);

        let mut e = vec![t1.to_owned(), t2a.to_owned()];

        r.sort_by_key(|p| p.signature());
        e.sort_by_key(|p| p.signature());

        assert_eq!(r, e);
    }

    #[test]
    fn test_reduce_tertiary_private() {
        let ns1: Service<Vec<u8>> = ServiceBuilder::ns("test-ns").encrypt().build().unwrap();
        let _ns2 = setup();

        let target1 = Id::from(rand::random::<[u8; 32]>());
        let target2 = Id::from(rand::random::<[u8; 32]>());

        let name = Options::name("test-name");
        let tid = ns1.resolve(&name).unwrap();

        let mut tertiary_opts = TertiaryOptions {
            index: 0,
            issued: DateTime::now(),
            expiry: DateTime::now().add(Duration::from_secs(60 * 60)),
        };

        // Generate two pages
        let (_, t1) = ns1
            .publish_tertiary_buff::<512>(
                target1.clone().into(),
                tertiary_opts.clone(),
                tid.clone(),
            )
            .unwrap();

        tertiary_opts.index = 1;
        let (_, t2) = ns1
            .publish_tertiary_buff::<512>(
                target2.clone().into(),
                tertiary_opts.clone(),
                tid.clone(),
            )
            .unwrap();

        let pages = vec![t1.to_owned(), t2.to_owned()];

        // Reduce should leave only the _later_ tertiary page
        // TODO: could sort by time equally as well as index?
        let mut r = dht_reducer(&tid, &pages);

        let mut e = vec![t1.to_owned(), t2.to_owned()];

        r.sort_by_key(|p| p.signature());
        e.sort_by_key(|p| p.signature());

        assert_eq!(r, e);
    }
}
