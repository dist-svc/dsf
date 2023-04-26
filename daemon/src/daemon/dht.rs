use std::collections::hash_map::RandomState;
use std::collections::HashMap;
use std::iter::FromIterator;

use dsf_core::wire::Container;
use kad::prelude::*;

use futures::channel::mpsc;

use dsf_core::net::{RequestBody, ResponseBody};
use dsf_core::prelude::*;
use dsf_core::types::{Data, Id, RequestId};

use super::{net::NetIf, Dsf};

use crate::core::peers::{Peer, PeerAddress, PeerFlags};
use crate::error::Error;

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
        req_id: RequestId,
        req: DhtRequest<Id, Data>,
    ) -> Result<DhtResponse<Id, Peer, Data>, Error> {
        // Map peer to existing DHT entry
        // TODO: resolve this rather than creating a new instance
        // (or, use only the index and rely on external storage etc.?)
        let entry = DhtEntry::new(from.into(), peer);

        // Pass to DHT
        let resp = self.dht_mut().handle_req(req_id, &entry, &req).unwrap();

        Ok(resp)
    }

    // Handle a DHT request message
    pub(crate) fn handle_dht_resp(
        &mut self,
        from: Id,
        peer: Peer,
        req_id: RequestId,
        resp: DhtResponse<Id, Peer, Data>,
    ) -> Result<(), Error> {
        // Map peer to existing DHT entry
        // TODO: resolve this rather than creating a new instance
        // (or, use only the index and rely on external storage etc.?)
        let entry = DhtEntry::new(from.into(), peer);

        // Pass to DHT
        let resp = self.dht_mut().handle_resp(req_id, &entry, &resp).unwrap();

        Ok(resp)
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

    pub(crate) fn dht_to_net_request(&mut self, req: DhtRequest<Id, Data>) -> NetRequestBody {
        match req {
            DhtRequest::Ping => RequestBody::Ping,
            DhtRequest::FindNode(id) => RequestBody::FindNode(Id::from(id.clone())),
            DhtRequest::FindValue(id) => RequestBody::FindValue(Id::from(id.clone())),
            DhtRequest::Store(id, values) => {
                RequestBody::Store(Id::from(id.clone()), values.to_vec())
            }
        }
    }

    pub(crate) fn dht_to_net_response(
        &mut self,
        resp: DhtResponse<Id, Peer, Data>,
    ) -> NetResponseBody {
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

    pub(crate) fn net_to_dht_request(
        &mut self,
        req: &NetRequestBody,
    ) -> Option<DhtRequest<Id, Data>> {
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

    pub(crate) fn net_to_dht_response(
        &mut self,
        resp: &NetResponseBody,
    ) -> Option<DhtResponse<Id, Peer, Data>> {
        // TODO: fix peers:new here peers:new
        match resp {
            ResponseBody::NodesFound(id, nodes) => {
                let mut dht_nodes = Vec::with_capacity(nodes.len());

                for (id, addr, key) in nodes {
                    // Add peer to local tracking
                    let node = self.peers().find_or_create(
                        id.clone(),
                        PeerAddress::Implicit(addr.clone()),
                        Some(key.clone()),
                        PeerFlags::empty(),
                    );

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
    // TODO: how to enable explicit version resets?

    let mut index = 0;
    let mut primary = None;

    for c in &ordered {
        let h = c.header();
        match c.info() {
            Ok(PageInfo::Primary(_)) if &c.id() == id && h.index() > index => {
                primary = Some(c.clone())
            }
            _ => (),
        }
    }

    if let Some(pri) = primary {
        filtered.push(pri.clone());
    }

    // Reduce secondary pages by peer_id (via a hashmap to get only the latest value)
    let secondaries = ordered.iter().filter_map(|c| match c.info() {
        Ok(PageInfo::Secondary(s)) if &c.id() == id => Some((s.peer_id, c.clone())),
        _ => None,
    });
    let mut map = HashMap::<_, _, RandomState>::from_iter(secondaries);
    filtered.extend(map.drain().map(|(_k, v)| v.clone()));

    // TODO: if there is no primary page, can we reject secondary pages?

    // Reduce tertiary pages by publisher (via a hashmap to leave only the latest value)
    let tertiary = ordered.iter().filter_map(|c| match c.info() {
        Ok(PageInfo::ServiceLink(s)) if &c.id() == id => Some((s.peer_id, c.clone())),
        Ok(PageInfo::BlockLink(s)) if &c.id() == id => Some((s.peer_id, c.clone())),

        _ => None,
    });
    let mut map = HashMap::<_, _, RandomState>::from_iter(tertiary);
    filtered.extend(map.drain().map(|(_k, v)| v.clone()));

    // TODO: should we be checking page sigs here or can we depend on these being validated earlier?
    // pretty sure it should be earlier...

    // TODO: we could also check publish / expiry times are valid and reasonable here
    // Secondary and tertiary pages _must_ contain issued / expiry times
    // Primary pages... are somewhat more difficult, eviction will need to be tracked based on when they are stored

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

        let (_, p1a) = peer1
            .publish_secondary_buff(&svc.id(), Default::default())
            .unwrap();
        let (_, p1b) = peer1
            .publish_secondary_buff(&svc.id(), Default::default())
            .unwrap();

        let (_, p2a) = peer2
            .publish_secondary_buff(&svc.id(), Default::default())
            .unwrap();
        let (_, p2b) = peer2
            .publish_secondary_buff(&svc.id(), Default::default())
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
    fn test_reduce_tertiary() {
        let mut ns1 = setup();
        let mut ns2 = setup();

        let name = Options::name("test-name");
        let id = ns1.resolve(&name).unwrap();

        let mut tertiary_opts = TertiaryOptions {
            index: 0,
            issued: DateTime::now(),
            expiry: DateTime::now().add(Duration::from_secs(60 * 60)),
        };

        // Generate two pages
        let (_, t1a) = ns1
            .publish_tertiary_buff::<256, _>(id.clone().into(), tertiary_opts.clone(), &name)
            .unwrap();

        tertiary_opts.index = 1;
        let (_, t1b) = ns1
            .publish_tertiary_buff::<256, _>(id.clone().into(), tertiary_opts.clone(), &name)
            .unwrap();

        let pages = vec![t1a.to_owned(), t1b.to_owned()];

        // Reduce should leave only the _later_ tertiary page
        // TODO: could sort by time equally as well as index?
        let mut r = dht_reducer(&id, &pages);

        let mut e = vec![t1b.to_owned()];

        r.sort_by_key(|p| p.signature());
        e.sort_by_key(|p| p.signature());

        assert_eq!(r, e);
    }
}
