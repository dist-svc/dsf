use std::collections::hash_map::{Entry, RandomState};
use std::collections::HashMap;
use std::iter::FromIterator;
use std::num;
use std::time::SystemTime;

use futures::{
    channel::mpsc::{self, unbounded as unbounded_channel, UnboundedReceiver, UnboundedSender},
    SinkExt as _, StreamExt as _,
};
use kad::dht::DhtHandle;
use kad::prelude::*;
use tokio::select;
use tracing::{debug, error, trace};

use dsf_core::net::{RequestBody, ResponseBody};
use dsf_core::options::Filters;
use dsf_core::prelude::*;
use dsf_core::types::{Data, Id, RequestId};
use dsf_core::wire::Container;
use dsf_rpc::{PeerAddress, PeerFlags, PeerInfo, PeerState};

use super::DsfDht;
use super::{net::NetIf, Dsf};

use crate::error::Error;
use crate::rpc::Engine;

/// Adaptor to convert between DSF and DHT requests/responses
#[derive(Clone)]
pub struct DhtAdaptor {
    dht_sink: mpsc::Sender<DsfDhtMessage>,
}

pub struct DsfDhtMessage {
    pub(crate) target: DhtEntry<Id, PeerInfo>,
    pub(crate) req: DhtRequest<Id, Data>,
    pub(crate) resp_sink: mpsc::Sender<DhtResponse<Id, PeerInfo, Data>>,
}

#[derive(Clone)]
pub struct AsyncDht {
    ctl: UnboundedSender<DhtCtl>,
}

#[derive(Clone)]
pub enum DhtCtl {
    /// Handle incoming DHT request
    Handle(
        PeerInfo,
        DhtRequest<Id, Data>,
        UnboundedSender<DhtResponse<Id, PeerInfo, Data>>,
    ),

    /// Fetch a DHT handle
    GetHandle(UnboundedSender<DhtHandle<Id, PeerInfo, Data>>),

    /// Exit DHT task
    Exit,
}

impl AsyncDht {
    pub fn new(mut dht: DsfDht) -> Self {
        // Setup control channel
        let (tx, mut rx) = unbounded_channel();

        // Setup message handler task
        tokio::task::spawn(async move {
            debug!("Starting DHT task");

            loop {
                select! {
                    // Poll on control messages
                    Some(op) = rx.next() => match op {
                        // Handle DHT requests
                        DhtCtl::Handle(from, req, mut resp_tx) => {
                            let peer = DhtEntry::new(from.id.clone(), from);

                            match dht.handle_req(&peer, &req) {
                                Ok(resp) => {
                                    if let Err(e) = resp_tx.send(resp).await {
                                        error!("Failed to forward dht response: {e:?}");
                                    }
                                },
                                Err(e) => {
                                    error!("DHT handle_req error: {e:?}");
                                }
                            }
                        },
                        DhtCtl::GetHandle(mut handle_tx) => {
                            let h = dht.get_handle();

                            if let Err(e) = handle_tx.send(h).await {
                                error!("Failed to forward dht handle: {e:?}");
                            }
                        }
                        DhtCtl::Exit => break,
                    },
                    // Poll on DHT for updates etc.
                    _ = (&mut dht) => (),
                    // TODO(low): DHT maintenance task could move here instead of inside DHT impl?
                }
            }

            debug!("Exit DHT task");
        });

        Self { ctl: tx }
    }

    /// Handle DHT requests
    pub async fn handle_req(
        &self,
        from: PeerInfo,
        req: DhtRequest<Id, Data>,
    ) -> Result<DhtResponse<Id, PeerInfo, Data>, Error> {
        let (tx, mut rx) = unbounded_channel();

        // Send control message
        let mut ctl = self.ctl.clone();
        ctl.send(DhtCtl::Handle(from, req, tx))
            .await
            .map_err(|_| Error::Closed)?;

        // Await response
        let resp = match rx.next().await {
            Some(r) => r,
            None => return Err(Error::Closed),
        };

        // Return response object
        Ok(resp)
    }

    /// Fetch a handle for executing DHT operations
    pub async fn get_handle(&self) -> Result<DhtHandle<Id, PeerInfo, Data>, Error> {
        let (tx, mut rx) = unbounded_channel();

        // Send control message
        let mut ctl = self.ctl.clone();
        ctl.send(DhtCtl::GetHandle(tx))
            .await
            .map_err(|_| Error::Closed)?;

        // Await response
        let resp = match rx.next().await {
            Some(r) => r,
            None => return Err(Error::Closed),
        };

        // Return handle object
        Ok(resp)
    }

    pub async fn dht_connect(&self, addr: Address, id: Option<Id>) -> Result<(), Error> {
        todo!()
    }

    pub async fn dht_locate(&self, id: Id) -> Result<Option<PeerInfo>, Error> {
        todo!()
    }

    pub async fn dht_search(&self, id: Id) -> Result<Vec<Container>, Error> {
        todo!()
    }

    pub async fn dht_put(&self, id: Id, pages: Vec<Container>) -> Result<Vec<Container>, Error> {
        todo!()
    }
}

impl<Net> Dsf<Net>
where
    Dsf<Net>: NetIf<Interface = Net>,
{
    /// Handle a DHT request message
    pub(crate) async fn handle_dht_req(
        &mut self,
        from: Id,
        peer: PeerInfo,
        req: DhtRequest<Id, Data>,
    ) -> Result<DhtResponse<Id, PeerInfo, Data>, Error> {
        // Pass to DHT
        match self.dht.handle_req(peer, req.clone()).await {
            Ok(resp) => Ok(resp),
            Err(e) => {
                error!("Error handling DHT request {:?}: {:?}", req, e);
                Err(Error::Unknown)
            }
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

/// Convert a DHT request to a DSF network message
pub(crate) fn dht_to_net_request(req: DhtRequest<Id, Data>) -> NetRequestBody {
    match req {
        DhtRequest::Ping => RequestBody::Ping,
        DhtRequest::FindNode(id) => RequestBody::FindNode(Id::from(id.clone())),
        DhtRequest::FindValue(id) => RequestBody::FindValue(Id::from(id.clone())),
        DhtRequest::Store(id, values) => RequestBody::Store(Id::from(id.clone()), values.to_vec()),
    }
}

/// Convert a DHT response to a DSF network message
pub(crate) fn dht_to_net_response(resp: DhtResponse<Id, PeerInfo, Data>) -> NetResponseBody {
    match resp {
        DhtResponse::NodesFound(id, nodes) => {
            let nodes = nodes
                .iter()
                .filter_map(|n| {
                    let i = n.info();

                    // Limit responses to contactable nodes
                    // TODO: they shouldn't be -added- to the DHT prior to being contacted but
                    // this needs to be confirmed / is a simple guard rail.
                    match (&i.state, i.seen.is_some()) {
                        (PeerState::Known(public_key), true) => {
                            Some((i.id.clone(), *i.address(), public_key.clone()))
                        }
                        _ => None,
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

pub(crate) async fn net_to_dht_response(
    resp: &NetResponseBody,
) -> Option<DhtResponse<Id, PeerInfo, Data>> {
    // TODO: fix peers:new here peers:new
    match resp {
        ResponseBody::NodesFound(id, nodes) => {
            // Convert response information to PeerInfo objects
            // (note that peers are only added to local tracking when contacted)
            let mut dht_nodes = Vec::with_capacity(nodes.len());
            for (id, addr, key) in nodes {
                let node = PeerInfo::new(
                    id.clone(),
                    PeerAddress::Implicit(*addr),
                    PeerState::Known(key.clone()),
                    0,
                    Some(SystemTime::now()),
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

/// Reducer function reduces pages stored in the database
pub(crate) fn dht_reducer(id: Id, pages: Vec<Container>) -> Vec<Container> {
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
            Ok(PageInfo::Primary(_)) if c.id() == id && h.index() >= index => {
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
        Ok(PageInfo::Secondary(s)) if c.id() == id && !c.expired() => {
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

    trace!("Public tertiaries: {:?}", public_tertiaries);

    // For private registries we just have to take the latest subset of pages
    // TODO: any better approach to this? some form of deterministic one-way fn for
    let private_tertiaries = ordered.iter().filter(|c| {
        let h = c.header();
        h.kind().is_page() && h.flags().contains(Flags::TERTIARY | Flags::ENCRYPTED) && !c.expired()
    });

    trace!("Private tertiaries: {:?}", private_tertiaries);

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
    let mut tertiaries: Vec<&Container> = tertiaries.values().copied().collect();
    tertiaries.sort_by_key(|c| c.header().index());

    let num_tertiaries = tertiaries.len().min(16);
    let tertiaries = tertiaries.drain(..num_tertiaries).cloned();

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

        let r = dht_reducer(svc.id(), vec![p1.to_owned(), p2.to_owned()]);
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

        let pages = [
            svc_page.to_owned(),
            p1a.to_owned(),
            p1b.to_owned(),
            p2a.to_owned(),
            p2b.to_owned(),
        ];

        let mut r = dht_reducer(svc.id(), pages.to_vec());

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

        let pages = [t1.to_owned(), t2.to_owned(), t2a.to_owned()];

        // Reduce should leave only the _later_ tertiary page for a given target
        let mut r = dht_reducer(tid, pages.to_vec());

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

        let pages = [t1.to_owned(), t2.to_owned()];

        // Reduce should leave only the _later_ tertiary page
        // TODO: could sort by time equally as well as index?
        let mut r = dht_reducer(tid, pages.to_vec());

        let mut e = vec![t1.to_owned(), t2.to_owned()];

        r.sort_by_key(|p| p.signature());
        e.sort_by_key(|p| p.signature());

        assert_eq!(r, e);
    }
}
