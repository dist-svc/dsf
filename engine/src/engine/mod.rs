use crate::log::{debug, error, info, trace, warn, Debug};
use crate::store::{ObjectFilter, SubscribeState};
use dsf_core::api::Application;
use dsf_core::types::{BaseKind, ImmutableData};
use dsf_core::wire::Container;

use dsf_core::base::Decode;
use dsf_core::service::Net;
use dsf_core::{net::Status, options::Options, prelude::*};

use crate::{
    comms::Comms,
    error::EngineError,
    store::{Peer, Store},
};

#[cfg(feature = "std")]
mod std_udp;

// Trying to build an abstraction over IP, LPWAN, (UNIX to daemon?)

pub struct Engine<A: Application, C: Comms, S: Store, const N: usize = 512> {
    svc: Service<A::Info>,

    pri: Signature,
    req_id: RequestId,

    comms: C,
    store: S,
}
pub trait Allocator {}

pub struct Alloc;

impl Allocator for Alloc {}

#[derive(Debug, PartialEq)]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub enum EngineEvent {
    None,
    Discover(Id),
    SubscribeFrom(Id),
    UnsubscribeFrom(Id),
    SubscribedTo(Id),
    UnsubscribedTo(Id),
    ServiceUpdate(Id, Signature),
    ReceivedData(Id, Signature),
}

#[derive(Debug, PartialEq)]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
enum EngineResponse<T: ImmutableData> {
    None,
    Net(NetResponseBody),
    Page(Container<T>),
}

impl<T: ImmutableData> From<NetResponseBody> for EngineResponse<T> {
    fn from(r: NetResponseBody) -> Self {
        Self::Net(r)
    }
}

impl<T: ImmutableData> From<Container<T>> for EngineResponse<T> {
    fn from(p: Container<T>) -> Self {
        Self::Page(p)
    }
}

pub trait Filter<V> {
    fn matches(&self, v: V) -> bool;
}

impl<V: PartialEq> Filter<V> for V {
    fn matches(&self, v: V) -> bool {
        self == &v
    }
}

impl<V: PartialEq> Filter<V> for &[V] {
    fn matches(&self, v: V) -> bool {
        self.contains(&v)
    }
}

impl<'a, Addr, A, C, S, const N: usize> Engine<A, C, S, N>
where
    Addr: PartialEq + Clone + Debug,
    A: Application,
    C: Comms<Address = Addr>,
    S: Store<Address = Addr>,
{
    pub fn new(
        info: A::Info,
        comms: C,
        mut store: S,
    ) -> Result<Self, EngineError<<C as Comms>::Error, <S as Store>::Error>> {
        let mut sb = ServiceBuilder::<A::Info>::default();

        // Start assembling the service
        sb = sb.application_id(A::APPLICATION_ID);

        // Attempt to load existing keys
        if let Some(k) = store.get_ident().map_err(EngineError::Store)? {
            debug!("Using existing service (keys: {:?})", k);
            sb = sb.keys(k);
        } else {
            debug!("Creating new service");
        }

        // Attempt to load last sig for continuation
        // TODO: should this fetch the index too?
        if let Some(s) = store.get_last().map_err(EngineError::Store)? {
            debug!("Using last info: {:?}", s);
            sb = sb.last_signature(s.sig);
            sb = sb.version(s.page_index);
            sb = sb.index(s.block_index.max(s.page_index) + 1)
        }

        // TODO: fetch existing page if available?

        // Create service
        let mut svc = sb.body(info).build().map_err(EngineError::Core)?;

        // Store created service keys
        store.set_ident(&svc.keys()).map_err(EngineError::Store)?;

        // TODO: do not regenerate page if not required

        // Generate initial page
        let mut page_buff = [0u8; N];
        let (_n, p) = svc
            .publish_primary(Default::default(), &mut page_buff)
            .map_err(EngineError::Core)?;

        let sig = p.signature();

        trace!("Generated new page: {:?} sig: {}", p, sig);

        // Store page, updating last published information
        // TODO: we _really_ do need to keep the primary page for continued use...
        store.store_page(&p).map_err(EngineError::Store)?;

        // TODO: setup forward to subscribers?

        // Return object
        Ok(Self {
            svc,
            pri: sig,
            req_id: 0,
            comms,
            store,
        })
    }

    pub fn id(&self) -> Id {
        self.svc.id()
    }

    pub fn comms(&mut self) -> &mut C {
        &mut self.comms
    }

    pub fn store(&mut self) -> &mut S {
        &mut self.store
    }

    fn next_req_id(&mut self) -> RequestId {
        self.req_id = self.req_id.wrapping_add(1);
        self.req_id
    }

    /// Discover local services
    pub fn discover(
        &mut self,
        body: &[u8],
        opts: &[Options],
    ) -> Result<RequestId, EngineError<<C as Comms>::Error, <S as Store>::Error>> {
        debug!("Generating local discovery request");

        // Generate discovery request
        let req_id = self.next_req_id();
        let req_body = NetRequestBody::Discover(self.svc.app_id(), body.to_vec(), opts.to_vec());
        let mut req = NetRequest::new(
            self.id(),
            req_id,
            req_body,
            Flags::PUB_KEY_REQUEST | Flags::NO_PERSIST,
        );
        req.common.public_key = Some(self.svc.public_key());

        debug!("Broadcasting discovery request: {:?}", req);

        // Sending discovery request
        let c = self
            .svc
            .encode_request_buff::<N>(&req, &Default::default())
            .map_err(EngineError::Core)?;

        trace!("Container: {:?}", c);

        self.comms.broadcast(c.raw()).map_err(EngineError::Comms)?;

        Ok(req_id)
    }

    pub fn register(
        &mut self,
        addr: &Addr,
    ) -> Result<Signature, EngineError<<C as Comms>::Error, <S as Store>::Error>> {
        let buff = [0u8; N];

        // Fetch or generate primary page
        let primary_page = match self
            .store
            .fetch_page(ObjectFilter::Sig(self.pri.clone()), buff)
            .map_err(EngineError::Store)?
        {
            Some(p) => p,
            None => self.generate_primary()?,
        };

        // Transmit new page
        self.comms
            .send(addr, primary_page.raw())
            .map_err(EngineError::Comms)?;

        // Return signature
        Ok(primary_page.signature())
    }

    fn generate_primary(
        &mut self,
    ) -> Result<Container<[u8; N]>, EngineError<<C as Comms>::Error, <S as Store>::Error>> {
        // Generate page
        let page_buff = [0u8; N];
        let (_n, p) = self
            .svc
            .publish_primary(Default::default(), page_buff)
            .map_err(EngineError::Core)?;

        let sig = p.signature();

        trace!("Generated new page: {:?} sig: {}", p, sig);

        // Store page, updating last published information
        self.store.store_page(&p).map_err(EngineError::Store)?;

        self.pri = sig;

        Ok(p)
    }

    /// Publish service data
    pub fn publish(
        &mut self,
        body: A::Data,
        opts: &[Options],
    ) -> Result<Signature, EngineError<<C as Comms>::Error, <S as Store>::Error>> {
        // TODO: Fetch last signature / associated primary page

        // Setup page options for encoding
        let page_opts = DataOptions::<A::Data> {
            body: Some(body),
            public_options: opts,
            ..Default::default()
        };

        // Publish data to buffer
        let (_n, p) = self
            .svc
            .publish_data_buff(page_opts)
            .map_err(EngineError::Core)?;

        let data = p.raw();
        let sig = p.signature();

        #[cfg(not(feature = "defmt"))]
        debug!("Publishing object: {:02x?}", p);

        // Store object, updating last published information
        self.store.store_page(&p).map_err(EngineError::Store)?;

        // Send updated page to subscribers
        for (id, peer) in self.store.peers() {
            match (&peer.subscriber, &peer.addr) {
                (true, Some(addr)) => {
                    debug!("Forwarding data to: {} ({:?})", id, addr);
                    self.comms.send(addr, data).map_err(EngineError::Comms)?;
                }
                _ => (),
            }
        }

        Ok(sig)
    }

    /// Subscribe to the specified service, optionally using the provided address
    pub fn subscribe(
        &mut self,
        id: Id,
        addr: Addr,
    ) -> Result<(), EngineError<<C as Comms>::Error, <S as Store>::Error>> {
        // TODO: for delegation peers != services, do we need to store separate objects for this?

        debug!("Attempting to subscribe to: {} at: {:?}", id, addr);

        // Generate request ID and update peer
        let req_id = self.next_req_id();

        // Update subscription
        self.store
            .update_peer(&id, |p| {
                // TODO: include parent for delegation support
                p.subscribed = SubscribeState::Subscribing(req_id);
            })
            .map_err(EngineError::Store)?;

        // Send subscribe request
        // TODO: how to separate target -service- from target -peer-
        let req = NetRequestBody::Subscribe(id);
        self.request(&addr, req_id, req)?;

        debug!("Subscribe TX done (req_id: {})", req_id);

        Ok(())
    }

    /// Update internal state, handling incoming messages and updating peers and subscriptions
    pub fn update(
        &mut self,
    ) -> Result<EngineEvent, EngineError<<C as Comms>::Error, <S as Store>::Error>> {
        let mut buff = [0u8; N];

        // Check for and handle received messages
        if let Some((n, a)) = Comms::recv(&mut self.comms, &mut buff).map_err(EngineError::Comms)? {
            debug!("Received {} bytes from {:?}", n, a);
            return self.handle(a, &mut buff[..n]);
        }

        // TODO: regenerate primary page if required

        // TODO: walk subscribers and expire if required

        // TODO: walk subscriptions and re-subscribe as required

        Ok(EngineEvent::None)
    }

    /// [internal] Send a request
    fn request(
        &mut self,
        addr: &Addr,
        req_id: RequestId,
        data: NetRequestBody,
    ) -> Result<(), EngineError<<C as Comms>::Error, <S as Store>::Error>> {
        let mut flags = Flags::empty();

        // TODO: set pub_key request flag for unknown peers
        let known_peer = self
            .store
            .peers()
            .find(|(_k, v)| v.addr.as_ref() == Some(addr))
            .is_some();

        if !known_peer {
            debug!("Unrecognised peer address, exchanging keys");
            flags |= Flags::PUB_KEY_REQUEST;
        }

        let mut req = NetRequest::new(self.svc.id(), req_id, data, flags);

        // Attach public key for unrecognised peers
        if !known_peer {
            req.set_public_key(self.svc.public_key())
        }

        // TODO: include peer keys here if available
        let c = self
            .svc
            .encode_request_buff::<N>(&req, &Default::default())
            .map_err(EngineError::Core)?;

        self.comms
            .send(&addr, c.raw())
            .map_err(EngineError::Comms)?;

        Ok(())
    }

    /// Handle received data
    pub fn handle<T: MutableData>(
        &mut self,
        from: Addr,
        data: T,
    ) -> Result<EngineEvent, EngineError<<C as Comms>::Error, <S as Store>::Error>> {
        debug!("Received {} bytes from {:?}", data.as_ref().len(), from);

        // Parse base object, using the store for validation and decryption
        let base = match Container::parse(data, &self.store) {
            Ok(v) => v,
            Err(e) => {
                error!("DSF parsing error: {:?}", e);
                return Err(EngineError::Core(e));
            }
        };

        #[cfg(not(feature = "defmt"))]
        trace!("Received object: {:02x?}", base);

        // Ignore our own packets
        if base.id() == self.svc.id() {
            debug!("Dropping own packet");
            return Ok(EngineEvent::None);
        }

        let req_id = base.header().index();
        let pub_key_requested = base.header().kind().is_request()
            && base.header().flags().contains(Flags::PUB_KEY_REQUEST);

        // Convert and handle messages
        let (resp, evt) = match base.header().kind().base_kind {
            BaseKind::Request | BaseKind::Response => {
                match NetMessage::parse(base.raw().to_vec(), &self.store)
                    .map_err(EngineError::Core)?
                {
                    (NetMessage::Request(req), _) => self.handle_req(&from, req)?,
                    (NetMessage::Response(resp), _) => self.handle_resp(&from, resp)?,
                }
            }
            BaseKind::Page => self.handle_page(&from, base)?,
            BaseKind::Block => self.handle_page(&from, base)?,
        };

        // Send responses
        match resp {
            EngineResponse::Net(net) => {
                debug!("Sending response {:?} (id: {}) to: {:?}", net, req_id, from);
                let mut r = NetResponse::new(self.svc.id(), req_id as u16, net, Default::default());

                // Include public key in responses if requested
                if pub_key_requested {
                    r.set_public_key(self.svc.public_key());
                }

                // TODO: pass peer keys here
                let c = self
                    .svc
                    .encode_response_buff::<N>(&r, &Default::default())
                    .map_err(EngineError::Core)?;

                self.comms
                    .send(&from, c.raw())
                    .map_err(EngineError::Comms)?;
            }
            EngineResponse::Page(p) => {
                debug!("Sending page {:?} to: {:?}", p, from);
                // TODO: ensure page is valid prior to sending?
                self.comms
                    .send(&from, p.raw())
                    .map_err(EngineError::Comms)?;
            }
            EngineResponse::None => (),
        }

        Ok(evt)
    }

    fn handle_req(
        &mut self,
        from: &Addr,
        req: NetRequest,
    ) -> Result<
        (EngineResponse<[u8; N]>, EngineEvent),
        EngineError<<C as Comms>::Error, <S as Store>::Error>,
    > {
        use NetRequestBody::*;

        debug!(
            "Received request: {:?} from: {} ({:?})",
            req, req.common.from, from
        );

        // Update peer information if available...
        // TODO: set short timeout if req.flags.contains(Flags::NO_PERSIST)
        match req.common.public_key {
            Some(pub_key) => {
                debug!("Update peer: {:?}", from);
                self.store
                    .update_peer(&req.common.from, |p| {
                        p.keys.pub_key = Some(pub_key.clone());
                        p.addr = Some(from.clone());
                    })
                    .map_err(EngineError::Store)?;
            }
            _ => (),
        }

        let mut evt = EngineEvent::None;

        // Handle request messages
        let resp: EngineResponse<[u8; N]> = match &req.data {
            Hello | Ping => NetResponseBody::Status(Status::Ok).into(),
            Discover(_, body, options) => {
                debug!("Received discovery from {} ({:?})", req.common.from, from);

                // TODO: only for matching application_ids

                // Check for matching service information
                let mut matches = match self.svc.body() {
                    // Skip for private services
                    _ if self.svc.encrypted() => false,
                    // Otherwie check for matching info
                    MaybeEncrypted::Cleartext(i) => A::matches(i, body),
                    // Respond to empty requests
                    _ => true,
                };

                // Iterate through matching options
                for o in options {
                    if self.svc.public_options().contains(o) {
                        debug!("Filter match on option: {:?}", o);
                        matches = true;
                        break;
                    }
                }

                if !matches {
                    debug!("No match for discovery message");
                    EngineResponse::None
                } else {
                    // TODO: check if page has expired and reissue if required
                    // Respond with page if filters pass
                    let buff = [0u8; N];
                    match self
                        .store
                        .fetch_page(ObjectFilter::Sig(self.pri.clone()), buff)
                    {
                        #[cfg(not(feature = "full"))]
                        Ok(Some(p)) => EngineResponse::Page(p),
                        #[cfg(feature = "full")]
                        Ok(Some(p)) => EngineResponse::Net(NetResponseBody::ValuesFound(
                            self.id(),
                            vec![p.to_owned()],
                        )),
                        _ => EngineResponse::None,
                    }
                }
            }
            Query(id, idx) if id == &self.svc.id() => {
                debug!(
                    "Sending service information to {} ({:?})",
                    req.common.from, from
                );

                let filter = match idx {
                    Some(n) => ObjectFilter::Index(*n),
                    _ => ObjectFilter::Latest,
                };

                let buff = [0u8; N];
                if let Some(p) = self
                    .store
                    .fetch_page(filter, buff)
                    .map_err(EngineError::Store)?
                {
                    #[cfg(not(feature = "full"))]
                    {
                        EngineResponse::Page(p)
                    }

                    #[cfg(feature = "full")]
                    {
                        NetResponseBody::PullData(self.id(), vec![p.to_owned()]).into()
                    }
                } else {
                    NetResponseBody::Status(Status::InvalidRequest).into()
                }
            }
            Subscribe(id) if id == &self.svc.id() => {
                debug!("Adding {} ({:?}) as a subscriber", req.common.from, from);

                self.store
                    .update_peer(&req.common.from, |p| {
                        p.subscriber = true;
                        p.addr = Some(from.clone());
                    })
                    .map_err(EngineError::Store)?;

                evt = EngineEvent::SubscribeFrom(req.common.from.clone());

                NetResponseBody::Status(Status::Ok).into()
            }
            Unsubscribe(id) if id == &self.svc.id() => {
                debug!("Removing {} ({:?}) as a subscriber", req.common.from, from);

                self.store
                    .update_peer(&req.common.from, |p| {
                        p.subscriber = false;
                    })
                    .map_err(EngineError::Store)?;

                evt = EngineEvent::UnsubscribeFrom(req.common.from.clone());

                NetResponseBody::Status(Status::Ok).into()
            }
            Subscribe(_id) | Unsubscribe(_id) => {
                NetResponseBody::Status(Status::InvalidRequest).into()
            }
            //PushData(id, pages) => ()
            _ => NetResponseBody::Status(Status::InvalidRequest).into(),
        };

        Ok((resp, evt))
    }

    fn handle_resp(
        &mut self,
        from: &Addr,
        resp: NetResponse,
    ) -> Result<
        (EngineResponse<[u8; N]>, EngineEvent),
        EngineError<<C as Comms>::Error, <S as Store>::Error>,
    > {
        //use NetResponseBody::*;

        debug!("Received response: {:?} from: {:?}", resp, from);

        let req_id = resp.common.id;
        let mut evt = EngineEvent::None;

        // Find matching peer for response
        let peer = match self
            .store
            .get_peer(&resp.common.from)
            .map_err(EngineError::Store)?
        {
            Some(p) => p,
            None => Peer {
                addr: Some(from.clone()),
                ..Default::default()
            },
        };

        // Update peer information if available...
        // TODO: set short timeout if req.flags.contains(Flags::NO_PERSIST)
        match resp.common.public_key {
            Some(pub_key) => {
                debug!("Update peer: {:?}", from);
                self.store
                    .update_peer(&resp.common.from, |p| {
                        p.keys.pub_key = Some(pub_key.clone());
                        p.addr = Some(from.clone());
                    })
                    .map_err(EngineError::Store)?;
            }
            _ => (),
        }

        // Handle response messages
        match (&peer.subscribed, &resp.data) {
            // Subscribe responses
            (SubscribeState::Subscribing(id), NetResponseBody::Status(st)) if req_id == *id => {
                if *st == Status::Ok {
                    #[cfg(not(feature = "defmt"))]
                    info!("Subscribe ok for {} ({:?})", resp.common.from, from);
                    #[cfg(feature = "defmt")]
                    info!(
                        "Subscribe ok for {} ({:?})",
                        resp.common.from,
                        defmt::Debug2Format(&from)
                    );

                    let p = self
                        .store
                        .update_peer(&resp.common.from, |p| {
                            p.subscribed = SubscribeState::Subscribed;
                        })
                        .map_err(EngineError::Store)?;

                    evt = EngineEvent::SubscribedTo(resp.common.from.clone());
                    p
                } else {
                    #[cfg(not(feature = "defmt"))]
                    info!("Subscribe failed for {} ({:?})", resp.common.from, from);
                    #[cfg(feature = "defmt")]
                    info!(
                        "Subscribe failed for {} ({:?})",
                        resp.common.from,
                        defmt::Debug2Format(&from)
                    );
                }
            }
            // Unsubscribe response
            (SubscribeState::Unsubscribing(id), NetResponseBody::Status(st)) if req_id == *id => {
                if *st == Status::Ok {
                    #[cfg(not(feature = "defmt"))]
                    info!("Unsubscribe ok for {} ({:?})", resp.common.from, from);
                    #[cfg(feature = "defmt")]
                    info!(
                        "Unsubscribe ok for {} ({:?})",
                        resp.common.from,
                        defmt::Debug2Format(&from)
                    );

                    let p = self
                        .store
                        .update_peer(&resp.common.from, |p| {
                            p.subscribed = SubscribeState::None;
                        })
                        .map_err(EngineError::Store)?;

                    evt = EngineEvent::UnsubscribedTo(resp.common.from.clone());
                    p
                } else {
                    #[cfg(not(feature = "defmt"))]
                    info!("Unsubscribe failed for {} ({:?})", resp.common.from, from);
                    #[cfg(feature = "defmt")]
                    info!(
                        "Unsubscribe failed for {} ({:?})",
                        resp.common.from,
                        defmt::Debug2Format(&from)
                    );
                }
            }
            // TODO: what other responses are important?
            //NoResult => (),
            //PullData(_, _) => (),
            (_, NetResponseBody::Status(status)) => {
                debug!("Received status: {:?} for peer: {:?}", status, peer);
            }
            (_, NetResponseBody::ValuesFound(_id, pages)) => {
                debug!("Received pages: {:?}", pages);
            }
            _ => todo!(),
        };

        Ok((EngineResponse::None, evt))
    }

    fn handle_page<T: ImmutableData>(
        &mut self,
        from: &Addr,
        page: Container<T>,
    ) -> Result<
        (EngineResponse<[u8; N]>, EngineEvent),
        EngineError<<C as Comms>::Error, <S as Store>::Error>,
    > {
        debug!("Received page: {:?} from: {:?}", page, from);

        // Find matching peer for rx'd page
        let peer = self
            .store
            .get_peer(&page.id())
            .map_err(EngineError::Store)?;
        let info = page.info();

        // Handle page types
        let (status, evt) = match (peer, info) {
            // New primary page
            // TODO: only if discovering..?
            (None, Ok(PageInfo::Primary(pri))) => {
                debug!("Discovered new service: {:?}", page.id());

                // Write peer info to store
                self.store
                    .update_peer(&page.id(), |peer| {
                        peer.keys.pub_key = Some(pri.pub_key.clone());
                    })
                    .map_err(EngineError::Store)?;

                // Attempt to decode page body
                let _body = match A::Info::decode(page.body_raw()) {
                    Ok(i) => debug!("Decode: {:?}", i),
                    Err(e) => error!("Failed to decode info: {:?}", e),
                };

                (Status::Ok, EngineEvent::Discover(page.id()))
            }
            // Updated primary page
            (Some(_peer), Ok(PageInfo::Primary(_pri))) => {
                debug!("Update service: {:?}", page.id());

                // TODO: update peer / service information

                // TODO: update peer if page is newer?
                (
                    Status::Ok,
                    EngineEvent::ServiceUpdate(page.id(), page.signature()),
                )
            }
            // Data without subscription
            (Some(peer), Ok(PageInfo::Data(_data))) if !peer.subscribed() => {
                warn!("Not subscribed to peer: {}", page.id());

                (Status::InvalidRequest, EngineEvent::None)
            }
            // Data with subscription
            (Some(_peer), Ok(PageInfo::Data(_data))) => {
                debug!("Received data for service: {:?}", page.id());

                // TODO: store or propagate data here?
                (
                    Status::Ok,
                    EngineEvent::ReceivedData(page.id(), page.signature()),
                )
            }
            // Unhandled page
            _ => {
                warn!("Received unexpected page {:?}", page);
                (Status::InvalidRequest, EngineEvent::None)
            }
        };

        // Respond with OK
        Ok((NetResponseBody::Status(status).into(), evt))
    }
}

#[cfg(test)]
mod test {

    //use dsf_core::prelude::*;
    use dsf_core::net::Status;

    use crate::{comms::mock::MockComms, store::MemoryStore};

    use super::*;

    /// Generic application for engine testing
    pub struct Generic {}

    impl Application for Generic {
        const APPLICATION_ID: u16 = 0x0102;

        type Info = Vec<u8>;

        type Data = Vec<u8>;

        fn matches(info: &Self::Info, req: &[u8]) -> bool {
            req.len() == 0 || info == req
        }
    }

    // Setup an engine instance for testing
    fn setup<'a>() -> (Service, Engine<Generic, MockComms, MemoryStore<u8>>) {
        // Setup debug logging
        let _ = simplelog::SimpleLogger::init(
            simplelog::LevelFilter::Debug,
            simplelog::Config::default(),
        );

        // Create peer for sending requests
        let p = ServiceBuilder::generic().build().unwrap();

        // Setup memory store with pre-filled peer keys
        let mut s = MemoryStore::<u8>::new();
        s.update(&p.id(), |k| *k = p.keys());

        // Setup service body
        let body = vec![0xaa, 0xbb, 0xcc, 0xdd];

        // Setup engine with default service
        let e = Engine::new(body, MockComms::default(), s).expect("Failed to create engine");

        (p, e)
    }

    #[test]
    fn test_handle_reqs() {
        // Create peer for sending requests
        let (p, mut e) = setup();
        let from = 1;

        let tests = [
            (NetRequestBody::Hello, NetResponseBody::Status(Status::Ok)),
            (NetRequestBody::Ping, NetResponseBody::Status(Status::Ok)),
            //(NetRequestBody::Query(e.svc.id()),         NetResponseBody::Status(Status::Ok)),
            //(NetRequestBody::Subscribe(e.svc.id()),     NetResponseBody::Status(Status::Ok)),
            //(NetRequestBody::Unsubscribe(e.svc.id()),   NetResponseBody::Status(Status::Ok)),
        ];

        for t in &tests {
            // Generate full request object
            let req = NetRequest::new(p.id(), 1, t.0.clone(), Default::default());

            // Pass to engine
            let (resp, _evt) = e
                .handle_req(&from, req.clone())
                .expect("Failed to handle message");

            // Check response
            assert_eq!(
                resp,
                t.1.clone().into(),
                "Unexpected response for request: {:#?}",
                req
            );
        }
    }

    #[test]
    fn test_handle_subscribe() {
        let (p, mut e) = setup();
        let from = 1;

        // Build subscribe request and execute
        let req = NetRequest::new(
            p.id(),
            1,
            NetRequestBody::Subscribe(e.svc.id()),
            Default::default(),
        );
        let (resp, _evt) = e.handle_req(&from, req).expect("Failed to handle message");

        // Check response
        assert_eq!(resp, NetResponseBody::Status(Status::Ok).into());

        // Check subscriber state
        assert_eq!(e.store.peers.get(&p.id()).map(|p| p.subscriber), Some(true));

        // TODO: expiry?
    }

    #[test]
    fn test_handle_unsubscribe() {
        let (p, mut e) = setup();
        let from = 1;

        // Pre-fill peer subscribed state
        e.store
            .update_peer(&p.id(), |p| p.subscriber = true)
            .unwrap();

        // Build subscribe request and execute
        let req = NetRequest::new(
            p.id(),
            1,
            NetRequestBody::Unsubscribe(e.svc.id()),
            Default::default(),
        );
        let (resp, _evt) = e.handle_req(&from, req).expect("Failed to handle message");

        // Check response
        assert_eq!(resp, NetResponseBody::Status(Status::Ok).into());

        // Check subscriber state
        assert_eq!(
            e.store.peers.get(&p.id()).map(|p| p.subscriber),
            Some(false)
        );
    }

    #[test]
    fn test_handle_discover() {
        let (p, mut e) = setup();
        let from = 1;

        // Setup filters
        let filter_body = vec![0xaa, 0xbb, 0xcc, 0xdd];

        // Execute net request
        let req = NetRequest::new(
            p.id(),
            1,
            NetRequestBody::Discover(0, filter_body, vec![]),
            Default::default(),
        );
        let (resp, _evt) = e.handle_req(&from, req).expect("Failed to handle message");

        // Check response
        let buff = [0u8; 512];
        let page = e
            .store
            .fetch_page(ObjectFilter::Sig(e.pri.clone()), buff)
            .unwrap()
            .unwrap();

        #[cfg(not(feature = "full"))]
        assert_eq!(resp, page.into());

        #[cfg(feature = "full")]
        {
            assert_eq!(
                resp,
                EngineResponse::Net(NetResponseBody::ValuesFound(e.id(), vec![page.to_owned()]))
            );
        }
    }

    #[test]
    fn test_publish() {
        let (p, mut e) = setup();
        let from = 1;

        // Setup peer as subscriber
        e.store
            .update_peer(&p.id(), |p| {
                p.subscriber = true;
                p.addr = Some(from);
            })
            .unwrap();

        // Build object for publishing
        let data_body = vec![0x11, 0x22, 0x33, 0x44];

        // Call publish operation
        e.publish(data_body.clone(), &[]).expect("Publishing error");

        // Check outgoing data
        let d = e.comms.tx.pop().unwrap();
        assert_eq!(d.0, from);

        // Parse out page
        let b = Container::parse(d.1, &e.svc.keys()).expect("Failed to parse object");

        // Check data push contains published data
        assert_eq!(b.body_raw(), &data_body);
    }

    #[test]
    fn test_subscribe() {
        let (mut p, mut e) = setup();
        let from = 1;
        let mut buff = [0u8; 256];

        // Setup peer to be subscribed to
        e.store
            .update_peer(&p.id(), |p| {
                p.addr = Some(from);
            })
            .unwrap();

        // Call subscribe operation
        e.subscribe(p.id(), from).expect("Subscribing error");

        // Check peer state updated to subscribing
        assert_eq!(
            e.store.peers.get(&p.id()).map(|p| p.subscribed),
            Some(SubscribeState::Subscribing(e.req_id))
        );

        // Check outgoing subscribe request
        let d = e.comms.tx.pop().expect("No outgoing data found");
        assert_eq!(d.0, from, "outgoing address mismatch");

        // Parse out page and convert back to message
        let (m, _) = NetMessage::parse(d.1, &e.svc.keys()).expect("Failed to parse object");

        let expected = NetRequest::new(
            e.svc.id(),
            e.req_id,
            NetRequestBody::Subscribe(p.id()),
            Default::default(),
        );

        assert_eq!(m, NetMessage::Request(expected), "Request mismatch");

        // Respond with subscribe ok
        let resp = NetResponse::new(
            p.id(),
            e.req_id,
            NetResponseBody::Status(Status::Ok),
            Default::default(),
        );
        e.handle_resp(&from, resp)
            .expect("Response handling failed");

        // Check peer state is now subscribed
        assert_eq!(
            e.store.peers.get(&p.id()).map(|p| p.subscribed),
            Some(SubscribeState::Subscribed)
        );

        // Test receiving page updates
        let (_n, sp) = p.publish_primary(Default::default(), &mut buff).unwrap();
        let (_, evt) = e
            .handle_page(&from, sp.to_owned())
            .expect("Failed to handle page");
        assert_eq!(evt, EngineEvent::ServiceUpdate(p.id(), sp.signature()));

        // Test receiving data
        let mut buff = [0u8; 256];
        let (_n, db) = p
            .publish_data(
                DataOptions {
                    body: Some(vec![0x11, 0x22]),
                    ..Default::default()
                },
                &mut buff,
            )
            .unwrap();
        let (_, evt) = e
            .handle_page(&from, db.to_owned())
            .expect("Failed to handle data");
        assert_eq!(evt, EngineEvent::ReceivedData(p.id(), db.signature()));
    }
}
