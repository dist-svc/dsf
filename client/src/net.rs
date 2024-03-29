use std::pin::Pin;
use std::task::{Context, Poll};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::Duration;
use std::str::FromStr;

use futures::{Stream};


use async_std::net::UdpSocket;
use async_std::channel::{unbounded, Sender, Receiver};
use tokio::task::{JoinHandle};

use log::{trace, debug, warn, error};

use dsf_core::prelude::*;
use dsf_core::net;

use crate::error::Error;

#[derive(Clone, PartialEq, Debug)]
pub struct NetClient {
    server: SocketAddr,
}

impl NetClient {
    pub fn new(uri: &str) -> Self {
        debug!("DSF peer URI: {:?}", uri);
        Self{
            server: uri.parse().unwrap(),
        }
    }
}

#[async_trait]
impl Driver for NetClient {
    type Client = DsfClient;

    /// Create a new client using the provided driver
    async fn new(&self, index: usize, name: String) -> Result<Self::Client, Error> {
        DsfClient::new(index, name, self.server.clone()).await
    }
}

/// Number of retries for lost messages
const DSF_RETRIES: usize = 1;

/// Enable symmetric crypto
const SYMMETRIC_EN: bool = true;

pub struct DsfClient {
    peer: SocketAddr,

    msg_in_rx: Receiver<Vec<u8>>,
    cmd_tx: Sender<(u16, SocketAddr, NetRequest, Sender<NetResponse>)>,

    topics: Vec<Id>,

    udp_handle: JoinHandle<Result<(), Error>>,
    exit_tx: Sender<()>,
}

impl DsfClient {
   pub async fn new(peer: SocketAddr) -> Result<Self, Error> {
        // Bind UDP socket
        let sock = UdpSocket::bind("0.0.0.0:0").await.unwrap();

        let (exit_tx, mut exit_rx) = channel(1);
        let (msg_in_tx, msg_in_rx) = channel(1000);
        let (cmd_tx, mut cmd_rx) = channel::<(u16, SocketAddr, NetRequest, Sender<NetResponse>)>(1000);

        let (id, svc_keys) = (svc.id(), svc.keys());

        // Start request task
        let udp_handle = async_std::spawn(async move {
            let mut buff = vec![0u8; 1024];
            let mut peer_id = None;
            let mut keys = HashMap::<Id, Keys>::new();
            let mut rx_handles = HashMap::<u16, Sender<NetResponse>>::new();

            loop {
                tokio::select!(
                    // Handle commands
                    Some((req_id, address, req, resp_ch)) = cmd_rx.recv() => {
                        let mut req = req;

                        // Fetch keying information
                        let enc_key = match peer_id.as_ref().map(|p| keys.get(p) ).flatten() {
                            Some(k) => {
                                if SYMMETRIC_EN {
                                    *req.flags() |= Flags::SYMMETRIC_MODE;
                                }
                                k
                            },
                            None => {
                                *req.flags() |= Flags::PUB_KEY_REQUEST;
                                req.set_public_key(svc_keys.pub_key.clone());
                                &svc_keys
                            },
                        };
                        
                        // Encode message
                        let mut buff = [0u8; 1024];
                        let n = match NetMessage::request(req).encode(enc_key, &mut buff[..]) {
                            Ok(n) => n,
                            Err(e) => {
                                error!("Error encoding message: {:?}", e);
                                return Err(Error::Unknown)
                            }
                        };
                        
                        // Add RX handle
                        rx_handles.insert(req_id, resp_ch);

                        if let Err(e) = sock.send_to(&buff[..n], &address).await {
                            error!("UDP send2 error: {:?}", e);
                            return Err(Error::Unknown);
                        }
                    },
                    // Handle incoming messages
                    Ok((n, address)) = sock.recv_from(&mut buff) => {
                        trace!("Recieve UDP from {}", address);

                        // Parse message (no key / secret stores)
                        let (base, _n) = match Base::parse(&buff[..n], &keys) {
                            Ok(v) => (v),
                            Err(e) => {
                                error!("DSF parsing error: {:?}", e);
                                continue;
                            }
                        };

                        // Store peer ID for later
                        if peer_id.is_none() {
                            peer_id = Some(base.id().clone());
                        }
                        
                        // Convert to network message
                        let req_id = base.header().index();
                        let m = match NetMessage::convert(base, &keys) {
                            Ok(m) => m,
                            Err(_e) => {
                                error!("DSF rx was not a network message");
                                continue;
                            }
                        };
                        
                        // Store symmetric keys on first receipt of public key
                        match (m.pub_key(), keys.contains_key(&m.from())) {
                            (Some(pk), false) => {
                                debug!("Enabling symmetric mode for peer: {:?}", peer_id);
                                let k = svc_keys.derive_peer(pk).unwrap(); 
                                keys.insert(m.from(), k);
                            },
                            _ => (),
                        }

                        // Locate matching request sender
                        let handle = rx_handles.remove(&req_id);
                        trace!("Rx: {:?}", m);

                        // Handle received message
                        match (m, handle) {
                            (NetMessage::Request(req), _) => {

                                // Respond with OK
                                let mut resp = net::Response::new(id.clone(), req_id, net::ResponseKind::Status(net::Status::Ok), Flags::empty());

                                // Fetch keys and enable symmetric mode if available
                                let enc_key = match keys.get(&req.from) {
                                    Some(k) => {
                                        if SYMMETRIC_EN {
                                            *resp.flags() |= Flags::SYMMETRIC_MODE;
                                        }
                                        k
                                    },
                                    None => {
                                        resp.set_public_key(svc_keys.pub_key.clone());
                                        &svc_keys
                                    },
                                };
                                
                                
                                let mut buff = [0u8; 1024];
                                let n = net::Message::response(resp).encode(&enc_key, &mut buff).unwrap();
                                let encoded = (&buff[..n]).to_vec();

                                if let Err(e) = sock.send_to(encoded.as_slice(), address.clone()).await {
                                    error!("UDP send error: {:?}", e);
                                    return Err(Error::Unknown);
                                }


                                // Handle RX'd data
                                let page = match req.data {
                                    NetRequestKind::PushData(_id, pages) if pages.len() > 0 => pages[0].clone(),
                                    _ => continue,
                                };

                                let data = match page.body() {
                                    Body::Cleartext(v) => v,
                                    _ => {
                                        warn!("Received push with no data");
                                        continue
                                    },
                                };

                                debug!("Received data push: {:?}", data);

                                if let Err(e) = msg_in_tx.send(data.clone()).await {
                                    error!("Message RX send error: {:?}", e);
                                    //break Err(Error::Unknown);
                                    continue;
                                }
                            },
                            (NetMessage::Response(resp), Some(resp_tx)) => {
                                match &resp.data {
                                    net::ResponseKind::ValuesFound(id, pages) => {
                                        for p in pages.iter().filter_map(|p| p.info().pub_key()) {
                                            keys.insert(id.clone(), Keys::new(p));
                                        }
                                    },
                                    _ => (),
                                }

                                // Forward received response to caller
                                if let Err(e) = resp_tx.send(resp).await {
                                    error!("Message TX send error: {:?}", e);
                                    return Err(Error::Unknown);
                                }
                            },
                            _ => (),
                        }
                      
                    },
                    Some(_) = exit_rx.recv() => {
                        debug!("Exiting receive task");
                        return Ok(());
                    }
                )
            }
        });


        let s = Self {
            index,
            server,
            req_id: 0,
            svc,
            cmd_tx,
            msg_in_rx,
            topics: vec![],
            udp_handle,
            exit_tx,
        };

        // Ping endpoint

        Ok(s)
   }

   pub async fn request(&mut self, kind: NetRequestKind) -> Result<NetResponseKind, Error> {
        self.req_id = rand::random();

        // Build and encode message
        let req = NetRequest::new(self.svc.id(), self.req_id, kind, Flags::empty());

        trace!("Request: {:?}", req); 

        // Generate response channel
        let (tx, mut rx) = channel(1);

        // Transmit request (with retries)
        for _i in 0..DSF_RETRIES {
            // Send request data
            let _n = self.cmd_tx.send((self.req_id, self.server, req.clone(), tx.clone())).await.unwrap();

            // Await response
            match time::timeout(Duration::from_secs(5), rx.recv()).await {
                Ok(Some(v)) => return Ok(v.data),
                _ => continue,
            }
        }

        Err(Error::Timeout)
   }
}

impl Stream  for DsfClient {
    type Item = Vec<u8>;

    fn poll_next(mut self: Pin<&mut Self>, ctx: &mut Context) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.msg_in_rx).poll_recv(ctx)
    }
}

#[async_trait]
impl Client for DsfClient {

    fn id(&self) -> usize {
        self.index
    }

    fn topic(&self) -> String {
        self.svc.id().to_string()
     }

    // Subscribe to a topic
    async fn subscribe(&mut self, topic: &str) -> Result<(), Error> {
        let id = Id::from_str(topic).unwrap();

        debug!("Issuing subscribe request for {}", self.svc.id().to_string());

        match self.request(NetRequestKind::Subscribe(id.clone())).await {
            Ok(NetResponseKind::ValuesFound(_, _)) => {
                self.topics.push(id.clone());

                debug!("Subscribed to service: {}", id);

                Ok(())
            },
            Ok(v) => {
                error!("subscribe {}, unexpected response: {:?}", id, v);
                Err(Error::Unknown)
            },
            Err(e) => {
                error!("subscribe {}, client error: {:?}", id, e);
                Err(Error::Unknown)
            }
        }
    }


    // Register a topic
    async fn register(&mut self, _topic: &str) -> Result<(), Error> {
        let mut buff = vec![0u8; 1024];
        let (n, mut p) = self.svc.publish_primary(&mut buff[..]).unwrap();
        p.raw = Some((&buff[..n]).to_vec());

        debug!("Issuing register request for {}", self.svc.id().to_string());

        // Request registration
        match self.request(NetRequestKind::Register(self.svc.id(), vec![p])).await {
            Ok(NetResponseKind::Status(net::Status::Ok)) => {
                debug!("Registered service: {}", self.svc.id());
                Ok(())
            },
            Ok(v) => {
                error!("register {}, unexpected response: {:?}", self.svc.id(), v);
                Err(Error::Unknown)
            },
            Err(e) => {
                error!("register {}, client error: {:?}", self.svc.id(), e);
                Err(Error::Unknown)
            }
        }
    }

    // Publish data to a topic
    async fn publish(&mut self, _topic: &str, data: &[u8]) -> Result<(), Error> {
        // Build data object
        let d = DataOptions{
            body: Body::Cleartext(data.to_vec()),
            ..Default::default()
        };

        let mut buff = vec![0u8; 1024];
        let (n, mut p) = self.svc.publish_data(d, &mut buff).unwrap();
        p.raw = Some((&buff[..n]).to_vec());

        debug!("Issuing publish request for {}", self.svc.id().to_string());

        // Request publishing
        match self.request(NetRequestKind::PushData(self.svc.id(), vec![p])).await {
            Ok(NetResponseKind::Status(net::Status::Ok)) => {
                debug!("publish OK");
                Ok(())
            },
            Ok(v) => {
                error!("publish, unexpected response: {:?}", v);
                Err(Error::Unknown)
            },
            Err(e) => {
                error!("publish, client error: {:?}", e);
                Err(Error::Unknown)
            }
        }
    }

    async fn close(mut self) -> Result<(), Error> {
        for t in self.topics.clone() {
            // Request de-registration
            let resp = self.request(NetRequestKind::Unsubscribe(t.clone())).await;
            match &resp {
                Ok(NetResponseKind::Status(net::Status::Ok)) => {
                    debug!("Deregistered service: {}", t);
                },
                _ => {
                    error!("Error deregistering service (resp: {:?})", resp);
                }
            }
        }

        // Signal for listener to exit
        if let Err(_e) = self.exit_tx.send(()).await {
            error!("Error sending exit signal");
        }

        // Join on handler
        if let Err(e) = self.udp_handle.await {
            error!("DSF client task error: {:?}", e);
        }

        Ok(())
    }
}
