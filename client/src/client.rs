use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use clap::Parser;
use futures::channel::mpsc;
use futures::prelude::*;
use futures::{SinkExt, StreamExt};
use home::home_dir;
use humantime::Duration as HumanDuration;
use log::{debug, error, trace, warn};
use tokio::select;
use tokio::{
    task::{self, JoinHandle},
    time::timeout,
};
use tracing::{span, Level};

use dsf_core::api::*;
use dsf_core::wire::Container;
use dsf_rpc::*;
use dsf_rpc::{Request as RpcRequest, Response as RpcResponse};

use crate::driver::{Driver, UnixDriver};
use crate::error::Error;

type RequestMap = Arc<Mutex<HashMap<u64, mpsc::Sender<ResponseKind>>>>;

/// Options for client instantiation
#[derive(Clone, Debug, Parser)]
pub struct Config {
    #[clap(short, long, env = "DSF_SOCK")]
    /// Override default daemon socket
    /// (otherwise this will attempt to connect to user then
    /// global scoped sockets)
    pub daemon_socket: Option<String>,

    /// Timeout for RPC requests
    #[clap(long, default_value = "10s")]
    pub timeout: HumanDuration,
}

impl Config {
    pub fn new(address: Option<&str>, timeout: Duration) -> Self {
        Self {
            daemon_socket: address.map(|v| v.to_string()),
            timeout: timeout.into(),
        }
    }

    pub fn daemon_socket(&self) -> String {
        // Use override if provided
        if let Some(s) = &self.daemon_socket {
            return s.clone();
        }

        // Use local / user socket if available
        if let Some(h) = home_dir() {
            let p = h.join(".dsfd/dsf.sock");
            if p.exists() {
                return p.to_string_lossy().to_string();
            }
        }

        // Otherwise fallback to system socket
        "/var/run/dsfd/dsf.sock".to_string()
    }
}

/// DSF client connector
#[derive(Debug)]
pub struct Client<D: Driver = UnixDriver> {
    addr: String,
    sink: mpsc::Sender<RpcRequest>,
    requests: RequestMap,

    timeout: Duration,

    _rx_handle: JoinHandle<()>,
    _stream_handle: JoinHandle<()>,
    _d: PhantomData<D>,
}

impl Client {
    /// Create a new client
    pub async fn new(options: &Config) -> Result<Self, Error> {
        let daemon_socket = options.daemon_socket();

        let span = span!(Level::DEBUG, "client", "{}", daemon_socket);
        let _enter = span.enter();

        debug!("Client connecting (address: {})", daemon_socket);

        // Connect to stream
        let mut driver = UnixDriver::new(&daemon_socket).await?;

        // Create internal streams
        let (tx_sink, mut tx_stream) = mpsc::channel::<RpcRequest>(0);
        let (mut rx_sink, mut rx_stream) = mpsc::channel::<RpcResponse>(0);

        // Route messages between streams and driver
        let stream_handle = task::spawn(async move {
            loop {
                select! {
                    Some(resp) = driver.next() => {
                        rx_sink.send(resp).await.unwrap();
                    },
                    Some(m) = tx_stream.next() => {
                        driver.send(m).await.unwrap();
                    }
                }
            }
        });

        // Create receiving task
        let requests = Arc::new(Mutex::new(HashMap::new()));
        let reqs = requests.clone();

        let rx_handle = task::spawn(async move {
            trace!("started client rx listener");
            loop {
                match rx_stream.next().await {
                    Some(resp) => Self::handle(&reqs, resp).await.unwrap(),
                    None => {
                        warn!("Client rx channel closed");
                        break;
                    }
                }
            }
        });

        Ok(Client {
            sink: tx_sink,
            addr: daemon_socket,
            requests,
            timeout: *options.timeout,
            _rx_handle: rx_handle,
            _stream_handle: stream_handle,
            _d: PhantomData,
        })
    }

    /// Issue a request to the daemon using a client instance, returning a response
    // TODO: #[instrument] when futures 0.3 support is viable
    pub async fn request(&mut self, rk: RequestKind) -> Result<ResponseKind, Error> {
        let span = span!(Level::DEBUG, "client", "{}", self.addr);
        let _enter = span.enter();

        debug!("Issuing request: {:?}", rk);

        let resp = self.do_request(rk).await.map(|(v, _)| v)?;

        debug!("Received response: {:?}", resp);

        Ok(resp)
    }

    // TODO: #[instrument]
    async fn do_request(
        &mut self,
        rk: RequestKind,
    ) -> Result<(ResponseKind, mpsc::Receiver<ResponseKind>), Error> {
        let (tx, mut rx) = mpsc::channel(0);
        let req = RpcRequest::new(rk);
        let id = req.req_id();

        // Add to tracking
        trace!("request add lock");
        self.requests.lock().unwrap().insert(id, tx);

        // Send message
        self.sink.send(req).await.unwrap();

        // Await and return response
        let res = timeout(self.timeout, rx.next()).await;

        // TODO: Handle timeout errors
        let res = match res {
            Ok(Some(v)) => Ok(v),
            // TODO: this seems like it should be a yeild / retry point..?
            Ok(None) => {
                error!("No response received");
                Err(Error::None(()))
            }
            Err(e) => {
                error!("Response error: {:?}", e);
                Err(Error::Timeout)
            }
        };

        // Remove request on failure
        if let Err(_e) = &res {
            trace!("request failure lock");
            self.requests.lock().unwrap().remove(&id);
        }

        res.map(|v| (v, rx))
    }

    // Internal function to handle received messages
    async fn handle(requests: &RequestMap, resp: RpcResponse) -> Result<(), Error> {
        // Find matching sender
        let id = resp.req_id();

        debug!("Response: {:?}", resp);

        trace!("receive request lock");
        let mut a = match requests.lock().unwrap().get_mut(&id) {
            Some(a) => a.clone(),
            None => {
                error!("Unix RX with no matching request ID");
                return Err(Error::Unknown);
            }
        };

        // Forward response
        match a.send(resp.kind()).await {
            Ok(_) => (),
            Err(e) => {
                error!("client send error: {:?}", e);
            }
        };

        Ok(())
    }

    /// Fetch daemon status information
    pub async fn status(&mut self) -> Result<StatusInfo, Error> {
        let req = RequestKind::Status;
        let resp = self.request(req).await?;

        match resp {
            ResponseKind::Status(info) => Ok(info),
            _ => Err(Error::UnrecognizedResult),
        }
    }

    /// Connect to another DSF instance
    pub async fn connect(
        &mut self,
        options: peer::ConnectOptions,
    ) -> Result<peer::ConnectInfo, Error> {
        let req = RequestKind::Peer(peer::PeerCommands::Connect(options));
        let resp = self.request(req).await?;

        match resp {
            ResponseKind::Connected(info) => Ok(info),
            ResponseKind::Error(e) => Err(Error::Remote(e)),
            _ => Err(Error::UnrecognizedResult),
        }
    }

    /// Search for a peer using the database
    pub async fn find(&mut self, options: peer::SearchOptions) -> Result<peer::PeerInfo, Error> {
        let req = RequestKind::Peer(peer::PeerCommands::Search(options));
        let resp = self.request(req).await?;

        match resp {
            ResponseKind::Peer(info) => Ok(info),
            ResponseKind::Error(e) => Err(Error::Remote(e)),
            _ => Err(Error::UnrecognizedResult),
        }
    }

    /// List known services
    pub async fn list(
        &mut self,
        options: service::ListOptions,
    ) -> Result<Vec<service::ServiceInfo>, Error> {
        let req = RequestKind::Service(service::ServiceCommands::List(options));
        let resp = self.request(req).await?;

        match resp {
            ResponseKind::Services(info) => Ok(info),
            ResponseKind::Error(e) => Err(Error::Remote(e)),
            _ => Err(Error::UnrecognizedResult),
        }
    }

    /// Fetch information for a given service
    pub async fn info(
        &mut self,
        options: service::InfoOptions,
    ) -> Result<(ServiceHandle, ServiceInfo), Error> {
        let req = RequestKind::Service(service::ServiceCommands::Info(options));
        let resp = self.request(req).await?;

        match resp {
            ResponseKind::Service(info) => Ok((ServiceHandle::new(info.id.clone()), info)),
            ResponseKind::Error(e) => Err(Error::Remote(e)),
            _ => Err(Error::UnrecognizedResult),
        }
    }

    /// Fetch pages from a given service
    pub async fn object(&mut self, options: data::FetchOptions) -> Result<DataInfo, Error> {
        let req = RequestKind::Data(DataCommands::Get(options));

        let resp = self.request(req).await?;

        match resp {
            ResponseKind::Data(data) if data.len() == 1 => Ok(data[0].clone()),
            ResponseKind::Data(_) => Err(Error::NoPageFound),
            ResponseKind::Error(e) => Err(Error::Remote(e)),
            _ => Err(Error::UnrecognizedResult),
        }
    }

    /// Create a new service with the provided options
    /// This MUST be stored locally for reuse
    pub async fn create(
        &mut self,
        options: service::CreateOptions,
    ) -> Result<ServiceHandle, Error> {
        let req = RequestKind::Service(service::ServiceCommands::Create(options));
        let resp = self.request(req).await?;

        match resp {
            ResponseKind::Service(info) => Ok(ServiceHandle::new(info.id)),
            ResponseKind::Error(e) => Err(Error::Remote(e)),
            _ => Err(Error::UnrecognizedResult),
        }
    }

    /// Register a service instance in the distributed database
    pub async fn register(
        &mut self,
        options: RegisterOptions,
    ) -> Result<dsf_rpc::service::RegisterInfo, Error> {
        let req = RequestKind::Service(dsf_rpc::service::ServiceCommands::Register(options));
        let resp = self.request(req).await?;

        match resp {
            ResponseKind::Registered(info) => Ok(info),
            ResponseKind::Error(e) => Err(Error::Remote(e)),
            _ => Err(Error::UnrecognizedResult),
        }
    }

    /// Locate a service instance in the distributed database
    /// This returns a future that will resolve to the desired service or an error
    pub async fn locate(
        &mut self,
        options: LocateOptions,
    ) -> Result<(ServiceHandle, LocateInfo), Error> {
        let id = options.id.clone();
        let req = RequestKind::Service(dsf_rpc::service::ServiceCommands::Locate(options));

        let resp = self.request(req).await?;

        match resp {
            ResponseKind::Located(info) if info.len() == 1 => {
                let handle = ServiceHandle { id: id.clone() };
                Ok((handle, info[0].clone()))
            }
            ResponseKind::Error(e) => Err(Error::Remote(e)),
            _ => Err(Error::UnrecognizedResult),
        }
    }

    pub async fn discover(&mut self, options: DiscoverOptions) -> Result<Vec<ServiceInfo>, Error> {
        let req = RequestKind::Service(dsf_rpc::service::ServiceCommands::Discover(options));

        let resp = self.request(req).await?;

        match resp {
            ResponseKind::Services(info) => Ok(info),
            ResponseKind::Error(e) => Err(Error::Remote(e)),
            _ => Err(Error::UnrecognizedResult),
        }
    }

    /// Publish data using an existing service
    pub async fn publish(&mut self, options: PublishOptions) -> Result<PublishInfo, Error> {
        let req = RequestKind::Data(DataCommands::Publish(options));

        let resp = self.request(req).await?;

        match resp {
            ResponseKind::Published(info) => Ok(info),
            ResponseKind::Error(e) => Err(Error::Remote(e)),
            _ => Err(Error::UnrecognizedResult),
        }
    }

    /// Subscribe to data from a given service
    pub async fn subscribe(
        &mut self,
        options: SubscribeOptions,
    ) -> Result<impl Stream<Item = ResponseKind>, Error> {
        let req = RequestKind::Service(ServiceCommands::Subscribe(options));

        let (resp, rx) = self.do_request(req).await?;

        match resp {
            ResponseKind::Subscribed(_info) => Ok(rx),
            ResponseKind::Error(e) => Err(Error::Remote(e)),
            _ => Err(Error::UnrecognizedResult),
        }
    }

    /// Fetch data from a given service
    pub async fn data(&mut self, options: data::ListOptions) -> Result<Vec<DataInfo>, Error> {
        let req = RequestKind::Data(DataCommands::List(options));

        let resp = self.request(req).await?;

        match resp {
            ResponseKind::Data(info) => Ok(info),
            ResponseKind::Error(e) => Err(Error::Remote(e)),
            _ => Err(Error::UnrecognizedResult),
        }
    }

    /// Register a service using the provided name service
    pub async fn ns_register(
        &mut self,
        options: name::NsRegisterOptions,
    ) -> Result<name::NsRegisterInfo, Error> {
        let req = RequestKind::Ns(NsCommands::Register(options));

        let resp = self.request(req).await?;

        match resp {
            ResponseKind::Ns(info) => Ok(info),
            ResponseKind::Error(e) => Err(Error::Remote(e)),
            _ => Err(Error::UnrecognizedResult),
        }
    }

    /// Search for a service using the provided name service
    pub async fn ns_search(
        &mut self,
        options: name::NsSearchOptions,
    ) -> Result<Vec<LocateInfo>, Error> {
        let req = RequestKind::Ns(NsCommands::Search(options));

        let resp = self.request(req).await?;

        match resp {
            ResponseKind::Located(info) => Ok(info),
            ResponseKind::Error(e) => Err(Error::Remote(e)),
            _ => Err(Error::UnrecognizedResult),
        }
    }
}
