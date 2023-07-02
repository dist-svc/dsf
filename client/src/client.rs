use std::time::Duration;

use clap::Parser;
use futures::channel::mpsc;
use futures::prelude::*;
use home::home_dir;
use humantime::Duration as HumanDuration;
use tracing::{debug, span, Level};

use dsf_core::api::*;
use dsf_rpc::*;

use crate::driver::{Driver, GenericDriver};
use crate::error::Error;

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
                return format!("unix://{}", p.to_string_lossy());
            }
        }

        // Otherwise fallback to system socket
        "unix:///var/run/dsfd/dsf.sock".to_string()
    }
}

impl From<&str> for Config {
    fn from(value: &str) -> Self {
        Self {
            daemon_socket: Some(value.to_string()),
            timeout: Duration::from_secs(10).into(),
        }
    }
}

/// DSF client connector
#[derive(Debug)]
pub struct Client<D: Driver = GenericDriver> {
    addr: String,

    driver: D,

    timeout: Duration,
}

impl Client {
    /// Create a new client
    pub async fn new<C: Into<Config>>(options: C) -> Result<Self, Error> {
        let c = options.into();

        let daemon_socket = c.daemon_socket();

        let span = span!(Level::DEBUG, "client", "{}", daemon_socket);
        let _enter = span.enter();

        debug!("Client connecting (address: {})", daemon_socket);

        // Connect to driver
        let driver = GenericDriver::new(&daemon_socket).await?;

        Ok(Client {
            addr: daemon_socket,
            driver,
            timeout: *c.timeout,
        })
    }

    /// Issue a request to the daemon using a client instance, returning a response
    // TODO: #[instrument] when futures 0.3 support is viable
    pub async fn request(&mut self, rk: RequestKind) -> Result<ResponseKind, Error> {
        let span = span!(Level::DEBUG, "client", "{}", self.addr);
        let _enter = span.enter();

        debug!("Issuing request: {:?}", rk);

        let resp = self.driver.exec(rk, self.timeout).await?;

        debug!("Received response: {:?}", resp);

        Ok(resp)
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
        options: service::ServiceListOptions,
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
        _options: SubscribeOptions,
    ) -> Result<impl Stream<Item = ResponseKind>, Error> {
        let (_tx, _rx) = mpsc::channel(1000);

        // TODO: re-implement with updated driver

        Ok(_rx)
    }

    /// Fetch data from a given service
    pub async fn data(&mut self, options: data::DataListOptions) -> Result<Vec<DataInfo>, Error> {
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
            ResponseKind::NsRegister(info) => Ok(info),
            ResponseKind::Error(e) => Err(Error::Remote(e)),
            _ => Err(Error::UnrecognizedResult),
        }
    }

    /// Search for a service using the provided name service
    pub async fn ns_search(
        &mut self,
        options: name::NsSearchOptions,
    ) -> Result<name::NsSearchInfo, Error> {
        let req = RequestKind::Ns(NsCommands::Search(options));

        let resp = self.request(req).await?;

        match resp {
            ResponseKind::NsSearch(info) => Ok(info),
            ResponseKind::Error(e) => Err(Error::Remote(e)),
            _ => Err(Error::UnrecognizedResult),
        }
    }
}
