use std::convert::TryFrom;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::PathBuf;
use std::time::Duration;

use bytes::Bytes;
use clap::Parser;
use futures::channel::mpsc;
use futures::{prelude::*, select, StreamExt};
use home::home_dir;
use log::{debug, error, info, trace, warn};
use tokio::task::{self, JoinHandle};
use tokio::{sync::mpsc::channel, time::interval};
use tracing::{span, Level};

use dsf_core::service::{Publisher, ServiceBuilder};
use dsf_core::types::{Address, Id};
use dsf_rpc::{Request as RpcRequest, Response as RpcResponse};
use kad::Config as DhtConfig;

use crate::daemon::net::NetIf;
use crate::daemon::*;
use crate::error::Error;
use crate::io::*;
use crate::rpc::bootstrap::Bootstrap;
use crate::store::*;

use crate::daemon::DsfOptions as DaemonOptions;

pub const DEFAULT_UNIX_SOCKET: &str = "/tmp/dsf.sock";
pub const DEFAULT_DATABASE_FILE: &str = "/tmp/dsf.db";
pub const DEFAULT_SERVICE: &str = "/tmp/dsf.svc";

#[derive(Parser, Debug, Clone, PartialEq)]
pub struct EngineOptions {
    #[clap(short = 'a', long = "bind-address", default_value = "0.0.0.0:10100")]
    /// Interface(s) to bind DSF daemon
    /// These may be reconfigured at runtime
    pub bind_addresses: Vec<SocketAddr>,

    #[clap(
        long = "database-file",
        default_value_t = EngineOptions::default().database_file,
        env = "DSF_DB_FILE"
    )]
    /// Database file for storage by the daemon
    pub database_file: String,

    #[clap(
        short = 's',
        long = "daemon-socket",
        default_value_t = EngineOptions::default().daemon_socket,
        env = "DSF_SOCK"
    )]
    /// Unix socket for communication with the daemon
    pub daemon_socket: String,

    #[clap(
        short = 's',
        long = "daemon-http",
        env = "DSF_HTTP"
    )]
    /// Unix socket for communication with the daemon
    pub daemon_http: Option<SocketAddr>,

    #[clap(long = "no-bootstrap")]
    /// Disable automatic bootstrapping
    pub no_bootstrap: bool,

    #[clap(flatten)]
    pub daemon_options: DaemonOptions,
}

impl Default for EngineOptions {
    fn default() -> Self {
        // Resolve home dir if available
        let h = match home_dir() {
            Some(h) => h.join(".dsfd/"),
            None => PathBuf::from("/var/dsfd/"),
        };

        // Build socket and database paths
        let daemon_socket = h.join("dsf.sock");
        let database_file = h.join("dsf.db");

        Self {
            bind_addresses: vec![SocketAddr::new(
                IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)),
                10100,
            )],
            daemon_socket: daemon_socket.to_string_lossy().to_string(),
            daemon_http: None,
            database_file: database_file.to_string_lossy().to_string(),
            no_bootstrap: false,
            daemon_options: DaemonOptions {
                dht: DhtConfig::default(),
            },
        }
    }
}

impl EngineOptions {
    /// Helper constructor to run multiple instances alongside each other
    pub fn with_suffix(&self, suffix: usize) -> Self {
        let bind_addresses = self
            .bind_addresses
            .iter()
            .map(|a| {
                let mut a = a.clone();
                let p = portpicker::pick_unused_port().unwrap();
                a.set_port(p);
                a
            })
            .collect();

        Self {
            bind_addresses,
            daemon_socket: format!("{}.{}", self.daemon_socket, suffix),
            daemon_http: None,
            database_file: format!("{}.{}", self.database_file, suffix),
            no_bootstrap: self.no_bootstrap,
            daemon_options: DaemonOptions {
                dht: self.daemon_options.dht.clone(),
            },
        }
    }
}

pub struct Engine {
    id: Id,
    dsf: Dsf<mpsc::Sender<(Address, Vec<u8>)>>,

    unix: Unix,
    http: Option<Http>,
    net: Net,

    net_source: mpsc::Receiver<(Address, Vec<u8>)>,

    options: EngineOptions,
}

impl Engine {
    /// Create a new daemon instance
    pub async fn new(options: EngineOptions) -> Result<Self, Error> {
        // Create new local data store
        info!(
            "Creating / connecting to database: {}",
            options.database_file
        );
        // Ensure directory exists
        if let Some(p) = PathBuf::from(&options.database_file).parent() {
            if !p.exists() {
                let _ = std::fs::create_dir(p);
            }
        }
        let store = Store::new(&options.database_file)?;

        // Fetch or create new peer service
        let mut service = match store.load_peer_service()? {
            Some(s) => {
                info!("Loaded existing peer service: {}", s.id());
                s
            }
            None => {
                let s = ServiceBuilder::peer().build().unwrap();
                info!("Created new peer service: {}", s.id());
                s
            }
        };

        // Clear old / expired options
        service.update(|_, o, _| {
            o.clear();
        })?;

        // Generate updated peer page
        let buff = vec![0u8; 2048];
        let (_n, page) = service.publish_primary(Default::default(), buff)?;

        // Store peer service identity for re-use
        store.set_peer_service(&service, &page)?;

        info!("Creating new engine");

        // Create new network connector
        info!(
            "Creating network connector on addresses: {:?}",
            options.bind_addresses
        );
        let mut net = Net::new();
        for addr in &options.bind_addresses {
            if let Err(e) = net.bind(NetKind::Udp, *addr).await {
                error!("Error binding interface: {:?}", addr);
                return Err(e.into());
            }
        }

        // Create new unix socket connector
        info!("Creating unix socket: {}", options.daemon_socket);
        // Ensure directory exists
        if let Some(p) = PathBuf::from(&options.daemon_socket).parent() {
            if !p.exists() {
                let _ = std::fs::create_dir(p);
            }
        }
        let unix = match Unix::new(&options.daemon_socket).await {
            Ok(u) => u,
            Err(e) => {
                error!("Error binding unix socket: {}", options.daemon_socket);
                return Err(e.into());
            }
        };

        let http = match options.daemon_http {
            Some(s) => {
                info!("Creating HTTP socket: {}", s);

                match Http::new(s.clone()).await {
                    Ok(v) => Some(v),
                    Err(e) => {
                        error!("Failed to create HTTP connector: {:?}", e);
                        return Err(e.into());
                    }
                }
            },
            None => None,
        };

        let (net_sink, net_source) = mpsc::channel::<(Address, Vec<u8>)>(1000);

        // Create new DSF instance
        let dsf = Dsf::new(options.daemon_options.clone(), service, store, net_sink)?;

        info!("Engine created!");

        Ok(Self {
            id: dsf.id(),
            dsf: dsf,
            net: net,
            net_source: net_source,
            unix,
            http,
            options,
        })
    }

    pub fn id(&self) -> Id {
        self.id.clone()
    }

    // Run the DSF daemon
    pub async fn start(self) -> Result<Instance, Error> {
        let Engine {
            id,
            mut dsf,
            mut net,
            mut net_source,
            mut unix,
            mut http,
            options,
        } = self;

        let span = span!(Level::DEBUG, "engine", "{}", dsf.id());
        let _enter = span.enter();

        if !options.no_bootstrap {
            // Create future bootstrap event
            let exec = dsf.exec();

            // Await on this in the future
            task::spawn(async move {
                tokio::time::sleep(Duration::from_secs(2)).await;
                let _ = exec.bootstrap().await;
            });
        }

        // Create periodic timer
        let mut update_timer = interval(Duration::from_secs(30));
        let mut tick_timer = interval(Duration::from_millis(200));

        let (mut net_in_tx, mut net_in_rx) = mpsc::channel(1000);
        let (mut net_out_tx, mut net_out_rx) = mpsc::channel(1000);

        let (exit_tx, mut exit_rx) = mpsc::channel(1);
        let (mut dsf_exit_tx, mut dsf_exit_rx) = mpsc::channel(1);
        let (mut net_exit_tx, mut net_exit_rx) = mpsc::channel(1);

        // Setup exist task
        let _exit_handle = task::spawn(async move {
            // Await exit signal
            exit_rx.next().await;

            debug!("Received exit signal");

            // Send othert exists
            net_exit_tx.send(()).await.unwrap();
            dsf_exit_tx.send(()).await.unwrap();
        });

        // Setup network IO task
        let net_handle: JoinHandle<Result<(), Error>> = task::spawn(async move {
            loop {
                select! {
                    // Incoming network messages
                    net_rx = net.next() => {
                        if let Some(m) = net_rx {
                            trace!("engine::net_rx {:?}", m);

                            // Forward to DSF for execution
                            if let Err(e) = net_in_tx.send(m).await {
                                error!("error forwarding incoming network message: {:?}", e);
                                return Err(Error::Unknown);
                            }
                        } else {
                            error!("engine::net_rx returned None");
                        }
                    },
                    net_tx = net_out_rx.next().fuse() => {
                        if let Some((address, data)) = net_tx {
                            trace!("engine::net_tx {:?} {:?}", address, data);

                            if let Err(e) = net.send(address, None, data).await {
                                error!("error sending ougoing network message: {:?}", e);
                            }
                        } else {
                            warn!("engine::net_out channel closed");
                            return Err(Error::Closed)
                        }
                    },
                    _exit = net_exit_rx.next().fuse() => {
                        debug!("Exiting network handler");
                        return Ok(())
                    }
                }
            }
        });

        // Setup DSF main task
        let dsf_handle: JoinHandle<Result<(), Error>> = task::spawn(async move {
            loop {
                select! {
                    // Incoming network _requests_
                    net_rx = net_in_rx.next().fuse() => {

                        if let Some(m) = net_rx {

                            // Handle request via DSF
                            match dsf.handle_net_raw(m).await {
                                Ok(v) => v,
                                Err(e) => {
                                    error!("error handling DSF message: {:?}", e);
                                    continue;
                                }
                            };
                        }
                    },
                    // Outgoing network _requests_
                    net_tx = net_source.next().fuse() => {
                        if let Some((addr, data)) = net_tx {
                            if let Err(e) = net_out_tx.send((addr.into(), Bytes::from(data))).await {
                                error!("error forwarding outgoing network message: {:?}", e);
                                return Err(Error::Unknown);
                            }
                        }
                    },
                    // Incoming RPC messages
                    rpc_rx = unix.next().fuse() => {
                        trace!("engine::unix_rx");

                        if let Some(m) = rpc_rx {
                            Self::handle_unix_rpc(&mut dsf, m).await.unwrap();
                        }
                    },
                    // TODO: periodic update
                    _interval = update_timer.tick().fuse() => {
                        trace!("engine::update");

                        // TODO: prompt dsf service updates?
                        // Maybe this should just use time internally?
                    },
                    // Poll on DSF internal state (this actually runs DSF logic)
                    _ = dsf => {
                        // TODO: handle results / errors here?
                    },
                    // Tick timer for process reactivity
                    _tick = tick_timer.tick().fuse() => {
                        trace!("engine::tick");

                        // Prompt DSF poll
                        dsf.wake();
                    },
                    // Exit signal
                    _exit = dsf_exit_rx.next().fuse() => {
                        debug!("Exiting DSF handler");
                        return Ok(())
                    }
                }
            }
        });

        Ok(Instance {
            id,
            dsf_handle,
            net_handle,
            exit_tx,
        })
    }

    async fn handle_unix_rpc<Net>(dsf: &mut Dsf<Net>, unix_req: UnixMessage) -> Result<(), Error>
    where
        Dsf<Net>: NetIf<Interface = Net>,
    {
        trace!("incoming RPC: {:?}", unix_req);

        // Parse out message
        let req: RpcRequest = match serde_json::from_slice(&unix_req.data) {
            Ok(v) => v,
            Err(e) => {
                warn!("Failed to decode RPC request: {:?}", e);
                return Ok(());
            }
        };

        debug!("engine, RPC req: {:?}", req);

        // Start RPC request
        let (tx, mut rx) = mpsc::channel(1);
        dsf.start_rpc(req, tx)?;

        // Spawn task to poll to RPC completion and forward result
        task::spawn(async move {
            let resp = rx.next().await;

            // Encode response
            let enc = serde_json::to_vec(&resp).unwrap();

            // Generate response with required socket info
            let unix_resp = unix_req.response(Bytes::from(enc));

            // Send response
            if let Err(e) = unix_resp.send().await {
                error!("Error sending RPC response: {:?}", e);
            }
        });

        Ok(())
    }
}

pub struct Instance {
    id: Id,

    dsf_handle: JoinHandle<Result<(), Error>>,
    net_handle: JoinHandle<Result<(), Error>>,

    exit_tx: mpsc::Sender<()>,
}

impl Instance {
    /// Fetch the ID for a given engine
    pub fn id(&self) -> Id {
        self.id.clone()
    }

    /// Fetch exit tx sender
    pub fn exit_tx(&self) -> mpsc::Sender<()> {
        self.exit_tx.clone()
    }

    /// Exit the running engine instance
    pub async fn join(self) -> Result<(), Error> {
        let (a, b) = futures::try_join!(self.dsf_handle, self.net_handle)?;

        a.and(b)
    }
}
