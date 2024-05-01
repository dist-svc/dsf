use std::convert::TryFrom;
use std::net::{IpAddr, Ipv4Addr, SocketAddr, SocketAddrV4};
use std::path::PathBuf;
use std::str::FromStr;
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

use crate::core::store::AsyncStore;
use crate::daemon::net::NetIf;
use crate::daemon::*;
use crate::error::Error;
use crate::io::*;
use crate::plugins::address::AddressPlugin;
use crate::plugins::upnp::UpnpPlugin;
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

    #[clap(short = 'w', long = "daemon-http", env = "DSF_HTTP")]
    /// Unix socket for communication with the daemon
    pub daemon_http: Option<SocketAddr>,

    #[clap(long = "no-bootstrap")]
    /// Disable automatic bootstrapping
    pub no_bootstrap: bool,

    #[clap(long, hide=true, value_parser=parse_human_duration, env)]
    /// Insert delays into message receive path to emulate constant
    /// point-to-point network latencies
    pub mock_rx_latency: Option<Duration>,

    #[clap(long)]
    /// Enable UPnP port forwarding
    pub enable_upnp: bool,

    #[clap(flatten)]
    pub daemon_options: DaemonOptions,
}

fn parse_human_duration(s: &str) -> Result<Duration, humantime::DurationError> {
    let h = humantime::Duration::from_str(s)?;
    Ok(h.into())
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
                ..Default::default()
            },
            mock_rx_latency: None,
            enable_upnp: false,
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
                let mut a = *a;
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
                ..Default::default()
            },
            mock_rx_latency: None,
            enable_upnp: false,
        }
    }
}

pub struct Engine {
    id: Id,
    dsf: Dsf<mpsc::Sender<(Address, Vec<u8>)>>,

    unix: Unix,
    http: Option<Http>,
    net: Net,

    net_tx_source: mpsc::Receiver<(Address, Vec<u8>)>,
    rpc_source: mpsc::Receiver<(RpcRequest, mpsc::Sender<RpcResponse>)>,

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
        if !options.database_file.contains(":memory:") {
            if let Some(p) = PathBuf::from(&options.database_file).parent() {
                if !p.exists() {
                    let _ = std::fs::create_dir(p);
                }
            }
        }
        // Create new store
        let s = Store::new_rc(&options.database_file, Default::default())?;

        debug!("Loading peer service");

        // Fetch or create new peer service
        let mut service = match s.load_peer_service() {
            Ok(Some(s)) => {
                info!("Loaded existing peer service: {}", s.id());
                s
            }
            Ok(None) => {
                let s = ServiceBuilder::peer().build().unwrap();
                info!("Created new peer service: {}", s.id());
                s
            }
            Err(e) => {
                error!("Failed to load peer service: {e:?}");
                return Err(e.into());
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
        s.set_peer_service(&service, &page)?;

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
        if let Some(s) = options.mock_rx_latency {
            warn!("using mock rx latency: {s:?}");
        }

        // Setup RPC channels
        let (rpc_tx, rpc_rx) = mpsc::channel(100);

        // Create new unix socket connector
        info!("Creating unix socket: {}", options.daemon_socket);
        // Ensure directory exists
        if let Some(p) = PathBuf::from(&options.daemon_socket).parent() {
            if !p.exists() {
                let _ = std::fs::create_dir(p);
            }
        }
        let unix = match Unix::new(&options.daemon_socket, rpc_tx.clone()).await {
            Ok(u) => u,
            Err(e) => {
                error!("Error binding unix socket: {}", options.daemon_socket);
                return Err(e.into());
            }
        };

        let http = match options.daemon_http {
            Some(s) => {
                info!("Creating HTTP socket: {}", s);

                match Http::new(s, rpc_tx.clone()).await {
                    Ok(v) => Some(v),
                    Err(e) => {
                        error!("Failed to create HTTP connector: {:?}", e);
                        return Err(e);
                    }
                }
            }
            None => None,
        };

        let (net_tx_sink, net_tx_source) = mpsc::channel::<(Address, Vec<u8>)>(1000);

        // Setup async store task
        let store = AsyncStore::new(s)?;

        // Create new DSF instance
        let dsf = Dsf::new(options.daemon_options.clone(), service, store, net_tx_sink).await?;

        info!("Engine created!");

        Ok(Self {
            id: dsf.id(),
            dsf,
            net,
            net_tx_source,
            rpc_source: rpc_rx,
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
            mut net_tx_source,
            mut rpc_source,
            unix: _,
            http: _,
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

        // TODO: plugin contexts should exist alongside engine
        let mut plugin_addr = AddressPlugin::new();
        let mut plugin_upnp = UpnpPlugin::new(dsf.id());
        let upnp_enabled = options.enable_upnp;

        if options.enable_upnp {
            match plugin_addr.update().await {
                // TODO: fix this for multiple bindings etc... maybe part of net actor?
                Ok(addr) if addr.len() > 0 && options.bind_addresses.len() > 0 => {
                    let upnp_addr = SocketAddr::new(addr[0], options.bind_addresses[0].port());
                    info!("Bind UPnP for address: {upnp_addr}");
                    if let Err(e) = plugin_upnp.register(upnp_addr.clone()).await {
                        error!("Failed to bind UPnP addr {upnp_addr}: {e:?}");
                    } else {
                        info!("UPnP bound for: {upnp_addr}");
                    }
                }
                _ => {
                    warn!("Failed to resolve local address, UPnP aborted");
                }
            }
        }

        // Create periodic timers
        let mut update_timer = interval(Duration::from_secs(30));
        let mut tick_timer = interval(Duration::from_millis(200));

        // Setup network channels
        // Note: this must have a non-zero buffer to avoid deadlocks
        let (net_in_tx, mut net_in_rx) = mpsc::channel(1000);
        let (mut net_out_tx, mut net_out_rx) = mpsc::channel(1000);

        // Setup exit channels
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

            if upnp_enabled {
                let _ = plugin_upnp.deregister().await;
            }
        });

        let mock_rx_latency = options.mock_rx_latency;

        // Setup network IO task
        let net_handle: JoinHandle<Result<(), Error>> = task::spawn(async move {
            loop {
                select! {
                    // Incoming network messages
                    net_rx = net.next() => {
                        if let Some(m) = net_rx {
                            trace!("engine::net::rx {:?}", m);
                            let mut net_in_tx = net_in_tx.clone();

                            // TODO(low): prefer not to spawn a task every rx but,
                            // need to be able to inject rx delays so this seems like
                            // an easy option for the moment...
                            task::spawn(async move {
                                // Inject mock rx delay if enabled
                                if let Some(d) = mock_rx_latency {
                                    tokio::time::sleep(d).await;
                                }

                                // Forward to DSF for execution
                                if let Err(e) = net_in_tx.send(m).await {
                                    error!("error forwarding incoming network message: {:?}", e);
                                }
                            });
                        } else {
                            error!("engine::net::rx returned None");
                        }
                    },
                    net_tx = net_out_rx.next().fuse() => {
                        if let Some((address, data)) = net_tx {
                            trace!("engine::net::tx {:?} {:?}", address, data);

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
                    // Incoming network _requests_ to the daemon
                    net_rx = net_in_rx.next().fuse() => {
                        trace!("engine::net_rx: {net_rx:?}");

                        if let Some(m) = net_rx {
                            // Handle request via DSF
                            match dsf.handle_net(m).await {
                                Ok(v) => v,
                                Err(e) => {
                                    error!("error handling DSF message: {:?}", e);
                                    continue;
                                }
                            };
                        }
                    },
                    // Outgoing network _requests_ from the daemon
                    net_tx = net_tx_source.next().fuse() => {
                        trace!("engine::net_tx: {net_tx:?}");

                        if let Some((addr, data)) = net_tx {
                            if let Err(e) = net_out_tx.send((addr.into(), Bytes::from(data))).await {
                                error!("error forwarding outgoing network message: {:?}", e);
                                return Err(Error::Unknown);
                            }
                        }
                    },
                    // Incoming RPC messages
                    rpc_rx = rpc_source.next().fuse() => {
                        trace!("engine::rpc_rx: {rpc_rx:?}");

                        let (req, mut tx) = match rpc_rx {
                            Some(v) => v,
                            None => {
                                error!("No RPC");
                                continue;
                            }
                        };

                        let (resp_sink, mut resp_source) = mpsc::channel(100);

                        tokio::task::spawn(async move {
                            match resp_source.next().await {
                                Some(r) => {
                                    debug!("engine::rpc_tx {r:?}");

                                    if let Err(e) = tx.send(r).await {
                                        error!("Failed to foward RPC response: {:?}", e);
                                    }
                                },
                                None => {
                                    error!("Empty response");
                                }
                            }
                        });

                        if let Err(e) = dsf.start_rpc(req, resp_sink) {
                            error!("Failed to start rpc: {e:?}");
                        }
                    },
                    // TODO: periodic update
                    _interval = update_timer.tick().fuse() => {
                        trace!("engine::update");

                        // TODO: prompt dsf service updates?
                        // Maybe this should just use an internal timer?
                    },
                    // Poll on DSF internal state (this actually runs DSF logic)
                    _ = dsf => {
                        // TODO: handle results / errors here?
                    },
                    // Tick timer for process reactivity
                    _tick = tick_timer.tick().fuse() => {
                        //trace!("engine::tick");

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
