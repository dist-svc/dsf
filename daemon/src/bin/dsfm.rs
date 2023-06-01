use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::time::Duration;

use async_signals::Signals;
use clap::Parser;
use futures::future::try_join_all;
use futures::prelude::*;
use log::error;
use tracing_futures::Instrument;
use tracing_subscriber::filter::LevelFilter;
use tracing_subscriber::FmtSubscriber;

use dsf_client::{Client, Config};
use dsf_daemon::engine::{Engine, EngineOptions};
use dsf_rpc::ConnectOptions;

#[derive(Debug, Parser)]
#[clap(name = "DSF Daemon Multi-runner")]
/// Distributed Service Framework (DSF) daemon multi-runner
struct Args {
    #[clap(long, default_value = "3")]
    /// Number of instances to run
    count: usize,

    #[clap(long, default_value = "0")]
    /// Offset for instance indexing
    offset: usize,

    #[clap(flatten)]
    daemon_opts: EngineOptions,

    #[clap(long, default_value = "debug")]
    /// Enable verbose logging
    level: LevelFilter,
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    // Fetch arguments
    let opts = Args::parse();

    // Initialise logging
    let _ = FmtSubscriber::builder()
        .with_max_level(opts.level)
        .try_init();

    // Bind exit handler
    let mut exit_rx = Signals::new(vec![libc::SIGINT]).expect("Error setting Ctrl-C handler");

    let mut handles = vec![];
    let mut ports = vec![];

    for i in opts.offset..opts.count + opts.offset {
        let o = opts.daemon_opts.with_suffix(i + 1);
        ports.push(o.bind_addresses[0].port());

        // Initialise daemon
        let d = match Engine::new(o).await {
            Ok(d) => d,
            Err(e) => {
                error!("Error running daemon: {:?}", e);
                return Err(e.into());
            }
        };

        let handle = d
            .start()
            .instrument(tracing::debug_span!("engine", i))
            .await?;

        handles.push(handle);
    }

    tokio::time::sleep(Duration::from_secs(3)).await;

    // Establish peer connections
    let root = opts.daemon_opts.with_suffix(1);
    let mut c = Client::new(Config::new(
        Some(&root.daemon_socket),
        Duration::from_secs(10),
    ))
    .await
    .unwrap();

    for i in 1..opts.count {
        let address = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), ports[i]));

        let _ = c
            .connect(ConnectOptions {
                address,
                id: None,
                timeout: None,
            })
            .await;
    }

    // Await exit signal
    // Again, this means no exiting on failure :-/
    let _ = exit_rx.next().await;

    let exits: Vec<_> = handles
        .drain(..)
        .map(|v| async move {
            // Send exit signal
            v.exit_tx().send(()).await.unwrap();
            // Await engine completion
            v.join().await
        })
        .collect();
    if let Err(e) = try_join_all(exits).await {
        error!("Daemon runtime error: {:?}", e);
        return Err(e.into());
    }

    Ok(())
}
