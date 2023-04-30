//! DSF Daemon (dsfd)
//!
//!

use async_signals::Signals;
use clap::Parser;
use futures::prelude::*;
use log::info;
use tokio::task;
use tracing_subscriber::filter::{EnvFilter, LevelFilter};
use tracing_subscriber::FmtSubscriber;

use dsf_daemon::engine::{Engine, EngineOptions};

#[derive(Debug, Parser)]
#[clap(name = "DSF Daemon")]
/// Distributed Service Framework (DSF) daemon
struct Args {
    #[clap(flatten)]
    daemon_opts: EngineOptions,

    #[clap(long, default_value = "debug", env = "LOG_LEVEL")]
    /// Enable verbose logging
    log_level: LevelFilter,
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    // Fetch arguments
    let opts = Args::parse();

    // Initialise logging
    let filter = EnvFilter::from_default_env()
        .add_directive(opts.log_level.into())
        .add_directive("kad::dht=info".parse()?);

    let _ = FmtSubscriber::builder()
        .compact()
        .with_max_level(opts.log_level)
        .with_env_filter(filter)
        .try_init();

    // Bind exit handler
    let mut exit_rx = Signals::new(vec![libc::SIGINT])?;

    // Initialise daemon
    let d = match Engine::new(opts.daemon_opts).await {
        Ok(d) => d,
        Err(e) => {
            return Err(anyhow::anyhow!("Daemon creation error: {:?}", e));
        }
    };

    // Spawn daemon instance
    let h = match d.start().await {
        Ok(i) => i,
        Err(e) => {
            return Err(anyhow::anyhow!("Daemon launch error: {:?}", e));
        }
    };

    // Setup exit task
    let mut exit_tx = h.exit_tx();
    task::spawn(async move {
        let _ = exit_rx.next().await;
        let _ = exit_tx.send(()).await;
    });

    // Execute daemon / await completion
    let res = h.join().await;

    info!("Exiting");

    // Return error on failure
    if let Err(e) = res {
        return Err(anyhow::anyhow!("Daemon error: {:?}", e));
    }

    Ok(())
}
