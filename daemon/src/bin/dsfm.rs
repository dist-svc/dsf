use futures::prelude::*;
use futures::{executor::block_on, future::try_join_all};

use async_signals::Signals;
use clap::Parser;
use log::error;
use tracing_futures::Instrument;
use tracing_subscriber::filter::LevelFilter;
use tracing_subscriber::FmtSubscriber;

use dsf_daemon::engine::{Engine, EngineOptions};

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

fn main() {
    // Fetch arguments
    let opts = Args::parse();

    // Initialise logging
    let _ = FmtSubscriber::builder()
        .with_max_level(opts.level)
        .try_init();

    // Bind exit handler
    let mut exit_rx = Signals::new(vec![libc::SIGINT]).expect("Error setting Ctrl-C handler");

    // Create async task
    let res = block_on(async move {
        let mut handles = vec![];

        for i in opts.offset..opts.count + opts.offset {
            let o = opts.daemon_opts.with_suffix(i + 1);

            // Initialise daemon
            let d = match Engine::new(o).await {
                Ok(d) => d,
                Err(e) => {
                    error!("Error running daemon: {:?}", e);
                    return Err(e);
                }
            };

            let handle = d
                .start()
                .instrument(tracing::debug_span!("engine", i))
                .await?;

            handles.push(handle);
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
            return Err(e);
        }

        Ok(())
    });

    // Return error on failure
    if let Err(_e) = res {
        std::process::exit(-1);
    }
}
