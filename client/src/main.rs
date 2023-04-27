use std::net::SocketAddr;
use std::time::SystemTime;

use clap::Parser;
use log::{debug, error, info, warn};
use prettytable::{cell, row, Table};
use simplelog::{LevelFilter, TermLogger};

use dsf_client::{Client, Options};
use dsf_core::types::Id;
use dsf_rpc::{PeerInfo, RequestKind, ResponseKind, ServiceInfo};

#[derive(Parser)]
#[clap(
    name = "DSF Client",
    about = "Distributed Service Discovery (DSF) client, interacts with the DSF daemon to publish and locate services"
)]
struct Config {
    #[clap(subcommand)]
    cmd: Commands,

    #[clap(flatten)]
    options: Options,

    #[clap(long = "log-level", default_value = "info")]
    /// Enable verbose logging
    level: LevelFilter,
}

#[derive(Parser)]
enum Commands {
    /// Flattened enum for DSF requests
    #[clap(flatten)]
    Request(RequestKind),

    /// Generate shell completion information
    #[cfg(nope)]
    Completion {
        #[clap(long, default_value = "zsh")]
        /// Shell for completion file output
        shell: Shell,

        #[clap(long, default_value = "./")]
        /// Completion file output directory
        dir: String,
    },
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    // Fetch arguments
    let opts = Config::parse();

    // Setup logging
    let mut log_config = simplelog::ConfigBuilder::new();
    log_config.add_filter_ignore_str("tokio");
    let log_config = log_config.build();

    if TermLogger::init(
        opts.level,
        log_config.clone(),
        simplelog::TerminalMode::Mixed,
    )
    .is_err()
    {
        let _ = simplelog::SimpleLogger::init(opts.level, log_config);
    }

    // Parse out commands
    let cmd = match &opts.cmd {
        Commands::Request(r) => r,
        #[cfg(nope)]
        Commands::Completion { shell, dir } => {
            info!("Writing completions for {} to: {}", *shell, dir);
            Config::clap().gen_completions("dsfc", *shell, dir);
            return Ok(());
        }
    };

    // Create client connector
    debug!(
        "Connecting to client socket: '{}'",
        &opts.options.daemon_socket()
    );
    let mut c = match Client::new(&opts.options).await {
        Ok(c) => c,
        Err(e) => {
            return Err(anyhow::anyhow!(
                "Error connecting to daemon on '{}': {:?}",
                &opts.options.daemon_socket(),
                e
            ));
        }
    };

    // Execute request and handle response
    let res = c.request(cmd.clone()).await;

    match res {
        Ok(resp) => {
            handle_response(resp);
        }
        Err(e) => {
            error!("error: {:?}", e);
        }
    }

    Ok(())
}

fn handle_response(resp: ResponseKind) {
    match resp {
        ResponseKind::Status(status) => {
            println!("Status: {:?}", status);
        }
        ResponseKind::Service(info) => {
            println!("Created / Located service");
            print_services(&[info]);
        }
        ResponseKind::Services(services) => {
            print_services(&services);
        }
        ResponseKind::Peers(peers) => {
            print_peers(&peers);
        }
        ResponseKind::Data(data) => {
            println!("Data:");
            for d in &data {
                println!("{:+}", d);
            }
        }
        ResponseKind::Error(e) => error!("{:?}", e),
        ResponseKind::Unrecognised => warn!("command not yet implemented"),
        _ => warn!("unhandled response: {:?}", resp),
    }
}

fn systemtime_to_humantime(s: SystemTime) -> String {
    let v = chrono::DateTime::<chrono::Local>::from(s);
    chrono_humanize::HumanTime::from(v).to_string()
}

fn print_peers(peers: &[(Id, PeerInfo)]) {
    if peers.len() == 0 {
        warn!("No peers found");
        return;
    }

    // Create the table
    let mut table = Table::new();

    table.set_format(*prettytable::format::consts::FORMAT_CLEAN);

    // Add a row per time
    table.add_row(row![b => "Peer ID", "Index", "State", "Address(es)", "Seen", "Sent", "Received", "Blocked"]);

    for (_id, p) in peers {
        table.add_row(row![
            p.id.to_string(),
            p.index.to_string(),
            p.state.to_string(),
            format!("{}", SocketAddr::from(*p.address())),
            p.seen
                .map(systemtime_to_humantime)
                .unwrap_or("Never".to_string()),
            format!("{}", p.sent),
            format!("{}", p.received),
            //format!("{}", p.blocked),
        ]);
    }

    // Print the table to stdout
    table.printstd();
}

fn print_services(services: &[ServiceInfo]) {
    if services.len() == 0 {
        warn!("No services found");
        return;
    }

    // Create the table
    let mut table = Table::new();

    table.set_format(*prettytable::format::consts::FORMAT_CLEAN);

    // Add a row per time
    table.add_row(row![b => "Service ID", "Index", "State", "Updated", "PublicKey", "PrivateKey", "SecretKey", "Subscribers", "Replicas"]);

    for s in services {
        table.add_row(row![
            s.id.to_string(),
            s.index.to_string(),
            s.state.to_string(),
            s.last_updated
                .map(systemtime_to_humantime)
                .unwrap_or("Never".to_string()),
            s.public_key.to_string(),
            s.private_key
                .as_ref()
                .map(|_| "True".to_string())
                .unwrap_or("False".to_string()),
            s.secret_key
                .as_ref()
                .map(|_| "True".to_string())
                .unwrap_or("False".to_string()),
            format!("{}", s.subscribers),
            format!("{}", s.replicas),
        ]);
    }

    // Print the table to stdout
    table.printstd();
}
