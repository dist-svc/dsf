use std::net::SocketAddr;
use std::time::SystemTime;

use clap::Parser;
use log::{debug, error, warn};
use prettytable::{row, Table};
use simplelog::{LevelFilter, TermLogger};

use dsf_client::{Client, Options};
use dsf_core::{helpers::print_bytes, prelude::MaybeEncrypted, types::Id};
use dsf_rpc::{DataInfo, PeerInfo, RequestKind, ResponseKind, ServiceInfo};

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

    /// Disable field truncation during display
    #[clap(long)]
    no_trunc: bool,

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
            handle_response(resp, opts.no_trunc);
        }
        Err(e) => {
            error!("error: {:?}", e);
        }
    }

    Ok(())
}

fn handle_response(resp: ResponseKind, no_trunc: bool) {
    match resp {
        ResponseKind::Status(status) => {
            println!("Status: {:?}", status);
        }
        ResponseKind::Service(info) => {
            println!("Created / Located service");
            print_service(&info, no_trunc);
        }
        ResponseKind::Services(services) => {
            print_services(&services, no_trunc);
        }
        ResponseKind::Peers(peers) => {
            print_peers(&peers, no_trunc);
        }
        ResponseKind::Data(data) => {
            println!("Data:");
            print_objects(&data, no_trunc);
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

fn print_peers(peers: &[(Id, PeerInfo)], no_trunc: bool) {
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
        let id = match no_trunc {
            true => format!("{}", p.id),
            false => format!("{:#}", p.id),
        };

        table.add_row(row![
            id,
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

fn print_service(service: &ServiceInfo, _no_trunc: bool) {
    println!("Service {} (index: {})", service.id, service.index);

    println!("  kind: {}", service.kind);
    println!("  state: {}", service.state);

    println!("  subscribers: {}", service.subscribers);
    println!("  replicas: {}", service.replicas);

    if let Some(u) = &service.last_updated {
        println!("  updated: {}", systemtime_to_humantime(u.clone()));
    }

    if let Some(s) = &service.primary_page {
        println!("  primary page: {:#}", s);
    }

    if let Some(s) = &service.replica_page {
        println!("  replica page: {}", s);
    }

    println!("  is_origin: {}", match service.origin {
        true => true,
        false => false,
    });

    println!("  has_private_key: {}", service.private_key.as_ref().map(|_| "true").unwrap_or("false"));

    println!("  has_secret_key: {}", service.secret_key.as_ref().map(|_| "true").unwrap_or("false"));
}

fn print_services(services: &[ServiceInfo], no_trunc: bool) {
    if services.len() == 0 {
        warn!("No services found");
        return;
    }

    // Create the table
    let mut table = Table::new();

    table.set_format(*prettytable::format::consts::FORMAT_CLEAN);

    // Add a row per time
    table.add_row(row![b => "Service ID", "Index", "Kind", "State", "Updated", "PublicKey", "PrivateKey", "SecretKey", "Subscribers", "Replicas", "Primary Page"]);

    for s in services {
        let id = match no_trunc {
            true => format!("{}", s.id),
            false => format!("{:#}", s.id),
        };

        let pk = match no_trunc {
            true => format!("{}", s.public_key),
            false => format!("{:#}", s.public_key),
        };

        let primary_page = match s.primary_page.as_ref() {
            Some(p) => match no_trunc {
                true => format!("{}", p),
                false => format!("{:#}", p),
            },
            None => format!("None"),
        };

        table.add_row(row![
            id,
            s.index.to_string(),
            s.kind.to_string(),
            s.state.to_string(),
            s.last_updated
                .map(systemtime_to_humantime)
                .unwrap_or("Never".to_string()),
            pk,
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
            primary_page,
        ]);
    }

    // Print the table to stdout
    table.printstd();
}

fn print_objects(objects: &[DataInfo], no_trunc: bool) {
    if objects.len() == 0 {
        warn!("No objects found");
        return;
    }

    for o in objects {
        println!("index: {}", o.index);

        println!("  - kind: {}", o.kind);

        let body = match &o.body {
            MaybeEncrypted::Cleartext(v) if v.len() > 0 => print_bytes(v),
            MaybeEncrypted::Encrypted(_) => "Encrypted".to_string(),
            _ => "Empty".to_string(),
        };
        println!("  - body: {}", body);

        print!("  - private_options: ");
        match &o.private_options {
            MaybeEncrypted::None => println!("Empty"),
            MaybeEncrypted::Cleartext(options) => {
                println!("");
                for o in options {
                    if no_trunc {
                        println!("    - {o}");
                    } else {
                        println!("    - {o:#}");
                    }
                }
            }
            MaybeEncrypted::Encrypted(_) => println!("Encrypted"),
        };

        print!("  - public_options: ");
        if o.public_options.len() == 0 {
            println!("Empty")
        } else {
            println!("");
            for o in &o.public_options {
                if no_trunc {
                    println!("    - {o}");
                } else {
                    println!("    - {o:#}");
                }
            }
        }

        if no_trunc {
            println!("  - signature: {}", o.signature);
        } else {
            println!("  - signature: {:#}", o.signature);
        }
    }
}
