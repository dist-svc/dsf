use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::time::Duration;

use log::info;
use rand::random;
use tempdir::TempDir;
use tracing_subscriber::{
    filter::{EnvFilter, LevelFilter},
    FmtSubscriber,
};

use dsf_client::{Client, Config as ClientConfig};
use dsf_daemon::engine::{Engine, EngineOptions};
use dsf_rpc::{self as rpc};

#[tokio::test]
async fn smol_scale() {
    scale(3, LevelFilter::DEBUG).await;
}

#[tokio::test]
//#[ignore]
async fn med_scale() {
    scale(20, LevelFilter::DEBUG).await;
}

#[tokio::test]
#[ignore]
async fn large_scale() {
    scale(100, LevelFilter::WARN).await;
}

async fn scale(n: usize, level: LevelFilter) {
    let d = TempDir::new("dsf-scale").unwrap();
    let d = d.path().to_str().unwrap().to_string();

    let filter = EnvFilter::from_default_env().add_directive(level.into());
    let _ = FmtSubscriber::builder().with_env_filter(filter).try_init();

    let mut daemons = vec![];
    let mut ids = vec![];

    // Set common configuration
    let mut config = EngineOptions::default();
    config.database_file = format!("{}/dsf-scale.db", d);
    config.daemon_socket = format!("{}/dsf.sock", d);
    config.bind_addresses = vec![SocketAddr::new(
        IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
        11200,
    )];
    config.no_bootstrap = true;

    // Create instances
    info!("******** Creating {} virtual daemon instances ********", n);
    for i in 0..n {
        let c = config.with_suffix(i);
        let c1 = c.clone();

        // Ensure database does not exist
        let _ = std::fs::remove_file(c.database_file);

        let addr = c.daemon_socket.clone();
        let net_addr = c.bind_addresses[0];
        let e = Engine::new(c1).await.expect("Error creating engine");

        // Build and run daemon
        let handle = e.start().await.unwrap();

        // Create client
        let mut client = Client::new(&ClientConfig::new(Some(&addr), Duration::from_secs(10)))
            .await
            .expect("Error connecting to client");

        // Fetch client status and ID
        let status = client.status().await.expect("Error fetching client info");
        let id = status.id;

        // Add the new daemon to the list
        ids.push(id.clone());
        daemons.push((id, net_addr, client, handle));
    }
    info!("created daemons, establishing connections");

    let base_addr = daemons[0].1;

    info!("******** Connecting daemons via {} ********", d);
    for (_id, _addr, client, _handle) in &mut daemons[1usize..] {
        client
            .connect(rpc::ConnectOptions {
                address: base_addr,
                id: None,
                timeout: Some(Duration::from_secs(10)),
            })
            .await
            .expect("connecting failed");
    }
    info!("******** Daemons connected ********");

    println!("Attempting peer searches");
    for i in 0..daemons.len() {
        let mut j = random::<usize>() % daemons.len();
        while i == j {
            j = random::<usize>() % daemons.len();
        }

        let (client_id, _addr, client, _handle) = &mut daemons[i];
        let id = ids[j].clone();

        info!("FindNode from: {} for: {}", client_id, id);

        client
            .find(rpc::peer::SearchOptions { id, timeout: None })
            .await
            .expect("search failed");
    }
    info!("Completed searches");
}
