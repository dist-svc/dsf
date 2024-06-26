use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::time::Duration;

use log::info;

use tracing_subscriber::{filter::LevelFilter, FmtSubscriber};

use tempdir::TempDir;

use dsf_daemon::engine::{Engine, EngineOptions};

use dsf_client::{Client, Config as ClientConfig};

use dsf_rpc::{self as rpc};

const NUM_DAEMONS: usize = 3;

#[tokio::test]
#[ignore]
async fn end_to_end() {
    let d = TempDir::new("dsf-e2e").unwrap();
    let d = d.path().to_str().unwrap().to_string();

    let _ = FmtSubscriber::builder()
        .compact()
        .with_max_level(LevelFilter::INFO)
        .try_init();

    let mut daemons = vec![];

    let mut config = EngineOptions::default();
    config.database_file = format!("{}/dsf-e2e.db", d);
    config.bind_addresses = vec![SocketAddr::new(
        IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
        11100,
    )];
    config.daemon_socket = format!("{}/dsf.sock", d);

    // Create daemons
    info!("Creating daemons");
    for i in 0..NUM_DAEMONS {
        let c = config.with_suffix(i);
        let c1 = c.clone();
        let addr = c.daemon_socket.clone();

        let e = Engine::new(c1).await.expect("Error creating engine");

        // Launch daemon
        let handle = e.start().await.expect("Error launching engine");

        // Create client
        let mut client = Client::new(ClientConfig::new(Some(&addr), Duration::from_secs(5)))
            .await
            .expect("Error creating client");

        // Fetch client status and ID
        let status = client.status().await.expect("Error fetching daemon status");
        let id = status.id;

        // Add the new daemon to the list
        daemons.push((id, c, client, handle));
    }
    info!("created {} daemons", daemons.len());

    let base_config = daemons[0].1.clone();
    info!("connecting peers");

    for (_id, _config, client, _) in &mut daemons[1..] {
        client
            .connect(rpc::ConnectOptions {
                address: base_config.bind_addresses[0],
                id: None,
                timeout: Some(Duration::from_secs(10)),
            })
            .await
            .expect("connecting failed");
    }
    info!("connecting complete");

    let mut services = vec![];
    info!("creating services");
    for (_id, _config, client, _) in &mut daemons[..] {
        let s = client
            .create(rpc::CreateOptions::default().and_register())
            .await
            .expect("creation failed");
        services.push(s);
    }
    info!("created services: {:?}", services);

    info!("searching for services");
    for i in 0..NUM_DAEMONS {
        let service_handle = &services[NUM_DAEMONS - i - 1].clone();

        let (_id, _config, client, _handle) = &mut daemons[i];

        client
            .locate(rpc::LocateOptions {
                id: service_handle.id.clone(),
                local_only: false,
                no_persist: false,
            })
            .await
            .expect("search failed");
    }
    info!("searching for services");

    info!("test complete, exiting");

    // TODO: shutdown daemon instances
}
