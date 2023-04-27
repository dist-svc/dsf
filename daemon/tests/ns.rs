use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    time::Duration,
};

use dsf_core::types::PageKind;
use rpc::{CreateOptions, NsRegisterOptions, NsSearchOptions, ServiceIdentifier};
use tempdir::TempDir;
use tracing_subscriber::{filter::LevelFilter, FmtSubscriber};

use dsf_client::{Client, Options as ClientOptions};
use dsf_daemon::engine::{Engine, EngineOptions};
use dsf_rpc::{self as rpc};

#[tokio::test]
async fn test_nameserver() {
    let _ = FmtSubscriber::builder()
        .with_max_level(LevelFilter::DEBUG)
        .try_init();

    let d = TempDir::new("dsf-ns").unwrap();
    let d = d.path().to_str().unwrap().to_string();

    let daemon_socket = format!("{}/dsf.sock", d);
    let _ns_name = "com.test.ns";

    // Setup daemon
    let config = EngineOptions {
        database_file: format!("{}/dsf-e2e.db", d),
        bind_addresses: vec![SocketAddr::new(
            IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
            11200,
        )],
        daemon_socket: daemon_socket.clone(),
        ..Default::default()
    };
    let e = Engine::new(config).await.expect("Error creating engine");
    let _h = e.start().await.expect("Error launching engine");

    // Setup client connector
    let mut client = Client::new(&ClientOptions::new(Some(&daemon_socket), Duration::from_secs(5)))
        .await
        .expect("Error creating client");

    // Create a new service for name registration
    let s = client
        .create(CreateOptions {
            register: true,
            ..Default::default()
        })
        .await
        .unwrap();

    // Create new name service
    let ns = client
        .create(CreateOptions {
            page_kind: Some(PageKind::Name),
            public: true,
            ..Default::default()
        })
        .await
        .unwrap();

    // TODO: List known name services

    let n = "test-service";

    println!("Registering service {}", s.id);

    // Register service using NS
    client
        .ns_register(NsRegisterOptions {
            ns: ServiceIdentifier::from(ns.id.clone()),
            target: s.id.clone(),
            name: Some(n.to_string()),
            hash: vec![],
        })
        .await
        .unwrap();

    println!("Searching for service {}", s.id);

    // Lookup using NS
    let found = client
        .ns_search(NsSearchOptions {
            ns: ServiceIdentifier::from(ns.id),
            name: Some(n.to_string()),
            hash: None,
        })
        .await
        .unwrap();

    println!("Found service(s) {:?}", found);

    // Check result
    assert_eq!(found[0].id(), s.id);

    // Ensure engine stays up until now
    let _ = e;
}
