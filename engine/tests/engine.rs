use std::net::{SocketAddr, UdpSocket};

use log::info;

use dsf_core::{api::Application, prelude::*};

use dsf_engine::{
    engine::{Engine, EngineEvent},
    store::MemoryStore,
};

/// Generic application for engine testing
pub struct Generic {}

impl Application for Generic {
    const APPLICATION_ID: u16 = 0x0102;

    type Info = Vec<u8>;

    type Data = Vec<u8>;

    fn matches(info: &Self::Info, req: &[u8]) -> bool {
        req.is_empty() || info == req
    }
}

type E = Engine<Generic, UdpSocket, MemoryStore, 512>;

fn new_engine(addr: &str, info: Vec<u8>) -> anyhow::Result<E> {
    // Create peer for sending requests
    let p = ServiceBuilder::<Vec<u8>>::generic().build()?;

    // Setup memory store with pre-filled peer keys
    let s = MemoryStore::<SocketAddr>::new();
    s.update(&p.id(), |k| *k = p.keys());

    // Setup engine with newly created service
    let e = E::udp(info, &[], addr, s)?;

    Ok(e)
}

#[test]
fn integration() -> anyhow::Result<()> {
    // Setup debug logging
    let _ = simplelog::SimpleLogger::init(simplelog::LevelFilter::Debug, Default::default());

    // Create a pair of engines
    let mut e1 = new_engine("127.0.0.1:11000", vec![0xaa, 0xbb, 0xcc])?;
    let mut e2 = new_engine("127.0.0.2:11000", vec![0x11, 0x22, 0x33])?;

    // Tick engines to get started
    e1.update()?;
    e2.update()?;

    info!("Attempting discovery");

    // Attempt local service discovery
    // TODO: use options here
    e1.discover(&[], &[])?;

    // Tick to update discovery state
    e1.update()?;
    e2.update()?;

    e1.update()?;
    e2.update()?;

    // TODO: broadcast doesn't seem to be working here..? 127.0.0.x address maybe?
    // hack to fix for now

    info!("Starting subscribe");

    // Attempt subscription
    e1.subscribe(e2.id(), e2.addr()?)?;

    // Tick to update discovery state
    assert_eq!(e2.tick()?, EngineEvent::SubscribeFrom(e1.id()));
    assert_eq!(e1.tick()?, EngineEvent::SubscribedTo(e2.id()));

    e2.update()?;

    info!("Publishing data");

    let data = vec![0xab, 0xcd, 0xef];

    let sig = e2.publish(data, &[])?;

    // Tick to update publish state
    assert_eq!(e1.tick()?, EngineEvent::ReceivedData(e2.id(), sig));

    Ok(())
}
