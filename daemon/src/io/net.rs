use std::io;
use std::net::SocketAddr;
use std::pin::Pin;

use std::collections::HashMap;

use log::{debug, error};
use stream::FusedStream;
use tracing::event;

use futures::channel::mpsc;
use futures::prelude::*;
use futures::task::{Context, Poll};
use futures::{select, Stream};

use tokio::net::UdpSocket;
use tokio::task::{self, JoinHandle};

use tracing::{span, Level};
use tracing_futures::Instrument;

use bytes::Bytes;

pub const UDP_BUFF_SIZE: usize = 4096;

#[derive(Debug, Clone, PartialEq)]
pub enum NetKind {
    Udp,
    Tcp,
}

/// NetCommands to support dynamic binding and unbinding of network
/// interfaces
/// TODO: all of this
#[derive(Debug, Clone, PartialEq)]
pub enum NetCommand {
    Bind(NetKind, SocketAddr),
    Unbind(NetKind, SocketAddr),
}

/// Network message
#[derive(Debug, Clone)]
pub struct NetMessage {
    pub interface: Option<u32>,
    pub address: SocketAddr,
    pub data: Bytes,

    pub resp_tx: Option<mpsc::Sender<NetMessage>>,
}

impl PartialEq for NetMessage {
    fn eq(&self, o: &Self) -> bool {
        self.interface == o.interface && self.address == o.address && self.data == o.data
    }
}

impl NetMessage {
    /// Create a new network message with no reply channel
    pub fn new(interface: Option<u32>, address: SocketAddr, data: Bytes) -> Self {
        Self {
            interface,
            address,
            data,
            resp_tx: None,
        }
    }

    /// Reply to a received network message
    pub async fn reply(&self, data: &[u8]) -> Result<(), NetError> {
        let mut resp_tx = self.resp_tx.clone();

        let d = Bytes::from(data.to_vec());

        let r = match resp_tx.as_mut() {
            Some(r) => r,
            None => return Err(NetError::NoResponseChannel),
        };

        r.send(NetMessage::new(self.interface, self.address, d))
            .await?;

        Ok(())
    }
}

#[derive(Debug, PartialEq, Clone)]
pub enum NetError {
    Io(io::ErrorKind),
    Sender(mpsc::SendError),
    NoMatchingInterface,
    NoResponseChannel,
}

impl From<io::Error> for NetError {
    fn from(e: io::Error) -> Self {
        Self::Io(e.kind())
    }
}

impl From<mpsc::SendError> for NetError {
    fn from(e: mpsc::SendError) -> Self {
        Self::Sender(e)
    }
}

impl Unpin for Net {}

/// Network manager object
pub struct Net {
    bindings: HashMap<u32, Binding>,
    index: u32,

    rx_sink: mpsc::Sender<NetMessage>,
    rx_stream: mpsc::Receiver<NetMessage>,

    default_interface: u32,
}

#[derive(Debug)]
struct Binding {
    handle: JoinHandle<Result<(), NetError>>,
    sink: mpsc::Sender<NetMessage>,
    exit: mpsc::Sender<()>,
    info: NetInfo,
}

/// Network binding information object
#[derive(Debug, Clone, PartialEq)]
pub struct NetInfo {
    pub kind: NetKind,
    pub addr: SocketAddr,
}

impl NetInfo {
    pub fn new(addr: SocketAddr, kind: NetKind) -> Self {
        Self { addr, kind }
    }
}

impl Net {
    /// Create a new network manager object
    pub fn new() -> Self {
        let (rx_sink, rx_stream) = mpsc::channel::<NetMessage>(1000);

        //let rx_stream = Box::pin(rx_stream);

        Net {
            bindings: HashMap::new(),
            index: 0,
            default_interface: 0,
            rx_sink,
            rx_stream,
        }
    }

    /// List bound network interfaces
    pub fn list(&self) -> Vec<NetInfo> {
        self.bindings.iter().map(|(_k, b)| b.info.clone()).collect()
    }

    /// Bind to a new interface
    pub async fn bind(&mut self, kind: NetKind, addr: SocketAddr) -> Result<(), NetError> {
        let interface = match kind {
            NetKind::Udp => self.listen_udp(addr).await?,
            NetKind::Tcp => unimplemented!(),
        };

        if self.bindings.len() == 1 {
            self.default_interface = interface;
        }

        Ok(())
    }

    /// Unbind from an existing interface
    pub async fn unbind(&mut self, interface: u32) -> Result<(), NetError> {
        let mut interface = match self.bindings.remove(&interface) {
            Some(v) => v,
            None => return Err(NetError::NoMatchingInterface),
        };

        interface.exit.send(()).await?;

        Ok(())
    }

    /// Send a network message
    /// TODO: what if you don't know what interface to send on??
    pub async fn send(
        &mut self,
        address: SocketAddr,
        interface: Option<u32>,
        data: Bytes,
    ) -> Result<(), NetError> {
        // Use interface by index if specified
        let index = match &interface {
            Some(v) => *v,
            None => self.default_interface,
        };

        // Find matching interface
        let binding = match self.bindings.get_mut(&index) {
            Some(v) => v,
            None => return Err(NetError::NoMatchingInterface),
        };

        // Build a message
        let msg = NetMessage::new(interface, address, data);

        // Send to appropriate binding
        binding.sink.send(msg).await?;

        Ok(())
    }

    /// Start listening on the provided UDP address
    async fn listen_udp(&mut self, address: SocketAddr) -> Result<u32, NetError> {
        let socket = UdpSocket::bind(address).await?;
        let interface = self.index;

        let mut rx_sink = self.rx_sink.clone();
        let (tx_sink, mut tx_stream) = mpsc::channel::<NetMessage>(0);

        let (exit_sink, mut exit_stream) = mpsc::channel::<()>(0);

        debug!("Starting UDP listener {}: {}", interface, address);

        let resp_tx = tx_sink.clone();
        let handle = task::spawn(
            async move {
                let mut buff = vec![0u8; UDP_BUFF_SIZE];

                loop {
                    select! {
                        // Handle incoming messages
                        res = socket.recv_from(&mut buff).fuse() => {
                            let (n, address) = match res {
                                Ok(v) => v,
                                Err(e) => {
                                    error!("receive error: {:?}", e);
                                    break
                                },
                            };

                            let data = Bytes::copy_from_slice(&buff[..n]);
                            event!(Level::TRACE, kind="UDP rx", address = %address);

                            let msg = NetMessage{
                                interface: Some(interface),
                                address,
                                data,
                                resp_tx: Some(resp_tx.clone()),
                            };

                            // TODO: sometimes this blocks, mitigated by increasing
                            // stream size in the constructor above but, really incoming messages
                            // should be serviced pretty much immediately?!
                            if let Err(e) = rx_sink.send(msg).await {
                                error!("rx sink error: {:?}", e);
                                break
                            }

                            event!(Level::TRACE, kind="UDP rx compete", address = %address);
                        },

                        // Handle outgoing messages
                        res = tx_stream.next() => {
                            let d = match res {
                                Some(v) => v,
                                None => {
                                    error!("tx stream closed");
                                    break;
                                }
                            };

                            event!(Level::TRACE, kind="UDP tx", address = %d.address);

                            if let Err(e) = socket.send_to(&d.data, &d.address).await {
                                error!("socket send error: {:?}", e);
                                continue;
                            }

                            event!(Level::TRACE, kind="UDP tx compete", address = %address);
                        },
                        // Handle the exit signal
                        res = exit_stream.next() => {
                            if let Some(_) = res {
                                debug!("Received exit");
                                break;
                            }
                        },
                    }
                }

                debug!("Exiting UDP listener");

                Ok(())
            }
            .instrument(span!(Level::TRACE, "UDP", interface, address=%address)),
        );

        let binding = Binding {
            handle,
            sink: tx_sink,
            exit: exit_sink,
            info: NetInfo::new(address, NetKind::Udp),
        };

        self.bindings.insert(interface, binding);
        self.index += 1;

        Ok(interface)
    }
}

impl Stream for Net {
    type Item = NetMessage;

    fn poll_next(mut self: Pin<&mut Self>, ctx: &mut Context) -> Poll<Option<Self::Item>> {
        #[cfg(feature = "profile")]
        let _fg = ::flame::start_guard("net::poll_next");

        Pin::new(&mut self.rx_stream).poll_next(ctx)
    }
}

impl FusedStream for Net {
    fn is_terminated(&self) -> bool {
        false
    }
}

#[cfg(test)]
mod test {
    use super::*;

    use tracing_subscriber::FmtSubscriber;

    #[tokio::test]
    async fn test_udp() {
        let addr_a = "127.0.0.1:19993".parse().unwrap();
        let addr_b = "127.0.0.1:19994".parse().unwrap();

        let _ = FmtSubscriber::builder()
            .with_max_level(Level::DEBUG)
            .try_init();

        let mut net = Net::new();
        assert_eq!(net.list().len(), 0);

        // Bind to a UDP port
        net.bind(NetKind::Udp, addr_a)
            .await
            .expect("error binding udp interface 1");
        assert_eq!(net.list().len(), 1);

        net.bind(NetKind::Udp, addr_b)
            .await
            .expect("error binding udp interface 2");
        assert_eq!(net.list().len(), 2);

        // Send some messages
        let data = Bytes::copy_from_slice(&[0x11, 0x22, 0x33, 0x44]);

        net.send(addr_b, Some(0), data.clone())
            .await
            .expect("Error sending net message");

        let res = net.next().await.expect("Error awaiting net message");

        assert_eq!(res, NetMessage::new(Some(1), addr_a, data.clone()));

        net.send(addr_a, Some(1), data.clone())
            .await
            .expect("Error sending net message");

        let res = net.next().await.expect("Error awaiting net message");

        assert_eq!(res, NetMessage::new(Some(0), addr_b, data.clone()));

        // Unbind from UDP port
        net.unbind(0).await.unwrap();
        net.unbind(1).await.unwrap();
        assert_eq!(net.list().len(), 0);
    }
}
