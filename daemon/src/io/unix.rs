use crate::sync::{Arc, Mutex};
use std::collections::HashMap;
use std::io;
use std::pin::Pin;

use futures::channel::mpsc;
use futures::prelude::*;
use futures::select;
use futures::task::{Context, Poll};
use log::{debug, error, trace};

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{UnixListener, UnixStream},
    task::{self, JoinHandle},
};

use tracing::{span, Level};
use tracing_futures::Instrument;

use bytes::Bytes;

pub const UNIX_BUFF_LEN: usize = 10 * 1024;

#[derive(Debug, PartialEq)]
pub enum UnixError {
    Io(io::ErrorKind),
    Sender(mpsc::SendError),
    NoMatchingConnection,
}

impl From<io::Error> for UnixError {
    fn from(e: io::Error) -> Self {
        Self::Io(e.kind())
    }
}

impl From<mpsc::SendError> for UnixError {
    fn from(e: mpsc::SendError) -> Self {
        Self::Sender(e)
    }
}

#[derive(Debug, Clone)]
pub struct UnixMessage {
    pub connection_id: u32,
    pub data: Bytes,

    sink: Option<mpsc::Sender<UnixMessage>>,
    exit: Option<mpsc::Sender<()>>,
}

impl UnixMessage {
    fn new(connection_id: u32, data: Bytes) -> Self {
        Self {
            connection_id,
            data,
            sink: None,
            exit: None,
        }
    }

    /// Generate a response for an existing unix message
    pub fn response(&self, data: Bytes) -> Self {
        Self {
            connection_id: self.connection_id,
            data,
            sink: self.sink.clone(),
            exit: self.exit.clone(),
        }
    }

    /// Send a response to this message
    pub(crate) async fn send(&self) -> Result<(), mpsc::SendError> {
        let mut ch = self.sink.as_ref().unwrap().clone();
        ch.send(self.clone()).await?;
        Ok(())
    }

    /// Close the channel associated with this message
    pub(crate) async fn close(&self) -> Result<(), mpsc::SendError> {
        let mut ch = self.exit.as_ref().unwrap().clone();
        ch.send(()).await?;
        Ok(())
    }
}

impl PartialEq for UnixMessage {
    fn eq(&self, o: &Self) -> bool {
        self.connection_id == o.connection_id && self.data == o.data
    }
}

pub struct Unix {
    connections: Arc<Mutex<HashMap<u32, Connection>>>,
    rx_stream: mpsc::Receiver<UnixMessage>,

    handle: JoinHandle<Result<(), UnixError>>,
}

struct Connection {
    index: u32,

    sink: mpsc::Sender<UnixMessage>,
    exit_sink: mpsc::Sender<()>,

    handle: JoinHandle<Result<(), UnixError>>,
}

impl Unix {
    /// Create a new unix socket IO connector
    pub async fn new(path: &str) -> Result<Self, UnixError> {
        debug!("Creating UnixActor with path: {}", path);

        let _ = std::fs::remove_file(&path);
        let listener = UnixListener::bind(&path)?;
        let mut index = 0;

        let connections = Arc::new(Mutex::new(HashMap::new()));
        let c = connections.clone();
        let (rx_sink, rx_stream) = mpsc::channel::<UnixMessage>(0);

        // Create listening task
        let handle = task::spawn(
            async move {
                while let Ok((stream, _addr)) = listener.accept().await {
                    let conn = Connection::new(stream, index, rx_sink.clone());

                    c.lock().unwrap().insert(index, conn);

                    index += 1;
                }

                Ok(())
            }
            .instrument(span!(Level::TRACE, "UNIX", path)),
        );

        Ok(Self {
            connections,
            rx_stream,
            handle,
        })
    }

    /// Send a network message
    pub async fn send(&mut self, msg: UnixMessage, close: bool) -> Result<(), UnixError> {
        // Sink must be cloned here so the connection lock can be dropped
        // before the await point, interestingly explicitly dropping doesn't
        // work, but, adding a scope does...

        let connection_id = msg.connection_id;

        let (mut tx_sink, mut exit_sink) = {
            let mut connections = self.connections.lock().unwrap();

            let interface = match connections.get_mut(&connection_id) {
                Some(v) => v,
                None => return Err(UnixError::NoMatchingConnection),
            };

            debug!("send on interface: {}", interface.index);

            (interface.sink.clone(), interface.exit_sink.clone())
        };

        tx_sink.send(msg).await?;

        if close {
            exit_sink.send(()).await?;
        }

        Ok(())
    }

    pub async fn close(self) -> Result<(), UnixError> {
        // TODO: add listener exit channel, handle close
        let _ = self.handle;

        unimplemented!()
    }
}

impl Stream for Unix {
    type Item = UnixMessage;

    fn poll_next(mut self: Pin<&mut Self>, ctx: &mut Context) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.rx_stream).poll_next(ctx)
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        // TODO: ensure task closes happily
        let _ = self.handle;
    }
}

impl Connection {
    fn new(
        mut unix_stream: UnixStream,
        index: u32,
        rx_sink: mpsc::Sender<UnixMessage>,
    ) -> Connection {
        let mut rx_sink = rx_sink;

        let (tx_sink, tx_stream) = mpsc::channel::<UnixMessage>(0);
        let tx = Some(tx_sink.clone());

        let (exit_sink, mut exit_stream) = mpsc::channel::<()>(0);
        let exit = Some(exit_sink.clone());

        let handle: JoinHandle<Result<(), UnixError>> = task::spawn(async move {
            debug!("new UNIX task {}", index);

            let (mut unix_rx, mut unix_tx) = unix_stream.split();

            let mut buff = vec![0u8; UNIX_BUFF_LEN];
            let mut tx_stream = tx_stream.fuse();
            //let mut unix_rx = unix_rx.fuse();

            loop {
                select!{
                    // Send outgoing messages
                    tx = tx_stream.next() => {
                        if let Some(tx) = tx {
                            unix_tx.write(&tx.data).await?;
                        }
                    },
                    // Forward incoming messages
                    res = unix_rx.read(&mut buff).fuse() => {
                        match res {
                            Ok(n) => {
                                // Exit on 0 length message
                                if n == 0 {
                                    break
                                }

                                debug!("RX: {:?}", &buff[..n]);

                                let mut u = UnixMessage::new(index, Bytes::copy_from_slice(&buff[..n]));
                                u.sink = tx.clone();
                                u.exit = exit.clone();

                                rx_sink.send(u).await?;
                            },
                            Err(e) => {
                                error!("rx error: {:?}", e);
                                break;
                            },
                        }
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

            debug!("task UNIX closed {}", index);

            Ok(())
        }.instrument(span!(Level::TRACE, "UNIX", index)) );

        Connection {
            index,
            handle,
            sink: tx_sink,
            exit_sink,
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    use tracing_subscriber::FmtSubscriber;

    #[tokio::test]
    async fn test_unix() {
        let _ = FmtSubscriber::builder()
            .with_max_level(Level::DEBUG)
            .try_init();

        let mut unix = Unix::new("/tmp/dsf-unix-test")
            .await
            .expect("Error creating unix socket listener");

        let mut stream = UnixStream::connect("/tmp/dsf-unix-test")
            .await
            .expect("Error connecting to unix socket");

        let data = Bytes::copy_from_slice(&[0x11, 0x22, 0x33, 0x44]);
        let mut buff = vec![0u8; UNIX_BUFF_LEN];

        // Client to server
        stream.write(&data).await.expect("Error writing data");

        let res = unix.next().await.expect("Error awaiting unix message");

        assert_eq!(res, UnixMessage::new(0, data.clone()));

        // Server to client
        unix.send(UnixMessage::new(0, data.clone()), false)
            .await
            .expect("Error sending message to client");

        let n = stream
            .read(&mut buff)
            .await
            .expect("Error reading from client");

        assert_eq!(&buff[..n], &data);
    }
}
