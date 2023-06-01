use crate::sync::{Arc, Mutex};
use std::{collections::HashMap, io, os::unix::fs::PermissionsExt, pin::Pin};

use futures::channel::mpsc;
use futures::prelude::*;
use futures::select;
use futures::task::{Context, Poll};
use log::{debug, error, trace, warn};

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
    pub req: dsf_rpc::Request,

    pub sink: mpsc::Sender<dsf_rpc::Response>,
}

impl PartialEq for UnixMessage {
    fn eq(&self, o: &Self) -> bool {
        self.connection_id == o.connection_id && self.req == o.req
    }
}

pub struct Unix {
    connections: Arc<Mutex<HashMap<u32, Connection>>>,
    handle: JoinHandle<Result<(), UnixError>>,
}

struct Connection {
    index: u32,

    sink: mpsc::Sender<dsf_rpc::Response>,
    handle: JoinHandle<Result<(), UnixError>>,
}

impl Unix {
    /// Create a new unix socket IO connector
    pub async fn new(
        path: &str,
        rx_sink: mpsc::Sender<(dsf_rpc::Request, mpsc::Sender<dsf_rpc::Response>)>,
    ) -> Result<Self, UnixError> {
        debug!("Creating UnixActor with path: {}", path);

        let _ = std::fs::remove_file(&path);
        let listener = UnixListener::bind(&path)?;
        let mut index = 0;

        // Setup socket permissions
        if let Ok(mut perms) = std::fs::metadata(&path).map(|p| p.permissions()) {
            // +RWX for running user and group
            perms.set_mode(0o770);
            // Write back permission
            if let Err(e) = std::fs::set_permissions(&path, perms) {
                warn!("Failed to set unix socket permissions: {:?}", e);
            }
        }

        let connections = Arc::new(Mutex::new(HashMap::new()));
        let c = connections.clone();

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
            handle,
        })
    }

    pub async fn close(self) -> Result<(), UnixError> {
        // TODO: add listener exit channel, handle close
        let _ = self.handle;

        unimplemented!()
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
        rx_sink: mpsc::Sender<(dsf_rpc::Request, mpsc::Sender<dsf_rpc::Response>)>,
    ) -> Connection {
        let mut rx_sink = rx_sink;

        let (tx_sink, tx_stream) = mpsc::channel::<dsf_rpc::Response>(0);
        let tx = tx_sink.clone();

        let handle: JoinHandle<Result<(), UnixError>> = task::spawn(async move {
            trace!("new UNIX task {}", index);

            let (mut unix_rx, mut unix_tx) = unix_stream.split();

            let mut buff = vec![0u8; UNIX_BUFF_LEN];
            let mut tx_stream = tx_stream.fuse();
            //let mut unix_rx = unix_rx.fuse();

            loop {
                select!{
                    // Encode and send outgoing messages
                    tx = tx_stream.next() => {
                        // Exit on tx stream closed
                        let tx = match tx {
                            Some(v) => v,
                            None => break,
                        };

                        trace!("tx: {:?}", tx);

                        // Encode message to JSON
                        let resp = match serde_json::to_vec(&tx) {
                            Ok(v) => v,
                            Err(e) => {
                                error!("Failed to encode RPC response: {:?}" ,e);
                                continue;
                            }
                        };

                        // Write encoded message to socket
                        if let Err(e) = unix_tx.write(&resp).await {
                            warn!("Unix socket closed: {:?}", e);
                            break;
                        }
                    },
                    // Parse and forward incoming messages
                    res = unix_rx.read(&mut buff).fuse() => {
                        match res {
                            Ok(n) => {
                                trace!("RX: {:?}", &buff[..n]);

                                // Exit on 0 length message
                                if n == 0 {
                                    break
                                }

                                // Parse message from JSON
                                let req: dsf_rpc::Request = match serde_json::from_slice(&buff[..n]) {
                                    Ok(v) => v,
                                    Err(e) => {
                                        warn!("Failed to decode RPC request: {:?}", e);
                                        continue;
                                    }
                                };

                                trace!("req: {:?}", req);

                                // Forward to client
                                if let Err(e) = rx_sink.send((req, tx.clone())).await {
                                    error!("Unix sink closed: {:?}", e);
                                    break;
                                }
                            },
                            Err(e) => {
                                error!("rx error: {:?}", e);
                                break;
                            },
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
        }
    }
}

#[cfg(test)]
mod test {
    use rand::random;

    use super::*;

    use dsf_core::types::Id;
    use dsf_rpc::StatusInfo;
    use tracing_subscriber::FmtSubscriber;

    #[tokio::test]
    async fn test_unix() {
        let _ = FmtSubscriber::builder()
            .with_max_level(Level::DEBUG)
            .try_init();

        let (unix_tx, mut unix_rx) = mpsc::channel(0);

        let _unix = Unix::new("/tmp/dsf-unix-test", unix_tx)
            .await
            .expect("Error creating unix socket listener");

        let mut stream = UnixStream::connect("/tmp/dsf-unix-test")
            .await
            .expect("Error connecting to unix socket");

        let id = Id::from(rand::random::<[u8; 32]>());

        // Client to server

        // Setup request
        let req = dsf_rpc::Request::new(dsf_rpc::RequestKind::Status);
        let data = serde_json::to_vec_pretty(&req).unwrap();

        // Write to client
        stream.write(&data).await.expect("Error writing data");
        // Poll from server
        let mut res = unix_rx.next().await.expect("Error awaiting unix message");
        // Check response
        assert_eq!(res.0, req);

        // Server to client

        // Setup response
        let resp = dsf_rpc::Response::new(
            req.req_id(),
            dsf_rpc::ResponseKind::Status(StatusInfo {
                id,
                peers: 10,
                services: 14,
            }),
        );

        // Forward to client
        res.1
            .send(resp.clone())
            .await
            .expect("Error sending message to client");

        // Poll from client
        let mut buff = vec![0u8; UNIX_BUFF_LEN];
        let n = stream
            .read(&mut buff)
            .await
            .expect("Error reading from client");

        // Parse out response
        let resp1: dsf_rpc::Response = serde_json::from_slice(&buff[..n]).unwrap();

        assert_eq!(resp1, resp);
    }
}
