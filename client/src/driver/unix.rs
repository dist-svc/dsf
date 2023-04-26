use futures::SinkExt;
use futures::{channel::mpsc, prelude::*};
use log::{debug, trace};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::UnixStream,
    select,
    task::JoinHandle,
};

use dsf_rpc::{Request, Response};

use crate::Error;

/// Unix socket driver for DSF client use
pub struct UnixDriver {
    tx_sink: mpsc::Sender<Request>,
    rx_stream: mpsc::Receiver<Response>,

    _handle: JoinHandle<()>,
}

impl UnixDriver {
    /// Create a new unix driver
    pub async fn new(address: &str) -> Result<Self, Error> {
        debug!("Unix socket connecting (address: {})", address);

        // Connect to stream
        let mut socket = UnixStream::connect(&address).await?;

        // Create internal channels
        let (tx_sink, mut tx_stream) = mpsc::channel::<Request>(0);
        let (mut rx_sink, rx_stream) = mpsc::channel::<Response>(0);

        // Setup socket task
        let _handle = tokio::task::spawn(async move {
            trace!("started unix socket task");

            let (mut unix_stream, mut unix_sink) = socket.split();
            let mut buff = [0u8; 10 * 1024];

            loop {
                select! {
                    // Read and decode incoming responses
                    Ok(n) = unix_stream.read(&mut buff) => {
                        if let Ok(req) = serde_json::from_slice(&buff[..n]) {
                            rx_sink.send(req).await.unwrap();
                        }
                    },
                    // Encode and write outgoing requests
                    Some(m) = tx_stream.next() => {
                        let v = serde_json::to_vec(&m).unwrap();
                        unix_sink.write(&v).await.unwrap();
                    }
                }
            }
        });

        Ok(Self {
            _handle,
            tx_sink,
            rx_stream,
        })
    }
}

impl Sink<Request> for UnixDriver {
    type Error = Error;

    fn poll_ready(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.tx_sink.poll_ready(cx).map_err(|_| Error::Socket)
    }

    fn start_send(mut self: std::pin::Pin<&mut Self>, item: Request) -> Result<(), Self::Error> {
        self.tx_sink.start_send(item).map_err(|_| Error::Socket)
    }

    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.tx_sink.poll_flush_unpin(cx).map_err(|_| Error::Socket)
    }

    fn poll_close(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.tx_sink.poll_close_unpin(cx).map_err(|_| Error::Socket)
    }
}

impl Stream for UnixDriver {
    type Item = Response;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.rx_stream.poll_next_unpin(cx)
    }
}
