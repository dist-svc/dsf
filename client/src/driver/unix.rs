use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::{channel::mpsc, prelude::*, SinkExt};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::UnixStream,
    select,
    task::JoinHandle,
};
use tracing::{debug, error, trace};

use dsf_rpc::{Request, RequestKind, Response, ResponseKind};

use super::Driver;
use crate::Error;

/// Unix socket driver for DSF client use
pub struct UnixDriver {
    tx_sink: mpsc::Sender<Request>,

    requests: RequestMap,

    _handle: JoinHandle<()>,
}

type RequestMap = Arc<Mutex<HashMap<u64, mpsc::Sender<ResponseKind>>>>;

impl UnixDriver {
    /// Create a new unix driver
    pub async fn new(address: &str) -> Result<Self, Error> {
        debug!("Unix socket connecting (address: {})", address);

        let address = address.trim_start_matches("unix://");

        // Connect to stream
        let mut socket = UnixStream::connect(&address).await?;

        // Create internal channels
        let (tx_sink, mut tx_stream) = mpsc::channel::<Request>(0);

        let requests: RequestMap = Arc::new(Mutex::new(HashMap::new()));
        let r = requests.clone();

        // Setup socket task
        let _handle = tokio::task::spawn(async move {
            trace!("started unix socket task");

            let (mut unix_stream, mut unix_sink) = socket.split();
            let mut buff = [0u8; 10 * 1024];

            loop {
                select! {
                    // Poll for incoming responses
                    Ok(n) = unix_stream.read(&mut buff) => {
                        // Decode these from JSON
                        let resp: Response = match serde_json::from_slice(&buff[..n]) {
                            Ok(v) => v,
                            Err(e) => {
                                error!("Failed to decode RPC: {:?}", e);
                                continue;
                            }
                        };

                        // Lookup matching requests
                        let mut a = match r.lock().unwrap().get_mut(&resp.req_id()) {
                            Some(a) => a.clone(),
                            None => {
                                error!("Unix RX with no matching request ID");
                                continue;
                            }
                        };

                        // Forward response
                        match a.send(resp.kind()).await {
                            Ok(_) => (),
                            Err(e) => {
                                error!("client send error: {:?}", e);
                            }
                        };

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
            requests,
        })
    }
}

impl Driver for UnixDriver {
    async fn exec(&self, req: RequestKind, timeout: Duration) -> Result<ResponseKind, Error> {
        let (tx, mut rx) = mpsc::channel(0);
        let req = Request::new(req);
        let id = req.req_id();

        // Add request to tracking
        trace!("request add lock");
        self.requests.lock().unwrap().insert(id, tx);

        // Send request
        self.tx_sink.clone().send(req).await.unwrap();

        // Await response
        let res = tokio::time::timeout(timeout, rx.next()).await;

        let res = match res {
            Ok(Some(v)) => Ok(v),
            // TODO: this seems like it should be a yield / retry point..?
            Ok(None) => {
                error!("No response received");
                Err(Error::None(()))
            }
            Err(e) => {
                error!("Response error: {:?}", e);
                Err(Error::Timeout)
            }
        };

        // Remove request on failure
        if let Err(_e) = &res {
            trace!("request failure lock");
            self.requests.lock().unwrap().remove(&id);
        }

        // Return response
        res
    }
}
