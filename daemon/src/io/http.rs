
use std::{net::SocketAddr, pin::Pin, task::{Context, Poll}, str::FromStr};

use futures::{Stream, SinkExt, StreamExt};
use futures::channel::mpsc;
use rocket::{get, post, State, http::Status, serde::json::Json, routes, Config};

use dsf_core::prelude::{Id, DsfError};
use dsf_rpc::{Request, Response, StatusInfo, CreateOptions, ServiceInfo, RequestKind, ResponseKind, ServiceCommands, ServiceListOptions, InfoOptions, ServiceIdentifier};
use tracing::error;

/// HTTP RPC implementation
pub struct Http {
    // Stream for incoming HttpMessages
    rx_stream: mpsc::Receiver<HttpMessage>,
}

pub struct HttpMessage {
    // Request object
    pub req: Request,
    // Sink for responses
    pub sink: mpsc::Sender<Response>,
}

struct HttpCtx {
    tx_stream: mpsc::Sender<HttpMessage>,
}

impl Http {
    pub async fn new(socket: SocketAddr) -> Result<Self, crate::error::Error> {
        let (tx_stream, rx_stream) = mpsc::channel(0);

        let ctx = HttpCtx{
            tx_stream,
        };

        tokio::task::spawn(async move {
            let _ = rocket::build()
                .configure(Config{
                    address: socket.ip(),
                    port: socket.port(),
                    ..Default::default()
                })
                .manage(ctx)
                .mount("/", routes![
                    status,
                    service_list,
                    service_get,
                    service_create,
                ])
                .launch().await;
        });

        Ok(Self{ rx_stream })
    }
}

impl Stream for Http {
    type Item = HttpMessage;

    fn poll_next(mut self: Pin<&mut Self>, ctx: &mut Context) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.rx_stream).poll_next(ctx)
    }
}

impl HttpCtx {
    /// Execute an RPC request via internal sink
    async fn exec(&self, req: RequestKind) -> ResponseKind {
        let (sink, mut stream) = mpsc::channel(0);
        let req = Request::new(req);

        // Forward RPC request
        if let Err(e) = self.tx_stream.clone().send(HttpMessage { req, sink }).await {
            error!("Failed to forward RPC request: {:?}", e);
            return ResponseKind::Error(DsfError::IO);
        }

        // Poll on RPC response
        let resp = match stream.next().await {
            Some(v) => v,
            None => {
                error!("Channel closed waiting for RPC response");
                return ResponseKind::Error(DsfError::IO);
            }
        };

        resp.kind()
    }
}

#[get("/status", format = "json")]
async fn status(h: &State<HttpCtx>) -> Result<Json<StatusInfo>, Json<DsfError>> {
    match h.exec(RequestKind::Status).await {
        ResponseKind::Status(i) => Ok(Json(i)),
        ResponseKind::Error(e) => Err(Json(e)),
        _ => Err(Json(DsfError::InvalidResponse)),
    }
}

#[get("/services", format = "json")]
async fn service_list(h: &State<HttpCtx>) -> Result<Json<Vec<ServiceInfo>>, Json<DsfError>> {
    match h.exec(RequestKind::Service(ServiceCommands::List(ServiceListOptions{
        application_id: None,
        kind: None,
    }))).await {
        ResponseKind::Services(i) => Ok(Json(i)),
        ResponseKind::Error(e) => Err(Json(e)),
        _ => Err(Json(DsfError::InvalidResponse)),
    }
}

#[get("/services?<id>&<index>", format = "json")]
async fn service_get(h: &State<HttpCtx>, id: Option<String>, index: Option<usize>) -> Result<Json<ServiceInfo>, Json<DsfError>> {
    let service: ServiceIdentifier = match (id, index) {
        (Some(id), _) => Id::from_str(&id).map_err(|_| DsfError::InvalidOption)?.into(),
        (_, Some(index)) => index.into(),
        _ => return Err(Json(DsfError::InvalidOption))
    };

    match h.exec(RequestKind::Service(ServiceCommands::Info(InfoOptions{
        service
    }))).await {
        ResponseKind::Service(i) => Ok(Json(i)),
        ResponseKind::Error(e) => Err(Json(e)),
        _ => Err(Json(DsfError::InvalidResponse)),
    }
}

#[post("/services", data = "<opts>")]
async fn service_create(h: &State<HttpCtx>, opts: Json<CreateOptions>) -> Json<ServiceInfo> {
    todo!()
}

