//! HTTP RPC Api Implementation

use std::{
    net::SocketAddr,
    pin::Pin,
    str::FromStr,
    task::{Context, Poll},
};

use futures::channel::mpsc;
use futures::{SinkExt, Stream, StreamExt};
use rocket::{get, http::Status, log::LogLevel, post, routes, serde::json::Json, Config, State};
use tokio::task::JoinHandle;
use tracing::error;

use dsf_core::prelude::{DsfError, Id};
use dsf_rpc::*;

/// HTTP RPC implementation
pub struct Http {
    // Stream for incoming HttpMessages
    _handle: JoinHandle<()>,
}

pub struct HttpMessage {
    // TODO: header information, origin etc.

    // Request object
    pub req: Request,
    // Sink for responses
    pub sink: mpsc::Sender<Response>,
}

struct HttpCtx {
    sink: mpsc::Sender<(Request, mpsc::Sender<Response>)>,
}

impl Http {
    pub async fn new(
        socket: SocketAddr,
        sink: mpsc::Sender<(Request, mpsc::Sender<Response>)>,
    ) -> Result<Self, crate::error::Error> {
        let ctx = HttpCtx { sink };

        let handle = tokio::task::spawn(async move {
            let _ = rocket::build()
                .configure(Config {
                    address: socket.ip(),
                    port: socket.port(),
                    log_level: LogLevel::Off,
                    ..Default::default()
                })
                .manage(ctx)
                .mount(
                    "/",
                    routes![
                        status,
                        service_list,
                        service_info,
                        service_create,
                        service_register,
                        service_subscribe,
                        service_unsubscribe,
                        peer_list,
                        peer_info,
                        peer_connect,
                        data_list,
                        ns_create,
                        ns_register,
                        ns_search,
                        exec,
                        debug,
                    ],
                )
                .launch()
                .await;
        });

        Ok(Self { _handle: handle })
    }
}

impl HttpCtx {
    /// Execute an RPC request via internal sink
    async fn exec(&self, req: RequestKind) -> ResponseKind {
        let (sink, mut stream) = mpsc::channel(100);
        let req = Request::new(req);

        // Forward RPC request
        if let Err(e) = self.sink.clone().send((req, sink)).await {
            error!("Failed to forward RPC request: {:?}", e);
            return ResponseKind::Error(DsfError::IO);
        }

        // Poll on RPC response
        // TODO: is it useful to apply timeouts at this level?
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

#[get("/status")]
async fn status(h: &State<HttpCtx>) -> Json<ResponseKind> {
    Json(h.exec(RequestKind::Status).await)
}

// Service endpoints

#[get("/services?<count>&<offset>")]
async fn service_list(h: &State<HttpCtx>,     count: Option<usize>,
    offset: Option<usize>) -> Json<ResponseKind> {
    Json(
        h.exec(RequestKind::Service(ServiceCommands::List(
            ServiceListOptions {
                application_id: None,
                kind: None,
                bounds: PageBounds {count, offset}
            },
        )))
        .await,
    )
}

#[get("/services/<id>")]
async fn service_info(h: &State<HttpCtx>, id: Option<String>) -> Json<ResponseKind> {
    let id = match id.map(|id| Id::from_str(&id)) {
        Some(Ok(id)) => id,
        _ => return Json(ResponseKind::Error(DsfError::InvalidOption)),
    };

    Json(
        h.exec(RequestKind::Service(ServiceCommands::Info(InfoOptions {
            service: id.into(),
        })))
        .await,
    )
}

#[post("/services", data = "<opts>")]
async fn service_create(h: &State<HttpCtx>, opts: Json<CreateOptions>) -> Json<ResponseKind> {
    Json(
        h.exec(RequestKind::Service(ServiceCommands::Create(opts.0)))
            .await,
    )
}

#[post("/services/register", data = "<opts>")]
async fn service_register(h: &State<HttpCtx>, opts: Json<RegisterOptions>) -> Json<ResponseKind> {
    Json(
        h.exec(RequestKind::Service(ServiceCommands::Register(opts.0)))
            .await,
    )
}

#[post("/services/subscribe", data = "<opts>")]
async fn service_subscribe(h: &State<HttpCtx>, opts: Json<SubscribeOptions>) -> Json<ResponseKind> {
    Json(
        h.exec(RequestKind::Service(ServiceCommands::Subscribe(opts.0)))
            .await,
    )
}

#[post("/services/unsubscribe", data = "<opts>")]
async fn service_unsubscribe(
    h: &State<HttpCtx>,
    opts: Json<UnsubscribeOptions>,
) -> Json<ResponseKind> {
    Json(
        h.exec(RequestKind::Service(ServiceCommands::Unsubscribe(opts.0)))
            .await,
    )
}

// Peer endpoints

#[get("/peers")]
async fn peer_list(h: &State<HttpCtx>) -> Json<ResponseKind> {
    Json(
        h.exec(RequestKind::Peer(PeerCommands::List(PeerListOptions {})))
            .await,
    )
}

#[post("/peers/connect", data = "<opts>")]
async fn peer_connect(h: &State<HttpCtx>, opts: Json<ConnectOptions>) -> Json<ResponseKind> {
    Json(
        h.exec(RequestKind::Peer(PeerCommands::Connect(opts.0)))
            .await,
    )
}

#[get("/peer/<id>")]
async fn peer_info(h: &State<HttpCtx>, id: Option<String>) -> Json<ResponseKind> {
    let id = match id.map(|id| Id::from_str(&id)) {
        Some(Ok(id)) => id,
        _ => return Json(ResponseKind::Error(DsfError::InvalidOption)),
    };

    Json(
        h.exec(RequestKind::Peer(PeerCommands::Info(id.into())))
            .await,
    )
}

// Data endpoints

#[get("/data/<id>?<count>&<offset>")]
async fn data_list(
    h: &State<HttpCtx>,
    id: Option<String>,
    count: Option<usize>,
    offset: Option<usize>,
) -> Json<ResponseKind> {
    let id = match id.map(|id| Id::from_str(&id)) {
        Some(Ok(id)) => id,
        _ => return Json(ResponseKind::Error(DsfError::InvalidOption)),
    };

    let page_bounds = PageBounds { count, offset };

    Json(
        h.exec(RequestKind::Data(DataCommands::List(DataListOptions {
            service: id.into(),
            page_bounds,
            time_bounds: Default::default(),
        })))
        .await,
    )
}

// NameService endpoints

#[post("/ns/create", data = "<opts>")]
async fn ns_create(h: &State<HttpCtx>, opts: Json<NsCreateOptions>) -> Json<ResponseKind> {
    Json(h.exec(RequestKind::Ns(NsCommands::Create(opts.0))).await)
}

#[post("/ns/register", data = "<opts>")]
async fn ns_register(h: &State<HttpCtx>, opts: Json<NsRegisterOptions>) -> Json<ResponseKind> {
    Json(h.exec(RequestKind::Ns(NsCommands::Register(opts.0))).await)
}

#[post("/ns/search", data = "<opts>")]
async fn ns_search(h: &State<HttpCtx>, opts: Json<NsSearchOptions>) -> Json<ResponseKind> {
    Json(h.exec(RequestKind::Ns(NsCommands::Search(opts.0))).await)
}

// Debug endpoints

/// Generic RPC endpoint, exposes all ops
#[post("/exec", data = "<opts>")]
async fn exec(h: &State<HttpCtx>, opts: Json<RequestKind>) -> Json<ResponseKind> {
    Json(h.exec(opts.0).await)
}

/// Debug RPC endpoint, exposes debug commands
#[post("/debug", data = "<opts>")]
async fn debug(h: &State<HttpCtx>, opts: Json<DebugCommands>) -> Json<ResponseKind> {
    Json(h.exec(RequestKind::Debug(opts.0)).await)
}
