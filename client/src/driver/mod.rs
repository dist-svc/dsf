//! Driver abstraction for generic client support

use dsf_rpc::{Request, Response};
use futures::{Sink, Stream};

mod http;

mod unix;
pub use unix::UnixDriver;

mod net;

/// Drivers implement [Request]/[Response] streams
pub trait Driver: Stream<Item = Response> + Sink<Request> {}

/// Automatic driver impl for rpc stream types
impl<T: Stream<Item = Response> + Sink<Request>> Driver for T {}
