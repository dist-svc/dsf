//! DSF Client Library and CLI, used to communicate with the DSF daemon
//!

pub mod client;
pub use client::{Client, Options};

//pub mod net;

pub mod error;
pub use error::Error;

pub mod prelude;
