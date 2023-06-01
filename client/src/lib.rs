//! DSF Client Library and CLI, used to communicate with the DSF daemon
//!

#![feature(async_fn_in_trait)]

pub mod client;
pub use client::{Client, Config};

//pub mod net;

pub mod error;
pub use error::Error;

pub mod prelude;

pub mod driver;
