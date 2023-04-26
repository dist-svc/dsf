#![cfg_attr(not(feature = "std"), no_std)]
#![feature(trait_alias)]

#[cfg(feature = "alloc")]
extern crate alloc;

pub mod comms;

pub mod store;

pub mod engine;

pub mod error;

#[cfg(feature = "defmt")]
mod log {
    pub use defmt::{debug, error, info, trace, warn};

    pub trait Debug = core::fmt::Debug + defmt::Format;
}

#[cfg(not(feature = "defmt"))]
mod log {
    pub use log::{debug, error, info, trace, warn};

    pub trait Debug = core::fmt::Debug;
}
