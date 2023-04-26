#![cfg_attr(not(feature = "std"), no_std)]
#![feature(test)]
#![feature(associated_type_defaults)]
#![feature(trait_alias)]

#[cfg(feature = "alloc")]
#[macro_use]
extern crate alloc;

#[cfg(feature = "libc")]
extern crate libc;

#[cfg(feature = "cty")]
extern crate cty as libc;

extern crate async_trait;
extern crate base64;
extern crate byteorder;
extern crate bytes;
extern crate managed;
extern crate slice_ext;

#[cfg(feature = "serde")]
extern crate serde;

#[macro_use]
extern crate log;

#[macro_use]
extern crate bitflags;

#[cfg(feature = "std")]
extern crate chrono;

pub mod types;

pub mod crypto;

pub mod options;

pub mod base;

pub mod service;

pub mod wire;

pub mod page;

pub mod net;

pub mod keys;

pub mod api;

pub mod prelude;

pub mod error;

#[cfg(feature = "defmt")]
pub trait Debug = core::fmt::Debug + defmt::Format;

#[cfg(not(feature = "defmt"))]
pub trait Debug = core::fmt::Debug;
