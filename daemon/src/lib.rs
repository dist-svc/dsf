#![recursion_limit = "512"]
#![allow(unused_imports)]
#![allow(dead_code)]

extern crate std;

#[macro_use]
extern crate diesel;

#[cfg(feature = "jemalloc")]
extern crate jemallocator;

#[cfg(feature = "jemalloc")]
#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

pub(crate) mod sync {
    pub(crate) type Arc<T> = std::sync::Arc<T>;

    pub(crate) type Mutex<T> = std::sync::Mutex<T>;
}

pub mod core;

pub mod error;

pub mod daemon;

pub mod rpc;

pub mod io;

pub mod store;

pub mod engine;

pub mod plugins;

#[cfg(test)]
mod tests {
    use ctor::ctor;

    #[ctor]
    fn init_color_backtrace() {
        color_backtrace::install();
    }
}
