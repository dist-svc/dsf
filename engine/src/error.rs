use core::fmt::Debug;

#[derive(Debug, PartialEq)]
#[cfg_attr(feature = "thiserror", derive(thiserror::Error))]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub enum EngineError<CommsError: Debug, StoreError: Debug> {
    #[cfg_attr(feature = "thiserror", error("core: {0:?}"))]
    Core(dsf_core::error::Error),

    #[cfg_attr(feature = "thiserror", error("comms: {0:?}"))]
    Comms(CommsError),

    #[cfg_attr(feature = "thiserror", error("store: {0:?}"))]
    Store(StoreError),

    #[cfg_attr(feature = "thiserror", error("unhandled"))]
    Unhandled,

    #[cfg_attr(feature = "thiserror", error("unsupported"))]
    Unsupported,

    #[cfg_attr(feature = "thiserror", error("No secret key for service"))]
    NoSecretKey,

    #[cfg_attr(feature = "thiserror", error("Overrun in static vector"))]
    Overrun,

    #[cfg_attr(feature = "thiserror", error("Object decode failed"))]
    Decode,
}
