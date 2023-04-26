//! Error types for DSF Core

/// Error enum represents possible core errors
///
/// For serialisation add `serde`, `thiserror`, `strum`, and/or `defmt` features
#[derive(PartialEq, Debug, Clone, strum::EnumString, strum::Display)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
#[cfg_attr(feature = "thiserror", derive(thiserror::Error))]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub enum Error {
    IO,
    Time,
    InvalidOption,
    InvalidOptionLength,
    InvalidPageLength,
    InvalidPageKind,
    InvalidResponseKind,
    InvalidRequestKind,
    CryptoError,
    UnexpectedPageType,
    UnexpectedServiceId,
    UnexpectedApplicationId,
    InvalidServiceVersion,
    NoPrivateKey,
    NoPublicKey,
    NoSignature,
    ExpectedPrimaryPage,
    ExpectedSecondaryPage,
    ExpectedDataObject,
    UnexpectedPeerId,
    NoPeerId,
    KeyIdMismatch,
    PublicKeyChanged,
    Unimplemented,
    SendError,
    NoRequestId,
    InvalidMessageType,
    InvalidJson,
    NoPeersFound,
    NotFound,
    InvalidResponse,
    UnknownService,
    InvalidSignature,
    UnexpectedPageKind,
    NoReplicasFound,
    UnknownPeer,
    NoSecretKey,
    SecretKeyMismatch,
    NoSymmetricKeys,
    UnsupportedSignatureMode,
    Timeout,
    Unknown,
    EncodeFailed,
    BufferLength,
    InvalidUtf8,
}

#[cfg(feature = "std")]
impl From<std::io::Error> for Error {
    fn from(_e: std::io::Error) -> Error {
        Error::IO
    }
}

#[cfg(feature = "std")]
impl From<std::time::SystemTimeError> for Error {
    fn from(_e: std::time::SystemTimeError) -> Error {
        Error::Time
    }
}

impl From<encdec::Error> for Error {
    fn from(e: encdec::Error) -> Self {
        match e {
            encdec::Error::Length => Error::BufferLength,
            encdec::Error::Utf8 => Error::InvalidUtf8,
        }
    }
}
