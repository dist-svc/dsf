//! Helpers for encoding/decoding types to and from strings

/// Encode bytes to printable string
#[cfg(feature = "alloc")]
pub fn print_bytes(data: &[u8]) -> String {
    bs58::encode(data).into_string()
}

/// Parse bytes from string encoding to fixed size array
pub fn parse_bytes<const N: usize>(value: &str, data: &mut [u8; N]) -> Result<(), bs58::decode::Error> {
    let n = bs58::decode(value).into(data)?;
    if n != N {
        return Err(bs58::decode::Error::BufferTooSmall)
    }
    Ok(())
}

/// Parse bytes from string encoding to vec
pub fn parse_bytes_vec(value: impl AsRef<[u8]>) -> Result<Vec<u8>, bs58::decode::Error> {
    bs58::decode(value).into_vec()
}

/// Error type alias for parsing byte string encodings
pub type ParseBytesError = bs58::decode::Error;
