//! Turning values into bytes and back, where an adapter stores or sends
//! bytes: the [`Codec`] port. serde is the contract everywhere else (the
//! adapters that persist ask for `Serialize + DeserializeOwned`); a codec is
//! only needed where the format is not one of serde's, such as protobuf,
//! whose binary encoding needs the number of each field, which serde's data
//! model does not carry.
//!
//! - [`Json`] (feature `json`): any `Serialize + DeserializeOwned` type.
//! - [`Protobuf`] (feature `protobuf`): any `prost::Message`.
//!
//! A format of one's own implements `Codec` for the types it encodes.

use std::fmt;

/// Encodes `T` to bytes and decodes it back.
pub trait Codec<T>: Send + Sync {
    /// The bytes of `value`.
    ///
    /// # Errors
    ///
    /// If `value` cannot be encoded.
    fn encode(&self, value: &T) -> Result<Vec<u8>, CodecError>;

    /// The value encoded in `bytes`.
    ///
    /// # Errors
    ///
    /// If `bytes` is not a value of `T` in this format.
    fn decode(&self, bytes: &[u8]) -> Result<T, CodecError>;
}

/// Why a value could not be encoded or decoded: the format's error, kept as
/// the source.
#[derive(Debug)]
pub struct CodecError(Box<dyn std::error::Error + Send + Sync>);

impl CodecError {
    /// An error caused by `error` (the format's, or a message).
    pub fn new(error: impl Into<Box<dyn std::error::Error + Send + Sync>>) -> Self {
        Self(error.into())
    }
}

impl fmt::Display for CodecError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "cannot encode or decode: {}", self.0)
    }
}

impl std::error::Error for CodecError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&*self.0)
    }
}

/// JSON, through serde: for every `Serialize + DeserializeOwned` type.
#[cfg(feature = "json")]
#[derive(Clone, Copy, Debug, Default)]
pub struct Json;

#[cfg(feature = "json")]
impl<T: serde::Serialize + serde::de::DeserializeOwned> Codec<T> for Json {
    fn encode(&self, value: &T) -> Result<Vec<u8>, CodecError> {
        serde_json::to_vec(value).map_err(CodecError::new)
    }

    fn decode(&self, bytes: &[u8]) -> Result<T, CodecError> {
        serde_json::from_slice(bytes).map_err(CodecError::new)
    }
}

/// Protobuf's binary encoding, through `prost`: for every generated message.
/// (Types generated with `pbjson-build` also get serde in protobuf's JSON
/// mapping, which is what a JSON store like PostgreSQL's keeps.)
#[cfg(feature = "protobuf")]
#[derive(Clone, Copy, Debug, Default)]
pub struct Protobuf;

#[cfg(feature = "protobuf")]
impl<T: prost::Message + Default> Codec<T> for Protobuf {
    fn encode(&self, value: &T) -> Result<Vec<u8>, CodecError> {
        Ok(value.encode_to_vec())
    }

    fn decode(&self, bytes: &[u8]) -> Result<T, CodecError> {
        T::decode(bytes).map_err(CodecError::new)
    }
}

#[cfg(all(test, feature = "json"))]
mod tests {
    use super::*;

    #[test]
    fn json_round_trips_and_says_why_it_cannot() {
        let bytes = Codec::<Vec<u32>>::encode(&Json, &vec![1, 2]).unwrap();
        assert_eq!(bytes, b"[1,2]");
        assert_eq!(Codec::<Vec<u32>>::decode(&Json, &bytes).unwrap(), [1, 2]);
        let error = Codec::<Vec<u32>>::decode(&Json, b"{").unwrap_err();
        assert!(std::error::Error::source(&error).is_some());
    }
}
