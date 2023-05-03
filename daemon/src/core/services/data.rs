use std::fmt;

use dsf_core::helpers::{parse_bytes, print_bytes, parse_bytes_vec};
use serde::{de, de::Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

#[derive(Clone, Debug, PartialEq)]
pub struct Data(pub Vec<u8>);

impl From<Vec<u8>> for Data {
    fn from(d: Vec<u8>) -> Self {
        Data(d)
    }
}

impl From<&[u8]> for Data {
    fn from(d: &[u8]) -> Self {
        Data(d.to_vec())
    }
}

impl Serialize for Data {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let s = print_bytes(&self.0);
        serializer.serialize_str(&s)
    }
}

impl<'de> Deserialize<'de> for Data {
    fn deserialize<D>(deserializer: D) -> Result<Data, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct B64Visitor;

        impl<'de> Visitor<'de> for B64Visitor {
            type Value = Data;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a string encoded data page")
            }

            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                let s = parse_bytes_vec(value)
                    .map_err(|_e| de::Error::custom("decoding bytes"))?;

                Ok(Data(s))
            }
        }

        deserializer.deserialize_str(B64Visitor)
    }
}
