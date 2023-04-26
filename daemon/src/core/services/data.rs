use std::fmt;

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
        let s = base64::encode_config(&self.0, base64::URL_SAFE);

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
                formatter.write_str("a base64 encoded data page")
            }

            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                let s = base64::decode_config(value, base64::URL_SAFE)
                    .map_err(|_e| de::Error::custom("decoding b64"))?;

                Ok(Data(s))
            }
        }

        deserializer.deserialize_str(B64Visitor)
    }
}
