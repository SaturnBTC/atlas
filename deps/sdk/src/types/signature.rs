use bitcode::{Decode, Encode};
use borsh::{BorshDeserialize, BorshSerialize};
#[cfg(feature = "fuzzing")]
use libfuzzer_sys::arbitrary;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, BorshSerialize, BorshDeserialize, Hash, Encode, Decode)]
#[cfg_attr(feature = "fuzzing", derive(arbitrary::Arbitrary))]
pub struct Signature(pub [u8; 64]);

impl Signature {
    pub fn to_array(&self) -> [u8; 64] {
        self.0
    }
}

impl From<[u8; 64]> for Signature {
    fn from(data: [u8; 64]) -> Self {
        Self(data)
    }
}

impl From<Signature> for [u8; 64] {
    fn from(signature: Signature) -> Self {
        signature.0
    }
}

impl Serialize for Signature {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_bytes(&self.0)
    }
}

impl<'de> Deserialize<'de> for Signature {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::{self, Visitor};

        struct SignatureVisitor;

        impl<'de> Visitor<'de> for SignatureVisitor {
            type Value = Signature;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("a 64-byte array")
            }

            fn visit_bytes<E>(self, v: &[u8]) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                if v.len() != 64 {
                    return Err(E::custom(format!("expected 64 bytes, got {}", v.len())));
                }
                let mut bytes = [0u8; 64];
                bytes.copy_from_slice(v);
                Ok(Signature(bytes))
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: de::SeqAccess<'de>,
            {
                let mut bytes = [0u8; 64];
                for (i, byte) in bytes.iter_mut().enumerate() {
                    *byte = seq
                        .next_element()?
                        .ok_or_else(|| de::Error::invalid_length(i, &self))?;
                }
                Ok(Signature(bytes))
            }
        }

        deserializer.deserialize_bytes(SignatureVisitor)
    }
}

use proptest::prelude::*;

proptest! {
    #[test]
    fn fuzz_serialize_deserialize_signature(bytes in prop::collection::vec(any::<u8>(), 64..=64)) {
        let mut signature_bytes = [0u8; 64];
        signature_bytes.copy_from_slice(&bytes);
        let signature = Signature::from(signature_bytes);

        // Test internal serialize method
        let serialized = signature.to_array();
        let deserialized = Signature::from(serialized);
        assert_eq!(signature, deserialized);

        // Test serde JSON serialization/deserialization
        let json = serde_json::to_string(&signature).unwrap();
        let from_json: Signature = serde_json::from_str(&json).unwrap();
        assert_eq!(signature, from_json);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_serde_serialize_deserialize() {
        // Test with a known signature
        let bytes = [42u8; 64];
        let signature = Signature::from(bytes);

        // Test JSON serialization/deserialization
        let json = serde_json::to_string(&signature).unwrap();
        let from_json: Signature = serde_json::from_str(&json).unwrap();
        assert_eq!(signature, from_json);

        // Test that the JSON is an array (serialize_bytes outputs as array in JSON)
        let json_value: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert!(json_value.is_array());

        // Test with all zeros
        let zeros = Signature::from([0u8; 64]);
        let json_zeros = serde_json::to_string(&zeros).unwrap();
        let from_json_zeros: Signature = serde_json::from_str(&json_zeros).unwrap();
        assert_eq!(zeros, from_json_zeros);

        // Test with all 255s
        let max = Signature::from([255u8; 64]);
        let json_max = serde_json::to_string(&max).unwrap();
        let from_json_max: Signature = serde_json::from_str(&json_max).unwrap();
        assert_eq!(max, from_json_max);
    }
}
