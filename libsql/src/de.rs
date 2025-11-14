//! Deserialization utilities.

use crate::{Row, Value};
use serde::de::{value::Error as DeError, Error, IntoDeserializer, MapAccess, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer};

struct RowDeserializer<'de> {
    row: &'de Row,
}

impl<'de> Deserializer<'de> for RowDeserializer<'de> {
    type Error = DeError;

    fn deserialize_any<V>(self, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(DeError::custom(
            "Expects a map, newtype, sequence, struct, or tuple",
        ))
    }

    fn deserialize_map<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        struct RowMapAccess<'a> {
            row: &'a Row,
            idx: std::ops::Range<usize>,
            value: Option<Value>,
        }

        impl<'de> MapAccess<'de> for RowMapAccess<'de> {
            type Error = DeError;

            fn next_key_seed<K>(&mut self, seed: K) -> Result<Option<K::Value>, Self::Error>
            where
                K: serde::de::DeserializeSeed<'de>,
            {
                match self.idx.next() {
                    None => Ok(None),
                    Some(i) => {
                        let value = self.row.get_value(i as i32).map_err(DeError::custom)?;
                        self.value = Some(value);
                        self.row
                            .column_name(i as i32)
                            .map(|name| seed.deserialize(name.into_deserializer()))
                            .transpose()
                    }
                }
            }

            fn next_value_seed<V>(&mut self, seed: V) -> Result<V::Value, Self::Error>
            where
                V: serde::de::DeserializeSeed<'de>,
            {
                let value = self
                    .value
                    .take()
                    .ok_or(DeError::custom("Expects a value but row is exhausted"))?;

                seed.deserialize(value.into_deserializer())
            }
        }

        visitor.visit_map(RowMapAccess {
            row: self.row,
            idx: 0..(self.row.inner.column_count() as usize),
            value: None,
        })
    }

    fn deserialize_struct<V>(
        self,
        _name: &'static str,
        _fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.deserialize_map(visitor)
    }

    fn deserialize_seq<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        struct RowSeqAccess<'a> {
            row: &'a Row,
            idx: std::ops::Range<usize>,
        }

        impl<'de> SeqAccess<'de> for RowSeqAccess<'de> {
            type Error = DeError;

            fn next_element_seed<T>(&mut self, seed: T) -> Result<Option<T::Value>, Self::Error>
            where
                T: serde::de::DeserializeSeed<'de>,
            {
                match self.idx.next() {
                    None => Ok(None),
                    Some(i) => {
                        let value = self.row.get_value(i as i32).map_err(DeError::custom)?;
                        seed.deserialize(value.into_deserializer()).map(Some)
                    }
                }
            }
        }

        visitor.visit_seq(RowSeqAccess {
            row: self.row,
            idx: 0..(self.row.column_count() as usize),
        })
    }

    fn deserialize_tuple<V>(self, _len: usize, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.deserialize_seq(visitor)
    }

    fn deserialize_tuple_struct<V>(
        self,
        _name: &'static str,
        _len: usize,
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.deserialize_seq(visitor)
    }

    fn deserialize_newtype_struct<V>(
        self,
        _name: &'static str,
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_newtype_struct(self)
    }

    serde::forward_to_deserialize_any! {
        bool i8 i16 i32 i64 i128 u8 u16 u32 u64 u128 f32 f64 char str string
        bytes byte_buf option unit unit_struct enum identifier ignored_any
    }
}

pub fn from_row<'de, T: Deserialize<'de>>(row: &'de Row) -> Result<T, DeError> {
    let de = RowDeserializer { row };
    T::deserialize(de)
}
