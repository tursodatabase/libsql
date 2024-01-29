use std::mem::replace;
use std::sync::Arc;

use ::bytes::{Buf, BufMut, Bytes};
use prost::encoding::{
    bytes, double, message, sint64, skip_field, string, uint32, DecodeContext, WireType,
};
use prost::DecodeError;

use super::proto::{
    BatchCond, BatchCondList, BatchResult, CursorEntry, StreamRequest, StreamResponse,
    StreamResult, Value,
};

impl prost::Message for StreamResult {
    fn encode_raw<B>(&self, buf: &mut B)
    where
        B: BufMut,
        Self: Sized,
    {
        match self {
            StreamResult::None => {}
            StreamResult::Ok { response } => message::encode(1, response, buf),
            StreamResult::Error { error } => message::encode(2, error, buf),
        }
    }

    fn encoded_len(&self) -> usize {
        match self {
            StreamResult::None => 0,
            StreamResult::Ok { response } => message::encoded_len(1, response),
            StreamResult::Error { error } => message::encoded_len(2, error),
        }
    }

    fn merge_field<B>(
        &mut self,
        _tag: u32,
        _wire_type: WireType,
        _buf: &mut B,
        _ctx: DecodeContext,
    ) -> Result<(), DecodeError>
    where
        B: Buf,
        Self: Sized,
    {
        panic!("StreamResult can only be encoded, not decoded")
    }

    fn clear(&mut self) {
        panic!("StreamResult can only be encoded, not decoded")
    }
}

impl prost::Message for StreamRequest {
    fn encode_raw<B>(&self, _buf: &mut B)
    where
        B: BufMut,
        Self: Sized,
    {
        panic!("StreamRequest can only be decoded, not encoded")
    }

    fn encoded_len(&self) -> usize {
        panic!("StreamRequest can only be decoded, not encoded")
    }

    fn merge_field<B>(
        &mut self,
        tag: u32,
        wire_type: WireType,
        buf: &mut B,
        ctx: DecodeContext,
    ) -> Result<(), DecodeError>
    where
        B: Buf,
        Self: Sized,
    {
        macro_rules! merge {
            ($variant:ident) => {{
                let mut msg = match replace(self, StreamRequest::None) {
                    StreamRequest::$variant(msg) => msg,
                    _ => Default::default(),
                };
                message::merge(wire_type, &mut msg, buf, ctx)?;
                *self = StreamRequest::$variant(msg);
            }};
        }

        match tag {
            1 => merge!(Close),
            2 => merge!(Execute),
            3 => merge!(Batch),
            4 => merge!(Sequence),
            5 => merge!(Describe),
            6 => merge!(StoreSql),
            7 => merge!(CloseSql),
            8 => merge!(GetAutocommit),
            _ => skip_field(wire_type, tag, buf, ctx)?,
        }
        Ok(())
    }

    fn clear(&mut self) {
        *self = StreamRequest::None;
    }
}

impl prost::Message for StreamResponse {
    fn encode_raw<B>(&self, buf: &mut B)
    where
        B: BufMut,
        Self: Sized,
    {
        match self {
            StreamResponse::Close(msg) => message::encode(1, msg, buf),
            StreamResponse::Execute(msg) => message::encode(2, msg, buf),
            StreamResponse::Batch(msg) => message::encode(3, msg, buf),
            StreamResponse::Sequence(msg) => message::encode(4, msg, buf),
            StreamResponse::Describe(msg) => message::encode(5, msg, buf),
            StreamResponse::StoreSql(msg) => message::encode(6, msg, buf),
            StreamResponse::CloseSql(msg) => message::encode(7, msg, buf),
            StreamResponse::GetAutocommit(msg) => message::encode(8, msg, buf),
        }
    }

    fn encoded_len(&self) -> usize {
        match self {
            StreamResponse::Close(msg) => message::encoded_len(1, msg),
            StreamResponse::Execute(msg) => message::encoded_len(2, msg),
            StreamResponse::Batch(msg) => message::encoded_len(3, msg),
            StreamResponse::Sequence(msg) => message::encoded_len(4, msg),
            StreamResponse::Describe(msg) => message::encoded_len(5, msg),
            StreamResponse::StoreSql(msg) => message::encoded_len(6, msg),
            StreamResponse::CloseSql(msg) => message::encoded_len(7, msg),
            StreamResponse::GetAutocommit(msg) => message::encoded_len(8, msg),
        }
    }

    fn merge_field<B>(
        &mut self,
        _tag: u32,
        _wire_type: WireType,
        _buf: &mut B,
        _ctx: DecodeContext,
    ) -> Result<(), DecodeError>
    where
        B: Buf,
        Self: Sized,
    {
        panic!("StreamResponse can only be encoded, not decoded")
    }

    fn clear(&mut self) {
        panic!("StreamResponse can only be encoded, not decoded")
    }
}

impl prost::Message for BatchResult {
    fn encode_raw<B>(&self, buf: &mut B)
    where
        B: BufMut,
        Self: Sized,
    {
        vec_as_map::encode(1, &self.step_results, buf);
        vec_as_map::encode(2, &self.step_errors, buf);
    }

    fn encoded_len(&self) -> usize {
        vec_as_map::encoded_len(1, &self.step_results)
            + vec_as_map::encoded_len(2, &self.step_errors)
    }

    fn merge_field<B>(
        &mut self,
        _tag: u32,
        _wire_type: WireType,
        _buf: &mut B,
        _ctx: DecodeContext,
    ) -> Result<(), DecodeError>
    where
        B: Buf,
        Self: Sized,
    {
        panic!("BatchResult can only be encoded, not decoded")
    }

    fn clear(&mut self) {
        self.step_results.clear();
        self.step_errors.clear();
    }
}

impl prost::Message for BatchCond {
    fn encode_raw<B>(&self, _buf: &mut B)
    where
        B: BufMut,
        Self: Sized,
    {
        panic!("BatchCond can only be decoded, not encoded")
    }

    fn encoded_len(&self) -> usize {
        panic!("BatchCond can only be decoded, not encoded")
    }

    fn merge_field<B>(
        &mut self,
        tag: u32,
        wire_type: WireType,
        buf: &mut B,
        ctx: DecodeContext,
    ) -> Result<(), DecodeError>
    where
        B: Buf,
        Self: Sized,
    {
        match tag {
            1 => {
                let mut step = 0;
                uint32::merge(wire_type, &mut step, buf, ctx)?;
                *self = BatchCond::Ok { step }
            }
            2 => {
                let mut step = 0;
                uint32::merge(wire_type, &mut step, buf, ctx)?;
                *self = BatchCond::Error { step }
            }
            3 => {
                let mut cond = match replace(self, BatchCond::None) {
                    BatchCond::Not { cond } => cond,
                    _ => Box::new(BatchCond::None),
                };
                message::merge(wire_type, &mut *cond, buf, ctx)?;
                *self = BatchCond::Not { cond };
            }
            4 => {
                let mut cond_list = match replace(self, BatchCond::None) {
                    BatchCond::And(cond_list) => cond_list,
                    _ => BatchCondList::default(),
                };
                message::merge(wire_type, &mut cond_list, buf, ctx)?;
                *self = BatchCond::And(cond_list);
            }
            5 => {
                let mut cond_list = match replace(self, BatchCond::None) {
                    BatchCond::Or(cond_list) => cond_list,
                    _ => BatchCondList::default(),
                };
                message::merge(wire_type, &mut cond_list, buf, ctx)?;
                *self = BatchCond::Or(cond_list);
            }
            6 => {
                skip_field(wire_type, tag, buf, ctx)?;
                *self = BatchCond::IsAutocommit {};
            }
            _ => {
                skip_field(wire_type, tag, buf, ctx)?;
            }
        }
        Ok(())
    }

    fn clear(&mut self) {
        *self = BatchCond::None;
    }
}

impl prost::Message for CursorEntry {
    fn encode_raw<B>(&self, buf: &mut B)
    where
        B: BufMut,
        Self: Sized,
    {
        match self {
            CursorEntry::None => {}
            CursorEntry::StepBegin(entry) => message::encode(1, entry, buf),
            CursorEntry::StepEnd(entry) => message::encode(2, entry, buf),
            CursorEntry::StepError(entry) => message::encode(3, entry, buf),
            CursorEntry::Row { row } => message::encode(4, row, buf),
            CursorEntry::Error { error } => message::encode(5, error, buf),
            CursorEntry::ReplicationIndex { replication_index } => {
                if let Some(replication_index) = replication_index {
                    message::encode(6, replication_index, buf)
                }
            }
        }
    }

    fn encoded_len(&self) -> usize {
        match self {
            CursorEntry::None => 0,
            CursorEntry::StepBegin(entry) => message::encoded_len(1, entry),
            CursorEntry::StepEnd(entry) => message::encoded_len(2, entry),
            CursorEntry::StepError(entry) => message::encoded_len(3, entry),
            CursorEntry::Row { row } => message::encoded_len(4, row),
            CursorEntry::Error { error } => message::encoded_len(5, error),
            CursorEntry::ReplicationIndex { replication_index } => {
                if let Some(replication_index) = replication_index {
                    message::encoded_len(6, replication_index)
                } else {
                    0
                }
            }
        }
    }

    fn merge_field<B>(
        &mut self,
        _tag: u32,
        _wire_type: WireType,
        _buf: &mut B,
        _ctx: DecodeContext,
    ) -> Result<(), DecodeError>
    where
        B: Buf,
        Self: Sized,
    {
        panic!("CursorEntry can only be encoded, not decoded")
    }

    fn clear(&mut self) {
        *self = CursorEntry::None;
    }
}

impl prost::Message for Value {
    fn encode_raw<B>(&self, buf: &mut B)
    where
        B: BufMut,
        Self: Sized,
    {
        match self {
            Value::None => {}
            Value::Null => empty_message::encode(1, buf),
            Value::Integer { value } => sint64::encode(2, value, buf),
            Value::Float { value } => double::encode(3, value, buf),
            Value::Text { value } => arc_str::encode(4, value, buf),
            Value::Blob { value } => bytes::encode(5, value, buf),
        }
    }

    fn encoded_len(&self) -> usize {
        match self {
            Value::None => 0,
            Value::Null => empty_message::encoded_len(1),
            Value::Integer { value } => sint64::encoded_len(2, value),
            Value::Float { value } => double::encoded_len(3, value),
            Value::Text { value } => arc_str::encoded_len(4, value),
            Value::Blob { value } => bytes::encoded_len(5, value),
        }
    }

    fn merge_field<B>(
        &mut self,
        tag: u32,
        wire_type: WireType,
        buf: &mut B,
        ctx: DecodeContext,
    ) -> Result<(), DecodeError>
    where
        B: Buf,
        Self: Sized,
    {
        match tag {
            1 => {
                skip_field(wire_type, tag, buf, ctx)?;
                *self = Value::Null
            }
            2 => {
                let mut value = 0;
                sint64::merge(wire_type, &mut value, buf, ctx)?;
                *self = Value::Integer { value };
            }
            3 => {
                let mut value = 0.;
                double::merge(wire_type, &mut value, buf, ctx)?;
                *self = Value::Float { value };
            }
            4 => {
                let mut value = String::new();
                string::merge(wire_type, &mut value, buf, ctx)?;
                // TODO: this makes an unnecessary copy
                let value: Arc<str> = value.into();
                *self = Value::Text { value };
            }
            5 => {
                let mut value = Bytes::new();
                bytes::merge(wire_type, &mut value, buf, ctx)?;
                *self = Value::Blob { value };
            }
            _ => {
                skip_field(wire_type, tag, buf, ctx)?;
            }
        }
        Ok(())
    }

    fn clear(&mut self) {
        *self = Value::None;
    }
}

mod vec_as_map {
    use bytes::BufMut;
    use prost::encoding::{
        encode_key, encode_varint, encoded_len_varint, key_len, message, uint32, WireType,
    };

    pub fn encode<B, M>(tag: u32, values: &[Option<M>], buf: &mut B)
    where
        B: BufMut,
        M: prost::Message,
    {
        for (index, msg) in values.iter().enumerate() {
            if let Some(msg) = msg {
                encode_map_entry(tag, index as u32, msg, buf);
            }
        }
    }

    pub fn encoded_len<M>(tag: u32, values: &[Option<M>]) -> usize
    where
        M: prost::Message,
    {
        values
            .iter()
            .enumerate()
            .map(|(index, msg)| match msg {
                Some(msg) => encoded_map_entry_len(tag, index as u32, msg),
                None => 0,
            })
            .sum()
    }

    fn encode_map_entry<B, M>(tag: u32, key: u32, value: &M, buf: &mut B)
    where
        B: BufMut,
        M: prost::Message,
    {
        encode_key(tag, WireType::LengthDelimited, buf);

        let entry_key_len = uint32::encoded_len(1, &key);
        let entry_value_len = message::encoded_len(2, value);

        encode_varint((entry_key_len + entry_value_len) as u64, buf);
        uint32::encode(1, &key, buf);
        message::encode(2, value, buf);
    }

    fn encoded_map_entry_len<M>(tag: u32, key: u32, value: &M) -> usize
    where
        M: prost::Message,
    {
        let entry_key_len = uint32::encoded_len(1, &key);
        let entry_value_len = message::encoded_len(2, value);
        let entry_len = entry_key_len + entry_value_len;
        key_len(tag) + encoded_len_varint(entry_len as u64) + entry_len
    }
}

mod empty_message {
    use bytes::BufMut;
    use prost::encoding::{encode_key, encode_varint, encoded_len_varint, key_len, WireType};

    pub fn encode<B>(tag: u32, buf: &mut B)
    where
        B: BufMut,
    {
        encode_key(tag, WireType::LengthDelimited, buf);
        encode_varint(0, buf);
    }

    pub fn encoded_len(tag: u32) -> usize {
        key_len(tag) + encoded_len_varint(0)
    }
}

mod arc_str {
    use bytes::BufMut;
    use prost::encoding::{encode_key, encode_varint, encoded_len_varint, key_len, WireType};
    use std::sync::Arc;

    pub fn encode<B>(tag: u32, value: &Arc<str>, buf: &mut B)
    where
        B: BufMut,
    {
        encode_key(tag, WireType::LengthDelimited, buf);
        encode_varint(value.len() as u64, buf);
        buf.put_slice(value.as_bytes());
    }

    pub fn encoded_len(tag: u32, value: &Arc<str>) -> usize {
        key_len(tag) + encoded_len_varint(value.len() as u64) + value.len()
    }
}
