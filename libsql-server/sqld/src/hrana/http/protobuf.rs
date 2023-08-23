use super::proto::{StreamRequest, StreamResponse, StreamResult};
use ::bytes::{Buf, BufMut};
use prost::encoding::{message, skip_field, DecodeContext, WireType};
use prost::DecodeError;
use std::mem::replace;

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
