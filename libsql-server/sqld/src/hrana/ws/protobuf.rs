use super::proto::{ClientMsg, HelloMsg, RequestMsg, ServerMsg};
use ::bytes::{Buf, BufMut};
use prost::encoding::{message, skip_field, DecodeContext, WireType};
use prost::DecodeError;
use std::mem::replace;

impl prost::Message for ClientMsg {
    fn encode_raw<B>(&self, _buf: &mut B)
    where
        B: BufMut,
        Self: Sized,
    {
        panic!("ClientMsg can only be decoded, not encoded")
    }

    fn encoded_len(&self) -> usize {
        panic!("ClientMsg can only be decoded, not encoded")
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
                let mut msg = match replace(self, ClientMsg::None) {
                    ClientMsg::Hello(msg) => msg,
                    _ => HelloMsg::default(),
                };
                message::merge(wire_type, &mut msg, buf, ctx)?;
                *self = ClientMsg::Hello(msg);
            }
            2 => {
                let mut msg = match replace(self, ClientMsg::None) {
                    ClientMsg::Request(msg) => msg,
                    _ => RequestMsg::default(),
                };
                message::merge(wire_type, &mut msg, buf, ctx)?;
                *self = ClientMsg::Request(msg);
            }
            _ => {
                skip_field(wire_type, tag, buf, ctx)?;
            }
        }
        Ok(())
    }

    fn clear(&mut self) {
        *self = ClientMsg::None;
    }
}

impl prost::Message for ServerMsg {
    fn encode_raw<B>(&self, buf: &mut B)
    where
        B: BufMut,
        Self: Sized,
    {
        match self {
            ServerMsg::HelloOk(msg) => message::encode(1, msg, buf),
            ServerMsg::HelloError(msg) => message::encode(2, msg, buf),
            ServerMsg::ResponseOk(msg) => message::encode(3, msg, buf),
            ServerMsg::ResponseError(msg) => message::encode(4, msg, buf),
        }
    }

    fn encoded_len(&self) -> usize {
        match self {
            ServerMsg::HelloOk(msg) => message::encoded_len(1, msg),
            ServerMsg::HelloError(msg) => message::encoded_len(2, msg),
            ServerMsg::ResponseOk(msg) => message::encoded_len(3, msg),
            ServerMsg::ResponseError(msg) => message::encoded_len(4, msg),
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
        panic!("ServerMsg can only be encoded, not decoded")
    }

    fn clear(&mut self) {
        panic!("ServerMsg can only be encoded, not decoded")
    }
}
