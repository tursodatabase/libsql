pub mod frame;
mod injector;
pub mod meta;
pub mod replicator;
pub mod rpc;
pub mod snapshot;

mod error;

pub const LIBSQL_PAGE_SIZE: usize = 4096;

#[cfg(test)]
pub mod test {
    use arbitrary::Unstructured;
    use bytes::Bytes;

    /// generate an arbitrary rpc value. see build.rs for usage.
    pub fn arbitrary_rpc_value(_u: &mut Unstructured) -> arbitrary::Result<Vec<u8>> {
        todo!();
        // let data = bincode::serialize(&crate::query::Value::arbitrary(u)?).unwrap();
        //
        // Ok(data)
    }

    /// generate an arbitrary `Bytes` value. see build.rs for usage.
    pub fn arbitrary_bytes(_u: &mut Unstructured) -> arbitrary::Result<Bytes> {
        todo!()
        // let v: Vec<u8> = Arbitrary::arbitrary(u)?;
        //
        // Ok(v.into())
    }
}
