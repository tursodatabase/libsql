#![allow(dead_code)]

use turmoil::Builder;

pub mod http;
pub mod net;

pub fn sim_builder() -> Builder {
    Builder::new()
}
