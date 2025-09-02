mod async_rpc;
mod error;
mod rpc;
mod websocket;

pub use async_rpc::*;
pub use error::*;
pub use rpc::*;
pub use websocket::*;

pub const NOT_FOUND_CODE: i64 = 404;
