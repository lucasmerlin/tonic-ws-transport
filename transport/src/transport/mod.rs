//! This is basically `tonic::transport` but stripped down to remove everything that's not needed
//! for wasm websocket connections.

pub mod channel;

mod error;
mod service;

#[doc(inline)]
pub use self::channel::{Channel, Endpoint};
pub use self::error::Error;

// pub use hyper::{body::Body, Uri};
