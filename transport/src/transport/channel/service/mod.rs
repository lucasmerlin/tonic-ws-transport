mod add_origin;
use self::add_origin::AddOrigin;

mod user_agent;
use self::user_agent::UserAgent;

mod reconnect;
use self::reconnect::Reconnect;

mod connection;
pub use self::connection::Connection;


mod io;
use self::io::BoxedIo;

mod connector;
pub use self::connector::Connector;

mod executor;
pub use self::executor::{Executor, SharedExec};

#[cfg(feature = "_tls-any")]
mod tls;
#[cfg(feature = "_tls-any")]
pub use self::tls::TlsConnector;
