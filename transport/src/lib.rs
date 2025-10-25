pub(crate) type BoxError = Box<dyn std::error::Error + Send + Sync>;
pub(crate) type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

#[cfg(not(target_arch = "wasm32"))]
pub use native::WsConnectionInfo;

use futures_util::{ready, sink::Sink};
use pin_project::pin_project;
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tungstenite::Message;

use std::fmt::Debug;
use std::future::Future;
use std::io;
use std::pin::Pin;
#[cfg(not(target_arch = "wasm32"))]
use std::sync::Arc;
use std::task::{Context, Poll};

#[cfg(not(target_arch = "wasm32"))]
mod native;
pub mod transport;
#[cfg(target_arch = "wasm32")]
mod web;

#[cfg(target_arch = "wasm32")]
pub type Endpoint = transport::channel::Endpoint;
#[cfg(not(target_arch = "wasm32"))]
pub type Endpoint = tonic::transport::channel::Endpoint;

#[cfg(target_arch = "wasm32")]
pub type Channel = transport::Channel;
#[cfg(not(target_arch = "wasm32"))]
pub type Channel = tonic::transport::Channel;

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum Error {
    #[error(transparent)]
    Tungstenite(#[from] tungstenite::Error),
    #[cfg(not(target_arch = "wasm32"))]
    #[error(transparent)]
    InvalidBearerToken(#[from] headers::authorization::InvalidBearerToken),
    #[error("Js error: {0}")]
    Js(String),
}

impl From<Error> for io::Error {
    fn from(e: Error) -> io::Error {
        io::Error::other(e)
    }
}

#[cfg(target_arch = "wasm32")]
impl From<wasm_bindgen::JsValue> for Error {
    fn from(value: wasm_bindgen::JsValue) -> Self {
        let s = js_sys::JSON::stringify(&value)
            .map(String::from)
            .unwrap_or_else(|_| "unknown".to_string());
        Error::Js(s)
    }
}

#[derive(Clone, Default)]
pub struct WsConnector {
    #[cfg(not(target_arch = "wasm32"))]
    resolve_bearer_token: Option<Arc<dyn Fn() -> String + Sync + Send + 'static>>,
}

impl Debug for WsConnector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WsConnector").finish_non_exhaustive()
    }
}

impl WsConnector {
    pub fn new() -> Self {
        Default::default()
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub fn with_bearer_resolver(
        resolve_token: impl Fn() -> String + Send + Sync + 'static,
    ) -> Self {
        Self {
            resolve_bearer_token: Some(Arc::new(resolve_token)),
        }
    }

    async fn connect(&mut self, dst: http::Uri) -> Result<WsConnection, Error> {
        cfg_if::cfg_if! {
            if #[cfg(not(target_arch = "wasm32"))] {
                self.connect_native_impl(dst).await
            } else if #[cfg(target_arch = "wasm32")] {
                web::connect(dst).await
            }
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub async fn connect_native_impl(&mut self, dst: http::Uri) -> Result<WsConnection, Error> {
        use headers::{Authorization, HeaderMapExt};
        use tungstenite::client::IntoClientRequest;

        let mut request = dst.into_client_request()?;
        if let Some(resolver) = self.resolve_bearer_token.as_ref() {
            let token = resolver();
            request
                .headers_mut()
                .typed_insert(Authorization::bearer(&token)?);
        }

        let (ws_stream, _) = tokio_tungstenite::connect_async(request).await?;
        Ok(WsConnection::from_combined_channel(ws_stream))
    }
}

impl tower::Service<http::Uri> for WsConnector {
    type Response = WsConnection;
    type Error = Error;
    type Future = WsConnecting;

    fn poll_ready(&mut self, _cx: &mut Context) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, dst: http::Uri) -> Self::Future {
        let mut self_ = self.clone();
        let fut = Box::pin(async move { self_.connect(dst).await });
        WsConnecting { fut }
    }
}

#[must_use = "futures do nothing unless polled"]
#[pin_project]
pub struct WsConnecting {
    #[pin]
    fut: BoxConnecting,
}

type ConnectResult = Result<WsConnection, Error>;

type BoxConnecting = Pin<Box<dyn Future<Output = ConnectResult> + Send>>;

impl Future for WsConnecting {
    type Output = ConnectResult;

    fn poll(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        self.project().fut.poll(cx)
    }
}

#[pin_project]
pub struct WsConnection {
    #[pin]
    pub(crate) sink: WsConnectionSink,
    #[pin]
    pub(crate) reader: hyper_util::rt::TokioIo<WsConnectionReader>,
}

type WsConnectionSink = Box<dyn Sink<Message, Error = Error> + Unpin + Send>;
type WsConnectionReader = Box<dyn AsyncRead + Unpin + Send>;

impl hyper::rt::Read for WsConnection {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: hyper::rt::ReadBufCursor<'_>,
    ) -> Poll<Result<(), io::Error>> {
        self.project().reader.poll_read(cx, buf)
    }
}

impl hyper::rt::Write for WsConnection {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, io::Error>> {
        let mut self_ = self.project();
        ready!(self_.sink.as_mut().poll_ready(cx)?);
        self_.sink.start_send(Message::Binary(buf.to_vec()))?;
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        self.project().sink.poll_flush(cx).map_err(io::Error::other)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        self.project().sink.poll_close(cx).map_err(io::Error::other)
    }
}

impl AsyncRead for WsConnection {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf,
    ) -> Poll<Result<(), io::Error>> {
        let mut pinned = std::pin::pin!(self.reader.inner_mut());
        pinned.as_mut().poll_read(cx, buf)
    }
}

impl AsyncWrite for WsConnection {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, io::Error>> {
        let mut self_ = self.project();
        ready!(self_.sink.as_mut().poll_ready(cx)?);
        self_.sink.start_send(Message::Binary(buf.to_vec()))?;
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        self.project().sink.poll_flush(cx).map_err(io::Error::other)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        self.project().sink.poll_close(cx).map_err(io::Error::other)
    }
}
