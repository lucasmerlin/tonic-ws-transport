use super::WsConnection;
use crate::Error;

use bytes::Bytes;
use futures_util::{
    future, ready,
    sink::{Sink, SinkExt},
    stream::{Stream, TryStreamExt},
};
use http::Uri;
use js_sys::Uint8Array;
use pin_project::pin_project;
use tokio::io::AsyncRead;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver};
use tungstenite::{Error as TungsteniteError, Message};
use wasm_bindgen::prelude::*;
use wasm_bindgen::JsCast;
use web_sys::{BinaryType, MessageEvent, WebSocket};

use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

pub async fn connect(dst: Uri) -> Result<WsConnection, Error> {
    let ws = Ws(Arc::new(WebSocket::new(&dst.to_string())?));
    (*ws).set_binary_type(BinaryType::Arraybuffer);
    let client = WebConnection { ws, wake_fn: None }.await?;

    let sink = WebClientSink {
        ws: client.ws.clone(),
        handlers: client.handlers.clone(),
    };
    let messages_sink = sink.with(|msg| match msg {
        Message::Binary(data) => future::ready(Ok(data)),
        _ => unreachable!(), // this sink supports only binary data
    });

    let bytes_stream = WebClientStream {
        ws: client.ws.clone(),
        handlers: client.handlers.clone(),
        rx: client.rx,
    };
    let bytes_stream = bytes_stream
        .map_ok(Bytes::from)
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e));

    Ok(WsConnection {
        sink: Box::new(messages_sink),
        reader: Box::new(tokio::io::stream_reader(bytes_stream)),
        addr: None,
    })
}

#[derive(Debug, Clone)]
struct Ws(Arc<WebSocket>);

unsafe impl Send for Ws {}

impl std::ops::Deref for Ws {
    type Target = WebSocket;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

pub struct WebConnection {
    ws: Ws,
    wake_fn: Option<WakeFn>, // keeps the callback alive and unsets it on drop
}

unsafe impl Send for WebConnection {}

impl Future for WebConnection {
    type Output = Result<WebClient, Error>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        match self.ws.ready_state() {
            WebSocket::OPEN => Poll::Ready(Ok(WebClient::new(self.ws.clone()))),
            WebSocket::CLOSING | WebSocket::CLOSED => {
                Poll::Ready(Err(TungsteniteError::ConnectionClosed.into()))
            }
            WebSocket::CONNECTING => {
                // poll can be called multiple times and only the last waker must be notified
                // => always replace the callbacks with the latest waker
                let wake_fn = WakeFn::register(
                    self.ws.clone(),
                    Some(Closure::once(Box::new({
                        let ws = self.ws.clone();
                        let waker = cx.waker().clone();
                        move || {
                            WakeFn::register(ws, None); // make sure the closure is called only once
                            waker.wake();
                        }
                    }) as Box<dyn FnOnce()>)),
                );
                self.as_mut().wake_fn = Some(wake_fn); // keep alive
                Poll::Pending
            }
            _ => unreachable!(),
        }
    }
}

struct WakeFn {
    ws: Ws,
    wake_fn: Option<Closure<dyn FnMut()>>,
}

unsafe impl Send for WakeFn {}

impl WakeFn {
    fn register(ws: Ws, wake_fn: Option<Closure<dyn FnMut()>>) -> Self {
        let handler = wake_fn.as_ref().map(|f| f.as_ref().unchecked_ref());
        ws.set_onopen(handler);
        ws.set_onerror(handler);
        Self { ws, wake_fn }
    }
}

impl Drop for WakeFn {
    fn drop(&mut self) {
        if self.wake_fn.is_some() {
            self.ws.set_onopen(None);
            self.ws.set_onerror(None);
        }
    }
}

#[derive(Debug)]
pub struct WebClient {
    ws: Ws,
    rx: UnboundedReceiver<Result<Vec<u8>, Error>>,
    handlers: Arc<Handlers>, // keeps the callbacks alive
}

impl WebClient {
    fn new(ws: Ws) -> Self {
        let (tx, rx) = unbounded_channel();

        let message_fn = Closure::wrap(Box::new(move |event: MessageEvent| {
            let array = Uint8Array::new(&event.data());
            let _ = tx.send(Ok(array.to_vec()));
        }) as Box<dyn FnMut(_)>);
        let close_fn = Closure::once(Box::new({
            let ws = ws.clone();
            move || {
                Handlers::register(ws, None, None); // make sure the closure is called only once
            }
        }) as Box<dyn FnOnce()>);

        let handlers = Arc::new(Handlers::register(
            ws.clone(),
            Some(message_fn),
            Some(close_fn),
        ));

        Self {
            ws,
            rx,
            handlers, // keep alive
        }
    }
}

#[derive(Debug)]
struct Handlers {
    ws: Ws,
    message_fn: Option<Closure<dyn FnMut(MessageEvent)>>,
    close_fn: Option<Closure<dyn FnMut()>>, // on close and error
}

impl Handlers {
    fn register(
        ws: Ws,
        message_fn: Option<Closure<dyn FnMut(MessageEvent)>>,
        close_fn: Option<Closure<dyn FnMut()>>,
    ) -> Self {
        ws.set_onmessage(message_fn.as_ref().map(|f| f.as_ref().unchecked_ref()));
        ws.set_onerror(close_fn.as_ref().map(|f| f.as_ref().unchecked_ref()));
        ws.set_onclose(close_fn.as_ref().map(|f| f.as_ref().unchecked_ref()));
        Self {
            ws,
            message_fn,
            close_fn,
        }
    }
}

impl Drop for Handlers {
    fn drop(&mut self) {
        if self.message_fn.is_some() {
            self.ws.set_onmessage(None);
        }
        if self.close_fn.is_some() {
            self.ws.set_onerror(None);
            self.ws.set_onclose(None);
        }
        if let Err(e) = self.ws.close() {
            panic!(Error::from(e));
        }
    }
}

#[derive(Debug)]
struct WebClientSink {
    ws: Ws,
    handlers: Arc<Handlers>, // keeps the callbacks alive
}

#[pin_project]
struct WebClientStream {
    ws: Ws,
    handlers: Arc<Handlers>, // keeps the callbacks alive
    #[pin]
    rx: UnboundedReceiver<Result<Vec<u8>, Error>>,
}

unsafe impl Send for WebClientSink {}
unsafe impl Send for WebClientStream {}

impl Sink<Vec<u8>> for WebClientSink {
    type Error = Error;

    fn poll_ready(self: Pin<&mut Self>, _cx: &mut Context) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(if self.ws.ready_state() == WebSocket::OPEN {
            Ok(())
        } else {
            Err(TungsteniteError::ConnectionClosed.into())
        })
    }

    fn start_send(self: Pin<&mut Self>, data: Vec<u8>) -> Result<(), Self::Error> {
        if self.ws.ready_state() == WebSocket::OPEN {
            Ok(self.ws.send_with_u8_array(&data[..])?)
        } else {
            Err(TungsteniteError::ConnectionClosed.into())
        }
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(if self.ws.ready_state() == WebSocket::OPEN {
            Ok(())
        } else {
            Err(TungsteniteError::ConnectionClosed.into())
        })
    }

    fn poll_close(self: Pin<&mut Self>, _cx: &mut Context) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
}

impl Stream for WebClientStream {
    type Item = Result<Vec<u8>, Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        self.project().rx.poll_next(cx)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.rx.size_hint()
    }
}
