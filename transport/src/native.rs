use super::WsConnection;

use bytes::Bytes;
use futures_util::{future, sink::Sink, stream::Stream, SinkExt, StreamExt};
use tonic::transport::server::Connected;
use tungstenite::{Error as TungsteniteError, Message};

use hyper_util::client::legacy::connect::Connection;
use hyper_util::rt::TokioIo;
use std::io;

impl<T> WsConnection<T> {
    pub fn from_combined_channel<S>(ws_stream: S, info: T) -> Self
    where
        S: Sink<Message, Error = TungsteniteError>
            + Stream<Item = Result<Message, TungsteniteError>>
            + Send
            + Unpin
            + 'static,
    {
        let (sink, stream) = ws_stream.split();
        Self::from_split_channels(sink, stream, info)
    }

    pub fn from_split_channels<S, St>(
        sink: S,
        stream: St,
        info: T,
    ) -> Self
    where
        S: Sink<Message, Error = TungsteniteError>
            + Send
            + Unpin
            + 'static,
        St: Stream<Item = Result<Message, TungsteniteError>>
            + Send
            + Unpin
            + 'static,
    {
        let sink = sink.sink_err_into();

        let bytes_stream = stream.filter_map(|msg| {
            future::ready(match msg {
                Ok(Message::Binary(data)) => Some(Ok(Bytes::from(data))),
                Ok(Message::Text(data)) => Some(Ok(Bytes::from(data))),
                Ok(Message::Ping(_)) | Ok(Message::Pong(_)) => None,
                Ok(Message::Close(_)) => Some(Err(io::Error::new(
                    io::ErrorKind::ConnectionAborted,
                    TungsteniteError::ConnectionClosed,
                ))),
                Ok(Message::Frame(_)) => None,
                Err(e) => Some(Err(io::Error::other(e))),
            })
        });
        let reader = Box::new(tokio_util::io::StreamReader::new(bytes_stream));
        Self {
            sink: Box::new(sink),
            reader: TokioIo::new(reader),
            info,
        }
    }
}

impl<T: Clone + Send + Sync + 'static> Connected for WsConnection<T> {
    type ConnectInfo = T;

    fn connect_info(&self) -> Self::ConnectInfo {
        self.info.clone()
    }
}

impl<T> Connection for WsConnection<T> {
    fn connected(&self) -> hyper_util::client::legacy::connect::Connected {
        hyper_util::client::legacy::connect::Connected::new()
    }
}
