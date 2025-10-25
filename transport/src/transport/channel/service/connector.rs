use super::BoxedIo;
use crate::transport::channel::BoxFuture;
use http::Uri;
use std::task::{Context, Poll};

use hyper::rt;

use tonic::ConnectError;
use tower_service::Service;

pub struct Connector<C> {
    inner: C,
}

impl<C> Connector<C> {
    pub fn new(inner: C) -> Self {
        Self { inner }
    }
}

impl<C> Service<Uri> for Connector<C>
where
    C: Service<Uri>,
    C::Response: rt::Read + rt::Write + Unpin + Send + 'static,
    C::Future: Send + 'static,
    crate::BoxError: From<C::Error> + Send + 'static,
{
    type Response = BoxedIo;
    type Error = ConnectError;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner
            .poll_ready(cx)
            .map_err(|err| ConnectError(From::from(err)))
    }

    fn call(&mut self, uri: Uri) -> Self::Future {
        let connect = self.inner.call(uri);

        Box::pin(async move {
            async {
                let io = connect.await?;

                Ok::<_, crate::BoxError>(BoxedIo::new(io))
            }
            .await
            .map_err(ConnectError)
        })
    }
}
