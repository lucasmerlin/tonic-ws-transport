use http::{header::USER_AGENT, HeaderValue, Request};
use std::task::{Context, Poll};
use tower_service::Service;

const TONIC_USER_AGENT: &str = concat!("tonic/", env!("CARGO_PKG_VERSION"));

#[derive(Debug)]
pub struct UserAgent<T> {
    inner: T,
    user_agent: HeaderValue,
}

impl<T> UserAgent<T> {
    pub fn new(inner: T, user_agent: Option<HeaderValue>) -> Self {
        let user_agent = user_agent
            .map(|value| {
                let mut buf = Vec::new();
                buf.extend(value.as_bytes());
                buf.push(b' ');
                buf.extend(TONIC_USER_AGENT.as_bytes());
                HeaderValue::from_bytes(&buf).expect("user-agent should be valid")
            })
            .unwrap_or_else(|| HeaderValue::from_static(TONIC_USER_AGENT));

        Self { inner, user_agent }
    }
}

impl<T, ReqBody> Service<Request<ReqBody>> for UserAgent<T>
where
    T: Service<Request<ReqBody>>,
{
    type Response = T::Response;
    type Error = T::Error;
    type Future = T::Future;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, mut req: Request<ReqBody>) -> Self::Future {
        if let Ok(Some(user_agent)) = req
            .headers_mut()
            .try_insert(USER_AGENT, self.user_agent.clone())
        {
            // The User-Agent header has already been set on the request. Let's
            // append our user agent to the end.
            let mut buf = Vec::new();
            buf.extend(user_agent.as_bytes());
            buf.push(b' ');
            buf.extend(self.user_agent.as_bytes());
            req.headers_mut().insert(
                USER_AGENT,
                HeaderValue::from_bytes(&buf).expect("user-agent should be valid"),
            );
        }

        self.inner.call(req)
    }
}
