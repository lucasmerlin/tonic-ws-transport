use crate::transport::channel::BoxFuture;
use hyper_util::rt::TokioExecutor;
use std::{future::Future, sync::Arc};

pub use hyper::rt::Executor;

#[derive(Clone)]
pub struct SharedExec {
    inner: Arc<dyn Executor<BoxFuture<'static, ()>> + Send + Sync + 'static>,
}

impl SharedExec {
    pub fn new<E>(exec: E) -> Self
    where
        E: Executor<BoxFuture<'static, ()>> + Send + Sync + 'static,
    {
        Self {
            inner: Arc::new(exec),
        }
    }

    pub fn tokio() -> Self {
        Self::new(TokioExecutor::new())
    }
}

impl<F> Executor<F> for SharedExec
where
    F: Future<Output = ()> + Send + 'static,
{
    fn execute(&self, fut: F) {
        self.inner.execute(Box::pin(fut))
    }
}
