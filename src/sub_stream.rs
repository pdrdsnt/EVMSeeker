use alloy::pubsub::Subscription;
use futures::stream::Next;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::sync::broadcast;
use tokio::sync::broadcast::error::{RecvError, TryRecvError};
use tokio_stream::{Stream, StreamExt};

pub struct SubStream<T: Unpin> {
    inner: Subscription<T>,
}

impl<T: Unpin> SubStream<T> {
    pub fn new(rx: Subscription<T>) -> Self {
        Self { inner: rx }
    }
}

impl<T: Unpin + Clone + Send + 'static> Stream for SubStream<T> {
    type Item = T;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match Pin::new(&mut self.inner).inner().try_recv() {
            Ok(item) => {
                let t: T = item.as_ref().clone();
                Poll::Ready(Some(t))
            }
            Err(err) => match err {
                TryRecvError::Empty => {
                    cx.waker().wake_by_ref();
                    Poll::Pending
                }
                TryRecvError::Closed => Poll::Ready(None),
                TryRecvError::Lagged(_) => {
                    cx.waker().wake_by_ref();
                    Poll::Pending
                }
            },
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (0, None)
    }
}
