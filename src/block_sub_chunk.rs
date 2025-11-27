use std::{
    pin::Pin,
    task::{Context, Poll},
};

use alloy::providers::Provider;
use futures::Stream;

use crate::WsProvider;

pub struct BlockSubChunk {}

pub async fn sub(provider: WsProvider) {
    println!("ws provider {:?}", &provider);
    loop {
        match provider.subscribe_blocks().await {
            Ok(mut msub) => match msub.recv().await {
                Ok(received) => println!("block tx received {:?}", received),
                Err(err) => {
                    println!("error receiving block data {:?}", err);
                    break;
                }
            },
            Err(err) => println!("error creating block subscription {}", err),
        }
    }
}

impl<T: Unpin + Clone + Send + 'static> Stream for BlockSubChunk {
    type Item = T;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match Pin::new(&mut self.inner).try_recv() {
            Ok(item) => Poll::Ready(Some(item)),
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
}
