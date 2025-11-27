use std::{
    collections::HashSet,
    pin::Pin,
    task::{Context, Poll},
};

use alloy::{
    primitives::Address,
    rpc::types::Log,
    transports::{RpcError, TransportErrorKind, http::reqwest::Url},
};
use futures::Stream;
use tokio::sync::broadcast::error::TryRecvError;

use crate::{WsProvider, pool_event::UnifiedPoolEvent};

pub struct WsSubChunk {
    pub targets: HashSet<Address>,
    pub events: Vec<&'static str>,
    pub sub: alloy::pubsub::Subscription<alloy::rpc::types::Log>,
}

impl WsSubChunk {
    pub async fn new(
        addresses: Vec<Address>,
        provider: WsProvider,
        events: Vec<&'static str>,
    ) -> Result<WsSubChunk, RpcError<TransportErrorKind>> {
        let sub = crate::ws_sub(&provider, events.clone(), addresses.clone()).await?;

        Ok(Self {
            targets: HashSet::from_iter(addresses),
            events,
            sub,
        })
    }
    pub fn join(&mut self, other: WsSubChunk) {}
}

impl Stream for WsSubChunk {
    type Item = Log;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match Pin::new(&mut self.sub).try_recv() {
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
