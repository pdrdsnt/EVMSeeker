use alloy::{
    primitives::{Address, LogData},
    providers::{Provider, ProviderBuilder},
    pubsub::{Subscription, SubscriptionStream},
    rpc::{
        client::RpcClient,
        types::{Filter, Log},
    },
    transports::{RpcError, TransportErrorKind, http::reqwest::Url, ws::WsConnect},
};
use futures::channel::mpsc::UnboundedReceiver;
use tokio::sync::mpsc::unbounded_channel;

use crate::{
    pool_event::{generate_pool_events, generate_pools_events_map},
    ws_funnel::ReceiverFunnel,
};

pub struct WsProviderFunnel {
    pub url: Url,
    pub events: Vec<&'static str>,
    funnel: ReceiverFunnel<Log, SubscriptionStream<Log>>,
}

impl WsProviderFunnel {
    pub async fn new(url: Url) -> (WsProviderFunnel, tokio::sync::mpsc::UnboundedReceiver<Log>) {
        let (funnel, mut rx) = ReceiverFunnel::<Log, SubscriptionStream<Log>>::start();
        let e_map = generate_pools_events_map();
        let e_enum = generate_pool_events();

        let (tx, rx) = unbounded_channel::<T>();

        tokio::spawn(async move {
            let s = 0_u8;
            while let Some(res) = rx.recv().await {
                if let Some(topic) = res.topic0() {
                    if let Some(t) = e_map.get(topic) {
                        match t {
                            crate::pool_event::UnifiedPoolEvent::V2Mint() => todo!(),
                            crate::pool_event::UnifiedPoolEvent::V2Burn() => todo!(),
                            crate::pool_event::UnifiedPoolEvent::V2Swap() => todo!(),
                            crate::pool_event::UnifiedPoolEvent::V2Sync() => todo!(),
                            crate::pool_event::UnifiedPoolEvent::V2Approval() => todo!(),
                            crate::pool_event::UnifiedPoolEvent::V2Transfer() => todo!(),
                            crate::pool_event::UnifiedPoolEvent::V3Mint() => todo!(),
                            crate::pool_event::UnifiedPoolEvent::V3Swap() => todo!(),
                            crate::pool_event::UnifiedPoolEvent::V3Collect() => todo!(),
                            crate::pool_event::UnifiedPoolEvent::V3Burn() => todo!(),
                            crate::pool_event::UnifiedPoolEvent::V3Flash() => todo!(),
                            crate::pool_event::UnifiedPoolEvent::V4Donate() => todo!(),
                            crate::pool_event::UnifiedPoolEvent::V4Initialize() => todo!(),
                            crate::pool_event::UnifiedPoolEvent::V4Modify() => todo!(),
                            crate::pool_event::UnifiedPoolEvent::V4Swap() => todo!(),
                        }
                    }
                }
            }
        });

        (
            Self {
                url,
                funnel,
                events: generate_pool_events(),
            },
            rx,
        )
    }

    async fn create_ws_sub(
        &self,
        adresses: Vec<Address>,
    ) -> Result<
        Option<alloy::pubsub::Subscription<alloy::rpc::types::Log>>,
        RpcError<TransportErrorKind>,
    > {
        generate_pools_events_map();
        let ws_connect: WsConnect = WsConnect::new(self.url.clone());

        let filter = Filter::new().address(adresses).events(self.events.clone());

        match RpcClient::connect_pubsub(ws_connect).await {
            Ok(rpc_client) => {
                let provider = ProviderBuilder::new().connect_client(rpc_client);
                let thing = Some(provider.subscribe_logs(&filter).await?);
                Ok(thing)
            }

            Err(err) => Err(err),
        }
    }

    pub async fn add(&self, adresses: Vec<Address>) {
        if let Ok(_sub) = self.create_ws_sub(adresses).await {
            if let Some(sub) = _sub {
                self.funnel.add_subscription(sub.into_stream()).unwrap();
            }
        }
    }
}
