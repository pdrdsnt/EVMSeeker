use std::collections::HashSet;

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
use sol::sol_types::{
    StateView::{self, Swap},
    V3Pool,
};
use tokio::sync::mpsc::unbounded_channel;

use crate::{
    WsProvider,
    pool_event::{
        UnifiedPoolEvent, UnifiedPoolEventResponse, generate_pool_events, generate_pools_events_map,
    },
    receiver_funnel::ReceiverFunnel,
    ws_sub_chunk::WsSubChunk,
};

pub struct WsProviderFunnel {
    targets: HashSet<Address>,
    pub provider: WsProvider,
    events: Vec<&'static str>,
    funnel: ReceiverFunnel<Log, WsSubChunk>,
}

impl WsProviderFunnel {
    pub async fn start(
        provider: WsProvider,
    ) -> (
        WsProviderFunnel,
        tokio::sync::mpsc::UnboundedReceiver<UnifiedPoolEventResponse>,
    ) {
        let (funnel, mut rx) = ReceiverFunnel::<Log, WsSubChunk>::start();
        let e_map = generate_pools_events_map();
        let e_enum = generate_pool_events();

        let (this_tx, this_rx) = unbounded_channel::<UnifiedPoolEventResponse>();

        tokio::spawn(async move {
            let s = 0_u8;
            println!("starting event listener thread");
            while let Some(res) = rx.recv().await {
                println!("awaiting");
                if let Some(topic) = res.topic0() {
                    if let Some(t) = e_map.get(topic) {
                        match t {
                            UnifiedPoolEvent::V2Mint() => continue,
                            UnifiedPoolEvent::V2Burn() => continue,
                            UnifiedPoolEvent::V2Swap() => continue,
                            UnifiedPoolEvent::V2Sync() => continue,
                            UnifiedPoolEvent::V2Approval() => continue,
                            UnifiedPoolEvent::V2Transfer() => continue,
                            UnifiedPoolEvent::V3Mint() => continue,
                            UnifiedPoolEvent::V3Swap() => {
                                println!("v4 liquidity modification detected");
                                if let Ok(decoded_log) = res.log_decode::<V3Pool::Swap>() {
                                    this_tx.send(UnifiedPoolEventResponse::V3Swap(decoded_log));
                                }
                            }
                            UnifiedPoolEvent::V3Collect() => continue,
                            UnifiedPoolEvent::V3Burn() => continue,
                            UnifiedPoolEvent::V3Flash() => continue,
                            UnifiedPoolEvent::V4Donate() => continue,
                            UnifiedPoolEvent::V4Initialize() => continue,
                            UnifiedPoolEvent::V4Modify() => {
                                println!("v4 liquidity modification detected");
                                if let Ok(decoded_log) =
                                    res.log_decode::<StateView::ModifyLiquidity>()
                                {
                                    this_tx.send(UnifiedPoolEventResponse::V4Modify(decoded_log));
                                }
                            }
                            UnifiedPoolEvent::V4Swap() => {
                                println!("v4 swap detected");
                                if let Ok(decoded_log) = res.log_decode::<StateView::Swap>() {
                                    this_tx.send(UnifiedPoolEventResponse::V4Swap(decoded_log));
                                }
                            }
                        }
                    }
                }
            }
        });

        (
            Self {
                funnel,
                targets: HashSet::new(),
                events: generate_pool_events(),
                provider,
            },
            this_rx,
        )
    }

    pub async fn add(&self, adresses: Vec<Address>) {
        let new_addresses: Vec<Address> = adresses
            .clone()
            .iter()
            .filter_map(|x| self.targets.get(x).cloned())
            .collect();
        let new_chunk = 
    }
}

pub async fn create_ws_sub(
    url: Url,
    events: Vec<&str>,
    adresses: Vec<Address>,
) -> Result<Option<alloy::pubsub::Subscription<alloy::rpc::types::Log>>, RpcError<TransportErrorKind>>
{
    let ws_connect: WsConnect = WsConnect::new(url.clone());

    let filter = Filter::new().address(adresses).events(events.clone());
    print!("new filter: {:?}", filter);

    match RpcClient::connect_pubsub(ws_connect).await {
        Ok(rpc_client) => {
            let provider = ProviderBuilder::new().connect_client(rpc_client);
            let thing = Some(provider.subscribe_logs(&filter).await?);

            println!("provider created {:?}", thing);
            Ok(thing)
        }
        Err(err) => Err(err),
    }
}
