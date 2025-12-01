use std::collections::{HashMap, HashSet};

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
    IUniswapV2Pair,
    StateView::{self, Swap},
    V3Pool,
};

use tokio::sync::mpsc::unbounded_channel;
use tokio_stream::wrappers::BroadcastStream;

use crate::{
    WsP,
    pool_event::{
        UnifiedPoolEvent, UnifiedPoolEventResponse, generate_pool_events, generate_pools_events_map,
    },
    receiver_funnel::ReceiverFunnel,
    seeker::WsProvider,
    ws_provider, ws_sub,
};

pub struct WsProviderFunnel {
    targets: HashSet<Address>,
    provider: WsProvider,
    events: Vec<&'static str>,
    funnel: ReceiverFunnel<Log, SubscriptionStream<Log>>,
}

impl WsProviderFunnel {
    pub async fn start(
        provider: WsProvider,
    ) -> (
        WsProviderFunnel,
        tokio::sync::mpsc::UnboundedReceiver<UnifiedPoolEventResponse>,
    ) {
        let (funnel, mut rx) = ReceiverFunnel::<Log, SubscriptionStream<Log>>::start();
        let e_map = generate_pools_events_map();
        let e_enum = generate_pool_events();

        let (this_tx, this_rx) = unbounded_channel::<UnifiedPoolEventResponse>();
        tokio::spawn(async move {
            let s = 0_u8;
            println!("starting event listener thread");
            while let Some(res) = rx.recv().await {
                if let Some(topic) = res.topic0() {
                    if let Some(t) = e_map.get(topic) {
                        match t {
                            UnifiedPoolEvent::V2Mint() => {
                                println!("v2 tranfert detected");
                                if let Ok(decoded_log) = res.log_decode::<IUniswapV2Pair::Mint>() {
                                    this_tx.send(UnifiedPoolEventResponse::V2Mint(decoded_log));
                                }
                            }
                            UnifiedPoolEvent::V2Burn() => {
                                println!("v2 burn detected");
                                if let Ok(decoded_log) = res.log_decode::<IUniswapV2Pair::Burn>() {
                                    this_tx.send(UnifiedPoolEventResponse::V2Burn(decoded_log));
                                }
                            }
                            UnifiedPoolEvent::V2Swap() => {
                                println!("v2 swap detected");
                                if let Ok(decoded_log) = res.log_decode::<IUniswapV2Pair::Swap>() {
                                    this_tx.send(UnifiedPoolEventResponse::V2Swap(decoded_log));
                                }
                            }
                            UnifiedPoolEvent::V2Sync() => continue,
                            UnifiedPoolEvent::V2Approval() => continue,
                            UnifiedPoolEvent::V2Transfer() => {
                                println!("v2 tranfert detected");
                                if let Ok(decoded_log) =
                                    res.log_decode::<IUniswapV2Pair::Transfer>()
                                {
                                    this_tx.send(UnifiedPoolEventResponse::V2Transfer(decoded_log));
                                }
                            }
                            UnifiedPoolEvent::V3Mint() => {
                                println!("v3 mint detected");
                                if let Ok(decoded_log) = res.log_decode::<V3Pool::Mint>() {
                                    this_tx.send(UnifiedPoolEventResponse::V3Mint(decoded_log));
                                }
                            }
                            UnifiedPoolEvent::V3Swap() => {
                                println!("v4 liquidity modification detected");
                                if let Ok(decoded_log) = res.log_decode::<V3Pool::Swap>() {
                                    this_tx.send(UnifiedPoolEventResponse::V3Swap(decoded_log));
                                }
                            }
                            UnifiedPoolEvent::V3Collect() => {
                                println!("v4 liquidity modification detected");
                                if let Ok(decoded_log) = res.log_decode::<V3Pool::Collect>() {
                                    this_tx.send(UnifiedPoolEventResponse::V3Collect(decoded_log));
                                }
                            }
                            UnifiedPoolEvent::V3Burn() => {
                                println!("v3 liquidity modification detected");
                                if let Ok(decoded_log) = res.log_decode::<V3Pool::Burn>() {
                                    this_tx.send(UnifiedPoolEventResponse::V3Burn(decoded_log));
                                }
                            }

                            UnifiedPoolEvent::V3Flash() => {
                                println!("v3 liquidity modification detected");
                                if let Ok(decoded_log) = res.log_decode::<V3Pool::Flash>() {
                                    this_tx.send(UnifiedPoolEventResponse::V3Flash(decoded_log));
                                }
                            }
                            UnifiedPoolEvent::V4Donate() => {
                                println!("v4 liquidity modification detected");
                                if let Ok(decoded_log) = res.log_decode::<StateView::Donate>() {
                                    this_tx.send(UnifiedPoolEventResponse::V4Donate(decoded_log));
                                }
                            }
                            UnifiedPoolEvent::V4Initialize() => {
                                println!("v4 liquidity modification detected");
                                if let Ok(decoded_log) = res.log_decode::<StateView::Initialize>() {
                                    this_tx
                                        .send(UnifiedPoolEventResponse::V4Initialize(decoded_log));
                                }
                            }
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

        if let Ok(new_chunk) = ws_sub(&self.provider, generate_pool_events(), adresses).await {
            self.funnel.add_subscription(new_chunk.into_stream());
        }
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
