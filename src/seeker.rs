use std::{collections::HashMap, io::Bytes, str::FromStr};

use alloy::{
    primitives::{Address, B256, FixedBytes},
    providers::{Provider, ProviderBuilder},
    rpc::{
        client::RpcClient,
        types::{Filter, Log},
    },
    transports::{RpcError, TransportErrorKind, http::reqwest::Url, ws::WsConnect},
};

use chain_json::{chain::ChainJsonInput, chain_json_model::JsonPoolKey, chains::ChainsJsonInput};
use futures::channel::mpsc::UnboundedReceiver;
use sol::sol_types::{StateView, V3Pool};
use tokio::main;

use crate::{
    pool_event::{
        UnifiedPoolEvent, UnifiedPoolEventResponse, generate_pool_events, generate_pools_events_map,
    },
    ws_subscription::WsProviderFunnel,
};

pub type WsProvider = alloy::providers::fillers::FillProvider<
    alloy::providers::fillers::JoinFill<
        alloy::providers::Identity,
        alloy::providers::fillers::JoinFill<
            alloy::providers::fillers::GasFiller,
            alloy::providers::fillers::JoinFill<
                alloy::providers::fillers::BlobGasFiller,
                alloy::providers::fillers::JoinFill<
                    alloy::providers::fillers::NonceFiller,
                    alloy::providers::fillers::ChainIdFiller,
                >,
            >,
        >,
    >,
    alloy::providers::RootProvider,
>;

#[tokio::main(flavor = "current_thread")]
async fn main(provider: WsProvider, events_map: HashMap<B256, UnifiedPoolEvent>) {
    let (funnel, mut rx) = WsProviderFunnel::start(ws_provider).await;
    funnel.add(init_pools(bsc_data)).await;

    while let Some(res) = rx.recv().await {
        match res {
            UnifiedPoolEventResponse::V2Mint(log) => continue,
            UnifiedPoolEventResponse::V2Burn(log) => continue,
            UnifiedPoolEventResponse::V2Swap(log) => {
                println!("v2 swap {:?}", log)
            }

            UnifiedPoolEventResponse::V2Sync(log) => continue,

            UnifiedPoolEventResponse::V2Approval(log) => continue,

            UnifiedPoolEventResponse::V2Transfer(log) => continue,

            UnifiedPoolEventResponse::V3Mint(log) => continue,

            UnifiedPoolEventResponse::V3Swap(log) => {
                println!("v3 swap {:?}", log)
            }

            UnifiedPoolEventResponse::V3Collect(log) => continue,

            UnifiedPoolEventResponse::V3Burn(log) => continue,

            UnifiedPoolEventResponse::V3Flash(log) => continue,

            UnifiedPoolEventResponse::V4Donate(donate) => continue,

            UnifiedPoolEventResponse::V4Initialize(initialize) => {
                println!("v4 initialized {:?}", initialize)
            }

            UnifiedPoolEventResponse::V4Modify(modify_liquidity) => {
                println!("v4 modified {:?}", modify_liquidity)
            }

            UnifiedPoolEventResponse::V4Swap(log) => println!("v4 swap {:?}", log),
        }
    }

    println!("exiting");
}

pub async fn ws_sub(
    provider: &WsProvider,
    events: Vec<&str>,
    adresses: Vec<Address>,
) -> Result<alloy::pubsub::Subscription<alloy::rpc::types::Log>, RpcError<TransportErrorKind>> {
    let filter = Filter::new().address(adresses).events(events.clone());
    print!("new filter: {:?}", filter);
    let r = provider.subscribe_logs(&filter).await?;
    return Ok(r);
}

pub async fn ws_provider(url: Url) -> Result<WsProvider, RpcError<TransportErrorKind>> {
    let ws_connect: WsConnect = WsConnect::new(url.clone());

    match RpcClient::connect_pubsub(ws_connect).await {
        Ok(rpc_client) => {
            println!("ws connection {:?}", &url);
            let provider = ProviderBuilder::new().connect_client(rpc_client);
            Ok(provider)
        }

        Err(err) => Err(err),
    }
}
pub trait EventDecoder<In, Out> {
    pub fn decode(res: In, e_map: &HashMap<B256, UnifiedPoolEvent>) -> Option<Out>;
}

pub fn decode_token_log(
    res: Log,
    e_map: &HashMap<B256, UnifiedPoolEvent>,
) -> Option<UnifiedPoolEventResponse> {
}

pub fn decode_pools_log(
    res: Log,
    e_map: &HashMap<B256, UnifiedPoolEvent>,
) -> Option<UnifiedPoolEventResponse> {
    if let Some(topic) = res.topic0() {
        if let Some(t) = e_map.get(topic) {
            match t {
                UnifiedPoolEvent::V2Mint() => return None,
                UnifiedPoolEvent::V2Burn() => return None,
                UnifiedPoolEvent::V2Swap() => return None,
                UnifiedPoolEvent::V2Sync() => return None,
                UnifiedPoolEvent::V2Approval() => return None,
                UnifiedPoolEvent::V2Transfer() => return None,
                UnifiedPoolEvent::V3Mint() => return None,
                UnifiedPoolEvent::V3Swap() => {
                    println!("v4 liquidity modification detected");
                    if let Ok(decoded_log) = res.log_decode::<V3Pool::Swap>() {
                        return Some(UnifiedPoolEventResponse::V3Swap(decoded_log));
                    }
                    return None;
                }
                UnifiedPoolEvent::V3Collect() => return None,
                UnifiedPoolEvent::V3Burn() => return None,
                UnifiedPoolEvent::V3Flash() => return None,
                UnifiedPoolEvent::V4Donate() => return None,
                UnifiedPoolEvent::V4Initialize() => return None,
                UnifiedPoolEvent::V4Modify() => {
                    println!("v4 liquidity modification detected");
                    if let Ok(decoded_log) = res.log_decode::<StateView::ModifyLiquidity>() {
                        return Some(UnifiedPoolEventResponse::V4Modify(decoded_log));
                    }
                    return None;
                }
                UnifiedPoolEvent::V4Swap() => {
                    println!("v4 swap detected");
                    if let Ok(decoded_log) = res.log_decode::<StateView::Swap>() {
                        return Some(UnifiedPoolEventResponse::V4Swap(decoded_log));
                    }
                    return None;
                }
            };
        };
    };
    return None;
}
