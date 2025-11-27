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
use pool_event::{
    UnifiedPoolEvent, UnifiedPoolEventResponse, generate_pool_events, generate_pools_events_map,
};
use receiver_funnel::ReceiverFunnel;
use sol::sol_types::{StateView, V3Pool};
use tokio::main;
use ws_subscription::{WsProviderFunnel, create_ws_sub};

pub mod block_sub_chunk;
pub mod pool_event;
pub mod receiver_funnel;
pub mod ws_sub_chunk;
pub mod ws_subscription;

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
async fn main() {
    let mut chains = ChainsJsonInput::default();
    let pool_events: HashMap<B256, UnifiedPoolEvent> = generate_pools_events_map();

    let (funnel, mut rx) = ReceiverFunnel::<
        UnifiedPoolEventResponse,
        UnboundedReceiver<UnifiedPoolEventResponse>,
    >::start();

    if let Some(chain) = chains.chains.remove(&56_u64) {
        for node in chain.ws_nodes_urls {
            if let Ok(url) = Url::from_str(&node) {
                let provider = ws_provider(url.clone()).await;

                println!("provider: {}", url);

                let dexes = chain.dexes.clone();

                let mut pools: Vec<Address> = chain
                    .pools
                    .iter()
                    .filter_map(|(key, pool)| match key {
                        JsonPoolKey::V2(address) => Some(*address),
                        JsonPoolKey::V3(address) => Some(*address),
                        JsonPoolKey::V4(address, fixed_bytes) => None,
                    })
                    .collect();

                for dex in dexes {
                    match dex {
                        chain_json::chain_json_model::DexJsonModel::V2 {
                            address,
                            fee,
                            stable_fee,
                        } => continue,
                        chain_json::chain_json_model::DexJsonModel::V3 { address, fee } => continue,
                        chain_json::chain_json_model::DexJsonModel::V4 { address, manager } => {
                            if let Some(man) = manager {
                                if let Ok(addr) = Address::from_str(&man) {
                                    &pools.push(addr);
                                }
                            }
                        }
                    }
                }

                if let Ok(p) = provider {
                    println!("ws provider {:?}", &p);
                    if let Ok(mut sub) = ws_sub(&p, generate_pool_events(), pools.clone()).await {
                        loop {
                            match sub.recv().await {
                                Ok(received) => {
                                    println!("something received {:?}", received);
                                    let decoded = decode_pools_log(received, &pool_events);
                                    print!("decoded log {:?}", decoded);
                                }

                                Err(err) => {
                                    println!("error receiving subscription data {:?}", err);
                                    break;
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    while let Some(res) = rx.recv().await {
        match res {
            UnifiedPoolEventResponse::V2Mint(_) => continue,
            UnifiedPoolEventResponse::V2Burn(_) => continue,
            UnifiedPoolEventResponse::V2Swap(_) => continue,
            UnifiedPoolEventResponse::V2Sync(_) => continue,
            UnifiedPoolEventResponse::V2Approval(_) => continue,
            UnifiedPoolEventResponse::V2Transfer(_) => continue,
            UnifiedPoolEventResponse::V3Mint(_) => continue,
            UnifiedPoolEventResponse::V3Swap(_) => continue,
            UnifiedPoolEventResponse::V3Collect(_) => continue,
            UnifiedPoolEventResponse::V3Burn(_) => continue,
            UnifiedPoolEventResponse::V3Flash(_) => continue,
            UnifiedPoolEventResponse::V4Donate(donate) => continue,
            UnifiedPoolEventResponse::V4Initialize(initialize) => {
                println!("v4 initialized {:?}", initialize)
            }
            UnifiedPoolEventResponse::V4Modify(modify_liquidity) => continue,
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
            let provider = ProviderBuilder::new().connect_client(rpc_client);
            Ok(provider)
        }
        Err(err) => Err(err),
    }
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
