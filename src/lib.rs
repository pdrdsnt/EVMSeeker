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
use seeker::{init_pools, ws_provider};
use sol::sol_types::{StateView, V3Pool};
use tokio::main;
use ws_subscription::{WsProviderFunnel, create_ws_sub};

pub mod pool_event;
pub mod seeker;
pub mod ws_subscription;

#[tokio::test]
async fn it_works() {
    let mut chains = ChainsJsonInput::default();
    let pool_events: HashMap<B256, UnifiedPoolEvent> = generate_pools_events_map();

    let mut bsc_data = chains.chains.remove(&56).unwrap();
    let provider_url = bsc_data.ws_nodes_urls.first().unwrap();

    let url = Url::from_str(provider_url).unwrap();
    let ws_provider = ws_provider(url).await.unwrap();
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

pub fn init_pools(mut chain: ChainJsonInput) -> Vec<Address> {
    let pools = Vec::new();
    for node in chain.ws_nodes_urls {
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
    }
    pools
}

#[tokio::test]
pub async fn test_sub(pools: Vec<Address>, provider: WsProvider) {
    println!("ws provider {:?}", &provider);
    let pool_events = generate_pools_events_map();
    if let Ok(mut sub) = ws_sub(&provider, generate_pool_events(), pools.clone()).await {
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
