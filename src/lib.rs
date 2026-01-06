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

use chains_json::{
    chain::ChainJsonInput,
    chain_json_model::{JsonPoolKey, PoolJsonModel},
    chains::ChainsJsonInput,
};
use futures::channel::mpsc::UnboundedReceiver;
use pool_event::{
    UnifiedPoolEvent, UnifiedPoolEventResponse, generate_pool_events, generate_pools_events_map,
};
use receiver_funnel::receiver_funnel;
use seeker::ws_provider;
use sol::sol_types::{StateView, V3Pool};
use tokio::main;
use ws_subscription::{WsProviderFunnel, create_ws_sub};

use crate::seeker::{WsProvider, decode_pools_log, ws_sub};

pub mod pool_event;
pub mod seeker;
pub mod token_event;
pub mod ws_subscription;

#[tokio::test]
async fn it_works() {
    let mut chains = ChainsJsonInput::default();
    let pool_events: HashMap<B256, UnifiedPoolEvent> = generate_pools_events_map();

    let mut bsc_data = chains.chains.remove(&130).unwrap();
    let provider_url = bsc_data.ws_nodes_urls.first().unwrap();

    let url = Url::from_str(provider_url).unwrap();
    println!("{:?}", &url);

    let ws_provider = match ws_provider(url).await {
        Ok(ok) => ok,
        Err(err) => {
            panic!("ws provider creation failed: {:?}", err)
        }
    };
    let (funnel, mut rx) = WsProviderFunnel::start(ws_provider).await;
    funnel.add(extract_pools_addresses(bsc_data)).await;

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

pub fn extract_pools_addresses(mut chain: ChainJsonInput) -> Vec<Address> {
    let pools = Vec::new();
    for node in chain.ws_nodes_urls {
        let dexes = chain.dexes.clone();

        let mut pools_addresses = Vec::new();
        let mut pools: &Vec<PoolJsonModel> = &chain.pools;
        for p in pools {
            let addr = match p {
                PoolJsonModel::V2 {
                    address,
                    token0,
                    token1,
                    fee,
                } => address,
                PoolJsonModel::V3 {
                    address,
                    token0,
                    token1,
                    fee,
                } => address,
                PoolJsonModel::V4 {
                    pool_manager,
                    state_view,
                    token0,
                    token1,
                    fee,
                    spacing,
                    hooks,
                } => state_view,
            };
            pools_addresses.push(p);
        }
    }
    pools
}

pub async fn listen_to_all(pools: Vec<Address>, provider: WsProvider) {
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
