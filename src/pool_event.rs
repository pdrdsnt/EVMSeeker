use alloy::{
    primitives::{
        Address, B256,
        map::{HashMap, HashSet},
    },
    providers::{Provider, ProviderBuilder},
    rpc::{
        client::RpcClient,
        types::{Filter, Log},
    },
    sol_types::SolEvent,
    transports::{RpcError, TransportErrorKind, http::reqwest::Url, ws::WsConnect},
};
use futures::channel::mpsc::UnboundedReceiver;
use sol::sol_types::{
    IUniswapV2Pair::{self, IUniswapV2PairEvents, Mint, Swap, Sync, Transfer},
    StateView::{self, Donate, Initialize, ModifyLiquidity, StateViewEvents},
    V3Pool::{self, Burn, Flash, V3PoolEvents},
};

pub struct Chunk {
    addrs: HashSet<Address>,
    tombstones: HashSet<Address>,
    id: u32,
}

pub struct PoolEvents {
    map: HashMap<alloy::primitives::FixedBytes<32>, UnifiedPoolEvent>,
}

pub fn generate_pools_events_map()
-> std::collections::HashMap<alloy::primitives::FixedBytes<32>, UnifiedPoolEvent> {
    let mut map = HashMap::new();

    let v2_sync_hash = sol::sol_types::IUniswapV2Pair::Sync::SIGNATURE_HASH;
    map.insert(v2_sync_hash, UnifiedPoolEvent::V2Mint());

    let v2_mint_hash = sol::sol_types::IUniswapV2Pair::Mint::SIGNATURE_HASH;
    map.insert(v2_mint_hash, UnifiedPoolEvent::V2Mint());

    let v2_approval_hash = sol::sol_types::IUniswapV2Pair::Approval::SIGNATURE_HASH;
    map.insert(v2_approval_hash, UnifiedPoolEvent::V2Approval());

    let v2_swap_hash = sol::sol_types::IUniswapV2Pair::Swap::SIGNATURE_HASH;
    map.insert(v2_swap_hash, UnifiedPoolEvent::V2Swap());

    let v2_burn_hash = sol::sol_types::IUniswapV2Pair::Burn::SIGNATURE_HASH;
    map.insert(v2_burn_hash, UnifiedPoolEvent::V2Burn());

    let v3_burn_hash = sol::sol_types::V3Pool::Burn::SIGNATURE_HASH;
    map.insert(v3_burn_hash, UnifiedPoolEvent::V3Burn());

    let v3_collect_hash = sol::sol_types::V3Pool::Collect::SIGNATURE_HASH;
    map.insert(v3_collect_hash, UnifiedPoolEvent::V3Collect());

    let v3_mint_hash = sol::sol_types::V3Pool::Mint::SIGNATURE_HASH;
    map.insert(v3_mint_hash, UnifiedPoolEvent::V3Mint());

    let v3_flash_hash = sol::sol_types::V3Pool::Flash::SIGNATURE_HASH;
    map.insert(v3_flash_hash, UnifiedPoolEvent::V3Flash());

    let v3_swap_hash = sol::sol_types::V3Pool::Swap::SIGNATURE_HASH;
    map.insert(v3_swap_hash, UnifiedPoolEvent::V3Swap());

    let v4_modify_liquidity = sol::sol_types::StateView::ModifyLiquidity::SIGNATURE_HASH;
    map.insert(v4_modify_liquidity, UnifiedPoolEvent::V4Modify());

    let v4_swap = sol::sol_types::StateView::Swap::SIGNATURE_HASH;
    map.insert(v4_swap, UnifiedPoolEvent::V4Swap());

    let v4_init = sol::sol_types::StateView::ModifyLiquidity::SIGNATURE_HASH;
    map.insert(v4_init, UnifiedPoolEvent::V4Initialize());

    let v4_donate = sol::sol_types::StateView::Donate::SIGNATURE_HASH;
    map.insert(v4_donate, UnifiedPoolEvent::V4Donate());

    map
}

pub fn generate_pool_events() -> Vec<&'static str> {
    let v2_events = IUniswapV2PairEvents::SIGNATURES.clone();
    let v3_events = V3PoolEvents::SIGNATURES.clone();
    let v4_events = StateViewEvents::SIGNATURES.clone();
    print!("==v4 events: {:?}", &v4_events);
    [v2_events, v3_events, v4_events].concat()
}

#[derive(Debug, Clone)]
pub enum UnifiedPoolEvent {
    // UNISWAP V2
    V2Mint(),
    V2Burn(),
    V2Swap(),
    V2Sync(),
    V2Approval(),
    V2Transfer(),

    // UNISWAP V3
    V3Mint(),
    V3Swap(),
    V3Collect(),
    V3Burn(),
    V3Flash(),

    // UNISWAP V4 STATEVIEW
    V4Donate(),
    V4Initialize(),
    V4Modify(),
    V4Swap(),
}

#[derive(Debug, Clone)]
pub enum UnifiedPoolEventResponse {
    // UNISWAP V2
    V2Mint(Log<IUniswapV2Pair::Mint>),
    V2Burn(Log<IUniswapV2Pair::Burn>),
    V2Swap(Log<IUniswapV2Pair::Swap>),
    V2Sync(Log<IUniswapV2Pair::Sync>),
    V2Approval(Log<IUniswapV2Pair::Approval>),
    V2Transfer(Log<IUniswapV2Pair::Transfer>),

    // UNISWAP V3
    V3Mint(Log<V3Pool::Mint>),
    V3Swap(Log<V3Pool::Swap>),
    V3Collect(Log<V3Pool::Collect>),
    V3Burn(Log<V3Pool::Burn>),
    V3Flash(Log<V3Pool::Flash>),

    // UNISWAP V4 STATEVIEW
    V4Donate(Log<StateView::Donate>),
    V4Initialize(Log<StateView::Initialize>),
    V4Modify(Log<StateView::ModifyLiquidity>),
    V4Swap(Log<StateView::Swap>),
}
