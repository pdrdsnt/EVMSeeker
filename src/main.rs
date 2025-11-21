use std::str::FromStr;

use alloy::{primitives::Address, transports::http::reqwest::Url};
use chain_json::{chain::ChainJsonInput, chain_json_model::JsonPoolKey, chains::ChainsJsonInput};
use futures::channel::mpsc::UnboundedReceiver;
use pool_event::UnifiedPoolEvent;
use tokio::main;
use ws_funnel::ReceiverFunnel;
use ws_subscription::WsProviderFunnel;

pub mod pool_event;
pub mod ws_funnel;
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
    let chains = ChainsJsonInput::default();

    let main_funnel =
        ReceiverFunnel::<UnifiedPoolEvent, UnboundedReceiver<UnifiedPoolEvent>>::start();

    for (id, chain) in chains.chains {
        if id != 56 {
            continue;
        }

        for node in chain.ws_nodes_urls {
            if let Ok(url) = Url::from_str(&node) {
                let sub = WsProviderFunnel::new(url).await;
                let pools: Vec<Address> = chain
                    .pools
                    .iter()
                    .map(|(key, pool)| match key {
                        JsonPoolKey::V2(address) => return *address,
                        JsonPoolKey::V3(address) => return *address,
                        JsonPoolKey::V4(address, fixed_bytes) => return *address,
                    })
                    .collect();

                sub.add(pools).await;
                main_funnel.0.add_subscription();
            }
        }
    }
}
