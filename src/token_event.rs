use std::hash::Hash;

use alloy::{
    primitives::{
        Address, B256, FixedBytes,
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

// Ajuste os imports abaixo conforme os tipos reais no seu crate `sol::sol_types`.
// Estou seguindo a mesma convenção do seu exemplo.
use sol::sol_types::{
    IERC20::{self, Approval as ERC20Approval, IERC20Events, Transfer as ERC20Transfer},
    IERC721::{
        self, Approval as ERC721Approval, ApprovalForAll as ERC721ApprovalForAll, IERC721Events,
        Transfer as ERC721Transfer,
    },
    IERC1155::{
        self, ApprovalForAll as ERC1155ApprovalForAll, IERC1155Events,
        TransferBatch as ERC1155TransferBatch, TransferSingle as ERC1155TransferSingle,
    },
    // Se tiver eventos adicionais (Mint/Burn em tokens específicos), importe aqui.
};

/// Estruturas auxiliares parecidas com o seu código original
pub struct Chunk {
    addrs: HashSet<Address>,
    tombstones: HashSet<Address>,
    id: u32,
}

pub struct TokenEvents {
    map: HashMap<FixedBytes<32>, UnifiedTokenEvent>,
}

/// Gera um HashMap que associa signature_hash -> UnifiedTokenEvent
pub fn generate_tokens_events_map() -> std::collections::HashMap<FixedBytes<32>, UnifiedTokenEvent>
{
    let mut map = HashMap::new();

    // ERC-20
    let erc20_transfer_hash = ERC20Transfer::SIGNATURE_HASH;
    map.insert(erc20_transfer_hash, UnifiedTokenEvent::ERC20Transfer());

    let erc20_approval_hash = ERC20Approval::SIGNATURE_HASH;
    map.insert(erc20_approval_hash, UnifiedTokenEvent::ERC20Approval());

    // ERC-721
    let erc721_transfer_hash = ERC721Transfer::SIGNATURE_HASH;
    map.insert(erc721_transfer_hash, UnifiedTokenEvent::ERC721Transfer());

    let erc721_approval_hash = ERC721Approval::SIGNATURE_HASH;
    map.insert(erc721_approval_hash, UnifiedTokenEvent::ERC721Approval());

    let erc721_approval_for_all_hash = ERC721ApprovalForAll::SIGNATURE_HASH;
    map.insert(
        erc721_approval_for_all_hash,
        UnifiedTokenEvent::ERC721ApprovalForAll(),
    );

    // ERC-1155
    let erc1155_transfer_single_hash = ERC1155TransferSingle::SIGNATURE_HASH;
    map.insert(
        erc1155_transfer_single_hash,
        UnifiedTokenEvent::ERC1155TransferSingle(),
    );

    let erc1155_transfer_batch_hash = ERC1155TransferBatch::SIGNATURE_HASH;
    map.insert(
        erc1155_transfer_batch_hash,
        UnifiedTokenEvent::ERC1155TransferBatch(),
    );

    let erc1155_approval_for_all_hash = ERC1155ApprovalForAll::SIGNATURE_HASH;
    map.insert(
        erc1155_approval_for_all_hash,
        UnifiedTokenEvent::ERC1155ApprovalForAll(),
    );

    // Se quiser, adicionar variantes para Mint/Burn custom (muitos tokens usam Transfer with zero address)
    // let token_mint_hash = MyToken::Mint::SIGNATURE_HASH;
    // map.insert(token_mint_hash, UnifiedTokenEvent::TokenMint());

    map
}

/// Retorna a concatenação das signatures conhecidas (útil pra `Filter::topics` etc)
pub fn generate_token_events() -> Vec<&'static str> {
    let erc20_events = IERC20Events::SIGNATURES.clone();
    let erc721_events = IERC721Events::SIGNATURES.clone();
    let erc1155_events = IERC1155Events::SIGNATURES.clone();

    [erc20_events, erc721_events, erc1155_events].concat()
}

#[derive(Debug, Clone)]
pub enum UnifiedTokenEvent {
    // ERC-20
    ERC20Transfer(),
    ERC20Approval(),

    // ERC-721
    ERC721Transfer(),
    ERC721Approval(),
    ERC721ApprovalForAll(),

    // ERC-1155
    ERC1155TransferSingle(),
    ERC1155TransferBatch(),
    ERC1155ApprovalForAll(),
    // Opcional:
    // TokenMint(),
    // TokenBurn(),
}

#[derive(Debug, Clone)]
pub enum UnifiedTokenEventResponse {
    // ERC-20
    ERC20Transfer(Log<ERC20Transfer>),
    ERC20Approval(Log<ERC20Approval>),

    // ERC-721
    ERC721Transfer(Log<ERC721Transfer>),
    ERC721Approval(Log<ERC721Approval>),
    ERC721ApprovalForAll(Log<ERC721ApprovalForAll>),

    // ERC-1155
    ERC1155TransferSingle(Log<ERC1155TransferSingle>),
    ERC1155TransferBatch(Log<ERC1155TransferBatch>),
    ERC1155ApprovalForAll(Log<ERC1155ApprovalForAll>),
    // Opcional: Mint/Burn se você declarar tipos específicos para eles
    // TokenMint(Log<MyToken::Mint>),
    // TokenBurn(Log<MyToken::Burn>),
}
