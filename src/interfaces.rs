// Copyright (C) 2026 Utexo.
// See LICENSE for copying information.

//! Abstract ports for sync — Bitcoin RPC and on-chain relay operations (no HTTP, no ABI, no IO).
//!
//! Traits so the sync engine can be tested with fakes and isn't married to `reqwest` or `alloy`.
//! If you add a second Bitcoin backend, implement `BitcoinRpcClient`; don't fork the loop.

use anyhow::Result;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlockRef {
    /// Chain height (genesis = 0 in Bitcoin land for `getblockcount`-style thinking).
    pub height: u64,
    /// Block hash hex string from Core (no `0x` prefix — Bitcoin style).
    pub hash: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawHeader {
    pub hash: String,
    /// Full 80-byte header as **even-length hex, no 0x prefix** — what `getblockheader` returns.
    pub hex: String,
}

/// Everything the relayer needs from Bitcoin Core (or anything that speaks the same JSON-RPC).
#[allow(dead_code)]
pub trait BitcoinRpcClient {
    /// Current chain tip height from `getblockcount`.
    fn get_block_count(&self) -> Result<u64>;
    /// `getblockhash(height)`.
    fn get_block_hash(&self, height: u64) -> Result<String>;
    /// Tip hash — convenience around the best chain.
    fn get_best_block_hash(&self) -> Result<String>;
    /// Serialized header hex for a hash (`getblockheader`, verbose=false style string).
    fn get_block_header_hex(&self, hash: &str) -> Result<String>;
    /// `chainwork` of `getblockheader(hash)` as 32 big-endian bytes.
    fn get_block_chainwork(&self, _hash: &str) -> Result<[u8; 32]> {
        anyhow::bail!("block chainwork not supported by this client")
    }
}

/// Read/write surface of the on-chain relay. **Canonical progress** for sync is `relay_tip_height()`.
#[allow(dead_code)]
pub trait BtcRelaySubmitter {
    /// `getBlockheight()` on the contract — how far the relay has ingested Bitcoin.
    fn relay_tip_height(&self) -> Result<u64>;

    /// `getChainwork()` padded to 32 bytes for the 160-byte relay prologue (big-endian uint224 in high bytes).
    fn relay_chain_work_bytes(&self) -> Result<[u8; 32]>;

    /// `getCommitHash(height)` — used at startup to prove we can read state at the tip.
    fn relay_commit_hash(&self, height: u64) -> Result<String>;

    /// Submit headers: `header_hex` is **no-0x**, even length; may be one header or a **batch** ABI encoding from the sync engine.
    /// Returns the tx hash so logs can correlate on-chain receipts.
    fn submit_header(&self, header_hex: &str) -> Result<String>;

    /// `submitShortForkBlockheaders`: same payload as `submit_header`, parent on the relay main chain.
    fn submit_short_fork(&self, _header_hex: &str) -> Result<String> {
        anyhow::bail!("short fork submission not supported by this submitter")
    }

    /// `submitForkBlockheaders(fork_id, …)`: one fork may take several calls.
    fn submit_fork(&self, _fork_id: u64, _header_hex: &str) -> Result<String> {
        anyhow::bail!("fork submission not supported by this submitter")
    }

    /// Commitment the relay stores for a 160-byte stored header, in the format of `relay_commit_hash`.
    fn stored_header_commitment(&self, stored_header: &[u8]) -> String {
        alloy::primitives::keccak256(stored_header).to_string()
    }

    /// Optional capability: relayer wallet address used for sending txs.
    /// Backends that do not manage a wallet can keep the default "unsupported" behavior.
    fn relayer_wallet_address(&self) -> Result<String> {
        anyhow::bail!("relayer wallet address not supported by this submitter")
    }

    /// Optional capability: relayer wallet balance in wei.
    /// Backends that do not manage a wallet can keep the default "unsupported" behavior.
    fn relayer_wallet_balance_wei(&self) -> Result<alloy::primitives::U256> {
        anyhow::bail!("relayer wallet balance not supported by this submitter")
    }
}
