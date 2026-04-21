use anyhow::Result;

/// Common Bitcoin block identity used across RPC, sync, and submitter modules.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlockRef {
    pub height: u64,
    pub hash: String,
}

/// Raw Bitcoin block header payload returned by bitcoind RPC.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawHeader {
    pub hash: String,
    pub hex: String,
}

/// Minimal boundary for Bitcoin JSON-RPC operations.
#[allow(dead_code)]
pub trait BitcoinRpcClient {
    fn get_block_count(&self) -> Result<u64>;
    fn get_block_hash(&self, height: u64) -> Result<String>;
    fn get_best_block_hash(&self) -> Result<String>;
    fn get_block_header_hex(&self, hash: &str) -> Result<String>;
}

/// Placeholder boundary for BTC relay contract writes.
#[allow(dead_code)]
pub trait BtcRelaySubmitter {
    /// Returns current relay tip height.
    fn relay_tip_height(&self) -> Result<u64>;

    /// Returns relay commitment hash at a given height.
    fn relay_commit_hash(&self, height: u64) -> Result<String>;

    /// Submit one raw Bitcoin block header (hex-encoded bytes).
    fn submit_header(&self, header_hex: &str) -> Result<String>;
}
