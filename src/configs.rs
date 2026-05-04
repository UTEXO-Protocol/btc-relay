//! All knobs come from the environment. No secret config files, no YAML ceremony — if it's not
//! in the process env, it doesn't exist. Field names map 1:1 to env vars via `serde` + `config` crate.

use anyhow::{Context, Result};
use config::{Config, Environment};
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct AppConfig {
    /// `bitcoind` HTTP JSON-RPC endpoint (must be `http` or `https`).
    pub bitcoin_rpc_url: String,
    /// Basic-auth user; leave **both** user and password empty if the key lives in the URL (hosted RPC).
    pub bitcoin_rpc_user: String,
    pub bitcoin_rpc_password: String,
    #[serde(default = "default_bitcoin_rpc_timeout_secs")]
    /// Per-request HTTP timeout for Bitcoin RPC. Stops one dead TCP session from wedging the relayer.
    pub bitcoin_rpc_timeout_secs: u64,
    #[serde(default = "default_bitcoin_ibd_poll_secs")]
    /// How long we sleep between `getblockchaininfo` polls while the node is still syncing (IBD).
    pub bitcoin_ibd_poll_secs: u64,
    /// EVM JSON-RPC URL (Alchemy, local Anvil, whatever — must speak Ethereum JSON-RPC).
    pub evm_rpc_url: String,
    /// Relay contract `0x…` address. This is the thing we're feeding headers into.
    pub relay_contract_address: String,
    /// Hex private key for EIP-1559 txs. Yes, env var is a rubbish secrets story; fix your deployment if that bothers you.
    pub relayer_private_key: String,
    #[serde(default = "default_evm_chain_id")]
    /// Chain ID for signing. Must match the network or your txs are garbage.
    pub evm_chain_id: u64,
    #[serde(default = "default_evm_tx_confirmations")]
    /// How deep we wait on L2/L1 before we call a submission "done".
    pub evm_tx_confirmations: u64,
    #[serde(default = "default_evm_tx_timeout_secs")]
    /// Wall-clock cap while polling receipts; avoids infinite spin on a dropped tx.
    pub evm_tx_timeout_secs: u64,
    /// Optional EIP-1559 `maxFeePerGas` in gwei; `None` lets the wallet/node guess.
    pub evm_max_fee_gwei: Option<u64>,
    /// Optional `maxPriorityFeePerGas` in gwei.
    pub evm_priority_fee_gwei: Option<u64>,
    /// Sleep between sync loop iterations when we're caught up or idle.
    pub poll_interval_secs: u64,
    /// Bootstrap override: first height to consider when **no** JSON state file exists. After that, on-chain tip wins.
    pub start_height: u64,
    #[serde(default = "default_catchup_batch_size")]
    /// Max headers per `submitMainBlockheaders` batch while we're far behind ("catch-up mode").
    pub catchup_batch_size: u64,
    #[serde(default = "default_live_lag_threshold")]
    /// When `bitcoin_tip - relay_tip` is at or below this, submit **one** header per tx ("live" tail).
    pub live_lag_threshold: u64,
    #[serde(default = "default_state_file_path")]
    /// Where we dump last-submitted height/hash JSON for operators (not the authority for resume — contract is).
    pub state_file_path: String,
}

impl AppConfig {
    /// Slurp env into `AppConfig`. Fails if required vars are missing — good, fail at startup not in the loop.
    pub fn load() -> Result<Self> {
        let config = Config::builder()
            .add_source(Environment::default())
            .build()
            .context("failed to build config from environment")?;

        config
            .try_deserialize::<AppConfig>()
            .context("failed to deserialize environment config")
    }

    /// Sanity checks only: URLs shaped right, numbers non-zero where that would be nonsense, auth pairing consistent.
    pub fn validate(&self) -> Result<()> {
        if !self.bitcoin_rpc_url.starts_with("http") {
            anyhow::bail!("BITCOIN_RPC_URL must start with http/https");
        }
        let bitcoin_rpc_user_empty = self.bitcoin_rpc_user.trim().is_empty();
        let bitcoin_rpc_password_empty = self.bitcoin_rpc_password.trim().is_empty();
        if bitcoin_rpc_user_empty ^ bitcoin_rpc_password_empty {
            anyhow::bail!(
                "BITCOIN_RPC_USER and BITCOIN_RPC_PASSWORD must be both set or both empty"
            );
        }
        if self.bitcoin_rpc_timeout_secs <= 0 {
            anyhow::bail!("BITCOIN_RPC_TIMEOUT_SECS must be > 0");
        }
        if self.bitcoin_ibd_poll_secs == 0 {
            anyhow::bail!("BITCOIN_IBD_POLL_SECS must be > 0");
        }
        if !self.evm_rpc_url.starts_with("http") {
            anyhow::bail!("EVM_RPC_URL must start with http/https");
        }
        if self.relay_contract_address.trim().is_empty() {
            anyhow::bail!("RELAY_CONTRACT_ADDRESS is required");
        }
        if !is_valid_evm_address(&self.relay_contract_address) {
            anyhow::bail!("RELAY_CONTRACT_ADDRESS must be a valid 0x-prefixed 20-byte hex address");
        }
        if self.relayer_private_key.trim().is_empty() {
            anyhow::bail!("RELAYER_PRIVATE_KEY is required");
        }
        if self.evm_chain_id == 0 {
            anyhow::bail!("EVM_CHAIN_ID must be > 0");
        }
        if self.evm_tx_confirmations == 0 {
            anyhow::bail!("EVM_TX_CONFIRMATIONS must be > 0");
        }
        if self.evm_tx_timeout_secs == 0 {
            anyhow::bail!("EVM_TX_TIMEOUT_SECS must be > 0");
        }
        if self.poll_interval_secs <= 0 {
            anyhow::bail!("POLL_INTERVAL_SECS must be > 0");
        }
        if self.catchup_batch_size == 0 {
            anyhow::bail!("CATCHUP_BATCH_SIZE must be > 0");
        }
        if self.state_file_path.trim().is_empty() {
            anyhow::bail!("STATE_FILE_PATH must be non-empty");
        }

        Ok(())
    }
}

fn default_bitcoin_rpc_timeout_secs() -> u64 {
    10 // seconds; enough for LAN, maybe tight on satellite — override if you know your network.
}

fn default_bitcoin_ibd_poll_secs() -> u64 {
    30 // don't hammer `getblockchaininfo` while the node grinds through IBD.
}

fn default_evm_chain_id() -> u64 {
    31337 // classic local/Hardhat default; **override in real deployments**.
}

fn default_evm_tx_confirmations() -> u64 {
    1 // MVP default; L1 users probably want more.
}

fn default_evm_tx_timeout_secs() -> u64 {
    120 // receipt polling budget.
}

fn default_state_file_path() -> String {
    "artifacts/relay-state.json".to_string()
}

fn default_catchup_batch_size() -> u64 {
    16 // trade gas vs round-trips; contract limits may force you lower.
}

fn default_live_lag_threshold() -> u64 {
    2 // within this many blocks of tip → single-header txs.
}

/// Cheap `0x` + 20-byte hex check. Not checksum-validated — we're not doing UX here.
fn is_valid_evm_address(value: &str) -> bool {
    value.len() == 42
        && value.starts_with("0x")
        && value.chars().skip(2).all(|c| c.is_ascii_hexdigit())
}
