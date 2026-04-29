use anyhow::{Context, Result};
use serde::Deserialize;
use config::{Config, Environment};

#[derive(Debug, Clone, Deserialize)]
pub struct AppConfig {
    pub bitcoin_rpc_url: String,
    pub bitcoin_rpc_user: String,
    pub bitcoin_rpc_password: String,
    #[serde(default = "default_bitcoin_rpc_timeout_secs")]
    pub bitcoin_rpc_timeout_secs: u64,
    /// Seconds between polls while bitcoind is in IBD (initial block download).
    #[serde(default = "default_bitcoin_ibd_poll_secs")]
    pub bitcoin_ibd_poll_secs: u64,
    pub evm_rpc_url: String,
    pub relay_contract_address: String,
    pub relayer_private_key: String,
    #[serde(default = "default_evm_chain_id")]
    pub evm_chain_id: u64,
    #[serde(default = "default_evm_tx_confirmations")]
    pub evm_tx_confirmations: u64,
    #[serde(default = "default_evm_tx_timeout_secs")]
    pub evm_tx_timeout_secs: u64,
    pub evm_max_fee_gwei: Option<u64>,
    pub evm_priority_fee_gwei: Option<u64>,
    pub poll_interval_secs: u64,
    pub start_height: u64,
    #[serde(default = "default_catchup_batch_size")]
    pub catchup_batch_size: u64,
    #[serde(default = "default_live_lag_threshold")]
    pub live_lag_threshold: u64,
    #[serde(default = "default_state_file_path")]
    pub state_file_path: String,
}

impl AppConfig {
    pub fn load() -> Result<Self> {
        let config = Config::builder()
            .add_source(Environment::default())
            .build()
            .context("failed to build config from environment")?;

        config
            .try_deserialize::<AppConfig>()
            .context("failed to deserialize environment config")
    }

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
    10
}

fn default_bitcoin_ibd_poll_secs() -> u64 {
    30
}

fn default_evm_chain_id() -> u64 {
    31337
}

fn default_evm_tx_confirmations() -> u64 {
    1
}

fn default_evm_tx_timeout_secs() -> u64 {
    120
}

fn default_state_file_path() -> String {
    "artifacts/relay-state.json".to_string()
}

fn default_catchup_batch_size() -> u64 {
    16
}

fn default_live_lag_threshold() -> u64 {
    2
}

fn is_valid_evm_address(value: &str) -> bool {
    value.len() == 42
        && value.starts_with("0x")
        && value.chars().skip(2).all(|c| c.is_ascii_hexdigit())
}
