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
    pub evm_rpc_url: String,
    pub relay_contract_address: String,
    pub relayer_private_key: String,
    pub poll_interval_secs: u64,
    pub start_height: u64,
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
        if self.bitcoin_rpc_user.trim().is_empty() {
            anyhow::bail!("BITCOIN_RPC_USER is required");
        }
        if self.bitcoin_rpc_password.trim().is_empty() {
            anyhow::bail!("BITCOIN_RPC_PASSWORD is required");
        }
        if self.bitcoin_rpc_timeout_secs <= 0 {
            anyhow::bail!("BITCOIN_RPC_TIMEOUT_SECS must be > 0");
        }
        if !self.evm_rpc_url.starts_with("http") {
            anyhow::bail!("EVM_RPC_URL must start with http/https");
        }
        if self.relay_contract_address.trim().is_empty() {
            anyhow::bail!("RELAY_CONTRACT_ADDRESS is required");
        }
        if self.relayer_private_key.trim().is_empty() {
            anyhow::bail!("RELAYER_PRIVATE_KEY is required");
        }
        if self.poll_interval_secs <= 0 {
            anyhow::bail!("POLL_INTERVAL_SECS must be > 0");
        }

        Ok(())
    }
}

fn default_bitcoin_rpc_timeout_secs() -> u64 {
    10
}

