use anyhow::{Context, Result};
use log::{info, warn};
use reqwest::blocking::Client;

use crate::bitcoin_rpc::HttpBitcoinRpcClient;
use crate::configs::AppConfig;
use crate::interfaces::BitcoinRpcClient;

pub fn run_startup_checks(cfg: &AppConfig) -> Result<()> {
    cfg.validate()?;

    if cfg.start_height > 0 {
        warn!("custom start height configured: {}", cfg.start_height);
    } else {
        info!("start height is 0; relayer will auto-discover from chain state in later tasks");
    }

    info!(
        "startup config validated: bitcoin_rpc_url={}, evm_rpc_url={}, poll_interval_secs={}",
        cfg.bitcoin_rpc_url,
        cfg.evm_rpc_url,
        cfg.poll_interval_secs
    );

    Ok(())
}

pub fn run_bitcoin_rpc_smoke_check(cfg: &AppConfig) -> Result<()> {
    let http = Client::builder()
        .build()
        .context("failed to build HTTP client for bitcoin rpc")?;

    let rpc = HttpBitcoinRpcClient::new(
        cfg.bitcoin_rpc_url.clone(),
        cfg.bitcoin_rpc_user.clone(),
        cfg.bitcoin_rpc_password.clone(),
        http,
    );

    let tip_height = rpc
        .get_block_count()
        .context("bitcoin rpc smoke check failed at get_block_count (check node URL/auth)")?;
    let best_hash = rpc
        .get_best_block_hash()
        .context("bitcoin rpc smoke check failed at get_best_block_hash")?;
    let header_hex = rpc
        .get_block_header_hex(&best_hash)
        .with_context(|| {
            format!(
                "bitcoin rpc smoke check failed at get_block_header_hex for best hash {}",
                best_hash
            )
        })?;

    info!(
        "bitcoin rpc smoke check passed: tip_height={}, best_hash={}, header_hex_len={}",
        tip_height,
        best_hash,
        header_hex.len()
    );

    Ok(())
}
