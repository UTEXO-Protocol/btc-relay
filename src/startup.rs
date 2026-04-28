use anyhow::{Context, Result};
use reqwest::blocking::Client;
use std::thread;
use std::time::Duration;
use tracing::{info, warn};

use crate::bitcoin_rpc::HttpBitcoinRpcClient;
use crate::btc_relay_client::EvmBtcRelaySubmitter;
use crate::configs::AppConfig;
use crate::interfaces::{BitcoinRpcClient, BtcRelaySubmitter};

pub fn run_startup_checks(cfg: &AppConfig) -> Result<()> {
    cfg.validate()?;

    if cfg.start_height > 0 {
        warn!(start_height = cfg.start_height, "custom start height configured");
    } else {
        info!("start height is 0; relayer will auto-discover from chain state in later tasks");
    }

    info!(
        bitcoin_rpc_url = %cfg.bitcoin_rpc_url,
        evm_rpc_url = %cfg.evm_rpc_url,
        poll_interval_secs = cfg.poll_interval_secs,
        "startup config validated"
    );

    Ok(())
}

/// Blocks until bitcoind reports `initialblockdownload == false`, matching atomiq-relay
/// `waitForBitcoinRpc` behavior (poll while IBD or RPC errors).
pub fn wait_for_bitcoin_ibd_complete(rpc: &HttpBitcoinRpcClient, poll_secs: u64) {
    loop {
        match rpc.initial_block_download() {
            Ok(false) => {
                info!("bitcoin RPC ready: initial block download (IBD) finished");
                return;
            }
            Ok(true) => {
                info!(retry_in_secs = poll_secs, "bitcoin node is still in initial block download (IBD)");
                thread::sleep(Duration::from_secs(poll_secs));
            }
            Err(e) => {
                warn!(error = %e, retry_in_secs = poll_secs, "bitcoin RPC not ready during IBD check");
                thread::sleep(Duration::from_secs(poll_secs));
            }
        }
    }
}

pub fn run_bitcoin_rpc_smoke_check(cfg: &AppConfig) -> Result<()> {
    let http = Client::builder()
        .timeout(Duration::from_secs(cfg.bitcoin_rpc_timeout_secs))
        .build()
        .context("failed to build HTTP client for bitcoin rpc")?;

    let rpc = HttpBitcoinRpcClient::new(
        cfg.bitcoin_rpc_url.clone(),
        cfg.bitcoin_rpc_user.clone(),
        cfg.bitcoin_rpc_password.clone(),
        http,
    );

    wait_for_bitcoin_ibd_complete(&rpc, cfg.bitcoin_ibd_poll_secs);

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

    info!(tip_height, best_hash = %best_hash, header_hex_len = header_hex.len(), "bitcoin rpc smoke check passed");

    Ok(())
}

pub fn run_evm_relay_read_check(cfg: &AppConfig) -> Result<()> {
    let submitter = EvmBtcRelaySubmitter::from_config(cfg);

    let tip_height = submitter
        .relay_tip_height()
        .context("evm relay read check failed at relay_tip_height")?;
    let tip_commit_hash = submitter
        .relay_commit_hash(tip_height)
        .with_context(|| format!("evm relay read check failed at relay_commit_hash({})", tip_height))?;

    info!(tip_height, tip_commit_hash = %tip_commit_hash, "evm relay read check passed");

    Ok(())
}
