// Copyright (C) 2026 Utexo.
// See LICENSE for copying information.

//! Pre-flight checks: validate config and prove Bitcoin + EVM relay are reachable (no header submission).
//!
//! Nothing here submits a header — it's all read-only checks except burning CPU waiting on IBD.

use anyhow::{Context, Result};
use reqwest::blocking::Client;
use std::thread;
use std::time::Duration;
use tracing::{info, warn};

use crate::bitcoin_rpc::HttpBitcoinRpcClient;
use crate::evm_relay_contract_client::EvmRelayContractClient;
use crate::configs::AppConfig;
use crate::interfaces::{BitcoinRpcClient, BtcRelaySubmitter};

/// Cheap config pass before network calls.
pub fn run_startup_checks(cfg: &AppConfig) -> Result<()> {
    cfg.validate()?;

    if cfg.start_height > 0 {
        // Non-zero START_HEIGHT is an operator override, not normal steady-state behavior.
        warn!(
            start_height = cfg.start_height,
            "custom start height configured"
        );
    } else {
        info!("start height is 0; relayer will auto-discover from chain state in later tasks");
    }

    // Log enough startup context to debug miswired deployments without printing secrets.
    info!(
        bitcoin_rpc_url = %cfg.bitcoin_rpc_url,
        evm_rpc_url = %cfg.evm_rpc_url,
        poll_interval_secs = cfg.poll_interval_secs,
        "startup config validated"
    );

    Ok(())
}

/// Spin until `initialblockdownload` is false. Yes, this blocks the main thread — startup is allowed to be dumb.
/// If RPC errors, we log and retry; a node that's down looks the same as one still syncing from our POV.
pub fn wait_for_bitcoin_ibd_complete(rpc: &HttpBitcoinRpcClient, poll_secs: u64) {
    loop {
        match rpc.initial_block_download() {
            Ok(false) => {
                info!("bitcoin RPC ready: initial block download (IBD) finished");
                return;
            }
            Ok(true) => {
                info!(
                    retry_in_secs = poll_secs,
                    "bitcoin node is still in initial block download (IBD)"
                );
                thread::sleep(Duration::from_secs(poll_secs));
            }
            Err(e) => {
                warn!(error = %e, retry_in_secs = poll_secs, "bitcoin RPC not ready during IBD check");
                thread::sleep(Duration::from_secs(poll_secs));
            }
        }
    }
}

/// Prove we can actually talk to Bitcoin: tip height, best hash, header for that hash. Catches auth and URL typos early.
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

    // Probe three methods so auth, route, and result-shape issues all fail before the loop starts.
    let tip_height = rpc
        .get_block_count()
        .context("bitcoin rpc smoke check failed at get_block_count (check node URL/auth)")?;
    let best_hash = rpc
        .get_best_block_hash()
        .context("bitcoin rpc smoke check failed at get_best_block_hash")?;
    let header_hex = rpc.get_block_header_hex(&best_hash).with_context(|| {
        format!(
            "bitcoin rpc smoke check failed at get_block_header_hex for best hash {}",
            best_hash
        )
    })?;

    info!(tip_height, best_hash = %best_hash, header_hex_len = header_hex.len(), "bitcoin rpc smoke check passed");

    Ok(())
}

/// `eth_call` the relay at tip: `getBlockheight` + `getCommitHash(tip)`. No wallet spend, just proves ABI/RPC/address line up.
pub fn run_evm_relay_read_check(cfg: &AppConfig) -> Result<()> {
    let evm_relay_contract = EvmRelayContractClient::from_config(cfg);

    // Same pattern as Bitcoin smoke check: one tip getter + one value-at-tip getter.
    let tip_height = evm_relay_contract
        .relay_tip_height()
        .context("evm relay read check failed at relay_tip_height")?;
    let tip_commit_hash = evm_relay_contract.relay_commit_hash(tip_height).with_context(|| {
        format!(
            "evm relay read check failed at relay_commit_hash({})",
            tip_height
        )
    })?;

    info!(tip_height, tip_commit_hash = %tip_commit_hash, "evm relay read check passed");

    Ok(())
}
