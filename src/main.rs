// Copyright (C) 2026 Utexo.
// See LICENSE for copying information.

//! Composition root: load config, run startup, construct gateways, start `run_sync_loop`.
//!
//! Flow is deliberately boring: load env, prove Bitcoin and EVM are reachable, then park in
//! `run_sync_loop` forever. If you want magic, look elsewhere; this is plumbing.

use anyhow::Result;
use btc_relayer::bitcoin_rpc::HttpBitcoinRpcClient;
use btc_relayer::configs::AppConfig;
use btc_relayer::evm_relay_contract_client::EvmRelayContractClient;
use btc_relayer::interfaces::{BitcoinRpcClient, BtcRelaySubmitter};
use btc_relayer::metrics;
use btc_relayer::persistence::JsonFileStateStore;
use btc_relayer::{startup, sync_engine};
use reqwest::blocking::Client;
use std::time::Duration;
use tracing::info;
use tracing_subscriber::EnvFilter;

/// Hook up `tracing` so operators can crank verbosity with `RUST_LOG` without recompiling.
fn init_logging() -> Result<()> {
    // `RUST_LOG` wins; if absent we default to `info` so production isn't drowned in debug spam.
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt()
        .with_env_filter(env_filter)
        .with_target(false)
        .with_level(true)
        .try_init()
        .map_err(|e| anyhow::anyhow!("failed to initialize tracing subscriber: {}", e))?;
    Ok(())
}

type StartupCheck = fn(&AppConfig) -> Result<()>;
type MetricsStarter = fn(&AppConfig) -> Result<()>;
type SyncRunner = fn(
    &dyn BitcoinRpcClient,
    &dyn BtcRelaySubmitter,
    u64,
    u64,
    u64,
    u64,
    &JsonFileStateStore,
) -> Result<()>;

fn run_app_with(
    cfg: AppConfig,
    start_metrics_exporter: MetricsStarter,
    run_startup_checks: StartupCheck,
    run_bitcoin_rpc_smoke_check: StartupCheck,
    run_evm_relay_read_check: StartupCheck,
    run_sync_loop: SyncRunner,
) -> Result<()> {
    start_metrics_exporter(&cfg)?;
    run_startup_checks(&cfg)?;
    run_bitcoin_rpc_smoke_check(&cfg)?;
    run_evm_relay_read_check(&cfg)?;

    info!("startup pipeline complete");

    // One HTTP client for Bitcoin RPC; timeout matches config so a stuck node doesn't hang the process forever.
    let bitcoin_http = Client::builder()
        .timeout(Duration::from_secs(cfg.bitcoin_rpc_timeout_secs))
        .build()?;
    let bitcoin = HttpBitcoinRpcClient::new(
        cfg.bitcoin_rpc_url.clone(),
        cfg.bitcoin_rpc_user.clone(),
        cfg.bitcoin_rpc_password.clone(),
        bitcoin_http,
    );
    let evm_relay_contract = EvmRelayContractClient::from_config(&cfg);
    // JSON checkpoint: nice for humans and TEEs that lose disk; sync logic still trusts on-chain tip first.
    let state_store = JsonFileStateStore::new(cfg.state_file_path.clone());

    run_sync_loop(
        &bitcoin,
        &evm_relay_contract,
        cfg.poll_interval_secs,
        cfg.start_height,
        cfg.catchup_batch_size,
        cfg.live_lag_threshold,
        &state_store,
    )?;

    Ok(())
}

fn main() -> Result<()> {
    dotenvy::dotenv().ok();
    init_logging()?;
    let cfg = AppConfig::load()?;
    run_app_with(
        cfg,
        start_metrics_exporter,
        startup::run_startup_checks,
        startup::run_bitcoin_rpc_smoke_check,
        startup::run_evm_relay_read_check,
        sync_engine::run_sync_loop,
    )
}

fn start_metrics_exporter(cfg: &AppConfig) -> Result<()> {
    metrics::start_exporter(cfg.metrics_bind_addr.as_str())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Mutex, OnceLock};

    static CAPTURED_ARGS: OnceLock<Mutex<Option<(u64, u64, u64, u64)>>> = OnceLock::new();

    fn no_op_metrics_start(_cfg: &AppConfig) -> Result<()> {
        Ok(())
    }

    fn no_op_startup_check(_cfg: &AppConfig) -> Result<()> {
        Ok(())
    }

    fn capturing_sync_runner(
        _bitcoin: &dyn BitcoinRpcClient,
        _submitter: &dyn BtcRelaySubmitter,
        poll_interval_secs: u64,
        start_height: u64,
        catchup_batch_size: u64,
        live_lag_threshold: u64,
        _state_store: &JsonFileStateStore,
    ) -> Result<()> {
        let slot = CAPTURED_ARGS.get_or_init(|| Mutex::new(None));
        *slot.lock().expect("capture lock") = Some((
            poll_interval_secs,
            start_height,
            catchup_batch_size,
            live_lag_threshold,
        ));
        Ok(())
    }

    fn test_config() -> AppConfig {
        AppConfig {
            bitcoin_rpc_url: "http://127.0.0.1:8332".to_string(),
            bitcoin_rpc_user: "user".to_string(),
            bitcoin_rpc_password: "pass".to_string(),
            bitcoin_rpc_timeout_secs: 10,
            bitcoin_ibd_poll_secs: 1,
            evm_rpc_url: "http://127.0.0.1:8545".to_string(),
            relay_contract_address: "0x1111111111111111111111111111111111111111".to_string(),
            relayer_private_key: "0x01".to_string(),
            evm_chain_id: 31337,
            evm_tx_confirmations: 1,
            evm_tx_timeout_secs: 30,
            evm_max_fee_gwei: None,
            evm_priority_fee_gwei: None,
            evm_low_balance_txs_left_warn: 50,
            poll_interval_secs: 7,
            start_height: 123,
            catchup_batch_size: 16,
            live_lag_threshold: 2,
            state_file_path: "artifacts/relay-state.json".to_string(),
            metrics_bind_addr: "127.0.0.1:9090".to_string(),
        }
    }

    #[test]
    fn run_app_with_builds_clients_and_forwards_loop_parameters() {
        let cfg = test_config();
        run_app_with(
            cfg,
            no_op_metrics_start,
            no_op_startup_check,
            no_op_startup_check,
            no_op_startup_check,
            capturing_sync_runner,
        )
        .expect("run_app_with should succeed");

        let captured = CAPTURED_ARGS
            .get()
            .expect("capture slot")
            .lock()
            .expect("capture lock")
            .expect("captured args");
        assert_eq!(captured, (7, 123, 16, 2));
    }
}
