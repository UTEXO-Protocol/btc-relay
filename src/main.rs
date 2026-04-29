mod bitcoin_rpc;
mod btc_relay_client;
mod interfaces;
mod sync_engine;
mod startup;
mod configs;
mod persistence;

use anyhow::Result;
use bitcoin_rpc::HttpBitcoinRpcClient;
use btc_relay_client::EvmBtcRelaySubmitter;
use configs::AppConfig;
use reqwest::blocking::Client;
use std::time::Duration;
use persistence::JsonFileStateStore;
use tracing::info;
use tracing_subscriber::EnvFilter;

fn init_logging() -> Result<()> {
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt()
        .with_env_filter(env_filter)
        .with_target(false)
        .with_level(true)
        .try_init()
        .map_err(|e| anyhow::anyhow!("failed to initialize tracing subscriber: {}", e))?;
    Ok(())
}

fn main() -> Result<()> {
    dotenvy::dotenv().ok();
    init_logging()?;

    // 1) Load config from environment.
    let cfg = AppConfig::load()?;
    // 2) Generic startup validation.
    startup::run_startup_checks(&cfg)?;
    // 3) Bitcoin RPC readiness + smoke checks.
    startup::run_bitcoin_rpc_smoke_check(&cfg)?;
    // 4) EVM relay read-only connectivity checks.
    startup::run_evm_relay_read_check(&cfg)?;

    info!("startup pipeline complete");

    let bitcoin_http = Client::builder()
        .timeout(Duration::from_secs(cfg.bitcoin_rpc_timeout_secs))
        .build()?;
    let bitcoin = HttpBitcoinRpcClient::new(
        cfg.bitcoin_rpc_url.clone(),
        cfg.bitcoin_rpc_user.clone(),
        cfg.bitcoin_rpc_password.clone(),
        bitcoin_http,
    );
    let submitter = EvmBtcRelaySubmitter::from_config(&cfg);
    let state_store = JsonFileStateStore::new(cfg.state_file_path.clone());

    sync_engine::run_sync_loop(
        &bitcoin,
        &submitter,
        cfg.poll_interval_secs,
        cfg.start_height,
        cfg.catchup_batch_size,
        cfg.live_lag_threshold,
        &state_store,
    )?;

    Ok(())
}
