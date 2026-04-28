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
use env_logger::{Builder, Env};
use configs::AppConfig;
use log::info;
use reqwest::blocking::Client;
use std::time::Duration;
use persistence::JsonFileStateStore;

fn init_logging() {
    let env = Env::default().default_filter_or("info");
    Builder::from_env(env)
        .format_target(false)
        .init();
}

fn main() -> Result<()> {
    dotenvy::dotenv().ok();
    init_logging();

    // 1) Load config from environment.
    let cfg = AppConfig::load()?;
    // 2) Generic startup validation.
    startup::run_startup_checks(&cfg)?;
    // 3) Bitcoin RPC readiness + smoke checks.
    startup::run_bitcoin_rpc_smoke_check(&cfg)?;
    // 4) EVM relay read-only connectivity checks.
    startup::run_evm_relay_read_check(&cfg)?;

    info!("startup pipeline complete: config, bitcoin checks, and evm read checks are working");

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
        &state_store,
    )?;

    Ok(())
}
