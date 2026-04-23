mod bitcoin_rpc;
mod btc_relay_client;
mod interfaces;
mod sync_engine;
mod startup;
mod configs;

use anyhow::Result;
use env_logger::{Builder, Env};
use configs::AppConfig;
use log::info;

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

    Ok(())
}
