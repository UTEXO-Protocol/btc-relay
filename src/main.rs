mod config;
mod interfaces;
mod sync_engine;
mod startup;

use anyhow::Result;
use env_logger::{Builder, Env};
use config::AppConfig;
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

    let cfg = AppConfig::load()?;
    startup::run_startup_checks(&cfg)?;

    info!("MVP skeleton ready: config, env loading, and startup checks are working");

    Ok(())
}
