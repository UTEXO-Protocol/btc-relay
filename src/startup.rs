use anyhow::Result;
use log::{info, warn};

use crate::config::AppConfig;

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
