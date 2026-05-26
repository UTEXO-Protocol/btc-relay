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
use crate::configs::AppConfig;
use crate::evm_relay_contract_client::EvmRelayContractClient;
use crate::interfaces::{BitcoinRpcClient, BtcRelaySubmitter};
use crate::metrics;

trait StartupBitcoinClient {
    fn initial_block_download(&self) -> Result<bool>;
    fn get_block_count(&self) -> Result<u64>;
    fn get_best_block_hash(&self) -> Result<String>;
    fn get_block_header_hex(&self, hash: &str) -> Result<String>;
}

impl StartupBitcoinClient for HttpBitcoinRpcClient {
    fn initial_block_download(&self) -> Result<bool> {
        HttpBitcoinRpcClient::initial_block_download(self)
    }

    fn get_block_count(&self) -> Result<u64> {
        BitcoinRpcClient::get_block_count(self)
    }

    fn get_best_block_hash(&self) -> Result<String> {
        BitcoinRpcClient::get_best_block_hash(self)
    }

    fn get_block_header_hex(&self, hash: &str) -> Result<String> {
        BitcoinRpcClient::get_block_header_hex(self, hash)
    }
}

trait StartupEvmRelayClient {
    fn relay_tip_height(&self) -> Result<u64>;
    fn relay_commit_hash(&self, height: u64) -> Result<String>;
    fn relayer_wallet_address(&self) -> Result<String>;
    fn relayer_wallet_balance_wei(&self) -> Result<alloy::primitives::U256>;
}

impl StartupEvmRelayClient for EvmRelayContractClient {
    fn relay_tip_height(&self) -> Result<u64> {
        BtcRelaySubmitter::relay_tip_height(self)
    }

    fn relay_commit_hash(&self, height: u64) -> Result<String> {
        BtcRelaySubmitter::relay_commit_hash(self, height)
    }

    fn relayer_wallet_address(&self) -> Result<String> {
        EvmRelayContractClient::relayer_wallet_address(self)
    }

    fn relayer_wallet_balance_wei(&self) -> Result<alloy::primitives::U256> {
        EvmRelayContractClient::relayer_wallet_balance_wei(self)
    }
}

/// Cheap config pass before network calls.
pub fn run_startup_checks(cfg: &AppConfig) -> Result<()> {
    if let Err(err) = cfg.validate() {
        metrics::observe_startup_check("config_validate", false);
        return Err(err);
    }
    metrics::observe_startup_check("config_validate", true);

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
#[allow(dead_code)]
pub fn wait_for_bitcoin_ibd_complete(rpc: &HttpBitcoinRpcClient, poll_secs: u64) {
    wait_for_bitcoin_ibd_complete_inner(rpc, poll_secs, |_dur| {
        thread::sleep(_dur);
    });
}

fn wait_for_bitcoin_ibd_complete_inner<C, F>(rpc: &C, poll_secs: u64, mut sleep_fn: F)
where
    C: StartupBitcoinClient,
    F: FnMut(Duration),
{
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
                sleep_fn(Duration::from_secs(poll_secs));
            }
            Err(e) => {
                warn!(error = %e, retry_in_secs = poll_secs, "bitcoin RPC not ready during IBD check");
                sleep_fn(Duration::from_secs(poll_secs));
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
    let result = run_bitcoin_rpc_smoke_check_with_client(cfg, &rpc);
    metrics::observe_startup_check("bitcoin_rpc_smoke_check", result.is_ok());
    result
}

fn run_bitcoin_rpc_smoke_check_with_client<C>(cfg: &AppConfig, rpc: &C) -> Result<()>
where
    C: StartupBitcoinClient,
{
    wait_for_bitcoin_ibd_complete_inner(rpc, cfg.bitcoin_ibd_poll_secs, thread::sleep);

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
    let result = run_evm_relay_read_check_with_client(&evm_relay_contract);
    metrics::observe_startup_check("evm_relay_read_check", result.is_ok());
    result
}

fn run_evm_relay_read_check_with_client<C>(evm_relay_contract: &C) -> Result<()>
where
    C: StartupEvmRelayClient,
{
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
    let wallet_address = evm_relay_contract
        .relayer_wallet_address()
        .context("evm relay read check failed at relayer_wallet_address")?;
    let wallet_balance_wei = evm_relay_contract
        .relayer_wallet_balance_wei()
        .context("evm relay read check failed at relayer_wallet_balance_wei")?;
    let wallet_balance_wei_f64 = wallet_balance_wei.to_string().parse::<f64>().unwrap_or(0.0);
    let wallet_balance_eth = wallet_balance_wei_f64 / 1_000_000_000_000_000_000_f64;
    metrics::set_relayer_wallet_balance(wallet_balance_wei_f64, wallet_balance_eth);

    info!(
        tip_height,
        tip_commit_hash = %tip_commit_hash,
        wallet_address = %wallet_address,
        wallet_balance_wei = %wallet_balance_wei,
        wallet_balance_eth,
        "evm relay read check passed"
    );

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::RefCell;
    use std::collections::VecDeque;
    use std::rc::Rc;

    struct FakeBitcoinClient {
        ibd_sequence: RefCell<VecDeque<Result<bool>>>,
        block_count: u64,
        best_hash: String,
        header_hex: String,
        header_requests: RefCell<Vec<String>>,
    }

    impl StartupBitcoinClient for FakeBitcoinClient {
        fn initial_block_download(&self) -> Result<bool> {
            self.ibd_sequence
                .borrow_mut()
                .pop_front()
                .unwrap_or(Ok(false))
        }

        fn get_block_count(&self) -> Result<u64> {
            Ok(self.block_count)
        }

        fn get_best_block_hash(&self) -> Result<String> {
            Ok(self.best_hash.clone())
        }

        fn get_block_header_hex(&self, hash: &str) -> Result<String> {
            self.header_requests.borrow_mut().push(hash.to_string());
            Ok(self.header_hex.clone())
        }
    }

    struct FakeEvmClient {
        tip_height: u64,
        commit_hash: String,
        commit_height_requests: RefCell<Vec<u64>>,
        wallet_address: String,
        wallet_balance_wei: alloy::primitives::U256,
    }

    impl StartupEvmRelayClient for FakeEvmClient {
        fn relay_tip_height(&self) -> Result<u64> {
            Ok(self.tip_height)
        }

        fn relay_commit_hash(&self, height: u64) -> Result<String> {
            self.commit_height_requests.borrow_mut().push(height);
            Ok(self.commit_hash.clone())
        }

        fn relayer_wallet_address(&self) -> Result<String> {
            Ok(self.wallet_address.clone())
        }

        fn relayer_wallet_balance_wei(&self) -> Result<alloy::primitives::U256> {
            Ok(self.wallet_balance_wei)
        }
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
            poll_interval_secs: 5,
            start_height: 0,
            catchup_batch_size: 16,
            live_lag_threshold: 2,
            state_file_path: "artifacts/relay-state.json".to_string(),
            metrics_bind_addr: "127.0.0.1:9090".to_string(),
        }
    }

    #[test]
    fn ibd_wait_retries_until_false() {
        let rpc = FakeBitcoinClient {
            ibd_sequence: RefCell::new(VecDeque::from(vec![Ok(true), Ok(true), Ok(false)])),
            block_count: 100,
            best_hash: "hash".to_string(),
            header_hex: "00".repeat(80),
            header_requests: RefCell::new(Vec::new()),
        };
        let sleeps = Rc::new(RefCell::new(Vec::new()));
        let sleeps_ref = Rc::clone(&sleeps);
        wait_for_bitcoin_ibd_complete_inner(&rpc, 3, move |dur| {
            sleeps_ref.borrow_mut().push(dur.as_secs());
        });
        assert_eq!(&*sleeps.borrow(), &[3, 3]);
    }

    #[test]
    fn ibd_wait_retries_after_error_then_succeeds() {
        let rpc = FakeBitcoinClient {
            ibd_sequence: RefCell::new(VecDeque::from(vec![
                Err(anyhow::anyhow!("rpc down")),
                Ok(false),
            ])),
            block_count: 100,
            best_hash: "hash".to_string(),
            header_hex: "00".repeat(80),
            header_requests: RefCell::new(Vec::new()),
        };
        let sleeps = Rc::new(RefCell::new(Vec::new()));
        let sleeps_ref = Rc::clone(&sleeps);
        wait_for_bitcoin_ibd_complete_inner(&rpc, 5, move |dur| {
            sleeps_ref.borrow_mut().push(dur.as_secs());
        });
        assert_eq!(&*sleeps.borrow(), &[5]);
    }

    #[test]
    fn bitcoin_smoke_check_runs_core_calls_after_ibd_wait() {
        let cfg = test_config();
        let rpc = FakeBitcoinClient {
            ibd_sequence: RefCell::new(VecDeque::from(vec![Ok(false)])),
            block_count: 42,
            best_hash: "best".to_string(),
            header_hex: "00".repeat(80),
            header_requests: RefCell::new(Vec::new()),
        };
        run_bitcoin_rpc_smoke_check_with_client(&cfg, &rpc).expect("smoke check");
        assert_eq!(&*rpc.header_requests.borrow(), &["best".to_string()]);
    }

    #[test]
    fn evm_read_check_uses_tip_height_for_commit_lookup() {
        let evm = FakeEvmClient {
            tip_height: 77,
            commit_hash: "0x00".to_string(),
            commit_height_requests: RefCell::new(Vec::new()),
            wallet_address: "0x1111111111111111111111111111111111111111".to_string(),
            wallet_balance_wei: alloy::primitives::U256::from(1_u64),
        };
        run_evm_relay_read_check_with_client(&evm).expect("evm read check");
        assert_eq!(&*evm.commit_height_requests.borrow(), &[77]);
    }

    #[test]
    fn bitcoin_smoke_check_propagates_block_count_failure() {
        let cfg = test_config();
        struct FailingBitcoin;
        impl StartupBitcoinClient for FailingBitcoin {
            fn initial_block_download(&self) -> Result<bool> {
                Ok(false)
            }
            fn get_block_count(&self) -> Result<u64> {
                anyhow::bail!("count unavailable");
            }
            fn get_best_block_hash(&self) -> Result<String> {
                Ok("best".to_string())
            }
            fn get_block_header_hex(&self, _hash: &str) -> Result<String> {
                Ok("00".repeat(80))
            }
        }

        let err = run_bitcoin_rpc_smoke_check_with_client(&cfg, &FailingBitcoin)
            .expect_err("expected smoke check failure");
        assert!(err.to_string().contains("get_block_count"));
    }

    #[test]
    fn evm_read_check_propagates_commit_hash_failure() {
        struct FailingEvm;
        impl StartupEvmRelayClient for FailingEvm {
            fn relay_tip_height(&self) -> Result<u64> {
                Ok(5)
            }
            fn relay_commit_hash(&self, _height: u64) -> Result<String> {
                anyhow::bail!("commit hash read failed");
            }
            fn relayer_wallet_address(&self) -> Result<String> {
                Ok("0x1111111111111111111111111111111111111111".to_string())
            }
            fn relayer_wallet_balance_wei(&self) -> Result<alloy::primitives::U256> {
                Ok(alloy::primitives::U256::from(1_u64))
            }
        }

        let err =
            run_evm_relay_read_check_with_client(&FailingEvm).expect_err("expected read failure");
        assert!(err.to_string().contains("relay_commit_hash(5)"));
    }

    #[test]
    fn run_startup_checks_propagates_config_validation_failure() {
        let mut cfg = test_config();
        cfg.evm_chain_id = 0;
        let err = run_startup_checks(&cfg).expect_err("expected config validation failure");
        assert!(err.to_string().contains("EVM_CHAIN_ID must be > 0"));
    }

    #[test]
    fn run_startup_checks_accepts_nonzero_start_height_override() {
        let mut cfg = test_config();
        cfg.start_height = 12345;
        run_startup_checks(&cfg).expect("nonzero start height should still validate");
    }

    #[test]
    fn bitcoin_smoke_check_propagates_best_hash_failure() {
        let cfg = test_config();
        struct FailingBestHash;
        impl StartupBitcoinClient for FailingBestHash {
            fn initial_block_download(&self) -> Result<bool> {
                Ok(false)
            }
            fn get_block_count(&self) -> Result<u64> {
                Ok(100)
            }
            fn get_best_block_hash(&self) -> Result<String> {
                anyhow::bail!("best hash unavailable");
            }
            fn get_block_header_hex(&self, _hash: &str) -> Result<String> {
                Ok("00".repeat(80))
            }
        }
        let err = run_bitcoin_rpc_smoke_check_with_client(&cfg, &FailingBestHash)
            .expect_err("expected best hash failure");
        assert!(err.to_string().contains("get_best_block_hash"));
    }

    #[test]
    fn bitcoin_smoke_check_propagates_header_fetch_failure() {
        let cfg = test_config();
        struct FailingHeaderFetch;
        impl StartupBitcoinClient for FailingHeaderFetch {
            fn initial_block_download(&self) -> Result<bool> {
                Ok(false)
            }
            fn get_block_count(&self) -> Result<u64> {
                Ok(100)
            }
            fn get_best_block_hash(&self) -> Result<String> {
                Ok("best".to_string())
            }
            fn get_block_header_hex(&self, _hash: &str) -> Result<String> {
                anyhow::bail!("header fetch failed");
            }
        }
        let err = run_bitcoin_rpc_smoke_check_with_client(&cfg, &FailingHeaderFetch)
            .expect_err("expected header fetch failure");
        assert!(err.to_string().contains("get_block_header_hex for best hash best"));
    }

    #[test]
    fn evm_read_check_propagates_tip_height_failure() {
        struct FailingTipHeight;
        impl StartupEvmRelayClient for FailingTipHeight {
            fn relay_tip_height(&self) -> Result<u64> {
                anyhow::bail!("tip height read failed");
            }
            fn relay_commit_hash(&self, _height: u64) -> Result<String> {
                Ok("0x00".to_string())
            }
            fn relayer_wallet_address(&self) -> Result<String> {
                Ok("0x1111111111111111111111111111111111111111".to_string())
            }
            fn relayer_wallet_balance_wei(&self) -> Result<alloy::primitives::U256> {
                Ok(alloy::primitives::U256::from(1_u64))
            }
        }
        let err = run_evm_relay_read_check_with_client(&FailingTipHeight)
            .expect_err("expected tip height failure");
        assert!(err.to_string().contains("relay_tip_height"));
    }

    #[test]
    fn bitcoin_smoke_check_retries_ibd_then_runs_rpc_calls() {
        let mut cfg = test_config();
        cfg.bitcoin_ibd_poll_secs = 0;
        let rpc = FakeBitcoinClient {
            ibd_sequence: RefCell::new(VecDeque::from(vec![Ok(true), Ok(false)])),
            block_count: 200,
            best_hash: "best-hash".to_string(),
            header_hex: "00".repeat(80),
            header_requests: RefCell::new(Vec::new()),
        };
        run_bitcoin_rpc_smoke_check_with_client(&cfg, &rpc).expect("smoke check should succeed");
        assert_eq!(
            &*rpc.header_requests.borrow(),
            &["best-hash".to_string()],
            "smoke check should proceed to header fetch after IBD clears"
        );
    }

    #[test]
    fn bitcoin_smoke_check_retries_after_ibd_error_then_succeeds() {
        let mut cfg = test_config();
        cfg.bitcoin_ibd_poll_secs = 0;
        let rpc = FakeBitcoinClient {
            ibd_sequence: RefCell::new(VecDeque::from(vec![
                Err(anyhow::anyhow!("temporary rpc error")),
                Ok(false),
            ])),
            block_count: 333,
            best_hash: "best-after-error".to_string(),
            header_hex: "00".repeat(80),
            header_requests: RefCell::new(Vec::new()),
        };
        run_bitcoin_rpc_smoke_check_with_client(&cfg, &rpc)
            .expect("smoke check should recover from temporary IBD error");
        assert_eq!(&*rpc.header_requests.borrow(), &["best-after-error".to_string()]);
    }
}
