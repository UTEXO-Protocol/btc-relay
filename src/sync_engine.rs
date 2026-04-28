use anyhow::{Context, Result};
use log::info;
use std::thread;
use std::time::Duration;

use crate::interfaces::{BitcoinRpcClient, BtcRelaySubmitter};

/// High-level lifecycle for the relayer sync process.
#[allow(dead_code)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncEngineState {
    Idle,
    AwaitingBitcoinRpc,
    CatchingUp,
    Submitting,
    WaitingConfirmations,
    Active,
    RetryBackoff,
    Error,
}

/// Source of a sync attempt.
#[allow(dead_code)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncTrigger {
    Startup,
    PollTick,
    ZmqNewBlock,
    Manual,
    RetryTimer,
}

/// Sync result used by loop orchestration and logs.
#[allow(dead_code)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncResult {
    UpToDate,
    Progressed,
    ReorgDetected,
    TemporaryFailure,
}

/// Runtime state carried by the sync orchestrator loop.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SyncLoopState {
    pub state: SyncEngineState,
    pub poll_count: u64,
    pub start_height: u64,
}

impl SyncLoopState {
    fn new(start_height: u64) -> Self {
        Self {
            state: SyncEngineState::Idle,
            poll_count: 0,
            start_height,
        }
    }
}

/// Loop orchestrator entrypoint.
///
/// Runs an infinite poll/sync cycle and leaves per-cycle behavior to `run_poll_cycle`
pub fn run_sync_loop(
    bitcoin: &dyn BitcoinRpcClient,
    submitter: &dyn BtcRelaySubmitter,
    poll_interval_secs: u64,
    start_height: u64,
) -> Result<()> {
    let poll_interval = Duration::from_secs(poll_interval_secs.max(1));
    let mut loop_state = SyncLoopState::new(start_height);

    info!(
        "sync loop started: poll_interval_secs={}, start_height={}",
        poll_interval.as_secs(),
        start_height
    );

    loop {
        loop_state.poll_count = loop_state.poll_count.saturating_add(1);
        let trigger = if loop_state.poll_count == 1 {
            SyncTrigger::Startup
        } else {
            SyncTrigger::PollTick
        };

        let cycle_result = run_poll_cycle(bitcoin, submitter, trigger, &mut loop_state)?;
        info!(
            "sync poll cycle complete: poll_count={}, state={:?}, result={:?}",
            loop_state.poll_count, loop_state.state, cycle_result
        );

        thread::sleep(poll_interval);
    }
}

fn run_poll_cycle(
    bitcoin: &dyn BitcoinRpcClient,
    submitter: &dyn BtcRelaySubmitter,
    trigger: SyncTrigger,
    loop_state: &mut SyncLoopState,
) -> Result<SyncResult> {
    loop_state.state = SyncEngineState::Active;

    let bitcoin_tip = bitcoin.get_block_count()?;
    let relay_tip = submitter.relay_tip_height()?;
    let lag = bitcoin_tip.saturating_sub(relay_tip);

    info!(
        "tip discovery: trigger={:?}, bitcoin_tip={}, relay_tip={}, lag={}",
        trigger, bitcoin_tip, relay_tip, lag
    );

    if relay_tip >= bitcoin_tip {
        info!(
            "sync is up to date: relay_tip={} >= bitcoin_tip={}, nothing to submit this cycle",
            relay_tip, bitcoin_tip
        );
        return Ok(SyncResult::UpToDate);
    }

    let (from_height, to_height) = compute_catchup_range(relay_tip, bitcoin_tip, loop_state.start_height)
        .context("failed to calculate catch-up range")?;

    loop_state.state = SyncEngineState::CatchingUp;
    let submitted = process_catchup_range(bitcoin, submitter, from_height, to_height, loop_state)?;
    info!(
        "relay behind bitcoin tip: synced range {}..={}, submitted_headers={}, lag={}",
        from_height,
        to_height,
        submitted,
        lag
    );
    Ok(SyncResult::Progressed)
}

fn process_catchup_range(
    bitcoin: &dyn BitcoinRpcClient,
    submitter: &dyn BtcRelaySubmitter,
    from_height: u64,
    to_height: u64,
    loop_state: &mut SyncLoopState,
) -> Result<u64> {
    let mut submitted = 0_u64;

    for height in from_height..=to_height {
        loop_state.state = SyncEngineState::Submitting;
        let block_hash = bitcoin
            .get_block_hash(height)
            .with_context(|| format!("failed get_block_hash at height {}", height))?;
        let header_hex = bitcoin
            .get_block_header_hex(&block_hash)
            .with_context(|| format!("failed get_block_header_hex at height {} hash {}", height, block_hash))?;

        info!(
            "submitting header: height={}, hash={}, header_hex_len={}",
            height,
            block_hash,
            header_hex.len()
        );

        loop_state.state = SyncEngineState::WaitingConfirmations;
        let tx_hash = submitter
            .submit_header(&header_hex)
            .with_context(|| format!("failed submit_header at height {}", height))?;

        submitted = submitted.saturating_add(1);
        info!(
            "header submitted: height={}, tx_hash={}, progress={}/{}",
            height,
            tx_hash,
            submitted,
            to_height.saturating_sub(from_height).saturating_add(1)
        );
    }

    Ok(submitted)
}

fn compute_catchup_range(relay_tip: u64, bitcoin_tip: u64, start_height: u64) -> Result<(u64, u64)> {
    if relay_tip >= bitcoin_tip {
        anyhow::bail!(
            "relay is already up to date or ahead (relay_tip={}, bitcoin_tip={})",
            relay_tip,
            bitcoin_tip
        );
    }

    let next_from_relay = relay_tip.saturating_add(1);
    let from_height = if start_height > 0 {
        start_height.max(next_from_relay)
    } else {
        next_from_relay
    };

    if from_height > bitcoin_tip {
        anyhow::bail!(
            "computed catch-up start {} is above bitcoin tip {}",
            from_height,
            bitcoin_tip
        );
    }

    Ok((from_height, bitcoin_tip))
}

#[cfg(test)]
mod tests {
    use super::{compute_catchup_range, process_catchup_range};
    use crate::interfaces::{BitcoinRpcClient, BtcRelaySubmitter};
    use anyhow::Result;
    use std::cell::RefCell;

    #[test]
    fn catchup_range_uses_relay_tip_plus_one_when_no_start_override() {
        let (from, to) = compute_catchup_range(100, 105, 0).expect("range");
        assert_eq!((from, to), (101, 105));
    }

    #[test]
    fn catchup_range_respects_start_height_override_when_higher_than_relay_next() {
        let (from, to) = compute_catchup_range(100, 120, 110).expect("range");
        assert_eq!((from, to), (110, 120));
    }

    #[test]
    fn catchup_range_uses_relay_next_when_start_override_is_lower() {
        let (from, to) = compute_catchup_range(100, 120, 90).expect("range");
        assert_eq!((from, to), (101, 120));
    }

    #[test]
    fn catchup_range_rejects_up_to_date_or_ahead_relay() {
        let err = compute_catchup_range(120, 120, 0).expect_err("expected error");
        assert!(err.to_string().contains("up to date or ahead"));
    }

    struct FakeBitcoinRpc {
        hash_calls: RefCell<Vec<u64>>,
        header_calls: RefCell<Vec<String>>,
    }

    impl FakeBitcoinRpc {
        fn new() -> Self {
            Self {
                hash_calls: RefCell::new(Vec::new()),
                header_calls: RefCell::new(Vec::new()),
            }
        }
    }

    impl BitcoinRpcClient for FakeBitcoinRpc {
        fn get_block_count(&self) -> Result<u64> {
            Ok(0)
        }

        fn get_block_hash(&self, height: u64) -> Result<String> {
            self.hash_calls.borrow_mut().push(height);
            Ok(format!("hash-{height}"))
        }

        fn get_best_block_hash(&self) -> Result<String> {
            Ok("best-hash".to_string())
        }

        fn get_block_header_hex(&self, hash: &str) -> Result<String> {
            self.header_calls.borrow_mut().push(hash.to_string());
            Ok(format!("header-for-{hash}"))
        }
    }

    struct FakeSubmitter {
        submitted_headers: RefCell<Vec<String>>,
    }

    impl FakeSubmitter {
        fn new() -> Self {
            Self {
                submitted_headers: RefCell::new(Vec::new()),
            }
        }
    }

    impl BtcRelaySubmitter for FakeSubmitter {
        fn relay_tip_height(&self) -> Result<u64> {
            Ok(0)
        }

        fn relay_commit_hash(&self, _height: u64) -> Result<String> {
            Ok("0x00".to_string())
        }

        fn submit_header(&self, header_hex: &str) -> Result<String> {
            self.submitted_headers
                .borrow_mut()
                .push(header_hex.to_string());
            Ok(format!("0xtx{}", self.submitted_headers.borrow().len()))
        }
    }

    #[test]
    fn catchup_pipeline_submits_headers_in_sequential_height_order() {
        let bitcoin = FakeBitcoinRpc::new();
        let submitter = FakeSubmitter::new();

        let mut loop_state = super::SyncLoopState::new(0);
        let submitted = process_catchup_range(&bitcoin, &submitter, 3, 5, &mut loop_state)
            .expect("pipeline");
        assert_eq!(submitted, 3);
        assert_eq!(loop_state.state, super::SyncEngineState::WaitingConfirmations);
        assert_eq!(bitcoin.hash_calls.borrow().as_slice(), &[3, 4, 5]);
        assert_eq!(
            bitcoin.header_calls.borrow().as_slice(),
            &["hash-3".to_string(), "hash-4".to_string(), "hash-5".to_string()]
        );
        assert_eq!(
            submitter.submitted_headers.borrow().as_slice(),
            &[
                "header-for-hash-3".to_string(),
                "header-for-hash-4".to_string(),
                "header-for-hash-5".to_string()
            ]
        );
    }
}
