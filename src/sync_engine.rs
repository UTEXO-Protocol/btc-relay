use anyhow::{Context, Result};
use log::{info, warn};
use std::thread;
use std::time::Duration;

use crate::interfaces::{BitcoinRpcClient, BtcRelaySubmitter};
use crate::persistence::{JsonFileStateStore, RelayProgressState};

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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RetryDecision {
    Retryable,
    HardFailure,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SyncProgress {
    submitted: u64,
    retries: u64,
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
    state_store: &JsonFileStateStore,
) -> Result<()> {
    let poll_interval = Duration::from_secs(poll_interval_secs.max(1));
    let mut loop_state = SyncLoopState::new(start_height);

    info!(
        "sync loop started: poll_interval_secs={}, start_height={}",
        poll_interval.as_secs(),
        start_height
    );
    if let Some(state) = state_store.load().context("failed to load persisted relay state")? {
        info!(
            "loaded persisted relay state: last_submitted_height={}, last_submitted_hash={}, updated_at={}",
            state.last_submitted_height,
            state.last_submitted_hash,
            state.updated_at_unix_secs
        );
    } else {
        info!("no persisted relay state found yet");
    }

    loop {
        loop_state.poll_count = loop_state.poll_count.saturating_add(1);
        let trigger = if loop_state.poll_count == 1 {
            SyncTrigger::Startup
        } else {
            SyncTrigger::PollTick
        };

        let cycle_result = run_poll_cycle(bitcoin, submitter, trigger, &mut loop_state, state_store)?;
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
    state_store: &JsonFileStateStore,
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

    let persisted_state = state_store
        .load()
        .context("failed to load persisted relay state in poll cycle")?;
    let resume_start_height = resolve_resume_start_height(
        relay_tip,
        loop_state.start_height,
        persisted_state.as_ref(),
    );
    let (from_height, to_height) = compute_catchup_range(relay_tip, bitcoin_tip, resume_start_height)
        .context("failed to calculate catch-up range")?;

    loop_state.state = SyncEngineState::CatchingUp;
    let progress = process_catchup_range(bitcoin, submitter, from_height, to_height, loop_state, state_store)?;
    info!(
        "relay behind bitcoin tip: synced range {}..={}, submitted_headers={}, retries={}, lag={}",
        from_height,
        to_height,
        progress.submitted,
        progress.retries,
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
    state_store: &JsonFileStateStore,
) -> Result<SyncProgress> {
    let mut submitted = 0_u64;
    let mut retries = 0_u64;
    let total = to_height.saturating_sub(from_height).saturating_add(1);

    for height in from_height..=to_height {
        let mut attempt = 0_u32;
        loop {
            attempt = attempt.saturating_add(1);

            match process_single_height(bitcoin, submitter, height, loop_state) {
                Ok((block_hash, tx_hash)) => {
                    submitted = submitted.saturating_add(1);
                    let state = RelayProgressState::new(height, block_hash);
                    state_store
                        .save(&state)
                        .with_context(|| format!("failed persisting relay state at height {}", height))?;
                    info!(
                        "header submitted: height={}, tx_hash={}, progress={}/{}, attempt={}",
                        height, tx_hash, submitted, total, attempt
                    );
                    break;
                }
                Err(err) => {
                    let message = format!("{:#}", err);
                    match classify_retry_decision(message.as_str()) {
                        RetryDecision::Retryable => {
                            let delay_secs = backoff_delay_secs(attempt);
                            loop_state.state = SyncEngineState::RetryBackoff;
                            retries = retries.saturating_add(1);
                            warn!(
                                "temporary sync failure at height={}, attempt={}, retry_in={}s, reason={}",
                                height, attempt, delay_secs, message
                            );
                            thread::sleep(Duration::from_secs(delay_secs));
                            continue;
                        }
                        RetryDecision::HardFailure => {
                            loop_state.state = SyncEngineState::Error;
                            return Err(err).with_context(|| {
                                format!(
                                    "hard failure while processing height {} after {} attempt(s)",
                                    height, attempt
                                )
                            });
                        }
                    }
                }
            }
        }
    }

    Ok(SyncProgress { submitted, retries })
}

fn process_single_height(
    bitcoin: &dyn BitcoinRpcClient,
    submitter: &dyn BtcRelaySubmitter,
    height: u64,
    loop_state: &mut SyncLoopState,
) -> Result<(String, String)> {
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
    Ok((block_hash, tx_hash))
}

fn classify_retry_decision(err_message: &str) -> RetryDecision {
    let msg = err_message.to_ascii_lowercase();
    let retryable_markers = [
        "timeout",
        "timed out",
        "temporarily unavailable",
        "connection reset",
        "connection refused",
        "transport failed",
        "429",
        "503",
        "network",
        "broken pipe",
    ];
    if retryable_markers.iter().any(|m| msg.contains(m)) {
        return RetryDecision::Retryable;
    }
    RetryDecision::HardFailure
}

fn backoff_delay_secs(attempt: u32) -> u64 {
    let shift = attempt.saturating_sub(1).min(5);
    1_u64 << shift
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

fn resolve_resume_start_height(
    relay_tip: u64,
    configured_start_height: u64,
    persisted_state: Option<&RelayProgressState>,
) -> u64 {
    let next_from_relay = relay_tip.saturating_add(1);
    if let Some(state) = persisted_state {
        if state.last_submitted_height < relay_tip {
            warn!(
                "persisted state is behind relay tip (persisted_height={}, relay_tip={}); resuming from relay tip + 1",
                state.last_submitted_height, relay_tip
            );
        } else if state.last_submitted_height > relay_tip {
            warn!(
                "persisted state is ahead of relay tip (persisted_height={}, relay_tip={}); relay tip remains source of truth",
                state.last_submitted_height, relay_tip
            );
        }
        if configured_start_height > 0 {
            info!(
                "ignoring START_HEIGHT={} because persisted state exists; resuming from relay tip + 1",
                configured_start_height
            );
        }
        return next_from_relay;
    }

    if configured_start_height > 0 {
        return configured_start_height.max(next_from_relay);
    }
    next_from_relay
}

#[cfg(test)]
mod tests {
    use super::{
        classify_retry_decision, compute_catchup_range, process_catchup_range,
        resolve_resume_start_height, RetryDecision,
    };
    use crate::interfaces::{BitcoinRpcClient, BtcRelaySubmitter};
    use crate::persistence::{JsonFileStateStore, RelayProgressState};
    use anyhow::Result;
    use std::cell::RefCell;
    use std::env;

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

    #[test]
    fn resume_start_uses_configured_start_when_no_persisted_state() {
        let start = resolve_resume_start_height(100, 110, None);
        assert_eq!(start, 110);
    }

    #[test]
    fn resume_start_uses_relay_tip_when_persisted_state_exists() {
        let state = RelayProgressState::new(99, "hash".to_string());
        let start = resolve_resume_start_height(100, 150, Some(&state));
        assert_eq!(start, 101);
    }

    #[test]
    fn resume_start_uses_relay_tip_when_persisted_ahead() {
        let state = RelayProgressState::new(250, "hash".to_string());
        let start = resolve_resume_start_height(100, 0, Some(&state));
        assert_eq!(start, 101);
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
        fail_once_for_header: Option<String>,
        failed_once: RefCell<bool>,
    }

    impl FakeSubmitter {
        fn new() -> Self {
            Self {
                submitted_headers: RefCell::new(Vec::new()),
                fail_once_for_header: None,
                failed_once: RefCell::new(false),
            }
        }

        fn with_one_temporary_failure(header: &str) -> Self {
            Self {
                submitted_headers: RefCell::new(Vec::new()),
                fail_once_for_header: Some(header.to_string()),
                failed_once: RefCell::new(false),
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
            if let Some(target) = &self.fail_once_for_header {
                if header_hex == target && !*self.failed_once.borrow() {
                    *self.failed_once.borrow_mut() = true;
                    anyhow::bail!("network timeout while submitting header");
                }
            }
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
        let state_store = test_state_store();

        let mut loop_state = super::SyncLoopState::new(0);
        let progress = process_catchup_range(&bitcoin, &submitter, 3, 5, &mut loop_state, &state_store)
            .expect("pipeline");
        assert_eq!(progress.submitted, 3);
        assert_eq!(progress.retries, 0);
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

    #[test]
    fn catchup_pipeline_retries_temporary_failure_and_continues() {
        let bitcoin = FakeBitcoinRpc::new();
        let submitter = FakeSubmitter::with_one_temporary_failure("header-for-hash-3");
        let state_store = test_state_store();
        let mut loop_state = super::SyncLoopState::new(0);

        let progress = process_catchup_range(&bitcoin, &submitter, 3, 4, &mut loop_state, &state_store)
            .expect("pipeline with retry");

        assert_eq!(progress.submitted, 2);
        assert_eq!(progress.retries, 1);
        assert_eq!(
            submitter.submitted_headers.borrow().as_slice(),
            &["header-for-hash-3".to_string(), "header-for-hash-4".to_string()]
        );
    }

    #[test]
    fn retry_classification_distinguishes_temporary_and_hard_failures() {
        assert_eq!(
            classify_retry_decision("bitcoin rpc transport failed: timeout"),
            RetryDecision::Retryable
        );
        assert_eq!(
            classify_retry_decision("failed to decode abi output"),
            RetryDecision::HardFailure
        );
    }

    fn test_state_store() -> JsonFileStateStore {
        let mut path = env::temp_dir();
        path.push(format!(
            "btc-relay-sync-engine-state-{}-{}.json",
            std::process::id(),
            current_test_timestamp()
        ));
        JsonFileStateStore::new(path)
    }

    fn current_test_timestamp() -> u128 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis())
            .unwrap_or(0)
    }
}
