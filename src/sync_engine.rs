use anyhow::{Context, Result};
use std::thread;
use std::time::Duration;
use tracing::{info, warn};

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

    info!(poll_interval_secs = poll_interval.as_secs(), start_height, "sync loop started");
    if let Some(state) = state_store.load().context("failed to load persisted relay state")? {
        info!(
            last_submitted_height = state.last_submitted_height,
            last_submitted_hash = %state.last_submitted_hash,
            updated_at_unix_secs = state.updated_at_unix_secs,
            "loaded persisted relay state"
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
        info!(poll_count = loop_state.poll_count, state = ?loop_state.state, result = ?cycle_result, "sync poll cycle complete");

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

    info!(trigger = ?trigger, bitcoin_tip, relay_tip, lag, "tip discovery");

    if relay_tip >= bitcoin_tip {
        info!(relay_tip, bitcoin_tip, "sync is up to date; nothing to submit this cycle");
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
        from_height,
        to_height,
        submitted_headers = progress.submitted,
        retries = progress.retries,
        lag,
        "relay behind bitcoin tip: catch-up cycle finished"
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
                    info!(height, tx_hash = %tx_hash, submitted, total, attempt, "header submitted");
                    break;
                }
                Err(err) => {
                    let message = format!("{:#}", err);
                    match classify_retry_decision(message.as_str()) {
                        RetryDecision::Retryable => {
                            let delay_secs = backoff_delay_secs(attempt);
                            loop_state.state = SyncEngineState::RetryBackoff;
                            retries = retries.saturating_add(1);
                            warn!(height, attempt, retry_in_secs = delay_secs, reason = %message, "temporary sync failure");
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
    if height == 0 {
        anyhow::bail!("cannot submit height 0: no previous stored header exists");
    }
    let block_hash = bitcoin
        .get_block_hash(height)
        .with_context(|| format!("failed get_block_hash at height {}", height))?;
    let header_hex = bitcoin
        .get_block_header_hex(&block_hash)
        .with_context(|| format!("failed get_block_header_hex at height {} hash {}", height, block_hash))?;
    let submit_payload_hex = build_submit_main_payload_hex(bitcoin, submitter, height, &header_hex)
        .with_context(|| format!("failed to build submit payload at height {}", height))?;

    info!(height, block_hash = %block_hash, header_hex_len = header_hex.len(), "submitting header");

    loop_state.state = SyncEngineState::WaitingConfirmations;
    let tx_hash = submitter
        .submit_header(&submit_payload_hex)
        .with_context(|| format!("failed submit_header at height {}", height))?;
    Ok((block_hash, tx_hash))
}

fn build_submit_main_payload_hex(
    bitcoin: &dyn BitcoinRpcClient,
    submitter: &dyn BtcRelaySubmitter,
    height: u64,
    current_header_hex: &str,
) -> Result<String> {
    let previous_height = height.saturating_sub(1);
    let previous_hash = bitcoin
        .get_block_hash(previous_height)
        .with_context(|| format!("failed get_block_hash for previous height {}", previous_height))?;
    let previous_header_hex = bitcoin
        .get_block_header_hex(&previous_hash)
        .with_context(|| format!("failed get_block_header_hex for previous height {}", previous_height))?;

    let previous_header_bytes = decode_even_hex(&previous_header_hex)
        .context("failed to decode previous block header hex")?;
    if previous_header_bytes.len() != 80 {
        anyhow::bail!(
            "previous block header must be 80 bytes, got {} bytes",
            previous_header_bytes.len()
        );
    }

    let current_header_bytes = decode_even_hex(current_header_hex)
        .context("failed to decode current block header hex")?;
    if current_header_bytes.len() != 80 {
        anyhow::bail!(
            "current block header must be 80 bytes, got {} bytes",
            current_header_bytes.len()
        );
    }

    if previous_height < 10 {
        anyhow::bail!("cannot construct previous timestamp window for height {}", previous_height);
    }
    let mut previous_timestamps = [0_u32; 10];
    for (idx, ts_height) in ((previous_height - 10)..previous_height).enumerate() {
        let ts_hash = bitcoin
            .get_block_hash(ts_height)
            .with_context(|| format!("failed get_block_hash for timestamp height {}", ts_height))?;
        let ts_header_hex = bitcoin
            .get_block_header_hex(&ts_hash)
            .with_context(|| format!("failed get_block_header_hex for timestamp height {}", ts_height))?;
        let ts_header_bytes = decode_even_hex(ts_header_hex.as_str())
            .with_context(|| format!("failed decode header for timestamp height {}", ts_height))?;
        previous_timestamps[idx] = parse_timestamp_from_header(&ts_header_bytes)?;
    }

    let epoch_start_height = (previous_height / 2016) * 2016;
    let epoch_start_hash = bitcoin
        .get_block_hash(epoch_start_height)
        .with_context(|| format!("failed get_block_hash for epoch start {}", epoch_start_height))?;
    let epoch_start_header_hex = bitcoin
        .get_block_header_hex(&epoch_start_hash)
        .with_context(|| format!("failed get_block_header_hex for epoch start {}", epoch_start_height))?;
    let epoch_start_header_bytes = decode_even_hex(epoch_start_header_hex.as_str())
        .with_context(|| format!("failed decode epoch start header at {}", epoch_start_height))?;
    let last_diff_adjustment = parse_timestamp_from_header(&epoch_start_header_bytes)?;

    let chain_work = submitter
        .relay_chain_work_bytes()
        .context("failed to fetch relay chainwork bytes")?;

    let mut payload = Vec::with_capacity(160 + 48);
    payload.extend_from_slice(&previous_header_bytes); // 80 bytes
    payload.extend_from_slice(&chain_work); // 32 bytes
    payload.extend_from_slice(&(previous_height as u32).to_be_bytes()); // 4 bytes
    payload.extend_from_slice(&last_diff_adjustment.to_be_bytes()); // 4 bytes
    for ts in previous_timestamps {
        payload.extend_from_slice(&ts.to_be_bytes()); // 40 bytes
    }
    if payload.len() != 160 {
        anyhow::bail!("stored header payload must be 160 bytes, got {}", payload.len());
    }

    // Compact header (48 bytes): versionLE + merkleRoot + timestampLE + nBitsLE + nonce
    payload.extend_from_slice(&current_header_bytes[0..4]); // versionLE
    payload.extend_from_slice(&current_header_bytes[36..68]); // merkleRoot
    payload.extend_from_slice(&current_header_bytes[68..72]); // timestampLE
    payload.extend_from_slice(&current_header_bytes[72..76]); // nBitsLE
    payload.extend_from_slice(&current_header_bytes[76..80]); // nonce

    if payload.len() < 208 {
        anyhow::bail!("submit payload too short: {} bytes", payload.len());
    }
    Ok(bytes_to_hex(&payload))
}

fn parse_timestamp_from_header(header_bytes: &[u8]) -> Result<u32> {
    if header_bytes.len() != 80 {
        anyhow::bail!("bitcoin header must be exactly 80 bytes");
    }
    let ts_le = [header_bytes[68], header_bytes[69], header_bytes[70], header_bytes[71]];
    Ok(u32::from_le_bytes(ts_le))
}

fn decode_even_hex(value: &str) -> Result<Vec<u8>> {
    if value.len() % 2 != 0 {
        anyhow::bail!("hex value must have even length");
    }
    let mut out = Vec::with_capacity(value.len() / 2);
    let bytes = value.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        let hi = hex_nibble(bytes[i]).context("hex contains non-hex character")?;
        let lo = hex_nibble(bytes[i + 1]).context("hex contains non-hex character")?;
        out.push((hi << 4) | lo);
        i += 2;
    }
    Ok(out)
}

fn hex_nibble(c: u8) -> Option<u8> {
    match c {
        b'0'..=b'9' => Some(c - b'0'),
        b'a'..=b'f' => Some(c - b'a' + 10),
        b'A'..=b'F' => Some(c - b'A' + 10),
        _ => None,
    }
}

fn bytes_to_hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        use std::fmt::Write as _;
        let _ = write!(&mut out, "{:02x}", b);
    }
    out
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
                persisted_height = state.last_submitted_height,
                relay_tip,
                "persisted state is behind relay tip; resuming from relay tip + 1"
            );
        } else if state.last_submitted_height > relay_tip {
            warn!(
                persisted_height = state.last_submitted_height,
                relay_tip,
                "persisted state is ahead of relay tip; relay tip remains source of truth"
            );
        }
        if configured_start_height > 0 {
            info!(configured_start_height, "ignoring START_HEIGHT because persisted state exists; resuming from relay tip + 1");
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

    #[test]
    fn downtime_resume_catches_up_from_persisted_state_with_relay_tip_source_of_truth() {
        let state = RelayProgressState::new(150, "persisted-hash".to_string());
        let resume_start = resolve_resume_start_height(150, 0, Some(&state));
        let (from, to) = compute_catchup_range(150, 153, resume_start).expect("catch-up range");
        assert_eq!((from, to), (151, 153));
    }

    struct FakeBitcoinRpc {
        hash_calls: RefCell<Vec<u64>>,
    }

    impl FakeBitcoinRpc {
        fn new() -> Self {
            Self {
                hash_calls: RefCell::new(Vec::new()),
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
            let h = hash
                .strip_prefix("hash-")
                .and_then(|v| v.parse::<u64>().ok())
                .unwrap_or(0);
            Ok(fake_full_header_hex(h))
        }
    }

    struct FakeSubmitter {
        submitted_headers: RefCell<Vec<String>>,
        fail_once: bool,
        failed_once: RefCell<bool>,
    }

    impl FakeSubmitter {
        fn new() -> Self {
            Self {
                submitted_headers: RefCell::new(Vec::new()),
                fail_once: false,
                failed_once: RefCell::new(false),
            }
        }

        fn with_one_temporary_failure() -> Self {
            Self {
                submitted_headers: RefCell::new(Vec::new()),
                fail_once: true,
                failed_once: RefCell::new(false),
            }
        }
    }

    impl BtcRelaySubmitter for FakeSubmitter {
        fn relay_tip_height(&self) -> Result<u64> {
            Ok(0)
        }

        fn relay_chain_work_bytes(&self) -> Result<[u8; 32]> {
            Ok([0_u8; 32])
        }

        fn relay_commit_hash(&self, _height: u64) -> Result<String> {
            Ok("0x00".to_string())
        }

        fn submit_header(&self, header_hex: &str) -> Result<String> {
            if self.fail_once && !*self.failed_once.borrow() {
                *self.failed_once.borrow_mut() = true;
                anyhow::bail!("network timeout while submitting header");
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
        let progress = process_catchup_range(&bitcoin, &submitter, 13, 15, &mut loop_state, &state_store)
            .expect("pipeline");
        assert_eq!(progress.submitted, 3);
        assert_eq!(progress.retries, 0);
        assert_eq!(loop_state.state, super::SyncEngineState::WaitingConfirmations);
        assert!(bitcoin.hash_calls.borrow().contains(&13));
        assert!(bitcoin.hash_calls.borrow().contains(&14));
        assert!(bitcoin.hash_calls.borrow().contains(&15));
        assert_eq!(submitter.submitted_headers.borrow().len(), 3);
        assert!(submitter.submitted_headers.borrow().iter().all(|p| p.len() >= 416));
    }

    #[test]
    fn catchup_pipeline_retries_temporary_failure_and_continues() {
        let bitcoin = FakeBitcoinRpc::new();
        let submitter = FakeSubmitter::with_one_temporary_failure();
        let state_store = test_state_store();
        let mut loop_state = super::SyncLoopState::new(0);

        let progress = process_catchup_range(&bitcoin, &submitter, 13, 14, &mut loop_state, &state_store)
            .expect("pipeline with retry");

        assert_eq!(progress.submitted, 2);
        assert_eq!(progress.retries, 1);
        assert_eq!(submitter.submitted_headers.borrow().len(), 2);
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

    fn fake_full_header_hex(height: u64) -> String {
        let mut header = vec![0_u8; 80];
        header[0..4].copy_from_slice(&1_u32.to_le_bytes()); // version
        let merkle_seed = height.to_le_bytes();
        for i in 0..32 {
            header[36 + i] = merkle_seed[i % merkle_seed.len()];
        }
        header[68..72].copy_from_slice(&(1_700_000_000_u32.saturating_add(height as u32)).to_le_bytes()); // timestamp LE
        header[72..76].copy_from_slice(&0x1d00ffff_u32.to_le_bytes()); // nBits LE
        header[76..80].copy_from_slice(&(height as u32).to_le_bytes()); // nonce
        let mut out = String::with_capacity(160);
        for b in header {
            use std::fmt::Write as _;
            let _ = write!(&mut out, "{:02x}", b);
        }
        out
    }
}
