// Copyright (C) 2026 Utexo.
// See LICENSE for copying information.

//! Sync orchestration: poll Bitcoin and relay tips, compute catch-up, build relay payloads (160-byte prologue + compact headers),
//! submit in batches with retries, persist JSON for operators.
//!
//! **Authoritative tip is always the contract** — on-disk checkpoint is advisory only.

use anyhow::{Context, Result};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tracing::{info, warn};

use crate::interfaces::{BitcoinRpcClient, BtcRelaySubmitter};
use crate::metrics;
use crate::persistence::{JsonFileStateStore, RelayProgressState};

/// Coarse FSM labels for logs / future metrics. Most transitions are `Active` ↔ `CatchingUp` in practice.
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

/// Why a poll cycle started. Today only `Startup` (first iteration) and `PollTick` are used; other variants reserved for future triggers.
#[allow(dead_code)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncTrigger {
    Startup,
    PollTick,
    ZmqNewBlock,
    Manual,
    RetryTimer,
}

/// Per-cycle outcome for logging. `ReorgDetected` / `TemporaryFailure` exist for future honesty — don't trust them blindly yet.
#[allow(dead_code)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncResult {
    UpToDate,
    Progressed,
    ReorgDetected,
    TemporaryFailure,
}

/// Retry vs abort is decided by substring heuristics on the error string (RPC errors are not typed consistently).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RetryDecision {
    Retryable,
    HardFailure,
}

/// Counters for one `process_catchup_range` invocation — how many headers advanced and how often we slept on errors.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SyncProgress {
    submitted: u64,
    retries: u64,
}

/// Snapshot of knobs + poll generation. Mutable `state` is mostly for logging what phase we're in.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SyncLoopState {
    pub state: SyncEngineState,
    /// Monotonic poll counter; `1` means first iteration (logged as `Startup` trigger).
    pub poll_count: u64,
    /// Copy of env `START_HEIGHT` — only affects resume when **no** JSON state file.
    pub start_height: u64,
    /// Max headers per tx in catch-up mode (throttled near tip by `live_lag_threshold`).
    pub catchup_batch_size: u64,
    /// When remaining lag ≤ this, force batch size 1 ("live" tail).
    pub live_lag_threshold: u64,
    /// Max headers per `submitShortForkBlockheaders` call. The contract's short-fork gas limit can be
    /// tighter than `catchup_batch_size`, so this is set independently; defaults to `catchup_batch_size`.
    pub short_fork_limit: u64,
    /// `(bitcoin_tip, relay_tip)` of a reorg the relay did not follow. No submit until a tip changes.
    pub reorg_hold: Option<(u64, u64)>,
}

impl SyncLoopState {
    fn new(start_height: u64, catchup_batch_size: u64, live_lag_threshold: u64) -> Self {
        Self {
            state: SyncEngineState::Idle,
            poll_count: 0,
            start_height,
            catchup_batch_size,
            live_lag_threshold,
            short_fork_limit: catchup_batch_size,
            reorg_hold: None,
        }
    }
}

/// **Never returns** unless something fatals — that's intentional for a daemon.
pub fn run_sync_loop(
    bitcoin: &dyn BitcoinRpcClient,
    submitter: &dyn BtcRelaySubmitter,
    poll_interval_secs: u64,
    start_height: u64,
    catchup_batch_size: u64,
    live_lag_threshold: u64,
    short_fork_limit: u64,
    state_store: &JsonFileStateStore,
) -> Result<()> {
    let poll_interval = Duration::from_secs(poll_interval_secs.max(1));
    let mut loop_state =
        SyncLoopState::new(start_height, catchup_batch_size.max(1), live_lag_threshold);
    loop_state.short_fork_limit = short_fork_limit.max(1);

    info!(
        poll_interval_secs = poll_interval.as_secs(),
        start_height,
        catchup_batch_size = loop_state.catchup_batch_size,
        live_lag_threshold = loop_state.live_lag_threshold,
        short_fork_limit = loop_state.short_fork_limit,
        "sync loop started"
    );
    if let Some(state) = state_store
        .load()
        .context("failed to load persisted relay state")?
    {
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
        metrics::inc_sync_poll_cycle();
        let trigger = if loop_state.poll_count == 1 {
            SyncTrigger::Startup
        } else {
            SyncTrigger::PollTick
        };

        let cycle_result =
            match run_poll_cycle(bitcoin, submitter, trigger, &mut loop_state, state_store) {
                Ok(result) => result,
                Err(err) => {
                    metrics::inc_sync_poll_cycle_error();
                    return Err(err);
                }
            };
        info!(poll_count = loop_state.poll_count, state = ?loop_state.state, result = ?cycle_result, "sync poll cycle complete");

        thread::sleep(poll_interval);
    }
}

/// One iteration: refresh tips, maybe run catch-up for the whole gap, sleep handled by caller.
fn run_poll_cycle(
    bitcoin: &dyn BitcoinRpcClient,
    submitter: &dyn BtcRelaySubmitter,
    trigger: SyncTrigger,
    loop_state: &mut SyncLoopState,
    state_store: &JsonFileStateStore,
) -> Result<SyncResult> {
    loop_state.state = SyncEngineState::Active;

    // Two truth sources each cycle: Bitcoin tip and relay tip. Everything else derives from this diff.
    let bitcoin_tip = bitcoin.get_block_count()?;
    let relay_tip = submitter.relay_tip_height()?;
    // Saturating to avoid underflow if relay ever reports ahead (misconfig/reorg edge).
    let lag = bitcoin_tip.saturating_sub(relay_tip);
    metrics::set_tip_gauges(bitcoin_tip, relay_tip, lag);
    refresh_wallet_balance_metrics(submitter);

    info!(trigger = ?trigger, bitcoin_tip, relay_tip, lag, "tip discovery");

    if relay_tip >= bitcoin_tip {
        metrics::inc_sync_poll_up_to_date();
        info!(
            relay_tip,
            bitcoin_tip, "sync is up to date; nothing to submit this cycle"
        );
        return Ok(SyncResult::UpToDate);
    }
    if loop_state.reorg_hold == Some((bitcoin_tip, relay_tip)) {
        warn!(
            relay_tip,
            bitcoin_tip, "relay did not follow the node branch; waiting for a new tip"
        );
        return Ok(SyncResult::ReorgDetected);
    }

    // Persisted state is advisory only; helper below still anchors to relay tip + 1.
    let persisted_state = state_store
        .load()
        .context("failed to load persisted relay state in poll cycle")?;
    let resume_start_height =
        resolve_resume_start_height(relay_tip, loop_state.start_height, persisted_state.as_ref());
    let (from_height, to_height) =
        compute_catchup_range(relay_tip, bitcoin_tip, resume_start_height)
            .context("failed to calculate catch-up range")?;

    loop_state.state = SyncEngineState::CatchingUp;
    let progress = match process_catchup_range(
        bitcoin,
        submitter,
        from_height,
        to_height,
        loop_state,
        state_store,
    ) {
        Ok(progress) => progress,
        Err(err) => {
            return follow_reorg(
                bitcoin,
                submitter,
                bitcoin_tip,
                err,
                loop_state,
                state_store,
            )
        }
    };
    info!(
        from_height,
        to_height,
        submitted_headers = progress.submitted,
        retries = progress.retries,
        lag,
        "relay behind bitcoin tip: catch-up cycle finished"
    );
    metrics::inc_sync_poll_progressed();
    metrics::add_sync_headers_submitted(progress.submitted);
    metrics::add_sync_retries(progress.retries);
    Ok(SyncResult::Progressed)
}

/// Main submit failed. If the relay holds blocks the node replaced, move the relay to the node branch.
fn follow_reorg(
    bitcoin: &dyn BitcoinRpcClient,
    submitter: &dyn BtcRelaySubmitter,
    bitcoin_tip: u64,
    main_err: anyhow::Error,
    loop_state: &mut SyncLoopState,
    state_store: &JsonFileStateStore,
) -> Result<SyncResult> {
    let relay_tip = submitter.relay_tip_height()?;
    let top = relay_tip.min(bitcoin_tip);
    let fork_point = find_fork_point(bitcoin, submitter, top)?;
    if fork_point == Some(top) {
        return Err(main_err);
    }
    let result = match fork_point {
        Some(fork_point) => submit_branch(
            bitcoin,
            submitter,
            fork_point,
            bitcoin_tip,
            loop_state.short_fork_limit,
            loop_state.catchup_batch_size,
        ),
        None => Err(anyhow::anyhow!(
            "no common block above the relay checkpoint"
        )),
    };
    match result {
        Ok(()) => {
            info!(
                ?fork_point,
                relay_tip, bitcoin_tip, "relay moved to the node branch"
            );
            let state = RelayProgressState::new(bitcoin_tip, bitcoin.get_block_hash(bitcoin_tip)?);
            state_store.save(&state)?;
            metrics::inc_sync_poll_progressed();
            if let Some(fork_point) = fork_point {
                metrics::add_sync_headers_submitted(bitcoin_tip.saturating_sub(fork_point));
            }
            metrics::set_tip_gauges(bitcoin_tip, bitcoin_tip, 0);
            Ok(SyncResult::Progressed)
        }
        Err(err) => {
            let reason = format!("{:#}", err);
            warn!(?fork_point, relay_tip, bitcoin_tip, reason = %reason, "relay did not follow the node branch");
            if classify_retry_decision(&reason) == RetryDecision::Retryable {
                return Ok(SyncResult::TemporaryFailure);
            }
            loop_state.reorg_hold = Some((bitcoin_tip, relay_tip));
            Ok(SyncResult::ReorgDetected)
        }
    }
}

/// Highest height at or below `top` where the relay holds the node block. `None` when no height above the checkpoint agrees.
/// Each side is one chain, so the heights that agree are contiguous below the fork point: scan down from `top`.
fn find_fork_point(
    bitcoin: &dyn BitcoinRpcClient,
    submitter: &dyn BtcRelaySubmitter,
    top: u64,
) -> Result<Option<u64>> {
    for height in (0..=top).rev() {
        match relay_holds_node_block(bitcoin, submitter, height)? {
            Some(true) => return Ok(Some(height)),
            Some(false) => continue,
            None => return Ok(None),
        }
    }
    Ok(None)
}

/// `None` below the relay checkpoint, else whether the relay commitment at `height` is the node block.
fn relay_holds_node_block(
    bitcoin: &dyn BitcoinRpcClient,
    submitter: &dyn BtcRelaySubmitter,
    height: u64,
) -> Result<Option<bool>> {
    // The stored header needs 10 earlier timestamps.
    if height < 10 {
        return Ok(None);
    }
    let commit = submitter.relay_commit_hash(height)?;
    if commit.trim_start_matches("0x").bytes().all(|b| b == b'0') {
        return Ok(None);
    }
    let chain_work = bitcoin.get_block_chainwork(&bitcoin.get_block_hash(height)?)?;
    let stored_header = build_stored_header(bitcoin, height, chain_work)?;
    Ok(Some(
        submitter.stored_header_commitment(&stored_header) == commit,
    ))
}

/// Submit the node blocks above `fork_point` up to `end` as a short fork, or as a fork in batches.
/// `short_fork_limit` and `batch_size` are separate: the contract's short-fork gas limit can be
/// tighter than the main-sync batch size used to chunk a long fork.
fn submit_branch(
    bitcoin: &dyn BitcoinRpcClient,
    submitter: &dyn BtcRelaySubmitter,
    fork_point: u64,
    end: u64,
    short_fork_limit: u64,
    batch_size: u64,
) -> Result<()> {
    let payload = |parent: u64, last: u64| -> Result<String> {
        let chain_work = bitcoin.get_block_chainwork(&bitcoin.get_block_hash(parent)?)?;
        build_payload_hex(bitcoin, parent, chain_work, last)
    };
    if end - fork_point <= short_fork_limit {
        submitter.submit_short_fork(&payload(fork_point, end)?)?;
    } else {
        // The contract keys forks by sender and id. A new id per attempt never continues a stale fork.
        let fork_id = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() as u64;
        let mut parent = fork_point;
        while parent < end {
            let last = parent.saturating_add(batch_size).min(end);
            submitter.submit_fork(fork_id, &payload(parent, last)?)?;
            parent = last;
        }
    }
    // A fork with less work stays off the main chain and does not revert.
    if submitter.relay_tip_height()? < end
        || relay_holds_node_block(bitcoin, submitter, end)? != Some(true)
    {
        anyhow::bail!("relay main chain does not hold the node block at {}", end);
    }
    Ok(())
}

fn refresh_wallet_balance_metrics(submitter: &dyn BtcRelaySubmitter) {
    let address = match submitter.relayer_wallet_address() {
        Ok(v) => v,
        Err(_) => return, // submitter backend does not expose wallet info
    };
    let wei = match submitter.relayer_wallet_balance_wei() {
        Ok(v) => v,
        Err(err) => {
            warn!(error = %err, "failed to refresh relayer wallet balance");
            return;
        }
    };
    let wei_f64 = wei.to_string().parse::<f64>().unwrap_or(0.0);
    let eth = wei_f64 / 1_000_000_000_000_000_000_f64;
    metrics::set_relayer_wallet_balance(wei_f64, eth);
    info!(
        wallet_address = %address,
        wallet_balance_wei = %wei,
        wallet_balance_eth = eth,
        "relayer wallet balance snapshot"
    );
}

/// Walk `from_height..=to_height` in batches; each successful batch persists JSON and logs tx hash.
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
    let mut current_height = from_height;
    while current_height <= to_height {
        let remaining = to_height.saturating_sub(current_height).saturating_add(1);
        let batch_size = choose_submission_batch_size(
            remaining,
            loop_state.catchup_batch_size,
            loop_state.live_lag_threshold,
        );
        // `mode` is log-only label so dashboards can split "catch-up" vs near-tip behavior.
        let mode = if batch_size > 1 { "batch" } else { "live" };
        let batch_end = current_height
            .saturating_add(batch_size.saturating_sub(1))
            .min(to_height);

        let mut attempt = 0_u32;
        loop {
            attempt = attempt.saturating_add(1);

            match process_submit_batch(bitcoin, submitter, current_height, batch_end, loop_state) {
                Ok((end_height, end_block_hash, tx_hash)) => {
                    let batch_submitted =
                        end_height.saturating_sub(current_height).saturating_add(1);
                    submitted = submitted.saturating_add(batch_submitted);
                    // Save immediately after a confirmed submission so operator state tracks on-chain progress.
                    let state = RelayProgressState::new(end_height, end_block_hash);
                    state_store.save(&state).with_context(|| {
                        format!("failed persisting relay state at height {}", end_height)
                    })?;
                    info!(
                        mode,
                        from_height = current_height,
                        to_height = end_height,
                        batch_submitted,
                        tx_hash = %tx_hash,
                        submitted,
                        total,
                        attempt,
                        "header batch submitted"
                    );
                    current_height = end_height.saturating_add(1);
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
                                mode,
                                from_height = current_height,
                                to_height = batch_end,
                                attempt,
                                retry_in_secs = delay_secs,
                                reason = %message,
                                "temporary sync failure"
                            );
                            thread::sleep(Duration::from_secs(delay_secs));
                            continue;
                        }
                        RetryDecision::HardFailure => {
                            loop_state.state = SyncEngineState::Error;
                            return Err(err).with_context(|| {
                                format!(
                                    "hard failure while processing range {}..={} after {} attempt(s)",
                                    current_height, batch_end, attempt
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

/// Build ABI blob for heights `[start_height, end_height]` inclusive, broadcast, return `(end_height, end_hash, tx_hash)`.
fn process_submit_batch(
    bitcoin: &dyn BitcoinRpcClient,
    submitter: &dyn BtcRelaySubmitter,
    start_height: u64,
    end_height: u64,
    loop_state: &mut SyncLoopState,
) -> Result<(u64, String, String)> {
    loop_state.state = SyncEngineState::Submitting;
    if start_height == 0 {
        anyhow::bail!("cannot submit height 0: no previous stored header exists");
    }
    if end_height < start_height {
        anyhow::bail!("invalid batch range {}..{}", start_height, end_height);
    }
    let end_block_hash = bitcoin
        .get_block_hash(end_height)
        .with_context(|| format!("failed get_block_hash at height {}", end_height))?;
    let submit_payload_hex =
        build_submit_main_payload_hex_for_range(bitcoin, submitter, start_height, end_height)
            .with_context(|| {
                format!(
                    "failed to build submit payload for range {}..{}",
                    start_height, end_height
                )
            })?;

    info!(
        mode = if end_height > start_height {
            "batch"
        } else {
            "live"
        },
        from_height = start_height,
        to_height = end_height,
        payload_hex_len = submit_payload_hex.len(),
        "submitting header batch"
    );

    loop_state.state = SyncEngineState::WaitingConfirmations;
    let tx_hash = submitter
        .submit_header(&submit_payload_hex)
        .with_context(|| {
            format!(
                "failed submit_header for range {}..{}",
                start_height, end_height
            )
        })?;
    Ok((end_height, end_block_hash, tx_hash))
}

/// Assemble the **relay contract's** `bytes` argument: fixed 160-byte prologue + 48 bytes per header (compact form).
/// This layout is not negotiable — it matches the on-chain verifier. Read the code before "optimizing".
fn build_submit_main_payload_hex_for_range(
    bitcoin: &dyn BitcoinRpcClient,
    submitter: &dyn BtcRelaySubmitter,
    start_height: u64,
    end_height: u64,
) -> Result<String> {
    // Chainwork comes from relay contract, not local recompute: matches contract internal accumulator.
    let chain_work = submitter
        .relay_chain_work_bytes()
        .context("failed to fetch relay chainwork bytes")?;
    // Contract payload needs the parent of `start_height` as context.
    build_payload_hex(
        bitcoin,
        start_height.saturating_sub(1),
        chain_work,
        end_height,
    )
}

/// 160-byte stored header of block `previous_height`, as the relay commits it, with `chain_work` at that block.
fn build_stored_header(
    bitcoin: &dyn BitcoinRpcClient,
    previous_height: u64,
    chain_work: [u8; 32],
) -> Result<Vec<u8>> {
    let previous_hash = bitcoin.get_block_hash(previous_height).with_context(|| {
        format!(
            "failed get_block_hash for previous height {}",
            previous_height
        )
    })?;
    let previous_header_hex = bitcoin
        .get_block_header_hex(&previous_hash)
        .with_context(|| {
            format!(
                "failed get_block_header_hex for previous height {}",
                previous_height
            )
        })?;

    let previous_header_bytes = decode_even_hex(&previous_header_hex)
        .context("failed to decode previous block header hex")?;
    if previous_header_bytes.len() != 80 {
        anyhow::bail!(
            "previous block header must be 80 bytes, got {} bytes",
            previous_header_bytes.len()
        );
    }

    if previous_height < 10 {
        anyhow::bail!(
            "cannot construct previous timestamp window for height {}",
            previous_height
        );
    }
    let mut previous_timestamps = [0_u32; 10];
    for (idx, ts_height) in ((previous_height - 10)..previous_height).enumerate() {
        let ts_hash = bitcoin
            .get_block_hash(ts_height)
            .with_context(|| format!("failed get_block_hash for timestamp height {}", ts_height))?;
        let ts_header_hex = bitcoin.get_block_header_hex(&ts_hash).with_context(|| {
            format!(
                "failed get_block_header_hex for timestamp height {}",
                ts_height
            )
        })?;
        let ts_header_bytes = decode_even_hex(ts_header_hex.as_str())
            .with_context(|| format!("failed decode header for timestamp height {}", ts_height))?;
        previous_timestamps[idx] = parse_timestamp_from_header(&ts_header_bytes)?;
    }

    // Difficulty epochs are 2016 blocks; relay verifier wants timestamp at epoch start.
    let epoch_start_height = (previous_height / 2016) * 2016;
    let epoch_start_hash = bitcoin
        .get_block_hash(epoch_start_height)
        .with_context(|| {
            format!(
                "failed get_block_hash for epoch start {}",
                epoch_start_height
            )
        })?;
    let epoch_start_header_hex = bitcoin
        .get_block_header_hex(&epoch_start_hash)
        .with_context(|| {
            format!(
                "failed get_block_header_hex for epoch start {}",
                epoch_start_height
            )
        })?;
    let epoch_start_header_bytes = decode_even_hex(epoch_start_header_hex.as_str())
        .with_context(|| format!("failed decode epoch start header at {}", epoch_start_height))?;
    let last_diff_adjustment = parse_timestamp_from_header(&epoch_start_header_bytes)?;

    // --- 160-byte prologue (parent header + relay context + MedianTimePast window) ---
    let mut payload = Vec::with_capacity(160 + 48);
    payload.extend_from_slice(&previous_header_bytes); // 80: full header of block before range
    payload.extend_from_slice(&chain_work); // 32: relay chainwork (big-endian in slot)
    payload.extend_from_slice(&(previous_height as u32).to_be_bytes()); // 4: height of that parent
    payload.extend_from_slice(&last_diff_adjustment.to_be_bytes()); // 4: timestamp of difficulty epoch start
    for ts in previous_timestamps {
        payload.extend_from_slice(&ts.to_be_bytes()); // 10×4: MTP window before parent
    }
    if payload.len() != 160 {
        anyhow::bail!(
            "stored header payload must be 160 bytes, got {}",
            payload.len()
        );
    }
    Ok(payload)
}

/// Stored header of `parent_height` with `chain_work`, then the compact headers up to `end_height`.
fn build_payload_hex(
    bitcoin: &dyn BitcoinRpcClient,
    parent_height: u64,
    chain_work: [u8; 32],
    end_height: u64,
) -> Result<String> {
    let mut payload = build_stored_header(bitcoin, parent_height, chain_work)?;

    // --- 48-byte "compact" headers appended in chain order (version + merkle + time + bits + nonce, LE where Bitcoin uses LE) ---
    for h in parent_height.saturating_add(1)..=end_height {
        let current_hash = bitcoin
            .get_block_hash(h)
            .with_context(|| format!("failed get_block_hash for compact header height {}", h))?;
        let current_header_hex =
            bitcoin
                .get_block_header_hex(&current_hash)
                .with_context(|| {
                    format!(
                        "failed get_block_header_hex for compact header height {}",
                        h
                    )
                })?;
        let current_header_bytes = decode_even_hex(&current_header_hex)
            .with_context(|| format!("failed decode compact header hex at height {}", h))?;
        if current_header_bytes.len() != 80 {
            anyhow::bail!(
                "current block header at height {} must be 80 bytes, got {} bytes",
                h,
                current_header_bytes.len()
            );
        }

        payload.extend_from_slice(&current_header_bytes[0..4]); // versionLE
        payload.extend_from_slice(&current_header_bytes[36..68]); // merkleRoot
        payload.extend_from_slice(&current_header_bytes[68..72]); // timestampLE
        payload.extend_from_slice(&current_header_bytes[72..76]); // nBitsLE
        payload.extend_from_slice(&current_header_bytes[76..80]); // nonce
    }

    if payload.len() < 208 {
        anyhow::bail!("submit payload too short: {} bytes", payload.len());
    }
    Ok(bytes_to_hex(&payload))
}

/// Far behind → big batches (capped by `catchup_batch_size`). Near tip → single-header txs to reduce reorg/gas pain.
fn choose_submission_batch_size(
    remaining: u64,
    catchup_batch_size: u64,
    live_lag_threshold: u64,
) -> u64 {
    if remaining > live_lag_threshold {
        remaining.min(catchup_batch_size.max(1))
    } else {
        1
    }
}

/// Bitcoin header timestamp is 4 bytes little-endian at offset 68..72.
fn parse_timestamp_from_header(header_bytes: &[u8]) -> Result<u32> {
    if header_bytes.len() != 80 {
        anyhow::bail!("bitcoin header must be exactly 80 bytes");
    }
    let ts_le = [
        header_bytes[68],
        header_bytes[69],
        header_bytes[70],
        header_bytes[71],
    ];
    Ok(u32::from_le_bytes(ts_le))
}

/// Bitcoin RPC gives header as hex string without `0x`; must be even length.
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

/// Lowercase hex **without** `0x` — matches what `submit_header` / contract side expect for this path.
fn bytes_to_hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        use std::fmt::Write as _;
        let _ = write!(&mut out, "{:02x}", b);
    }
    out
}

/// Maps error text to retry vs fatal; keep markers aligned with real RPC failure strings you see in production.
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

/// Exponential backoff capped at 2^5 seconds — keeps us from hammering a sick RPC into the ground.
fn backoff_delay_secs(attempt: u32) -> u64 {
    let shift = attempt.saturating_sub(1).min(5);
    1_u64 << shift
}

/// Inclusive range `[from, to]` to submit. `start_height` is already resolved (relay+1 vs bootstrap override).
fn compute_catchup_range(
    relay_tip: u64,
    bitcoin_tip: u64,
    start_height: u64,
) -> Result<(u64, u64)> {
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

/// **Truth:** `relay_tip + 1`. JSON file only influences warnings and whether `START_HEIGHT` is ignored.
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
                relay_tip, "persisted state is behind relay tip; resuming from relay tip + 1"
            );
        } else if state.last_submitted_height > relay_tip {
            warn!(
                persisted_height = state.last_submitted_height,
                relay_tip,
                "persisted state is ahead of relay tip; relay tip remains source of truth"
            );
        }
        if configured_start_height > 0 {
            info!(
                configured_start_height,
                "ignoring START_HEIGHT because persisted state exists; resuming from relay tip + 1"
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
        backoff_delay_secs, build_submit_main_payload_hex_for_range, choose_submission_batch_size,
        classify_retry_decision, compute_catchup_range, decode_even_hex,
        parse_timestamp_from_header, process_catchup_range, process_submit_batch,
        resolve_resume_start_height, run_poll_cycle, RetryDecision, SyncEngineState, SyncLoopState,
        SyncResult, SyncTrigger,
    };
    use crate::interfaces::{BitcoinRpcClient, BtcRelaySubmitter};
    use crate::persistence::{JsonFileStateStore, RelayProgressState};
    use anyhow::Result;
    use std::cell::{Cell, RefCell};
    use std::collections::HashMap;
    use std::env;
    use std::fs;

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

        let mut loop_state = super::SyncLoopState::new(0, 16, 2);
        let progress =
            process_catchup_range(&bitcoin, &submitter, 13, 15, &mut loop_state, &state_store)
                .expect("pipeline");
        assert_eq!(progress.submitted, 3);
        assert_eq!(progress.retries, 0);
        assert_eq!(
            loop_state.state,
            super::SyncEngineState::WaitingConfirmations
        );
        assert!(bitcoin.hash_calls.borrow().contains(&13));
        assert!(bitcoin.hash_calls.borrow().contains(&14));
        assert!(bitcoin.hash_calls.borrow().contains(&15));
        assert_eq!(submitter.submitted_headers.borrow().len(), 1);
        assert_eq!(submitter.submitted_headers.borrow()[0].len(), 608);
    }

    #[test]
    fn catchup_pipeline_retries_temporary_failure_and_continues() {
        let bitcoin = FakeBitcoinRpc::new();
        let submitter = FakeSubmitter::with_one_temporary_failure();
        let state_store = test_state_store();
        let mut loop_state = super::SyncLoopState::new(0, 16, 2);

        let progress =
            process_catchup_range(&bitcoin, &submitter, 13, 14, &mut loop_state, &state_store)
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

    #[test]
    fn choose_submission_batch_size_switches_between_catchup_and_live_modes() {
        assert_eq!(choose_submission_batch_size(20, 16, 2), 16);
        assert_eq!(choose_submission_batch_size(2, 16, 2), 1);
        assert_eq!(choose_submission_batch_size(100, 0, 2), 1);
    }

    #[test]
    fn decode_even_hex_rejects_odd_and_non_hex_input() {
        let odd = decode_even_hex("abc").expect_err("odd hex should fail");
        assert!(odd.to_string().contains("even length"));

        let non_hex = decode_even_hex("zz").expect_err("non-hex should fail");
        assert!(non_hex.to_string().contains("non-hex"));
    }

    #[test]
    fn parse_timestamp_from_header_requires_exact_header_length() {
        let err = parse_timestamp_from_header(&[0_u8; 79]).expect_err("short header must fail");
        assert!(err.to_string().contains("exactly 80 bytes"));
    }

    #[test]
    fn backoff_delay_caps_exponential_growth() {
        assert_eq!(backoff_delay_secs(1), 1);
        assert_eq!(backoff_delay_secs(2), 2);
        assert_eq!(backoff_delay_secs(3), 4);
        assert_eq!(backoff_delay_secs(6), 32);
        assert_eq!(backoff_delay_secs(12), 32);
    }

    #[test]
    fn payload_build_fails_when_previous_header_is_not_80_bytes() {
        struct BadPreviousHeaderBitcoinRpc;
        impl BitcoinRpcClient for BadPreviousHeaderBitcoinRpc {
            fn get_block_count(&self) -> Result<u64> {
                Ok(0)
            }
            fn get_block_hash(&self, height: u64) -> Result<String> {
                Ok(format!("hash-{height}"))
            }
            fn get_best_block_hash(&self) -> Result<String> {
                Ok("best".to_string())
            }
            fn get_block_header_hex(&self, hash: &str) -> Result<String> {
                let h = hash
                    .strip_prefix("hash-")
                    .and_then(|v| v.parse::<u64>().ok())
                    .unwrap_or(0);
                if h == 10 {
                    return Ok("00".repeat(79)); // 79 bytes instead of 80
                }
                Ok(fake_full_header_hex(h))
            }
        }
        let bitcoin = BadPreviousHeaderBitcoinRpc;
        let submitter = FakeSubmitter::new();
        let err = build_submit_main_payload_hex_for_range(&bitcoin, &submitter, 11, 11)
            .expect_err("previous header with invalid length should fail");
        assert!(err
            .to_string()
            .contains("previous block header must be 80 bytes"));
    }

    #[test]
    fn payload_build_fails_when_timestamp_window_header_is_malformed_hex() {
        struct BadTimestampWindowBitcoinRpc;
        impl BitcoinRpcClient for BadTimestampWindowBitcoinRpc {
            fn get_block_count(&self) -> Result<u64> {
                Ok(0)
            }
            fn get_block_hash(&self, height: u64) -> Result<String> {
                Ok(format!("hash-{height}"))
            }
            fn get_best_block_hash(&self) -> Result<String> {
                Ok("best".to_string())
            }
            fn get_block_header_hex(&self, hash: &str) -> Result<String> {
                let h = hash
                    .strip_prefix("hash-")
                    .and_then(|v| v.parse::<u64>().ok())
                    .unwrap_or(0);
                if h == 5 {
                    return Ok("zz".to_string()); // invalid hex in timestamp window
                }
                Ok(fake_full_header_hex(h))
            }
        }
        let bitcoin = BadTimestampWindowBitcoinRpc;
        let submitter = FakeSubmitter::new();
        let err = build_submit_main_payload_hex_for_range(&bitcoin, &submitter, 12, 12)
            .expect_err("malformed timestamp-window header should fail");
        assert!(err
            .to_string()
            .contains("failed decode header for timestamp height 5"));
    }

    #[test]
    fn compute_catchup_range_rejects_start_above_tip() {
        let err = compute_catchup_range(10, 12, 99).expect_err("start above tip must fail");
        assert!(err.to_string().contains("computed catch-up start"));
    }

    #[test]
    fn process_submit_batch_rejects_invalid_ranges() {
        let bitcoin = FakeBitcoinRpc::new();
        let submitter = FakeSubmitter::new();
        let mut loop_state = SyncLoopState::new(0, 16, 2);

        let err_zero = process_submit_batch(&bitcoin, &submitter, 0, 1, &mut loop_state)
            .expect_err("height 0 should fail");
        assert!(err_zero.to_string().contains("cannot submit height 0"));

        let err_reversed = process_submit_batch(&bitcoin, &submitter, 20, 19, &mut loop_state)
            .expect_err("reversed range should fail");
        assert!(err_reversed.to_string().contains("invalid batch range"));
    }

    #[test]
    fn process_catchup_range_sets_error_state_on_hard_failure() {
        struct HardFailSubmitter;
        impl BtcRelaySubmitter for HardFailSubmitter {
            fn relay_tip_height(&self) -> Result<u64> {
                Ok(0)
            }
            fn relay_chain_work_bytes(&self) -> Result<[u8; 32]> {
                Ok([0_u8; 32])
            }
            fn relay_commit_hash(&self, _height: u64) -> Result<String> {
                Ok("0x00".to_string())
            }
            fn submit_header(&self, _header_hex: &str) -> Result<String> {
                anyhow::bail!("abi decode mismatch");
            }
        }

        let bitcoin = FakeBitcoinRpc::new();
        let submitter = HardFailSubmitter;
        let state_store = test_state_store();
        let mut loop_state = SyncLoopState::new(0, 16, 2);
        let err =
            process_catchup_range(&bitcoin, &submitter, 13, 13, &mut loop_state, &state_store)
                .expect_err("hard failure should bubble");
        assert!(err
            .to_string()
            .contains("hard failure while processing range"));
        assert_eq!(loop_state.state, SyncEngineState::Error);
    }

    #[test]
    fn process_catchup_range_reports_state_persist_failure() {
        let bitcoin = FakeBitcoinRpc::new();
        let submitter = FakeSubmitter::new();
        let mut bad_path = env::temp_dir();
        bad_path.push(format!(
            "btc-relay-sync-engine-dir-as-file-{}-{}",
            std::process::id(),
            current_test_timestamp()
        ));
        fs::create_dir_all(&bad_path).expect("create dir path");
        let state_store = JsonFileStateStore::new(&bad_path);
        let mut loop_state = SyncLoopState::new(0, 16, 2);
        let err =
            process_catchup_range(&bitcoin, &submitter, 13, 13, &mut loop_state, &state_store)
                .expect_err("persist failure should bubble");
        assert!(err.to_string().contains("failed persisting relay state"));
        let _ = fs::remove_dir_all(&bad_path);
    }

    #[test]
    fn run_poll_cycle_returns_up_to_date_when_relay_is_ahead() {
        struct UpToDateBitcoin;
        impl BitcoinRpcClient for UpToDateBitcoin {
            fn get_block_count(&self) -> Result<u64> {
                Ok(100)
            }
            fn get_block_hash(&self, _height: u64) -> Result<String> {
                Ok("hash".to_string())
            }
            fn get_best_block_hash(&self) -> Result<String> {
                Ok("best".to_string())
            }
            fn get_block_header_hex(&self, _hash: &str) -> Result<String> {
                Ok("00".repeat(80))
            }
        }
        struct AheadSubmitter;
        impl BtcRelaySubmitter for AheadSubmitter {
            fn relay_tip_height(&self) -> Result<u64> {
                Ok(120)
            }
            fn relay_chain_work_bytes(&self) -> Result<[u8; 32]> {
                Ok([0_u8; 32])
            }
            fn relay_commit_hash(&self, _height: u64) -> Result<String> {
                Ok("0x00".to_string())
            }
            fn submit_header(&self, _header_hex: &str) -> Result<String> {
                Ok("0xtx".to_string())
            }
        }

        let bitcoin = UpToDateBitcoin;
        let submitter = AheadSubmitter;
        let state_store = test_state_store();
        let mut loop_state = SyncLoopState::new(0, 16, 2);
        let result = run_poll_cycle(
            &bitcoin,
            &submitter,
            SyncTrigger::PollTick,
            &mut loop_state,
            &state_store,
        )
        .expect("poll cycle");
        assert_eq!(result, SyncResult::UpToDate);
    }

    #[test]
    fn run_poll_cycle_moves_relay_to_node_branch_after_reorg() {
        // Node replaced blocks 109 and 110 with 109'..111'.
        let node = BranchNode {
            tip: Cell::new(111),
            fork_from: 109,
        };
        let relay = BranchRelay::new(110, false);
        let result = poll(&node, &relay, 16);
        assert!(matches!(result, Ok(SyncResult::Progressed)), "{result:?}");
        assert_eq!(relay.relay_tip_height().unwrap(), 111);
        assert_eq!(
            relay.relay_commit_hash(111).unwrap(),
            fake_full_header_hex(1111)
        );
    }

    /// Node whose blocks from `fork_from` up come from a second branch.
    struct BranchNode {
        tip: Cell<u64>,
        fork_from: u64,
    }

    impl BitcoinRpcClient for BranchNode {
        fn get_block_count(&self) -> Result<u64> {
            Ok(self.tip.get())
        }
        fn get_block_hash(&self, height: u64) -> Result<String> {
            if height > self.tip.get() {
                anyhow::bail!("block height out of range");
            }
            if height >= self.fork_from {
                return Ok(format!("new-{height}"));
            }
            Ok(format!("old-{height}"))
        }
        fn get_best_block_hash(&self) -> Result<String> {
            self.get_block_hash(self.tip.get())
        }
        fn get_block_header_hex(&self, hash: &str) -> Result<String> {
            let (branch, height) = hash.split_once('-').expect("hash");
            let height = height.parse::<u64>()?;
            Ok(fake_full_header_hex(if branch == "new" {
                height + 1000
            } else {
                height
            }))
        }
        fn get_block_chainwork(&self, _hash: &str) -> Result<[u8; 32]> {
            Ok([0_u8; 32])
        }
    }

    /// Relay with checkpoint 100 that holds the old branch up to `tip`. Each block has the same work.
    struct BranchRelay {
        // Header hex by height; empty below the checkpoint.
        main: RefCell<Vec<String>>,
        forks: RefCell<HashMap<u64, Vec<String>>>,
        calls: RefCell<Vec<String>>,
        lighter: bool,
    }

    impl BranchRelay {
        fn new(tip: u64, lighter: bool) -> Self {
            let main = (0..=tip)
                .map(|h| {
                    if h < 100 {
                        String::new()
                    } else {
                        fake_full_header_hex(h)
                    }
                })
                .collect();
            Self {
                main: RefCell::new(main),
                forks: RefCell::new(HashMap::new()),
                calls: RefCell::new(Vec::new()),
                lighter,
            }
        }

        // Check the stored header against `chain`, then append the compact headers to it.
        fn extend(chain: &mut Vec<String>, header_hex: &str) -> Result<(u64, u64)> {
            let payload = decode_even_hex(header_hex)?;
            let parent = u32::from_be_bytes(payload[112..116].try_into()?) as usize;
            if parent >= chain.len() || header_hex[..160] != chain[parent] {
                anyhow::bail!("execution reverted: block commitment");
            }
            chain.truncate(parent + 1);
            for c in payload[160..].chunks(48) {
                let h = [&c[0..4], &[0_u8; 32][..], &c[4..48]].concat();
                chain.push(h.iter().map(|b| format!("{b:02x}")).collect());
            }
            Ok((parent as u64, chain.len() as u64 - 1))
        }

        fn record(&self, kind: &str, result: &Result<(u64, u64)>) {
            self.calls.borrow_mut().push(match result {
                Ok((parent, last)) => format!("{kind} {parent}..{last}"),
                Err(err) => format!("{kind} {err}"),
            });
        }
    }

    impl BtcRelaySubmitter for BranchRelay {
        fn relay_tip_height(&self) -> Result<u64> {
            Ok(self.main.borrow().len() as u64 - 1)
        }
        fn relay_chain_work_bytes(&self) -> Result<[u8; 32]> {
            Ok([0_u8; 32])
        }
        fn relay_commit_hash(&self, height: u64) -> Result<String> {
            let commit = self.main.borrow().get(height as usize).cloned();
            Ok(commit
                .filter(|c| !c.is_empty())
                .unwrap_or_else(|| format!("0x{}", "0".repeat(64))))
        }
        fn submit_header(&self, header_hex: &str) -> Result<String> {
            let mut chain = self.main.borrow().clone();
            let result = Self::extend(&mut chain, header_hex).and_then(|r| {
                if r.0 + 1 != self.main.borrow().len() as u64 {
                    anyhow::bail!("execution reverted: block commitment");
                }
                Ok(r)
            });
            self.record("main", &result);
            if result.is_ok() {
                *self.main.borrow_mut() = chain;
            }
            result.map(|_| "0xtx".to_string())
        }
        fn submit_short_fork(&self, header_hex: &str) -> Result<String> {
            let mut chain = self.main.borrow().clone();
            let result = Self::extend(&mut chain, header_hex).and_then(|r| {
                if self.lighter || chain.len() <= self.main.borrow().len() {
                    anyhow::bail!("execution reverted: shortFork: not enough work");
                }
                Ok(r)
            });
            self.record("short", &result);
            if result.is_ok() {
                *self.main.borrow_mut() = chain;
            }
            result.map(|_| "0xtx".to_string())
        }
        fn submit_fork(&self, fork_id: u64, header_hex: &str) -> Result<String> {
            let mut forks = self.forks.borrow_mut();
            let mut chain = forks
                .remove(&fork_id)
                .unwrap_or_else(|| self.main.borrow().clone());
            let result = Self::extend(&mut chain, header_hex);
            self.record("fork", &result);
            result?;
            if chain.len() > self.main.borrow().len() {
                *self.main.borrow_mut() = chain;
            } else {
                forks.insert(fork_id, chain);
            }
            Ok("0xtx".to_string())
        }
        fn stored_header_commitment(&self, stored_header: &[u8]) -> String {
            stored_header[..80]
                .iter()
                .map(|b| format!("{b:02x}"))
                .collect()
        }
    }

    fn poll(node: &BranchNode, relay: &BranchRelay, batch: u64) -> Result<SyncResult> {
        let mut loop_state = SyncLoopState::new(0, batch, 0);
        poll_with(node, relay, &mut loop_state)
    }

    fn poll_with(
        node: &BranchNode,
        relay: &BranchRelay,
        loop_state: &mut SyncLoopState,
    ) -> Result<SyncResult> {
        let state_store = test_state_store();
        run_poll_cycle(node, relay, SyncTrigger::PollTick, loop_state, &state_store)
    }

    #[test]
    fn reorg_follows_node_branch_by_short_fork_or_fork() {
        let rejected = "main execution reverted: block commitment";
        // (first replaced height, node tip, expected calls)
        let cases: [(u64, u64, &[&str]); 3] = [
            (110, 111, &[rejected, "short 109..111"]),
            (108, 111, &[rejected, "short 107..111"]),
            (
                106,
                115,
                &[rejected, "fork 105..109", "fork 109..113", "fork 113..115"],
            ),
        ];
        for (fork_from, node_tip, calls) in cases {
            let node = BranchNode {
                tip: Cell::new(node_tip),
                fork_from,
            };
            let relay = BranchRelay::new(110, false);

            let result = poll(&node, &relay, 4);
            assert!(matches!(result, Ok(SyncResult::Progressed)), "{result:?}");
            assert_eq!(*relay.calls.borrow(), calls);
            assert_eq!(relay.relay_tip_height().unwrap(), node_tip);
            assert_eq!(
                relay.relay_commit_hash(node_tip).unwrap(),
                fake_full_header_hex(node_tip + 1000)
            );

            // Main sync continues on the new branch.
            node.tip.set(node_tip + 1);
            relay.calls.borrow_mut().clear();
            let result = poll(&node, &relay, 4);
            assert!(matches!(result, Ok(SyncResult::Progressed)), "{result:?}");
            let main_call = format!("main {}..{}", node_tip, node_tip + 1);
            assert_eq!(*relay.calls.borrow(), [main_call]);
        }
    }

    #[test]
    fn short_fork_limit_is_independent_of_catchup_batch_size() {
        // catchup_batch_size alone would fit a 3-block branch in one short-fork call;
        // a tighter short_fork_limit must still force the multi-call fork path.
        let node = BranchNode {
            tip: Cell::new(111),
            fork_from: 108,
        };
        let relay = BranchRelay::new(110, false);
        let mut loop_state = SyncLoopState::new(0, 16, 0);
        loop_state.short_fork_limit = 2;

        let result = poll_with(&node, &relay, &mut loop_state);
        assert!(matches!(result, Ok(SyncResult::Progressed)), "{result:?}");
        assert_eq!(
            *relay.calls.borrow(),
            ["main execution reverted: block commitment", "fork 107..111"]
        );
        assert_eq!(relay.relay_tip_height().unwrap(), 111);
    }

    #[test]
    fn reorg_relay_cannot_follow_is_reported_once_per_tip() {
        let rejected = "main execution reverted: block commitment";
        // Fork point below the checkpoint, and a branch the relay rejects as lighter.
        let cases: [(u64, bool, &[&str]); 2] = [
            (100, false, &[rejected]),
            (
                110,
                true,
                &[
                    rejected,
                    "short execution reverted: shortFork: not enough work",
                ],
            ),
        ];
        for (fork_from, lighter, calls) in cases {
            let node = BranchNode {
                tip: Cell::new(111),
                fork_from,
            };
            let relay = BranchRelay::new(110, lighter);
            let mut loop_state = SyncLoopState::new(0, 4, 0);

            for _ in 0..3 {
                let result = poll_with(&node, &relay, &mut loop_state);
                assert!(
                    matches!(result, Ok(SyncResult::ReorgDetected)),
                    "{result:?}"
                );
            }
            assert_eq!(*relay.calls.borrow(), calls);
            assert_eq!(relay.relay_tip_height().unwrap(), 110);

            // A new node tip allows one new attempt.
            node.tip.set(112);
            let result = poll_with(&node, &relay, &mut loop_state);
            assert!(
                matches!(result, Ok(SyncResult::ReorgDetected)),
                "{result:?}"
            );
            assert_eq!(relay.calls.borrow().len(), calls.len() * 2);
        }
    }

    fn test_state_store() -> JsonFileStateStore {
        // Tests run in parallel. The counter keeps each path unique within one millisecond.
        static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let mut path = env::temp_dir();
        path.push(format!(
            "btc-relay-sync-engine-state-{}-{}-{}.json",
            std::process::id(),
            current_test_timestamp(),
            NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
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
        header[68..72]
            .copy_from_slice(&(1_700_000_000_u32.saturating_add(height as u32)).to_le_bytes()); // timestamp LE
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
