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
    info!(
        "relay behind bitcoin tip: syncing range {}..={}, lag={}",
        from_height,
        to_height,
        lag
    );
    Ok(SyncResult::Progressed)
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
    use super::compute_catchup_range;

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
}
