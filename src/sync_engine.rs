use anyhow::Result;
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
}

impl SyncLoopState {
    fn new() -> Self {
        Self {
            state: SyncEngineState::Idle,
            poll_count: 0,
        }
    }
}

/// Loop orchestrator entrypoint.
///
/// Runs an infinite poll/sync cycle and leaves per-cycle behavior to
/// `run_poll_cycle`, which will be expanded.
pub fn run_sync_loop(
    bitcoin: &dyn BitcoinRpcClient,
    submitter: &dyn BtcRelaySubmitter,
    poll_interval_secs: u64,
) -> Result<()> {
    let poll_interval = Duration::from_secs(poll_interval_secs.max(1));
    let mut loop_state = SyncLoopState::new();

    info!(
        "sync loop started: poll_interval_secs={}",
        poll_interval.as_secs()
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
    _bitcoin: &dyn BitcoinRpcClient,
    _submitter: &dyn BtcRelaySubmitter,
    _trigger: SyncTrigger,
    loop_state: &mut SyncLoopState,
) -> Result<SyncResult> {
    // Now: scaffold: keep loop alive and mark engine as active.
    // Next: will implement tip discovery + catch-up processing.
    loop_state.state = SyncEngineState::Active;
    Ok(SyncResult::UpToDate)
}
