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
