# btc-relay

Rust daemon that keeps an on-chain BTC relay contract synchronized with Bitcoin headers.

## What This Service Does

Functional statement:

- Read Bitcoin tip and headers from Bitcoin RPC.
- Read current relay tip/chainwork from EVM BTCRelay contract.
- Build relay payloads and submit missing headers.
- Wait for confirmations and retry temporary failures.
- Persist local progress for visibility/debugging.

This project is MVP-first and intentionally narrow: one daemon process, one sync loop, one relay contract write path (`submitMainBlockheaders`).

## Architecture

This is not full onion/clean architecture yet, but responsibilities are separated enough to navigate safely.

- **Entry / wiring layer**: `src/main.rs`
  - Load config and logging.
  - Run startup checks.
  - Construct dependencies and start sync loop.
- **Startup checks layer**: `src/startup.rs`
  - Validate config and external connectivity before loop starts.
- **Bitcoin gateway layer**: `src/bitcoin_rpc.rs`
  - Bitcoin JSON-RPC client (tip, hash, raw header, IBD flag).
- **EVM relay gateway layer**: `src/evm_relay_contract_client.rs`
  - EVM read/write client for BTCRelay contract and tx confirmations.
- **Business logic layer**: `src/sync_engine.rs`
  - Poll loop, catch-up range, batching mode, retry/backoff, payload assembly.
- **State/checkpoint layer**: `src/persistence.rs`
  - JSON checkpoint load/save (`STATE_FILE_PATH`).
- **Shared boundaries**: `src/interfaces.rs`
  - Traits and shared types used by sync logic.
- **Configuration model**: `src/configs.rs`
  - Env-to-struct config with validation/defaults.

## Main Runtime Flow

1. `main` loads `.env` and initializes `tracing`.
2. Startup validates config + probes Bitcoin RPC + probes EVM relay reads.
3. `run_sync_loop` starts and repeats forever:
   - fetch Bitcoin tip
   - fetch relay tip (`getBlockheight`)
   - compute missing range
   - submit in catch-up batches or single-header live mode
   - wait tx confirmations
   - persist local checkpoint
   - retry temporary failures with exponential backoff

## Module Responsibility Map 

- **Config/env vars**: `src/configs.rs`, `.env.example`
- **Startup behavior / smoke checks**: `src/startup.rs`
- **Bitcoin RPC integration**: `src/bitcoin_rpc.rs`
- **EVM tx sending / ABI / confirmations**: `src/evm_relay_contract_client.rs`
- **Sync policy, ranges, batching, retry logic**: `src/sync_engine.rs`
- **Persisted checkpoint format/location**: `src/persistence.rs`
- **Trait contracts/fakes for tests**: `src/interfaces.rs`

## Run

1. Copy env file and fill values:
   - `cp .env.example .env`
2. Run daemon:
   - `cargo run`

## Test

- Run unit tests:
  - `cargo test`
- Compile-only check:
  - `cargo check`

Current test focus:

- Bitcoin RPC payload/response validation (`src/bitcoin_rpc.rs` tests).
- EVM payload encoding and helper parsing (`src/evm_relay_contract_client.rs` tests).
- Sync engine range math, retry classification, catch-up behavior with fakes (`src/sync_engine.rs` tests).
- JSON persistence roundtrip (`src/persistence.rs` tests).

Coverage gap to keep in mind:

- Integration/e2e paths (real Bitcoin RPC + real EVM endpoint + funded tx path) are documented but not fully automated in CI.

## Known Design Debt 

- Sync engine and payload builder are still dense and carry multiple responsibilities.
- EVM client currently creates some dependencies inside methods (testability can be improved with injected abstractions).
- Error classification for retries is string-based.
- No production metrics/alerts layer yet (only logs).
