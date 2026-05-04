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

Each module has a clear role (see `//!` headers in source). Not full onion architecture yet, but boundaries are explicit.

| Module | Responsibility |
|--------|----------------|
| `src/main.rs` | Composition root: wire deps, start the loop. |
| `src/configs.rs` | Env → `AppConfig` + validation. |
| `src/startup.rs` | Pre-flight checks (no submissions). |
| `src/bitcoin_rpc.rs` | Bitcoin Core JSON-RPC client. |
| `src/evm_relay_contract_client.rs` | EVM BTCRelay contract client (`EvmRelayContractClient`). |
| `src/interfaces.rs` | Port traits (`BitcoinRpcClient`, `BtcRelaySubmitter`) + shared DTOs. |
| `src/sync_engine.rs` | Sync loop + payload assembly + retry policy. |
| `src/persistence.rs` | JSON checkpoint I/O only. |

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

## Module Responsibility Map (If You Need To Change X)

- **Config/env vars**: `src/configs.rs`, `.env.example`
- **Startup / smoke checks**: `src/startup.rs`
- **Bitcoin RPC**: `src/bitcoin_rpc.rs`
- **EVM relay contract (reads + signed txs)**: `src/evm_relay_contract_client.rs` (`EvmRelayContractClient`)
- **Sync orchestration**: `src/sync_engine.rs`
- **JSON checkpoint**: `src/persistence.rs`
- **Ports / test doubles**: `src/interfaces.rs`

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
