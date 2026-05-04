// Copyright (C) 2026 Utexo.
// See LICENSE for copying information.

//! HTTP/JSON-RPC client for the **on-chain BTCRelay contract** (reads via `eth_call`, writes via signed txs).
//!
//! Implementation detail: encode relay ABI with `alloy`, sign and send with `ethers`, poll receipts with bare JSON-RPC — two stacks, one job.
//! MVP path only: `submitMainBlockheaders`. Fork helpers are compiled but unused — delete them when you're sure you won't need them.

use alloy::primitives::U256 as AlloyU256;
use anyhow::{Context, Result};
use ethers::middleware::SignerMiddleware;
use ethers::providers::{Http, Middleware, Provider};
use ethers::signers::{LocalWallet, Signer};
use ethers::types::{
    transaction::eip2718::TypedTransaction, Address, Bytes, Eip1559TransactionRequest,
    NameOrAddress, U256 as EthersU256,
};
use serde::Deserialize;
use serde_json::{json, Value};
use std::thread;
use std::time::{Duration, Instant};

use alloy::sol;
use alloy::sol_types::SolCall;

use crate::configs::AppConfig;
use crate::interfaces::BtcRelaySubmitter;

/// `0x` + 64 hex nibbles = 32-byte keccak tx hash. Anything else is not a real `eth_sendRawTransaction` return.
const EVM_TX_HASH_HEX_LEN: usize = 66;

// `IBtcRelayView`: alloy `sol!` view of the on-chain BTCRelay ABI we call (historical name; contract is BTCRelay).
// Must match deployed bytecode. MVP calls only `submitMainBlockheaders`; fork entries keep unused calldata builders compiling.
sol! {
    interface IBtcRelayView {
        function getBlockheight() external view returns (uint32);
        function getChainwork() external view returns (uint224);
        function getCommitHash(uint256 height) external view returns (bytes32);
        function submitMainBlockheaders(bytes headers) external;
        function submitShortForkBlockheaders(bytes headers) external;
        function submitForkBlockheaders(uint256 forkId, bytes headers) external;
    }
}

/// Config + credentials to talk to **one** deployed relay contract address on **one** EVM chain.
#[allow(dead_code)]
pub struct EvmRelayContractClient {
    /// JSON-RPC HTTP(S) endpoint — same string you’d paste into `cast rpc --rpc-url`.
    pub evm_rpc_url: String,
    /// Relay proxy address; both `eth_call` and txs target this contract.
    pub relay_contract_address: String,
    /// Hex-encoded secp256k1 key with `0x` prefix; signs submissions (keep out of logs).
    pub relayer_private_key: String,
    /// EIP-155 chain id; must match `eth_chainId` or signatures are rejected.
    pub evm_chain_id: u64,
    /// Depth for `wait_for_confirmation` — compares head block from `eth_blockNumber` vs receipt block.
    pub evm_tx_confirmations: u64,
    /// Hard stop for receipt polling so we don't loop until heat death.
    pub evm_tx_timeout_secs: u64,
    /// If set, caps `maxFeePerGas` (gwei). If `None`, ethers/node fills it in.
    pub evm_max_fee_gwei: Option<u64>,
    /// If set, sets `maxPriorityFeePerGas` (gwei) for EIP-1559.
    pub evm_priority_fee_gwei: Option<u64>,
}

#[allow(dead_code)]
impl EvmRelayContractClient {
    /// Copy strings and numbers out of `AppConfig` — client owns its snapshot so callers can drop the config.
    pub fn from_config(cfg: &AppConfig) -> Self {
        Self {
            evm_rpc_url: cfg.evm_rpc_url.clone(),
            relay_contract_address: cfg.relay_contract_address.clone(),
            relayer_private_key: cfg.relayer_private_key.clone(),
            evm_chain_id: cfg.evm_chain_id,
            evm_tx_confirmations: cfg.evm_tx_confirmations,
            evm_tx_timeout_secs: cfg.evm_tx_timeout_secs,
            evm_max_fee_gwei: cfg.evm_max_fee_gwei,
            evm_priority_fee_gwei: cfg.evm_priority_fee_gwei,
        }
    }

    /// Sync engine gives us **no-0x** hex (concatenated ABI blob). This turns nibbles into bytes or bails loudly.
    fn payload_hex_to_bytes(&self, payload_hex: &str) -> Result<Vec<u8>> {
        if payload_hex.trim().is_empty() {
            anyhow::bail!("submit_header requires non-empty payload");
        }
        if payload_hex.len() % 2 != 0 {
            anyhow::bail!("submit_header requires even-length hex payload");
        }

        let mut out = Vec::with_capacity(payload_hex.len() / 2);
        let bytes = payload_hex.as_bytes(); // ASCII hex digits, two per output byte
        let mut i = 0;
        while i < bytes.len() {
            let hi = hex_nibble(bytes[i]).context("payload contains non-hex character")?;
            let lo = hex_nibble(bytes[i + 1]).context("payload contains non-hex character")?;
            out.push((hi << 4) | lo); // one byte from two nibbles
            i += 2;
        }

        Ok(out)
    }

    /// Spawn a one-off current-thread tokio runtime because the rest of the daemon is sync. Not pretty; works.
    fn send_tx(&self, calldata: &[u8]) -> Result<String> {
        if calldata.is_empty() {
            anyhow::bail!("cannot send header submission tx with empty calldata");
        }
        if self.evm_chain_id == 0 {
            anyhow::bail!("cannot send tx: EVM chain id must be > 0");
        }
        if self.evm_tx_timeout_secs == 0 {
            anyhow::bail!("cannot send tx: EVM tx timeout must be > 0");
        }

        // `async move` takes ownership — clone anything from `self` we need inside the block.
        let rpc_url = self.evm_rpc_url.clone();
        let private_key = self.relayer_private_key.clone();
        let relay_contract_address = self.relay_contract_address.clone();
        let chain_id = self.evm_chain_id;
        let max_fee_gwei = self.evm_max_fee_gwei;
        let priority_fee_gwei = self.evm_priority_fee_gwei;
        let calldata_bytes = calldata.to_vec();

        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .context("failed to initialize tokio runtime for evm tx send")?;

        let tx_hash = runtime.block_on(async move {
            // HTTP provider: one JSON-RPC endpoint for read + send in this closure.
            let provider = Provider::<Http>::try_from(rpc_url.as_str())
                .context("failed to create EVM provider")?;
            let wallet = private_key
                .parse::<LocalWallet>()
                .context("invalid RELAYER_PRIVATE_KEY format")?
                .with_chain_id(chain_id);
            // Signs txs locally; never sends the raw key to the RPC (only signed raw tx).
            let client = SignerMiddleware::new(provider, wallet);

            let to = relay_contract_address
                .parse::<Address>()
                .context("invalid RELAY_CONTRACT_ADDRESS")?;
            let mut req = Eip1559TransactionRequest {
                to: Some(NameOrAddress::Address(to)),
                data: Some(Bytes::from(calldata_bytes)), // full calldata: selector + ABI-encoded args
                chain_id: Some(chain_id.into()),
                ..Default::default()
            };
            // Gwei → wei. Omit both fields = node estimates; set one or both = you own the fees.
            if let Some(max_fee) = max_fee_gwei {
                req.max_fee_per_gas =
                    Some(EthersU256::from(max_fee) * EthersU256::from(1_000_000_000_u64));
            }
            if let Some(priority_fee) = priority_fee_gwei {
                req.max_priority_fee_per_gas =
                    Some(EthersU256::from(priority_fee) * EthersU256::from(1_000_000_000_u64));
            }
            let tx: TypedTransaction = req.into();

            // `pending` resolves once the tx is in the mempool / RPC accepted it — not yet mined.
            let pending = client
                .send_transaction(tx, None)
                .await
                .context("eth_sendRawTransaction failed")?;
            Ok::<String, anyhow::Error>(format!("{:#x}", pending.tx_hash()))
        })?;

        if !is_valid_tx_hash(&tx_hash) {
            anyhow::bail!(
                "eth_sendRawTransaction returned invalid tx hash: {}",
                tx_hash
            );
        }

        Ok(tx_hash)
    }

    /// Poll `eth_getTransactionReceipt` + `eth_blockNumber` until enough confirmations or timeout. Revert = hard error.
    fn wait_for_confirmation(&self, tx_hash: &str) -> Result<()> {
        if !is_valid_tx_hash(tx_hash) {
            anyhow::bail!(
                "invalid tx hash format: expected 0x-prefixed 32-byte hash ({} chars)",
                EVM_TX_HASH_HEX_LEN
            );
        }
        if self.evm_tx_confirmations == 0 {
            anyhow::bail!("cannot wait for confirmation: EVM_TX_CONFIRMATIONS must be > 0");
        }
        if self.evm_tx_timeout_secs == 0 {
            anyhow::bail!("cannot wait for confirmation: EVM_TX_TIMEOUT_SECS must be > 0");
        }

        #[derive(Debug, Deserialize)]
        struct Receipt {
            /// `0x1` success, `0x0` revert — both mean "mined"; null receipt earlier means "pending".
            status: Option<String>,
            #[serde(rename = "blockNumber")]
            /// Hex quantity string, e.g. `0x3b` — block that included this tx.
            block_number: Option<String>,
        }
        // Receipt shape is minimal on purpose — we only need success bit + block number.

        let deadline = Instant::now() + Duration::from_secs(self.evm_tx_timeout_secs);
        let poll_interval = Duration::from_secs(2); // don't spam the RPC every millisecond

        loop {
            if Instant::now() >= deadline {
                anyhow::bail!(
                    "timed out waiting for tx confirmation after {}s (tx: {})",
                    self.evm_tx_timeout_secs,
                    tx_hash
                );
            }

            let receipt_value = self
                .rpc_request("eth_getTransactionReceipt", json!([tx_hash]))
                .with_context(|| format!("failed eth_getTransactionReceipt for {}", tx_hash))?;

            if receipt_value.is_null() {
                // Not mined yet — normal right after broadcast.
                thread::sleep(poll_interval);
                continue;
            }

            let receipt: Receipt = serde_json::from_value(receipt_value)
                .context("failed to decode eth_getTransactionReceipt result")?;

            match receipt.status.as_deref() {
                Some("0x1") => {} // execution succeeded
                Some("0x0") => anyhow::bail!("transaction reverted on-chain: {}", tx_hash),
                Some(other) => {
                    anyhow::bail!("unexpected transaction status {} for {}", other, tx_hash)
                }
                None => anyhow::bail!("transaction receipt missing status for {}", tx_hash),
            }

            let tx_block = receipt
                .block_number
                .as_deref()
                .context("transaction receipt missing blockNumber")?;
            let tx_block_num = parse_hex_quantity_u64(tx_block)
                .context("invalid tx receipt blockNumber format")?;

            // Chain head — "how far has the network moved since this tx landed?"
            let head = self
                .rpc_request("eth_blockNumber", json!([]))
                .context("failed to fetch eth_blockNumber")?
                .as_str()
                .context("eth_blockNumber returned non-string result")?
                .to_string();

            let head_num =
                parse_hex_quantity_u64(head.as_str()).context("invalid eth_blockNumber format")?;

            // Inclusive depth: same block as head => 1 confirmation; one block later => 2; etc.
            let confirmations = head_num.saturating_sub(tx_block_num) + 1;
            if confirmations >= self.evm_tx_confirmations {
                return Ok(());
            }

            thread::sleep(poll_interval);
        }
    }

    /// ABI encode `submitMainBlockheaders(bytes)` — `headers_bytes` is already the concatenation the contract expects.
    fn build_submit_main_calldata(&self, headers_bytes: &[u8]) -> Vec<u8> {
        // alloy wants owned `Bytes`-like; `.into()` on the call struct consumes the vec.
        let owned_headers: Vec<u8> = headers_bytes.to_vec();
        let call = IBtcRelayView::submitMainBlockheadersCall {
            headers: owned_headers.into(),
        };
        call.abi_encode() // 4-byte selector + ABI-encoded `bytes` (offset + length + payload)
    }

    /// Dead code today: would call `submitShortForkBlockheaders`. Wire it when fork drama is your problem.
    #[allow(dead_code)]
    fn build_submit_short_fork_calldata(&self, headers_bytes: &[u8]) -> Vec<u8> {
        let owned_headers: Vec<u8> = headers_bytes.to_vec();
        let call = IBtcRelayView::submitShortForkBlockheadersCall {
            headers: owned_headers.into(),
        };
        call.abi_encode()
    }

    /// Dead code today: `submitForkBlockheaders(forkId, bytes)`.
    #[allow(dead_code)]
    fn build_submit_fork_calldata(&self, fork_id: u64, headers_bytes: &[u8]) -> Vec<u8> {
        let owned_headers: Vec<u8> = headers_bytes.to_vec();
        let call = IBtcRelayView::submitForkBlockheadersCall {
            forkId: AlloyU256::try_from(fork_id).unwrap(),
            headers: owned_headers.into(),
        };
        call.abi_encode()
    }

    /// `eth_call` at `latest` — returns raw return bytes for us to ABI-decode per method.
    fn evm_call_latest(&self, to: &str, data: &[u8]) -> Result<Vec<u8>> {
        // `data` is already full calldata for the view function (selector + args). Returns hex-encoded ABI return blob.
        let result = self
            .rpc_request(
                "eth_call",
                json!([
                    {
                        "to": to,
                        "data": bytes_to_prefixed_hex(data),
                    },
                    "latest"
                ]),
            )
            .context("eth_call failed")?
            .as_str()
            .context("eth_call returned non-string result")?
            .to_string();

        hex_prefixed_to_bytes(&result).context("eth_call returned invalid hex result")
    }

    /// Generic JSON-RPC 2.0 POST — used for reads and receipt polling (not the ethers provider path).
    fn rpc_request(&self, method: &str, params: Value) -> Result<Value> {
        #[derive(Debug, Deserialize)]
        struct JsonRpcResponse {
            result: Option<Value>,
            error: Option<Value>,
        }

        let request = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": method,
            "params": params,
        });

        // Fresh client per call — simple, slightly wasteful; daemon isn't latency-critical here.
        let response: JsonRpcResponse = reqwest::blocking::Client::new()
            .post(&self.evm_rpc_url)
            .json(&request)
            .send()
            .with_context(|| format!("{} transport failed", method))?
            .json()
            .with_context(|| format!("{} response decode failed", method))?;

        if let Some(err) = response.error {
            anyhow::bail!("{} returned error: {}", method, err);
        }

        response
            .result
            .with_context(|| format!("{} response missing result field", method))
    }
}

/// Quick shape check before we enter the receipt polling loop.
fn is_valid_tx_hash(value: &str) -> bool {
    value.len() == EVM_TX_HASH_HEX_LEN
        && value.starts_with("0x")
        && value.chars().skip(2).all(|c| c.is_ascii_hexdigit())
}

impl BtcRelaySubmitter for EvmRelayContractClient {
    /// On-chain height — **the** number the sync loop uses to decide how far behind we are.
    fn relay_tip_height(&self) -> Result<u64> {
        let call = IBtcRelayView::getBlockheightCall {};
        let raw = self
            .evm_call_latest(&self.relay_contract_address, &call.abi_encode())
            .context("failed to call BTCRelay.getBlockheight")?;

        // `raw` is exactly 32 bytes ABI-encoded uint32 (left-padded) — alloy strips padding for us.
        let height = IBtcRelayView::getBlockheightCall::abi_decode_returns(&raw)
            .context("failed to decode BTCRelay.getBlockheight return value")?;

        Ok(u64::from(height))
    }

    /// Contract returns uint224; we right-pad to 32 bytes for the 160-byte prologue in the submit payload.
    fn relay_chain_work_bytes(&self) -> Result<[u8; 32]> {
        let call = IBtcRelayView::getChainworkCall {};
        let raw = self
            .evm_call_latest(&self.relay_contract_address, &call.abi_encode())
            .context("failed to call BTCRelay.getChainwork")?;

        let chain_work = IBtcRelayView::getChainworkCall::abi_decode_returns(&raw)
            .context("failed to decode BTCRelay.getChainwork return value")?;
        // uint224 in ABI is still 32 bytes on the wire; in Rust we get the integer and re-serialize to 28 BE bytes.
        let chain_work_be_28 = chain_work.to_be_bytes::<28>();
        let mut out = [0_u8; 32];
        // Sync engine's prologue expects 32 bytes; chainwork is only 224 bits → pad 4 zero bytes on the **left** (big-endian layout in high end).
        out[4..].copy_from_slice(&chain_work_be_28);
        Ok(out)
    }

    /// bytes32 as `0x…` hex — startup uses tip height to prove reads work.
    fn relay_commit_hash(&self, height: u64) -> Result<String> {
        let call = IBtcRelayView::getCommitHashCall {
            height: AlloyU256::try_from(height)?,
        };
        let raw = self
            .evm_call_latest(&self.relay_contract_address, &call.abi_encode())
            .with_context(|| format!("failed to call BTCRelay.getCommitHash({})", height))?;

        let commit_hash = IBtcRelayView::getCommitHashCall::abi_decode_returns(&raw)
            .context("failed to decode BTCRelay.getCommitHash return value")?;

        Ok(bytes_to_prefixed_hex(commit_hash.as_slice()))
    }

    /// Full pipeline: hex → bytes → `submitMainBlockheaders` calldata → sign → send → wait confirmations → return tx hash.
    fn submit_header(&self, header_hex: &str) -> Result<String> {
        // `header_hex` can be huge (batched compact headers); still no `0x` prefix — see sync_engine.
        let header_bytes = self
            .payload_hex_to_bytes(header_hex)
            .context("failed to validate/convert submit payload hex")?;
        let calldata = self.build_submit_main_calldata(&header_bytes);

        let tx_hash = self
            .send_tx(&calldata)
            .context("failed to send header submission transaction")?;

        // Only return once mined deep enough — sync loop assumes relay state reflects this tx.
        self.wait_for_confirmation(&tx_hash)
            .context("header submission transaction failed confirmation step")?;

        Ok(tx_hash)
    }
}

/// Lowercase `0x` hex for JSON-RPC `data` fields.
fn bytes_to_prefixed_hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(2 + bytes.len() * 2);
    out.push_str("0x");
    for b in bytes {
        use std::fmt::Write as _;
        let _ = write!(&mut out, "{:02x}", b);
    }
    out
}

/// Ethereum JSON-RPC quantities: `0x` prefixed hex for block numbers etc.
fn parse_hex_quantity_u64(value: &str) -> Result<u64> {
    let raw = value
        .strip_prefix("0x")
        .context("hex quantity must start with 0x")?;
    if raw.is_empty() {
        return Ok(0); // `0x` alone means zero per JSON-RPC examples
    }
    u64::from_str_radix(raw, 16).context("failed to parse hex quantity as u64")
}

/// Single ASCII hex digit → 0..15. Garbage in → `None`.
fn hex_nibble(c: u8) -> Option<u8> {
    match c {
        b'0'..=b'9' => Some(c - b'0'),
        b'a'..=b'f' => Some(c - b'a' + 10),
        b'A'..=b'F' => Some(c - b'A' + 10),
        _ => None,
    }
}

/// Decode `eth_call` return strings (`0x` + even hex) into raw bytes.
fn hex_prefixed_to_bytes(value: &str) -> Result<Vec<u8>> {
    if !value.starts_with("0x") {
        anyhow::bail!("hex value must start with 0x");
    }
    let s = &value[2..];
    if s.len() % 2 != 0 {
        anyhow::bail!("hex value must have even length");
    }

    let mut out = Vec::with_capacity(s.len() / 2);
    let bytes = s.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        let hi = hex_nibble(bytes[i]).context("hex contains non-hex character")?;
        let lo = hex_nibble(bytes[i + 1]).context("hex contains non-hex character")?;
        out.push((hi << 4) | lo);
        i += 2;
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy::primitives::keccak256;

    fn test_relay_client() -> EvmRelayContractClient {
        EvmRelayContractClient {
            evm_rpc_url: "http://127.0.0.1:8545".to_string(),
            relay_contract_address: "0x1111111111111111111111111111111111111111".to_string(),
            relayer_private_key: "0x01".to_string(),
            evm_chain_id: 31337,
            evm_tx_confirmations: 1,
            evm_tx_timeout_secs: 10,
            evm_max_fee_gwei: None,
            evm_priority_fee_gwei: None,
        }
    }

    #[test]
    fn payload_hex_to_bytes_accepts_even_hex_payload() {
        let submitter = test_relay_client();
        let input = "00".repeat(80);
        let bytes = submitter
            .payload_hex_to_bytes(&input)
            .expect("payload should parse");
        assert_eq!(bytes.len(), 80);
        assert!(bytes.iter().all(|b| *b == 0));
    }

    #[test]
    fn payload_hex_to_bytes_rejects_odd_length() {
        let submitter = test_relay_client();
        let err = submitter
            .payload_hex_to_bytes("abc")
            .expect_err("odd payload length should fail");
        assert!(err.to_string().contains("even-length"));
    }

    #[test]
    fn payload_hex_to_bytes_rejects_non_hex_chars() {
        let submitter = test_relay_client();
        let mut header = "00".repeat(3);
        header.push_str("zz");
        let err = submitter
            .payload_hex_to_bytes(&header)
            .expect_err("non-hex payload should fail");
        assert!(err.to_string().contains("non-hex"));
    }

    #[test]
    fn build_submit_main_calldata_has_expected_selector() {
        let submitter = test_relay_client();
        let header_bytes = vec![0u8; 80];
        let calldata = submitter.build_submit_main_calldata(&header_bytes);
        assert!(calldata.len() > 4);

        let selector = &calldata[..4];
        let expected = &keccak256("submitMainBlockheaders(bytes)")[..4];
        assert_eq!(selector, expected);
    }

    #[test]
    fn parse_hex_quantity_handles_zero_and_regular_values() {
        assert_eq!(parse_hex_quantity_u64("0x").expect("empty hex quantity"), 0);
        assert_eq!(parse_hex_quantity_u64("0x0").expect("zero quantity"), 0);
        assert_eq!(parse_hex_quantity_u64("0x2a").expect("0x2a"), 42);
    }

    #[test]
    fn is_valid_tx_hash_checks_shape() {
        assert!(is_valid_tx_hash(
            "0x0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
        ));
        assert!(!is_valid_tx_hash("0x1234"));
        assert!(!is_valid_tx_hash(
            "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
        ));
    }
}
