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
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};
use tracing::{info, warn};

use alloy::sol;
use alloy::sol_types::SolCall;

use crate::configs::AppConfig;
use crate::interfaces::BtcRelaySubmitter;
use crate::metrics;

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
    /// Warn when estimated txs left at current fee falls below this threshold.
    pub evm_low_balance_txs_left_warn: u64,
    /// Transport boundary: keeps client logic testable without real RPC/provider setup.
    transport: Arc<dyn EvmTransport>,
}

#[derive(Debug, Clone)]
struct SendTxRequest {
    rpc_url: String,
    private_key: String,
    relay_contract_address: String,
    chain_id: u64,
    max_fee_gwei: Option<u64>,
    priority_fee_gwei: Option<u64>,
    calldata: Vec<u8>,
}

#[derive(Debug, Clone, Copy)]
struct ConfirmationStats {
    tx_fee_wei: Option<AlloyU256>,
}

trait EvmTransport: Send + Sync {
    fn rpc_request(&self, rpc_url: &str, method: &str, params: Value) -> Result<Value>;
    fn send_transaction(&self, request: SendTxRequest) -> Result<String>;
}

/// JSON-RPC allows `"result": null` (e.g. pending `eth_getTransactionReceipt`).
/// That is distinct from a response that omits `result` entirely.
fn parse_json_rpc_response(response: Value, method: &str) -> Result<Value> {
    if response
        .get("error")
        .is_some_and(|err| !err.is_null())
    {
        let err = response.get("error").expect("checked above");
        anyhow::bail!("{} returned error: {}", method, err);
    }

    response
        .get("result")
        .cloned()
        .with_context(|| format!("{} response missing result field", method))
}

#[derive(Default)]
struct HttpEvmTransport;

impl EvmTransport for HttpEvmTransport {
    fn rpc_request(&self, rpc_url: &str, method: &str, params: Value) -> Result<Value> {
        let request = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": method,
            "params": params,
        });

        let response: Value = reqwest::blocking::Client::new()
            .post(rpc_url)
            .json(&request)
            .send()
            .with_context(|| format!("{} transport failed", method))?
            .json()
            .with_context(|| format!("{} response decode failed", method))?;

        parse_json_rpc_response(response, method)
    }

    fn send_transaction(&self, request: SendTxRequest) -> Result<String> {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .context("failed to initialize tokio runtime for evm tx send")?;

        runtime.block_on(async move {
            let provider = Provider::<Http>::try_from(request.rpc_url.as_str())
                .context("failed to create EVM provider")?;
            let wallet = request
                .private_key
                .parse::<LocalWallet>()
                .context("invalid RELAYER_PRIVATE_KEY format")?
                .with_chain_id(request.chain_id);
            let client = SignerMiddleware::new(provider, wallet);

            let to = request
                .relay_contract_address
                .parse::<Address>()
                .context("invalid RELAY_CONTRACT_ADDRESS")?;
            let mut req = Eip1559TransactionRequest {
                to: Some(NameOrAddress::Address(to)),
                data: Some(Bytes::from(request.calldata)),
                chain_id: Some(request.chain_id.into()),
                ..Default::default()
            };
            if let Some(max_fee) = request.max_fee_gwei {
                req.max_fee_per_gas =
                    Some(EthersU256::from(max_fee) * EthersU256::from(1_000_000_000_u64));
            }
            if let Some(priority_fee) = request.priority_fee_gwei {
                req.max_priority_fee_per_gas =
                    Some(EthersU256::from(priority_fee) * EthersU256::from(1_000_000_000_u64));
            }
            let tx: TypedTransaction = req.into();

            let pending = client
                .send_transaction(tx, None)
                .await
                .context("eth_sendRawTransaction failed")?;
            Ok::<String, anyhow::Error>(format!("{:#x}", pending.tx_hash()))
        })
    }
}

#[allow(dead_code)]
impl EvmRelayContractClient {
    /// Copy strings and numbers out of `AppConfig` — client owns its snapshot so callers can drop the config.
    pub fn from_config(cfg: &AppConfig) -> Self {
        Self::from_config_with_transport(cfg, Arc::new(HttpEvmTransport))
    }

    fn from_config_with_transport(cfg: &AppConfig, transport: Arc<dyn EvmTransport>) -> Self {
        Self {
            evm_rpc_url: cfg.evm_rpc_url.clone(),
            relay_contract_address: cfg.relay_contract_address.clone(),
            relayer_private_key: cfg.relayer_private_key.clone(),
            evm_chain_id: cfg.evm_chain_id,
            evm_tx_confirmations: cfg.evm_tx_confirmations,
            evm_tx_timeout_secs: cfg.evm_tx_timeout_secs,
            evm_max_fee_gwei: cfg.evm_max_fee_gwei,
            evm_priority_fee_gwei: cfg.evm_priority_fee_gwei,
            evm_low_balance_txs_left_warn: cfg.evm_low_balance_txs_left_warn,
            transport,
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

        let tx_hash = self.transport.send_transaction(SendTxRequest {
            rpc_url: self.evm_rpc_url.clone(),
            private_key: self.relayer_private_key.clone(),
            relay_contract_address: self.relay_contract_address.clone(),
            chain_id: self.evm_chain_id,
            max_fee_gwei: self.evm_max_fee_gwei,
            priority_fee_gwei: self.evm_priority_fee_gwei,
            calldata: calldata.to_vec(),
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
    fn wait_for_confirmation(&self, tx_hash: &str) -> Result<ConfirmationStats> {
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
            #[serde(rename = "gasUsed")]
            gas_used: Option<String>,
            #[serde(rename = "effectiveGasPrice")]
            effective_gas_price: Option<String>,
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
                .transport
                .rpc_request(&self.evm_rpc_url, "eth_getTransactionReceipt", json!([tx_hash]))
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
                .transport
                .rpc_request(&self.evm_rpc_url, "eth_blockNumber", json!([]))
                .context("failed to fetch eth_blockNumber")?
                .as_str()
                .context("eth_blockNumber returned non-string result")?
                .to_string();

            let head_num =
                parse_hex_quantity_u64(head.as_str()).context("invalid eth_blockNumber format")?;

            // Inclusive depth: same block as head => 1 confirmation; one block later => 2; etc.
            let confirmations = head_num.saturating_sub(tx_block_num) + 1;
            if confirmations >= self.evm_tx_confirmations {
                let tx_fee_wei = match (
                    receipt.gas_used.as_deref(),
                    receipt.effective_gas_price.as_deref(),
                ) {
                    (Some(gas_used), Some(effective_gas_price)) => {
                        let gas_used_u256 = parse_hex_quantity_u256(gas_used)
                            .context("invalid receipt gasUsed format")?;
                        let gas_price_u256 = parse_hex_quantity_u256(effective_gas_price)
                            .context("invalid receipt effectiveGasPrice format")?;
                        Some(gas_used_u256.saturating_mul(gas_price_u256))
                    }
                    _ => None,
                };
                return Ok(ConfirmationStats { tx_fee_wei });
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
            .transport
            .rpc_request(
                &self.evm_rpc_url,
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

    /// Relayer EOA derived from `RELAYER_PRIVATE_KEY` (used for tx signing and balance checks).
    pub fn relayer_wallet_address(&self) -> Result<String> {
        let wallet = self
            .relayer_private_key
            .parse::<LocalWallet>()
            .context("invalid RELAYER_PRIVATE_KEY format")?;
        Ok(format!("{:#x}", wallet.address()))
    }

    /// Current relayer wallet balance in wei (`eth_getBalance` at `latest`).
    pub fn relayer_wallet_balance_wei(&self) -> Result<AlloyU256> {
        let address = self.relayer_wallet_address()?;
        let value = self
            .transport
            .rpc_request(
                &self.evm_rpc_url,
                "eth_getBalance",
                json!([address, "latest"]),
            )
            .context("failed eth_getBalance")?
            .as_str()
            .context("eth_getBalance returned non-string result")?
            .to_string();
        parse_hex_quantity_u256(value.as_str()).context("invalid eth_getBalance format")
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
        let confirmation = self
            .wait_for_confirmation(&tx_hash)
            .context("header submission transaction failed confirmation step")?;
        if let Some(tx_fee_wei) = confirmation.tx_fee_wei {
            let tx_fee_wei_f64 = tx_fee_wei.to_string().parse::<f64>().unwrap_or(0.0);
            let tx_fee_eth = tx_fee_wei_f64 / 1_000_000_000_000_000_000_f64;
            metrics::record_confirmed_tx_fee_wei(tx_fee_wei_f64);
            match self.relayer_wallet_balance_wei() {
                Ok(balance_wei) => {
                    let balance_wei_f64 = balance_wei.to_string().parse::<f64>().unwrap_or(0.0);
                    let balance_eth = balance_wei_f64 / 1_000_000_000_000_000_000_f64;
                    let txs_left = if tx_fee_wei > AlloyU256::from(0_u8) {
                        balance_wei / tx_fee_wei
                    } else {
                        AlloyU256::from(0_u8)
                    };
                    let txs_left_f64 = txs_left.to_string().parse::<f64>().unwrap_or(0.0);
                    metrics::set_estimated_txs_left(txs_left_f64);
                    info!(
                        tx_hash = %tx_hash,
                        tx_fee_wei = %tx_fee_wei,
                        tx_fee_eth,
                        wallet_balance_wei = %balance_wei,
                        wallet_balance_eth = balance_eth,
                        est_txs_left_at_current_fee = %txs_left,
                        "header submission confirmed"
                    );
                    if self.evm_low_balance_txs_left_warn > 0
                        && txs_left <= AlloyU256::from(self.evm_low_balance_txs_left_warn)
                    {
                        warn!(
                            tx_hash = %tx_hash,
                            est_txs_left_at_current_fee = %txs_left,
                            threshold = self.evm_low_balance_txs_left_warn,
                            wallet_balance_eth = balance_eth,
                            tx_fee_eth,
                            "relayer funds are running low"
                        );
                    }
                }
                Err(err) => {
                    warn!(tx_hash = %tx_hash, error = %err, "failed reading relayer wallet balance after tx confirmation");
                }
            }
        }

        Ok(tx_hash)
    }

    fn relayer_wallet_address(&self) -> Result<String> {
        self.relayer_wallet_address()
    }

    fn relayer_wallet_balance_wei(&self) -> Result<AlloyU256> {
        self.relayer_wallet_balance_wei()
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

fn parse_hex_quantity_u256(value: &str) -> Result<AlloyU256> {
    let raw = value
        .strip_prefix("0x")
        .context("hex quantity must start with 0x")?;
    if raw.is_empty() {
        return Ok(AlloyU256::from(0_u8));
    }
    AlloyU256::from_str_radix(raw, 16).context("failed to parse hex quantity as u256")
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
    use std::sync::Mutex;

    struct MockEvmTransport {
        sent: Mutex<Vec<SendTxRequest>>,
        rpc_methods: Mutex<Vec<String>>,
        receipt_status: Mutex<String>,
        receipt_block: Mutex<String>,
        head_block: Mutex<String>,
        send_hash: Mutex<String>,
    }

    impl MockEvmTransport {
        fn new() -> Self {
            Self {
                sent: Mutex::new(Vec::new()),
                rpc_methods: Mutex::new(Vec::new()),
                receipt_status: Mutex::new("0x1".to_string()),
                receipt_block: Mutex::new("0x10".to_string()),
                head_block: Mutex::new("0x10".to_string()),
                send_hash: Mutex::new(
                    "0x0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
                        .to_string(),
                ),
            }
        }
    }

    impl EvmTransport for MockEvmTransport {
        fn rpc_request(&self, _rpc_url: &str, method: &str, _params: Value) -> Result<Value> {
            self.rpc_methods.lock().expect("rpc methods lock").push(method.to_string());
            match method {
                "eth_getTransactionReceipt" => Ok(json!({
                    "status": self.receipt_status.lock().expect("receipt status lock").clone(),
                    "blockNumber": self.receipt_block.lock().expect("receipt block lock").clone()
                })),
                "eth_blockNumber" => Ok(Value::String(
                    self.head_block.lock().expect("head block lock").clone(),
                )),
                _ => anyhow::bail!("unexpected rpc method {}", method),
            }
        }

        fn send_transaction(&self, request: SendTxRequest) -> Result<String> {
            self.sent.lock().expect("sent tx lock").push(request);
            Ok(self.send_hash.lock().expect("send hash lock").clone())
        }
    }

    fn test_relay_client() -> EvmRelayContractClient {
        test_relay_client_with_transport(Arc::new(MockEvmTransport::new()))
    }

    fn test_relay_client_with_transport(transport: Arc<dyn EvmTransport>) -> EvmRelayContractClient {
        EvmRelayContractClient {
            evm_rpc_url: "http://127.0.0.1:8545".to_string(),
            relay_contract_address: "0x1111111111111111111111111111111111111111".to_string(),
            relayer_private_key: "0x01".to_string(),
            evm_chain_id: 31337,
            evm_tx_confirmations: 1,
            evm_tx_timeout_secs: 10,
            evm_max_fee_gwei: None,
            evm_priority_fee_gwei: None,
            evm_low_balance_txs_left_warn: 50,
            transport,
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
    fn parse_hex_quantity_rejects_missing_prefix_and_invalid_digits() {
        let missing_prefix =
            parse_hex_quantity_u64("2a").expect_err("missing 0x prefix should fail");
        assert!(missing_prefix.to_string().contains("must start with 0x"));

        let bad_digits = parse_hex_quantity_u64("0xgg").expect_err("invalid hex should fail");
        assert!(bad_digits.to_string().contains("failed to parse hex quantity"));
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

    #[test]
    fn submit_header_orchestrates_send_and_confirmation_through_transport() {
        let mock = Arc::new(MockEvmTransport::new());
        let submitter = test_relay_client_with_transport(mock.clone());
        let tx_hash = submitter
            .submit_header(&"00".repeat(80))
            .expect("submit header should succeed");
        assert!(is_valid_tx_hash(&tx_hash));

        let sent = mock.sent.lock().expect("sent lock");
        assert_eq!(sent.len(), 1);
        assert!(!sent[0].calldata.is_empty());
        drop(sent);

        let methods = mock.rpc_methods.lock().expect("methods lock");
        assert_eq!(
            methods.as_slice(),
            &["eth_getTransactionReceipt".to_string(), "eth_blockNumber".to_string()]
        );
    }

    #[test]
    fn submit_header_fails_when_transport_returns_invalid_tx_hash() {
        let mock = Arc::new(MockEvmTransport::new());
        *mock.send_hash.lock().expect("send hash lock") = "0x1234".to_string();
        let submitter = test_relay_client_with_transport(mock);
        let err = submitter
            .submit_header(&"00".repeat(80))
            .expect_err("invalid tx hash should fail");
        assert!(err
            .to_string()
            .contains("failed to send header submission transaction"));
    }

    #[test]
    fn wait_for_confirmation_rejects_reverted_receipt() {
        let mock = Arc::new(MockEvmTransport::new());
        *mock.receipt_status.lock().expect("receipt status lock") = "0x0".to_string();
        let submitter = test_relay_client_with_transport(mock);
        let err = submitter
            .wait_for_confirmation(
                "0x0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
            )
            .expect_err("reverted receipt should fail");
        assert!(err.to_string().contains("reverted on-chain"));
    }

    #[test]
    fn wait_for_confirmation_rejects_invalid_hash_shape_early() {
        let submitter = test_relay_client();
        let err = submitter
            .wait_for_confirmation("0x1234")
            .expect_err("invalid hash should fail");
        assert!(err.to_string().contains("invalid tx hash format"));
    }

    #[test]
    fn wait_for_confirmation_rejects_unexpected_status_value() {
        let mock = Arc::new(MockEvmTransport::new());
        *mock.receipt_status.lock().expect("receipt status lock") = "0x2".to_string();
        let submitter = test_relay_client_with_transport(mock);
        let err = submitter
            .wait_for_confirmation(
                "0x0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
            )
            .expect_err("unexpected status should fail");
        assert!(err.to_string().contains("unexpected transaction status"));
    }

    #[test]
    fn wait_for_confirmation_rejects_missing_block_number() {
        struct MissingBlockTransport;
        impl EvmTransport for MissingBlockTransport {
            fn rpc_request(&self, _rpc_url: &str, method: &str, _params: Value) -> Result<Value> {
                match method {
                    "eth_getTransactionReceipt" => Ok(json!({"status":"0x1"})),
                    "eth_blockNumber" => Ok(Value::String("0x10".to_string())),
                    _ => anyhow::bail!("unexpected rpc method {}", method),
                }
            }
            fn send_transaction(&self, _request: SendTxRequest) -> Result<String> {
                Ok("0x0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
                    .to_string())
            }
        }
        let submitter = test_relay_client_with_transport(Arc::new(MissingBlockTransport));
        let err = submitter
            .wait_for_confirmation(
                "0x0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
            )
            .expect_err("missing blockNumber should fail");
        assert!(err.to_string().contains("missing blockNumber"));
    }

    #[test]
    fn wait_for_confirmation_rejects_missing_status() {
        struct MissingStatusTransport;
        impl EvmTransport for MissingStatusTransport {
            fn rpc_request(&self, _rpc_url: &str, method: &str, _params: Value) -> Result<Value> {
                match method {
                    "eth_getTransactionReceipt" => Ok(json!({"blockNumber":"0x10"})),
                    "eth_blockNumber" => Ok(Value::String("0x10".to_string())),
                    _ => anyhow::bail!("unexpected rpc method {}", method),
                }
            }
            fn send_transaction(&self, _request: SendTxRequest) -> Result<String> {
                Ok("0x0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
                    .to_string())
            }
        }
        let submitter = test_relay_client_with_transport(Arc::new(MissingStatusTransport));
        let err = submitter
            .wait_for_confirmation(
                "0x0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
            )
            .expect_err("missing status should fail");
        assert!(err.to_string().contains("missing status"));
    }

    #[test]
    fn wait_for_confirmation_rejects_non_string_head_block() {
        struct NonStringHeadTransport;
        impl EvmTransport for NonStringHeadTransport {
            fn rpc_request(&self, _rpc_url: &str, method: &str, _params: Value) -> Result<Value> {
                match method {
                    "eth_getTransactionReceipt" => {
                        Ok(json!({"status":"0x1","blockNumber":"0x10"}))
                    }
                    "eth_blockNumber" => Ok(json!({"not":"a string"})),
                    _ => anyhow::bail!("unexpected rpc method {}", method),
                }
            }
            fn send_transaction(&self, _request: SendTxRequest) -> Result<String> {
                Ok("0x0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
                    .to_string())
            }
        }
        let submitter = test_relay_client_with_transport(Arc::new(NonStringHeadTransport));
        let err = submitter
            .wait_for_confirmation(
                "0x0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
            )
            .expect_err("non-string head should fail");
        assert!(err.to_string().contains("eth_blockNumber returned non-string result"));
    }

    #[test]
    fn wait_for_confirmation_rejects_invalid_receipt_block_number_format() {
        struct BadReceiptBlockTransport;
        impl EvmTransport for BadReceiptBlockTransport {
            fn rpc_request(&self, _rpc_url: &str, method: &str, _params: Value) -> Result<Value> {
                match method {
                    "eth_getTransactionReceipt" => {
                        Ok(json!({"status":"0x1","blockNumber":"zz"}))
                    }
                    "eth_blockNumber" => Ok(Value::String("0x10".to_string())),
                    _ => anyhow::bail!("unexpected rpc method {}", method),
                }
            }
            fn send_transaction(&self, _request: SendTxRequest) -> Result<String> {
                Ok("0x0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
                    .to_string())
            }
        }
        let submitter = test_relay_client_with_transport(Arc::new(BadReceiptBlockTransport));
        let err = submitter
            .wait_for_confirmation(
                "0x0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
            )
            .expect_err("bad receipt blockNumber should fail");
        assert!(err
            .to_string()
            .contains("invalid tx receipt blockNumber format"));
    }

    #[test]
    fn relay_tip_height_fails_on_non_hex_eth_call_result() {
        struct BadEthCallTransport;
        impl EvmTransport for BadEthCallTransport {
            fn rpc_request(&self, _rpc_url: &str, method: &str, _params: Value) -> Result<Value> {
                match method {
                    "eth_call" => Ok(Value::String("not-hex".to_string())),
                    _ => anyhow::bail!("unexpected rpc method {}", method),
                }
            }
            fn send_transaction(&self, _request: SendTxRequest) -> Result<String> {
                Ok("0x0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
                    .to_string())
            }
        }
        let submitter = test_relay_client_with_transport(Arc::new(BadEthCallTransport));
        let err = submitter
            .relay_tip_height()
            .expect_err("invalid eth_call result should fail");
        assert!(err.to_string().contains("failed to call BTCRelay.getBlockheight"));
    }

    #[test]
    fn relay_commit_hash_fails_when_eth_call_result_is_not_string() {
        struct NonStringEthCallTransport;
        impl EvmTransport for NonStringEthCallTransport {
            fn rpc_request(&self, _rpc_url: &str, method: &str, _params: Value) -> Result<Value> {
                match method {
                    "eth_call" => Ok(json!({"not":"a string"})),
                    _ => anyhow::bail!("unexpected rpc method {}", method),
                }
            }
            fn send_transaction(&self, _request: SendTxRequest) -> Result<String> {
                Ok("0x0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
                    .to_string())
            }
        }
        let submitter = test_relay_client_with_transport(Arc::new(NonStringEthCallTransport));
        let err = submitter
            .relay_commit_hash(1)
            .expect_err("non-string eth_call result should fail");
        assert!(err
            .to_string()
            .contains("failed to call BTCRelay.getCommitHash(1)"));
    }

    #[test]
    fn wait_for_confirmation_times_out_when_receipt_stays_pending() {
        struct PendingReceiptTransport;
        impl EvmTransport for PendingReceiptTransport {
            fn rpc_request(&self, _rpc_url: &str, method: &str, _params: Value) -> Result<Value> {
                match method {
                    "eth_getTransactionReceipt" => Ok(Value::Null),
                    "eth_blockNumber" => Ok(Value::String("0x10".to_string())),
                    _ => anyhow::bail!("unexpected rpc method {}", method),
                }
            }
            fn send_transaction(&self, _request: SendTxRequest) -> Result<String> {
                Ok("0x0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
                    .to_string())
            }
        }
        let mut submitter = test_relay_client_with_transport(Arc::new(PendingReceiptTransport));
        submitter.evm_tx_timeout_secs = 1;
        let err = submitter
            .wait_for_confirmation(
                "0x0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
            )
            .expect_err("pending receipt should eventually timeout");
        assert!(err.to_string().contains("timed out waiting for tx confirmation"));
    }

    #[test]
    fn wait_for_confirmation_rejects_zero_confirmation_setting() {
        let mut submitter = test_relay_client();
        submitter.evm_tx_confirmations = 0;
        let err = submitter
            .wait_for_confirmation(
                "0x0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
            )
            .expect_err("zero confirmations should fail");
        assert!(err.to_string().contains("EVM_TX_CONFIRMATIONS must be > 0"));
    }

    #[test]
    fn wait_for_confirmation_rejects_zero_timeout_setting() {
        let mut submitter = test_relay_client();
        submitter.evm_tx_timeout_secs = 0;
        let err = submitter
            .wait_for_confirmation(
                "0x0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
            )
            .expect_err("zero timeout should fail");
        assert!(err.to_string().contains("EVM_TX_TIMEOUT_SECS must be > 0"));
    }

    #[test]
    fn submit_header_rejects_empty_payload_before_transport() {
        let submitter = test_relay_client();
        let err = submitter
            .submit_header("")
            .expect_err("empty payload should fail");
        assert!(err
            .to_string()
            .contains("failed to validate/convert submit payload hex"));
    }

    #[test]
    fn relay_chain_work_bytes_fails_when_eth_call_result_is_not_string() {
        struct NonStringEthCallTransport;
        impl EvmTransport for NonStringEthCallTransport {
            fn rpc_request(&self, _rpc_url: &str, method: &str, _params: Value) -> Result<Value> {
                match method {
                    "eth_call" => Ok(json!({"unexpected":"object"})),
                    _ => anyhow::bail!("unexpected rpc method {}", method),
                }
            }
            fn send_transaction(&self, _request: SendTxRequest) -> Result<String> {
                Ok("0x0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
                    .to_string())
            }
        }
        let submitter = test_relay_client_with_transport(Arc::new(NonStringEthCallTransport));
        let err = submitter
            .relay_chain_work_bytes()
            .expect_err("non-string eth_call should fail");
        assert!(err.to_string().contains("failed to call BTCRelay.getChainwork"));
    }

    #[test]
    fn relay_chain_work_bytes_fails_on_invalid_hex_payload() {
        struct BadHexEthCallTransport;
        impl EvmTransport for BadHexEthCallTransport {
            fn rpc_request(&self, _rpc_url: &str, method: &str, _params: Value) -> Result<Value> {
                match method {
                    "eth_call" => Ok(Value::String("0xzz".to_string())),
                    _ => anyhow::bail!("unexpected rpc method {}", method),
                }
            }
            fn send_transaction(&self, _request: SendTxRequest) -> Result<String> {
                Ok("0x0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
                    .to_string())
            }
        }
        let submitter = test_relay_client_with_transport(Arc::new(BadHexEthCallTransport));
        let err = submitter
            .relay_chain_work_bytes()
            .expect_err("invalid hex eth_call should fail");
        assert!(err.to_string().contains("failed to call BTCRelay.getChainwork"));
    }

    #[test]
    fn relay_chain_work_bytes_fails_on_wrong_abi_shape() {
        struct WrongAbiShapeTransport;
        impl EvmTransport for WrongAbiShapeTransport {
            fn rpc_request(&self, _rpc_url: &str, method: &str, _params: Value) -> Result<Value> {
                match method {
                    "eth_call" => Ok(Value::String("0x01".to_string())),
                    _ => anyhow::bail!("unexpected rpc method {}", method),
                }
            }
            fn send_transaction(&self, _request: SendTxRequest) -> Result<String> {
                Ok("0x0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
                    .to_string())
            }
        }
        let submitter = test_relay_client_with_transport(Arc::new(WrongAbiShapeTransport));
        let err = submitter
            .relay_chain_work_bytes()
            .expect_err("wrong abi shape should fail");
        assert!(err
            .to_string()
            .contains("failed to decode BTCRelay.getChainwork return value"));
    }

    #[test]
    fn wait_for_confirmation_rejects_invalid_head_block_format() {
        struct BadHeadFormatTransport;
        impl EvmTransport for BadHeadFormatTransport {
            fn rpc_request(&self, _rpc_url: &str, method: &str, _params: Value) -> Result<Value> {
                match method {
                    "eth_getTransactionReceipt" => {
                        Ok(json!({"status":"0x1","blockNumber":"0x10"}))
                    }
                    "eth_blockNumber" => Ok(Value::String("zz".to_string())),
                    _ => anyhow::bail!("unexpected rpc method {}", method),
                }
            }
            fn send_transaction(&self, _request: SendTxRequest) -> Result<String> {
                Ok("0x0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
                    .to_string())
            }
        }
        let submitter = test_relay_client_with_transport(Arc::new(BadHeadFormatTransport));
        let err = submitter
            .wait_for_confirmation(
                "0x0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
            )
            .expect_err("bad head format should fail");
        assert!(err.to_string().contains("invalid eth_blockNumber format"));
    }

    #[test]
    fn parse_json_rpc_response_accepts_null_result() {
        let body = json!({"jsonrpc":"2.0","id":1,"result":null});
        let result = parse_json_rpc_response(body, "eth_getTransactionReceipt")
            .expect("null result is valid JSON-RPC");
        assert!(result.is_null());
    }

    #[test]
    fn parse_json_rpc_response_rejects_missing_result_field() {
        let body = json!({"jsonrpc":"2.0","id":1});
        let err = parse_json_rpc_response(body, "eth_getTransactionReceipt")
            .expect_err("missing result should fail");
        assert!(err.to_string().contains("missing result field"));
    }

    #[test]
    fn parse_json_rpc_response_surfaces_rpc_error() {
        let body = json!({
            "jsonrpc":"2.0",
            "id":1,
            "error":{"code":-32000,"message":"rate limited"}
        });
        let err = parse_json_rpc_response(body, "eth_getTransactionReceipt")
            .expect_err("rpc error should fail");
        assert!(err.to_string().contains("returned error"));
    }

    #[test]
    fn http_transport_preserves_null_json_rpc_result_for_pending_receipt() {
        let server = tiny_http::Server::http("127.0.0.1:0").expect("bind mock rpc");
        let port = server
            .server_addr()
            .to_ip()
            .expect("mock rpc bound address")
            .port();

        let server_thread = thread::spawn(move || {
            let request = server.recv().expect("mock rpc request");
            let response = tiny_http::Response::from_string(
                r#"{"jsonrpc":"2.0","id":1,"result":null}"#,
            )
            .with_header(
                tiny_http::Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..])
                    .expect("content-type header"),
            );
            request.respond(response).expect("mock rpc response");
        });

        let transport = HttpEvmTransport;
        let result = transport
            .rpc_request(
                &format!("http://127.0.0.1:{port}"),
                "eth_getTransactionReceipt",
                json!(["0x89c0666fac899083fdf5442b920271f647b8c000c000db839e817bb4673e1a48"]),
            )
            .expect("pending receipt null result should not error");
        assert!(result.is_null());

        server_thread.join().expect("mock rpc thread");
    }
}
