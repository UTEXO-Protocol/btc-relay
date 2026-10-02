// Copyright (C) 2026 Utexo.
// See LICENSE for copying information.

//! HTTP/JSON-RPC client for the **on-chain BTCRelay contract** (reads via `eth_call`, writes via signed txs).
//!
//! Implementation detail: encode relay ABI, sign and send with `alloy`, poll receipts with bare JSON-RPC.
//! Main sync uses `submitMainBlockheaders`; a Bitcoin reorg uses `submitShortForkBlockheaders` or `submitForkBlockheaders`.

use alloy::consensus::{SignableTransaction, TxEnvelope, TypedTransaction};
use alloy::eips::eip2718::Encodable2718;
use alloy::network::TxSignerSync;
use alloy::primitives::{Address, TxKind, U128, U256 as AlloyU256, U64};
use alloy::providers::{Provider, ProviderBuilder};
use alloy::rpc::types::TransactionRequest;
use alloy::signers::local::PrivateKeySigner;
use anyhow::{Context, Result};
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

/// Chain ids without EIP-1559. These chains get a legacy tx, as in ethers 2.0.14.
const LEGACY_CHAIN_IDS: [u64; 25] = [
    20, 30, 56, 69, 88, 97, 250, 280, 288, 324, 1088, 1101, 1442, 4002, 5000, 5001, 26863, 42220,
    42261, 42262, 44787, 62320, 421611, 534351, 534352,
];

// `IBtcRelayView`: alloy `sol!` view of the on-chain BTCRelay ABI we call (historical name; contract is BTCRelay).
// Must match deployed bytecode.
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
    /// If set, caps `maxFeePerGas` (gwei). If `None`, the client estimates it from the node.
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
    if response.get("error").is_some_and(|err| !err.is_null()) {
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
            let url = request
                .rpc_url
                .parse()
                .context("failed to create EVM provider")?;
            let provider = ProviderBuilder::new()
                .disable_recommended_fillers()
                .connect_http(url);
            let signer = request
                .private_key
                .parse::<PrivateKeySigner>()
                .context("invalid RELAYER_PRIVATE_KEY format")?;
            let to = request
                .relay_contract_address
                .parse::<Address>()
                .context("invalid RELAY_CONTRACT_ADDRESS")?;
            let gwei = |fee: Option<u64>| fee.map(|fee| u128::from(fee) * 1_000_000_000);
            let (max_fee, priority_fee) =
                (gwei(request.max_fee_gwei), gwei(request.priority_fee_gwei));

            #[derive(Debug, Deserialize)]
            struct LatestBlock {
                #[serde(rename = "baseFeePerGas")]
                base_fee_per_gas: Option<U128>,
            }
            #[derive(Debug, Deserialize)]
            struct FeeHistory {
                #[serde(default)]
                reward: Vec<Vec<U128>>,
            }

            // The RPC calls, their params and the fee rules are those of ethers 2.0.14.
            let send = async move {
                let from = format!("{:#x}", signer.address());
                let nonce = provider
                    .raw_request::<_, U64>("eth_getTransactionCount".into(), (&from, "latest"))
                    .await?
                    .to::<u64>();
                let mut call = json!({
                    "from": from,
                    "to": format!("{to:#x}"),
                    "nonce": format!("{nonce:#x}"),
                    "data": bytes_to_prefixed_hex(&request.calldata),
                });

                let (fee, priority_fee) = if LEGACY_CHAIN_IDS.contains(&request.chain_id) {
                    let gas_price = match max_fee {
                        Some(fee) => fee,
                        None => provider
                            .raw_request::<_, U128>("eth_gasPrice".into(), ())
                            .await?
                            .to::<u128>(),
                    };
                    call["gasPrice"] = json!(format!("{gas_price:#x}"));
                    call["type"] = json!("0x00");
                    (gas_price, None)
                } else {
                    let (max_fee, priority_fee) = match (max_fee, priority_fee) {
                        (Some(max_fee), Some(priority_fee)) => (max_fee, priority_fee),
                        _ => {
                            let base_fee = provider
                                .raw_request::<_, Option<LatestBlock>>(
                                    "eth_getBlockByNumber".into(),
                                    ("latest", false),
                                )
                                .await?
                                .context("Latest block not found")?
                                .base_fee_per_gas
                                .context("EIP-1559 not activated")?
                                .to::<u128>();
                            // Old nodes take the block count as an integer.
                            let percentiles = [5.0_f64];
                            let history = match provider
                                .raw_request::<_, FeeHistory>(
                                    "eth_feeHistory".into(),
                                    ("0xa", "latest", percentiles),
                                )
                                .await
                            {
                                Ok(history) => history,
                                Err(err) => provider
                                    .raw_request::<_, FeeHistory>(
                                        "eth_feeHistory".into(),
                                        (10, "latest", percentiles),
                                    )
                                    .await
                                    .map_err(|_| err)?,
                            };
                            let rewards = history
                                .reward
                                .iter()
                                .filter_map(|block| block.first())
                                .map(|reward| reward.to::<u128>())
                                .collect();
                            let (est_max_fee, est_priority_fee) =
                                eip1559_default_fees(base_fee, rewards);
                            let max_fee = max_fee.unwrap_or(est_max_fee);
                            // Only a configured tip is capped at the max fee.
                            let priority_fee =
                                priority_fee.map_or(est_priority_fee, |fee| fee.min(max_fee));
                            (max_fee, priority_fee)
                        }
                    };
                    call["accessList"] = json!([]);
                    call["maxFeePerGas"] = json!(format!("{max_fee:#x}"));
                    call["maxPriorityFeePerGas"] = json!(format!("{priority_fee:#x}"));
                    call["type"] = json!("0x02");
                    (max_fee, Some(priority_fee))
                };

                // No block tag: some nodes reject it.
                let gas_limit = provider
                    .raw_request::<_, U64>("eth_estimateGas".into(), (call,))
                    .await?
                    .to::<u64>();

                let mut tx: TypedTransaction = TransactionRequest {
                    chain_id: Some(request.chain_id),
                    nonce: Some(nonce),
                    gas: Some(gas_limit),
                    to: Some(TxKind::Call(to)),
                    input: request.calldata.into(),
                    gas_price: priority_fee.is_none().then_some(fee),
                    max_fee_per_gas: priority_fee.is_some().then_some(fee),
                    max_priority_fee_per_gas: priority_fee,
                    ..Default::default()
                }
                .build_consensus_tx()
                .map_err(|err| anyhow::anyhow!("failed to build transaction: {}", err.error))?;
                let signature = signer.sign_transaction_sync(&mut tx)?;
                let raw = TxEnvelope::from(tx.into_signed(signature)).encoded_2718();
                let pending = provider.send_raw_transaction(&raw).await?;
                Ok::<String, anyhow::Error>(format!("{:#x}", pending.tx_hash()))
            };
            send.await.context("eth_sendRawTransaction failed")
        })
    }
}

/// Default EIP-1559 estimate of ethers 2.0.14. Returns `(max fee, priority fee)` in wei.
fn eip1559_default_fees(base_fee: u128, rewards: Vec<u128>) -> (u128, u128) {
    const DEFAULT_PRIORITY_FEE: u128 = 3_000_000_000;
    let priority_fee = if base_fee < 100_000_000_000 {
        DEFAULT_PRIORITY_FEE
    } else {
        // Median of the nonzero rewards. A jump of 200% or more in the upper half drops the lower values.
        let mut rewards: Vec<u128> = rewards.into_iter().filter(|r| *r > 0).collect();
        rewards.sort();
        let changes: Vec<u128> = rewards
            .windows(2)
            .map(|pair| (pair[1] - pair[0]) * 100 / pair[0])
            .collect();
        let mut values = &rewards[..];
        if let Some(max_change) = changes.iter().max() {
            let index = changes
                .iter()
                .position(|change| change == max_change)
                .expect("max is in changes");
            if *max_change >= 200 && index >= rewards.len() / 2 {
                values = &rewards[index..];
            }
        }
        let estimate = values.get(values.len() / 2).copied().unwrap_or(0);
        estimate.max(DEFAULT_PRIORITY_FEE)
    };
    let surged = if base_fee <= 40_000_000_000 {
        base_fee * 2
    } else if base_fee <= 100_000_000_000 {
        base_fee * 16 / 10
    } else if base_fee <= 200_000_000_000 {
        base_fee * 14 / 10
    } else {
        base_fee * 12 / 10
    };
    let max_fee = if priority_fee > surged {
        priority_fee + surged
    } else {
        surged
    };
    (max_fee, priority_fee)
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
                .rpc_request(
                    &self.evm_rpc_url,
                    "eth_getTransactionReceipt",
                    json!([tx_hash]),
                )
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

    /// ABI encode `submitShortForkBlockheaders(bytes)`.
    fn build_submit_short_fork_calldata(&self, headers_bytes: &[u8]) -> Vec<u8> {
        let owned_headers: Vec<u8> = headers_bytes.to_vec();
        let call = IBtcRelayView::submitShortForkBlockheadersCall {
            headers: owned_headers.into(),
        };
        call.abi_encode()
    }

    /// ABI encode `submitForkBlockheaders(forkId, bytes)`.
    fn build_submit_fork_calldata(&self, fork_id: u64, headers_bytes: &[u8]) -> Vec<u8> {
        let owned_headers: Vec<u8> = headers_bytes.to_vec();
        let call = IBtcRelayView::submitForkBlockheadersCall {
            forkId: AlloyU256::from(fork_id),
            headers: owned_headers.into(),
        };
        call.abi_encode()
    }

    /// Sign, send, wait for confirmations, record fee metrics; return the tx hash.
    fn submit_calldata(&self, calldata: &[u8]) -> Result<String> {
        let tx_hash = self
            .send_tx(calldata)
            .context("failed to send header submission transaction")?;

        // Only return once mined deep enough: sync loop assumes relay state reflects this tx.
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
        let signer = self
            .relayer_private_key
            .parse::<PrivateKeySigner>()
            .context("invalid RELAYER_PRIVATE_KEY format")?;
        Ok(format!("{:#x}", signer.address()))
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

        self.submit_calldata(&calldata)
    }

    fn submit_short_fork(&self, header_hex: &str) -> Result<String> {
        let header_bytes = self
            .payload_hex_to_bytes(header_hex)
            .context("failed to validate/convert submit payload hex")?;
        self.submit_calldata(&self.build_submit_short_fork_calldata(&header_bytes))
    }

    fn submit_fork(&self, fork_id: u64, header_hex: &str) -> Result<String> {
        let header_bytes = self
            .payload_hex_to_bytes(header_hex)
            .context("failed to validate/convert submit payload hex")?;
        self.submit_calldata(&self.build_submit_fork_calldata(fork_id, &header_bytes))
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
    use crate::sync_engine::{classify_retry_decision, RetryDecision};
    use alloy::consensus::{Transaction as _, TxEnvelope};
    use alloy::eips::eip2718::Decodable2718;
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
            self.rpc_methods
                .lock()
                .expect("rpc methods lock")
                .push(method.to_string());
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

    fn test_relay_client_with_transport(
        transport: Arc<dyn EvmTransport>,
    ) -> EvmRelayContractClient {
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

    const STUB_KEY: &str = "0x0000000000000000000000000000000000000000000000000000000000000001";
    const STUB_TX_HASH: &str = "0xabababababababababababababababababababababababababababababababab";
    const STUB_CONTRACT: &str = "0x1111111111111111111111111111111111111111";

    type StubAnswer = Result<Value, Value>;

    /// Local JSON-RPC server. It records each `(method, params)` and answers with `handler`.
    struct RpcStub {
        url: String,
        log: Arc<Mutex<Vec<(String, Value)>>>,
        server: Arc<tiny_http::Server>,
        thread: Option<thread::JoinHandle<()>>,
    }

    impl RpcStub {
        fn start(handler: impl Fn(&str, &Value) -> StubAnswer + Send + 'static) -> Self {
            let server = Arc::new(tiny_http::Server::http("127.0.0.1:0").expect("bind rpc stub"));
            let port = server.server_addr().to_ip().expect("stub address").port();
            let log = Arc::new(Mutex::new(Vec::new()));
            let (srv, calls) = (server.clone(), log.clone());
            let thread = thread::spawn(move || {
                for mut request in srv.incoming_requests() {
                    let mut body = String::new();
                    std::io::Read::read_to_string(request.as_reader(), &mut body)
                        .expect("read stub request");
                    let call: Value = serde_json::from_str(&body).expect("stub request json");
                    let method = call["method"].as_str().unwrap_or_default().to_string();
                    let params = call["params"].clone();
                    calls
                        .lock()
                        .expect("stub log")
                        .push((method.clone(), params.clone()));
                    let mut reply = json!({"jsonrpc": "2.0", "id": call["id"]});
                    match handler(&method, &params) {
                        Ok(result) => reply["result"] = result,
                        Err(error) => reply["error"] = error,
                    }
                    let response = tiny_http::Response::from_string(reply.to_string()).with_header(
                        tiny_http::Header::from_bytes(
                            &b"Content-Type"[..],
                            &b"application/json"[..],
                        )
                        .expect("content-type header"),
                    );
                    let _ = request.respond(response);
                }
            });
            Self {
                url: format!("http://127.0.0.1:{port}"),
                log,
                server,
                thread: Some(thread),
            }
        }

        fn calls(&self) -> Vec<(String, Value)> {
            self.log.lock().expect("stub log").clone()
        }

        fn raw_txs(&self) -> Vec<String> {
            self.calls()
                .into_iter()
                .filter(|(method, _)| method == "eth_sendRawTransaction")
                .map(|(_, params)| params[0].as_str().expect("raw tx hex").to_string())
                .collect()
        }
    }

    impl Drop for RpcStub {
        fn drop(&mut self) {
            self.server.unblock();
            if let Some(thread) = self.thread.take() {
                let _ = thread.join();
            }
        }
    }

    /// A node with base fee 1 gwei, gas price 10 gwei, nonce 7 and a mined receipt.
    fn node_answer(method: &str, _params: &Value) -> StubAnswer {
        match method {
            "eth_getTransactionCount" => Ok(json!("0x7")),
            "eth_getBlockByNumber" => Ok(json!({"number": "0x10", "baseFeePerGas": "0x3b9aca00"})),
            "eth_feeHistory" => Ok(json!({
                "oldestBlock": "0x7",
                "baseFeePerGas": ["0x3b9aca00", "0x3b9aca00"],
                "gasUsedRatio": [0.5],
                "reward": [["0x77359400"]]
            })),
            "eth_gasPrice" => Ok(json!("0x2540be400")),
            "eth_estimateGas" => Ok(json!("0x1e8480")),
            "eth_sendRawTransaction" => Ok(json!(STUB_TX_HASH)),
            "eth_getTransactionReceipt" => Ok(json!({"status": "0x1", "blockNumber": "0x10"})),
            "eth_blockNumber" => Ok(json!("0x10")),
            _ => Err(json!({"code": -32601, "message": "method not found"})),
        }
    }

    fn stub_send_request(
        url: &str,
        chain_id: u64,
        max_fee_gwei: Option<u64>,
        priority_fee_gwei: Option<u64>,
    ) -> SendTxRequest {
        SendTxRequest {
            rpc_url: url.to_string(),
            private_key: STUB_KEY.to_string(),
            relay_contract_address: STUB_CONTRACT.to_string(),
            chain_id,
            max_fee_gwei,
            priority_fee_gwei,
            calldata: vec![0xde, 0xad, 0xbe, 0xef],
        }
    }

    fn stub_relay_client(url: &str) -> EvmRelayContractClient {
        let mut client = test_relay_client_with_transport(Arc::new(HttpEvmTransport));
        client.evm_rpc_url = url.to_string();
        client.relayer_private_key = STUB_KEY.to_string();
        client
    }

    /// `Error(string)` ABI encoding of `block commitment`.
    const BLOCK_COMMITMENT_REVERT_DATA: &str = concat!(
        "0x08c379a0",
        "0000000000000000000000000000000000000000000000000000000000000020",
        "0000000000000000000000000000000000000000000000000000000000000010",
        "626c6f636b20636f6d6d69746d656e7400000000000000000000000000000000"
    );

    fn decode_raw_tx(raw_hex: &str) -> TxEnvelope {
        let raw = hex_prefixed_to_bytes(raw_hex).expect("raw tx hex");
        TxEnvelope::decode_2718(&mut raw.as_slice()).expect("decode raw tx")
    }

    #[test]
    fn submit_calls_put_pinned_calldata_on_the_wire() {
        let headers_tail = concat!(
            "0000000000000000000000000000000000000000000000000000000000000050",
            "1111111111111111111111111111111111111111111111111111111111111111",
            "1111111111111111111111111111111111111111111111111111111111111111",
            "1111111111111111111111111111111100000000000000000000000000000000"
        );
        let offset_32 = "0000000000000000000000000000000000000000000000000000000000000020";
        let fork_7 = concat!(
            "0000000000000000000000000000000000000000000000000000000000000007",
            "0000000000000000000000000000000000000000000000000000000000000040"
        );
        let expected = [
            format!("0x59533237{offset_32}{headers_tail}"),
            format!("0x98c650d5{offset_32}{headers_tail}"),
            format!("0x2bb52aad{fork_7}{headers_tail}"),
        ];

        let stub = RpcStub::start(node_answer);
        let client = stub_relay_client(&stub.url);
        let headers = "11".repeat(80);
        client.submit_header(&headers).expect("submit main");
        client
            .submit_short_fork(&headers)
            .expect("submit short fork");
        client.submit_fork(7, &headers).expect("submit fork");

        let estimated: Vec<String> = stub
            .calls()
            .into_iter()
            .filter(|(method, _)| method == "eth_estimateGas")
            .map(|(_, params)| params[0]["data"].as_str().expect("data").to_string())
            .collect();
        let sent: Vec<String> = stub
            .raw_txs()
            .iter()
            .map(|raw| bytes_to_prefixed_hex(decode_raw_tx(raw).input()))
            .collect();
        assert_eq!(estimated, expected);
        assert_eq!(sent, expected);
    }

    #[test]
    fn ethers_fee_estimator_port_matches_vectors() {
        const GWEI: u128 = 1_000_000_000;
        // Base fee below 100 gwei: default tip, surged base fee.
        assert_eq!(
            eip1559_default_fees(100 * GWEI - 1, vec![]),
            (159_999_999_998, 3 * GWEI)
        );
        // Base fee above 100 gwei: median reward.
        assert_eq!(
            eip1559_default_fees(100 * GWEI + 1, vec![100 * GWEI, 105 * GWEI, 102 * GWEI]),
            (140_000_000_001, 102 * GWEI)
        );
        // Rewards above u32 do not overflow.
        let large = u128::from(u32::MAX) + 1;
        assert_eq!(
            eip1559_default_fees(200 * GWEI + 1, vec![large, large]),
            (240_000_000_001, large)
        );
        // A jump of 200% in the upper half drops the lower rewards.
        assert_eq!(
            eip1559_default_fees(100 * GWEI, vec![GWEI, GWEI, GWEI, 4 * GWEI, 4 * GWEI]),
            (160 * GWEI, 4 * GWEI)
        );
    }

    #[test]
    fn relayer_wallet_address_matches_key() {
        let mut client = test_relay_client();
        client.relayer_private_key = STUB_KEY.to_string();
        assert_eq!(
            client.relayer_wallet_address().expect("address"),
            "0x7e5f4552091a69125d5dfcb7b8c2659029395bdf"
        );

        client.relayer_private_key = "0x01".to_string();
        let err = client.relayer_wallet_address().expect_err("short key");
        assert!(err
            .to_string()
            .contains("invalid RELAYER_PRIVATE_KEY format"));
    }

    #[test]
    fn http_transport_sends_signed_tx() {
        const SENDER: &str = "0x7e5f4552091a69125d5dfcb7b8c2659029395bdf";
        const GWEI: u128 = 1_000_000_000;
        let nonce = json!(["eth_getTransactionCount", [SENDER, "latest"]]);
        let block = json!(["eth_getBlockByNumber", ["latest", false]]);
        let history = json!(["eth_feeHistory", ["0xa", "latest", [5.0]]]);
        let estimate = |fees: Value| {
            let mut tx =
                json!({"data": "0xdeadbeef", "from": SENDER, "nonce": "0x7", "to": STUB_CONTRACT});
            for (key, value) in fees.as_object().expect("fee fields") {
                tx[key] = value.clone();
            }
            json!(["eth_estimateGas", [tx]])
        };
        let eip1559 = |max: &str, tip: &str| {
            estimate(
                json!({"accessList": [], "maxFeePerGas": max, "maxPriorityFeePerGas": tip, "type": "0x02"}),
            )
        };
        let legacy = |price: &str| estimate(json!({"gasPrice": price, "type": "0x00"}));
        let send = |raw: &str| json!(["eth_sendRawTransaction", [raw]]);

        // (chain id, max fee, tip, fee history fails, calls, max fee or gas price, tip)
        let rows = vec![
            (421614, Some(30), Some(2), false, vec![
                nonce.clone(),
                eip1559("0x6fc23ac00", "0x77359400"),
                send("0x02f87383066eee0784773594008506fc23ac00831e84809411111111111111111111111111111111111111118084deadbeefc001a0e4a8ff81ea96e10cd51d6f8b6a6a3ea5d2ca229df4f125cb2ebcaea43b6e74f4a0778fe5e166f022ca691187d5b02a53c20b94f8939d2d53fcc07f49ac7ef38862"),
            ], 30 * GWEI, Some(2 * GWEI)),
            (421614, Some(1), Some(2), false, vec![
                nonce.clone(),
                eip1559("0x3b9aca00", "0x77359400"),
                send("0x02f87283066eee078477359400843b9aca00831e84809411111111111111111111111111111111111111118084deadbeefc001a0460100f9fe49b6611288c1121a22d0a064d4a564f07ac8bc7490135af539d415a01b6f0532f6f606f9c982264ee4c7f582762c1647c980b6becce88a95a54d4cb5"),
            ], GWEI, Some(2 * GWEI)),
            (421614, None, Some(50), false, vec![
                nonce.clone(),
                block.clone(),
                history.clone(),
                eip1559("0x12a05f200", "0x12a05f200"),
                send("0x02f87483066eee0785012a05f20085012a05f200831e84809411111111111111111111111111111111111111118084deadbeefc080a01c547ef7d34647e1ecc96fc0a500d217cc24d425b65c16a3c0b6349e39a79671a02b3f3c8cafda61892914e5144f7c14afa07795c916eb697e663e0065f0ad7c6c"),
            ], 5 * GWEI, Some(5 * GWEI)),
            (421614, None, Some(1), false, vec![
                nonce.clone(),
                block.clone(),
                history.clone(),
                eip1559("0x12a05f200", "0x3b9aca00"),
                send("0x02f87383066eee07843b9aca0085012a05f200831e84809411111111111111111111111111111111111111118084deadbeefc001a0c0d620855b110589045b635353098cdaa5d1a53ccbef1b8fc25042b8c82c1cf1a060c468129298562fb4daffcd4f0138e4af9a8bdbc141d7f7d9ab197d2dd26abf"),
            ], 5 * GWEI, Some(GWEI)),
            (421614, Some(4), None, false, vec![
                nonce.clone(),
                block.clone(),
                history.clone(),
                eip1559("0xee6b2800", "0xb2d05e00"),
                send("0x02f87283066eee0784b2d05e0084ee6b2800831e84809411111111111111111111111111111111111111118084deadbeefc080a01c0a5baba98cbbbf025050dba620f4e6667c1d9e5879362dad2b1cd61b49f1eca05c9cb3cec88e734c559425ba556c4ece7f463274807e941cbdc0c18c95975ae6"),
            ], 4 * GWEI, Some(3 * GWEI)),
            (31337, None, None, false, vec![
                nonce.clone(),
                block.clone(),
                history.clone(),
                eip1559("0x12a05f200", "0xb2d05e00"),
                send("0x02f872827a690784b2d05e0085012a05f200831e84809411111111111111111111111111111111111111118084deadbeefc001a0fe813dc5f8e3b0c4742d56149c77bdd2bafb67a4c5237cd2597e00ba7c05cf7ba012b31c7cf1a69f22f3702481c8fb48e7086f260f642c17bc48ed3fc93d426e1a"),
            ], 5 * GWEI, Some(3 * GWEI)),
            (421614, None, None, true, vec![
                nonce.clone(),
                block.clone(),
                history.clone(),
                json!(["eth_feeHistory", [10, "latest", [5.0]]]),
                eip1559("0x12a05f200", "0xb2d05e00"),
                send("0x02f87383066eee0784b2d05e0085012a05f200831e84809411111111111111111111111111111111111111118084deadbeefc001a095f3f63e29a1296383f233f2f9144fccc11e81477638f358c10e7d8b4b7271d4a0787c0cca9dd2cc4071f4f279f8c3684052366ac220ca1a6735a12702ef4d58b6"),
            ], 5 * GWEI, Some(3 * GWEI)),
            (56, Some(5), Some(2), false, vec![
                nonce.clone(),
                legacy("0x12a05f200"),
                send("0xf86a0785012a05f200831e84809411111111111111111111111111111111111111118084deadbeef8193a03ece024368986b63533bae9fd8e90360d2062c1558f1c6ba94bba4c972fc930aa06d5700d0ea78715429b76a84ff83a2fdc89cb94f61af99a7362cd26b56e85b3e"),
            ], 5 * GWEI, None),
            (56, None, None, false, vec![
                nonce.clone(),
                json!(["eth_gasPrice", null]),
                legacy("0x2540be400"),
                send("0xf86a078502540be400831e84809411111111111111111111111111111111111111118084deadbeef8193a02553cb6593d490ac14e2c04dcfb9f182863d0ea4f45480fabdf238820d7f4c2ca01eff79175ddfe9047aa74a4920b4e5dcc44e00a7e5bd91c439814fc5eb66c9fe"),
            ], 10 * GWEI, None),
        ];

        for (chain_id, max_fee, tip, history_fails, calls, fee, priority) in rows {
            let stub = RpcStub::start(move |method: &str, params: &Value| {
                if history_fails && method == "eth_feeHistory" && params[0] == json!("0xa") {
                    return Err(json!({"code": -32602, "message": "invalid argument 0"}));
                }
                node_answer(method, params)
            });
            let hash = HttpEvmTransport
                .send_transaction(stub_send_request(&stub.url, chain_id, max_fee, tip))
                .expect("send tx");
            assert_eq!(hash, STUB_TX_HASH);
            assert_eq!(
                json!(stub.calls()),
                json!(calls),
                "chain {chain_id} {max_fee:?}/{tip:?}"
            );

            let tx = decode_raw_tx(&stub.raw_txs()[0]);
            assert_eq!(tx.chain_id(), Some(chain_id));
            assert_eq!(
                format!(
                    "{:#x}",
                    tx.signature()
                        .recover_address_from_prehash(&tx.signature_hash())
                        .expect("signer")
                ),
                SENDER
            );
            assert_eq!(tx.nonce(), 7);
            assert_eq!(tx.gas_limit(), 2_000_000);
            assert_eq!(tx.max_fee_per_gas(), fee);
            assert_eq!(tx.max_priority_fee_per_gas(), priority);
            assert_eq!(tx.is_legacy(), priority.is_none());
        }
    }

    #[test]
    fn http_transport_errors_keep_reason_and_retry_class() {
        let revert = |message: &'static str| {
            move |method: &str, params: &Value| {
                if method == "eth_estimateGas" {
                    return Err(
                        json!({"code": 3, "message": message, "data": BLOCK_COMMITMENT_REVERT_DATA}),
                    );
                }
                node_answer(method, params)
            }
        };
        let send_error = |method: &str, params: &Value| {
            if method == "eth_sendRawTransaction" {
                return Err(json!({"code": -32000, "message": "nonce too low"}));
            }
            node_answer(method, params)
        };
        let send = |stub: &RpcStub| {
            HttpEvmTransport
                .send_transaction(stub_send_request(&stub.url, 421614, None, None))
                .expect_err("send must fail")
        };

        let bare = RpcStub::start(revert("execution reverted"));
        let with_reason = RpcStub::start(revert("execution reverted: block commitment"));
        let rejected = RpcStub::start(send_error);
        let mined_revert = RpcStub::start(|method: &str, params: &Value| {
            if method == "eth_getTransactionReceipt" {
                return Ok(json!({"status": "0x0", "blockNumber": "0x10"}));
            }
            node_answer(method, params)
        });
        let closed_port = std::net::TcpListener::bind("127.0.0.1:0")
            .and_then(|listener| listener.local_addr())
            .expect("free port")
            .port();

        let mined_revert_text = format!("transaction reverted on-chain: {STUB_TX_HASH}");
        let cases = [
            (
                send(&bare),
                vec![
                    "eth_sendRawTransaction failed",
                    "execution reverted",
                    &BLOCK_COMMITMENT_REVERT_DATA[2..],
                ],
                RetryDecision::HardFailure,
            ),
            (
                send(&with_reason),
                vec!["execution reverted: block commitment"],
                RetryDecision::HardFailure,
            ),
            (
                send(&rejected),
                vec!["eth_sendRawTransaction failed", "nonce too low"],
                RetryDecision::HardFailure,
            ),
            (
                stub_relay_client(&mined_revert.url)
                    .submit_header(&"00".repeat(80))
                    .expect_err("mined revert must fail"),
                vec![mined_revert_text.as_str()],
                RetryDecision::HardFailure,
            ),
            (
                HttpEvmTransport
                    .send_transaction(stub_send_request(
                        &format!("http://127.0.0.1:{closed_port}"),
                        421614,
                        None,
                        None,
                    ))
                    .expect_err("closed port must fail"),
                vec!["eth_sendRawTransaction failed", "connection refused"],
                RetryDecision::Retryable,
            ),
        ];
        for (err, needles, decision) in cases {
            let text = format!("{:#}", err);
            for needle in needles {
                assert!(
                    text.to_lowercase().contains(&needle.to_lowercase()),
                    "{text:?} lacks {needle:?}"
                );
            }
            assert_eq!(classify_retry_decision(&text), decision, "{text}");
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
        assert!(bad_digits
            .to_string()
            .contains("failed to parse hex quantity"));
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
            &[
                "eth_getTransactionReceipt".to_string(),
                "eth_blockNumber".to_string()
            ]
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
                Ok(
                    "0x0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
                        .to_string(),
                )
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
                Ok(
                    "0x0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
                        .to_string(),
                )
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
                    "eth_getTransactionReceipt" => Ok(json!({"status":"0x1","blockNumber":"0x10"})),
                    "eth_blockNumber" => Ok(json!({"not":"a string"})),
                    _ => anyhow::bail!("unexpected rpc method {}", method),
                }
            }
            fn send_transaction(&self, _request: SendTxRequest) -> Result<String> {
                Ok(
                    "0x0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
                        .to_string(),
                )
            }
        }
        let submitter = test_relay_client_with_transport(Arc::new(NonStringHeadTransport));
        let err = submitter
            .wait_for_confirmation(
                "0x0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
            )
            .expect_err("non-string head should fail");
        assert!(err
            .to_string()
            .contains("eth_blockNumber returned non-string result"));
    }

    #[test]
    fn wait_for_confirmation_rejects_invalid_receipt_block_number_format() {
        struct BadReceiptBlockTransport;
        impl EvmTransport for BadReceiptBlockTransport {
            fn rpc_request(&self, _rpc_url: &str, method: &str, _params: Value) -> Result<Value> {
                match method {
                    "eth_getTransactionReceipt" => Ok(json!({"status":"0x1","blockNumber":"zz"})),
                    "eth_blockNumber" => Ok(Value::String("0x10".to_string())),
                    _ => anyhow::bail!("unexpected rpc method {}", method),
                }
            }
            fn send_transaction(&self, _request: SendTxRequest) -> Result<String> {
                Ok(
                    "0x0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
                        .to_string(),
                )
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
                Ok(
                    "0x0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
                        .to_string(),
                )
            }
        }
        let submitter = test_relay_client_with_transport(Arc::new(BadEthCallTransport));
        let err = submitter
            .relay_tip_height()
            .expect_err("invalid eth_call result should fail");
        assert!(err
            .to_string()
            .contains("failed to call BTCRelay.getBlockheight"));
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
                Ok(
                    "0x0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
                        .to_string(),
                )
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
                Ok(
                    "0x0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
                        .to_string(),
                )
            }
        }
        let mut submitter = test_relay_client_with_transport(Arc::new(PendingReceiptTransport));
        submitter.evm_tx_timeout_secs = 1;
        let err = submitter
            .wait_for_confirmation(
                "0x0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
            )
            .expect_err("pending receipt should eventually timeout");
        assert!(err
            .to_string()
            .contains("timed out waiting for tx confirmation"));
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
                Ok(
                    "0x0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
                        .to_string(),
                )
            }
        }
        let submitter = test_relay_client_with_transport(Arc::new(NonStringEthCallTransport));
        let err = submitter
            .relay_chain_work_bytes()
            .expect_err("non-string eth_call should fail");
        assert!(err
            .to_string()
            .contains("failed to call BTCRelay.getChainwork"));
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
                Ok(
                    "0x0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
                        .to_string(),
                )
            }
        }
        let submitter = test_relay_client_with_transport(Arc::new(BadHexEthCallTransport));
        let err = submitter
            .relay_chain_work_bytes()
            .expect_err("invalid hex eth_call should fail");
        assert!(err
            .to_string()
            .contains("failed to call BTCRelay.getChainwork"));
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
                Ok(
                    "0x0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
                        .to_string(),
                )
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
                    "eth_getTransactionReceipt" => Ok(json!({"status":"0x1","blockNumber":"0x10"})),
                    "eth_blockNumber" => Ok(Value::String("zz".to_string())),
                    _ => anyhow::bail!("unexpected rpc method {}", method),
                }
            }
            fn send_transaction(&self, _request: SendTxRequest) -> Result<String> {
                Ok(
                    "0x0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
                        .to_string(),
                )
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
            let response =
                tiny_http::Response::from_string(r#"{"jsonrpc":"2.0","id":1,"result":null}"#)
                    .with_header(
                        tiny_http::Header::from_bytes(
                            &b"Content-Type"[..],
                            &b"application/json"[..],
                        )
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
