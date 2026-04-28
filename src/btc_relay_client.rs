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

const BTC_HEADER_HEX_LEN: usize = 160;
const EVM_TX_HASH_HEX_LEN: usize = 66;

sol! {
    interface IBtcRelayView {
        function getBlockheight() external view returns (uint32);
        function getCommitHash(uint256 height) external view returns (bytes32);
        function submitMainBlockheaders(bytes headers) external;
        function submitShortForkBlockheaders(bytes headers) external;
        function submitForkBlockheaders(uint256 forkId, bytes headers) external;
    }
}

/// EVM submitter used to read/write BTC relay contract state.
#[allow(dead_code)]
pub struct EvmBtcRelaySubmitter {
    pub evm_rpc_url: String,
    pub relay_contract_address: String,
    pub relayer_private_key: String,
    pub evm_chain_id: u64,
    pub evm_tx_confirmations: u64,
    pub evm_tx_timeout_secs: u64,
    pub evm_max_fee_gwei: Option<u64>,
    pub evm_priority_fee_gwei: Option<u64>,
}

#[allow(dead_code)]
impl EvmBtcRelaySubmitter {
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

    /// Validates and decodes one raw BTC header (80 bytes / 160 hex chars).
    fn header_hex_to_bytes(&self, header_hex: &str) -> Result<Vec<u8>> {
        if header_hex.trim().is_empty() {
            anyhow::bail!("submit_header requires non-empty header");
        }
        if header_hex.len() != BTC_HEADER_HEX_LEN {
            anyhow::bail!(
                "submit_header requires {} hex chars, got {}",
                BTC_HEADER_HEX_LEN,
                header_hex.len()
            );
        }

        let mut out = Vec::with_capacity(BTC_HEADER_HEX_LEN / 2);
        let bytes = header_hex.as_bytes();
        let mut i = 0;
        while i < bytes.len() {
            let hi = hex_nibble(bytes[i]).context("header contains non-hex character")?;
            let lo = hex_nibble(bytes[i + 1]).context("header contains non-hex character")?;
            out.push((hi << 4) | lo);
            i += 2;
        }

        Ok(out)
    }

    /// Sends encoded calldata as a transaction through the configured EVM RPC.
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
            let provider = Provider::<Http>::try_from(rpc_url.as_str())
                .context("failed to create EVM provider")?;
            let wallet = private_key
                .parse::<LocalWallet>()
                .context("invalid RELAYER_PRIVATE_KEY format")?
                .with_chain_id(chain_id);
            let client = SignerMiddleware::new(provider, wallet);

            let to = relay_contract_address
                .parse::<Address>()
                .context("invalid RELAY_CONTRACT_ADDRESS")?;
            let mut req = Eip1559TransactionRequest {
                to: Some(NameOrAddress::Address(to)),
                data: Some(Bytes::from(calldata_bytes)),
                chain_id: Some(chain_id.into()),
                ..Default::default()
            };
            if let Some(max_fee) = max_fee_gwei {
                req.max_fee_per_gas = Some(EthersU256::from(max_fee) * EthersU256::from(1_000_000_000_u64));
            }
            if let Some(priority_fee) = priority_fee_gwei {
                req.max_priority_fee_per_gas =
                    Some(EthersU256::from(priority_fee) * EthersU256::from(1_000_000_000_u64));
            }
            let tx: TypedTransaction = req.into();

            let pending = client
                .send_transaction(tx, None)
                .await
                .context("eth_sendRawTransaction failed")?;
            Ok::<String, anyhow::Error>(format!("{:#x}", pending.tx_hash()))
        })?;

        if !is_valid_tx_hash(&tx_hash) {
            anyhow::bail!("eth_sendRawTransaction returned invalid tx hash: {}", tx_hash);
        }

        Ok(tx_hash)
    }

    /// Waits until the submitted tx reaches configured confirmation depth.
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
            status: Option<String>,
            #[serde(rename = "blockNumber")]
            block_number: Option<String>,
        }

        let deadline = Instant::now() + Duration::from_secs(self.evm_tx_timeout_secs);
        let poll_interval = Duration::from_secs(2);

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
                thread::sleep(poll_interval);
                continue;
            }

            let receipt: Receipt = serde_json::from_value(receipt_value)
                .context("failed to decode eth_getTransactionReceipt result")?;

            match receipt.status.as_deref() {
                Some("0x1") => {}
                Some("0x0") => anyhow::bail!("transaction reverted on-chain: {}", tx_hash),
                Some(other) => anyhow::bail!("unexpected transaction status {} for {}", other, tx_hash),
                None => anyhow::bail!("transaction receipt missing status for {}", tx_hash),
            }

            let tx_block = receipt
                .block_number
                .as_deref()
                .context("transaction receipt missing blockNumber")?;
            let tx_block_num =
                parse_hex_quantity_u64(tx_block).context("invalid tx receipt blockNumber format")?;

            let head = self
                .rpc_request("eth_blockNumber", json!([]))
                .context("failed to fetch eth_blockNumber")?
                .as_str()
                .context("eth_blockNumber returned non-string result")?
                .to_string();
            
            let head_num =
                parse_hex_quantity_u64(head.as_str()).context("invalid eth_blockNumber format")?;

            let confirmations = head_num.saturating_sub(tx_block_num) + 1;
            if confirmations >= self.evm_tx_confirmations {
                return Ok(());
            }

            thread::sleep(poll_interval);
        }
    }

    /// MVP submit path: send canonical/main-chain headers only.
    /// Fork-specific submit methods are intentionally left for next iteration.
    fn build_submit_main_calldata(&self, headers_bytes: &[u8]) -> Vec<u8> {
        let owned_headers: Vec<u8> = headers_bytes.to_vec();
        let call = IBtcRelayView::submitMainBlockheadersCall {
            headers: owned_headers.into(),
        };
        call.abi_encode()
    }

    /// Next iteration hook: short competing fork support.
    #[allow(dead_code)]
    fn build_submit_short_fork_calldata(&self, headers_bytes: &[u8]) -> Vec<u8> {
        let owned_headers: Vec<u8> = headers_bytes.to_vec();
        let call = IBtcRelayView::submitShortForkBlockheadersCall {
            headers: owned_headers.into(),
        };
        call.abi_encode()
    }

    /// Next iteration hook: append headers to an existing fork.
    #[allow(dead_code)]
    fn build_submit_fork_calldata(&self, fork_id: u64, headers_bytes: &[u8]) -> Vec<u8> {
        let owned_headers: Vec<u8> = headers_bytes.to_vec();
        let call = IBtcRelayView::submitForkBlockheadersCall {
            forkId: AlloyU256::try_from(fork_id).unwrap(),
            headers: owned_headers.into(),
        };
        call.abi_encode()
    }

    /// Minimal `eth_call` helper used for ABI read calls.
    fn evm_call_latest(&self, to: &str, data: &[u8]) -> Result<Vec<u8>> {
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

fn is_valid_tx_hash(value: &str) -> bool {
    value.len() == EVM_TX_HASH_HEX_LEN
        && value.starts_with("0x")
        && value.chars().skip(2).all(|c| c.is_ascii_hexdigit())
}

impl BtcRelaySubmitter for EvmBtcRelaySubmitter {
    fn relay_tip_height(&self) -> Result<u64> {
        let call = IBtcRelayView::getBlockheightCall {};
        let raw = self
            .evm_call_latest(&self.relay_contract_address, &call.abi_encode())
            .context("failed to call BTCRelay.getBlockheight")?;

        let height = IBtcRelayView::getBlockheightCall::abi_decode_returns(&raw)
            .context("failed to decode BTCRelay.getBlockheight return value")?;

        Ok(u64::from(height))
    }

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

    fn submit_header(&self, header_hex: &str) -> Result<String> {
        let header_bytes = self
            .header_hex_to_bytes(header_hex)
            .context("failed to validate/convert header hex")?;
        let calldata = self.build_submit_main_calldata(&header_bytes);

        let tx_hash = self
            .send_tx(&calldata)
            .context("failed to send header submission transaction")?;

        self.wait_for_confirmation(&tx_hash)
            .context("header submission transaction failed confirmation step")?;

        Ok(tx_hash)
    }
}

fn bytes_to_prefixed_hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(2 + bytes.len() * 2);
    out.push_str("0x");
    for b in bytes {
        use std::fmt::Write as _;
        let _ = write!(&mut out, "{:02x}", b);
    }
    out
}

fn parse_hex_quantity_u64(value: &str) -> Result<u64> {
    let raw = value
        .strip_prefix("0x")
        .context("hex quantity must start with 0x")?;
    if raw.is_empty() {
        return Ok(0);
    }
    u64::from_str_radix(raw, 16).context("failed to parse hex quantity as u64")
}

fn hex_nibble(c: u8) -> Option<u8> {
    match c {
        b'0'..=b'9' => Some(c - b'0'),
        b'a'..=b'f' => Some(c - b'a' + 10),
        b'A'..=b'F' => Some(c - b'A' + 10),
        _ => None,
    }
}

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

    fn test_submitter() -> EvmBtcRelaySubmitter {
        EvmBtcRelaySubmitter {
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
    fn header_hex_to_bytes_accepts_80_byte_header() {
        let submitter = test_submitter();
        let input = "00".repeat(80);
        let bytes = submitter
            .header_hex_to_bytes(&input)
            .expect("header should parse");
        assert_eq!(bytes.len(), 80);
        assert!(bytes.iter().all(|b| *b == 0));
    }

    #[test]
    fn header_hex_to_bytes_rejects_invalid_length() {
        let submitter = test_submitter();
        let err = submitter
            .header_hex_to_bytes("abcd")
            .expect_err("short header should fail");
        assert!(err.to_string().contains("requires 160 hex chars"));
    }

    #[test]
    fn header_hex_to_bytes_rejects_non_hex_chars() {
        let submitter = test_submitter();
        let mut header = "00".repeat(79);
        header.push_str("zz");
        let err = submitter
            .header_hex_to_bytes(&header)
            .expect_err("non-hex header should fail");
        assert!(err.to_string().contains("non-hex"));
    }

    #[test]
    fn build_submit_main_calldata_has_expected_selector() {
        let submitter = test_submitter();
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
