use alloy::primitives::U256;
use anyhow::{Context, Result};
use serde::Deserialize;
use serde_json::json;

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
    }
}

/// Task 3 scaffolding for EVM BTC relay submitter.
///
/// ABI-specific contract calls are intentionally deferred to point 5
/// (ABI integration). This module locks submitter boundaries now.
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

    /// Helper stub for point 3 internal structure.
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

        fn hex_nibble(c: u8) -> Option<u8> {
            match c {
                b'0'..=b'9' => Some(c - b'0'),
                b'a'..=b'f' => Some(c - b'a' + 10),
                b'A'..=b'F' => Some(c - b'A' + 10),
                _ => None,
            }
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

    /// Helper stub for point 3 internal structure.
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

        anyhow::bail!(
            "send_tx is preflight-ready but ABI call path is not wired yet; implement in Task 3 point 5"
        )
    }

    /// Helper stub for point 3 internal structure.
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

        anyhow::bail!(
            "wait_for_confirmation preflight is ready but receipt polling is not wired yet; implement in Task 3 point 5/6"
        )
    }

    /// Minimal `eth_call` helper used for ABI read calls.
    fn evm_call_latest(&self, to: &str, data: &[u8]) -> Result<Vec<u8>> {
        #[derive(Debug, Deserialize)]
        struct EthCallResponse {
            result: Option<String>,
            error: Option<serde_json::Value>,
        }

        let request = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "eth_call",
            "params": [
                {
                    "to": to,
                    "data": bytes_to_prefixed_hex(data),
                },
                "latest"
            ],
        });

        let response: EthCallResponse = reqwest::blocking::Client::new()
            .post(&self.evm_rpc_url)
            .json(&request)
            .send()
            .context("eth_call transport failed")?
            .json()
            .context("eth_call response decode failed")?;

        if let Some(err) = response.error {
            anyhow::bail!("eth_call returned error: {}", err);
        }

        let result = response
            .result
            .context("eth_call response missing result field")?;

        hex_prefixed_to_bytes(&result).context("eth_call returned invalid hex result")
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
            height: U256::try_from(height)?,
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

        let tx_hash = self
            .send_tx(&header_bytes)
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

fn hex_prefixed_to_bytes(value: &str) -> Result<Vec<u8>> {
    if !value.starts_with("0x") {
        anyhow::bail!("hex value must start with 0x");
    }
    let s = &value[2..];
    if s.len() % 2 != 0 {
        anyhow::bail!("hex value must have even length");
    }

    fn hex_nibble(c: u8) -> Option<u8> {
        match c {
            b'0'..=b'9' => Some(c - b'0'),
            b'a'..=b'f' => Some(c - b'a' + 10),
            b'A'..=b'F' => Some(c - b'A' + 10),
            _ => None,
        }
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
