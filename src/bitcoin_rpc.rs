use anyhow::{Context, Result};
use reqwest::blocking::Client;
use serde_json::{json, Value};

use crate::interfaces::BitcoinRpcClient;

const BTC_HEADER_HEX_LEN: usize = 160;

#[allow(dead_code)]
pub struct HttpBitcoinRpcClient {
    pub url: String,
    pub user: String,
    pub password: String,
    pub http: Client,
}

#[allow(dead_code)]
impl HttpBitcoinRpcClient {
    pub fn new(url: String, user: String, password: String, http: Client) -> Self {
        Self {
            url,
            user,
            password,
            http,
        }
    }

    fn rpc_call(&self, method: &str, params: Value) -> Result<Value> {
        let payload = json!({
            "jsonrpc": "1.0",
            "id": "btc-relayer",
            "method": method,
            "params": params,
        });

        let response = self
            .http
            .post(&self.url)
            .basic_auth(&self.user, Some(&self.password))
            .json(&payload)
            .send()
            .with_context(|| format!("bitcoin rpc transport failed for method {}", method))?;

        let status = response.status();
        let body: Value = response
            .json()
            .with_context(|| format!("bitcoin rpc response json decode failed for method {}", method))?;

        if !status.is_success() {
            anyhow::bail!("bitcoin rpc http status {} for method {}: {}", status, method, body);
        }

        if !body["error"].is_null() {
            anyhow::bail!("bitcoin rpc returned error for method {}: {}", method, body["error"]);
        }

        Ok(body["result"].clone())
    }
}

impl BitcoinRpcClient for HttpBitcoinRpcClient {
    fn get_block_count(&self) -> Result<u64> {
        let result = self
            .rpc_call("getblockcount", json!([]))
            .context("getblockcount rpc call failed")?;

        result
            .as_u64()
            .context("getblockcount returned non-u64 result")
    }

    fn get_block_hash(&self, height: u64) -> Result<String> {
        let result = self
            .rpc_call("getblockhash", json!([height]))
            .with_context(|| format!("getblockhash rpc call failed for height {}", height))?;

        let hash = result
            .as_str()
            .context("getblockhash returned non-string result")?
            .to_string();

        if hash.trim().is_empty() {
            anyhow::bail!("getblockhash returned empty hash for height {}", height);
        }

        Ok(hash)
    }

    fn get_best_block_hash(&self) -> Result<String> {
        let result = self
            .rpc_call("getbestblockhash", json!([]))
            .context("getbestblockhash rpc call failed")?;

        let hash = result
            .as_str()
            .context("getbestblockhash returned non-string result")?
            .to_string();

        if hash.trim().is_empty() {
            anyhow::bail!("getbestblockhash returned empty hash");
        }

        Ok(hash)
    }

    fn get_block_header_hex(&self, hash: &str) -> Result<String> {
        if hash.trim().is_empty() {
            anyhow::bail!("getblockheader requires non-empty hash");
        }

        let result = self
            .rpc_call("getblockheader", json!([hash, false]))
            .with_context(|| format!("getblockheader rpc call failed for hash {}", hash))?;

        let header_hex = result
            .as_str()
            .context("getblockheader returned non-string result")?
            .to_string();

        if header_hex.trim().is_empty() {
            anyhow::bail!("getblockheader returned empty header for hash {}", hash);
        }
        if header_hex.len() != BTC_HEADER_HEX_LEN {
            anyhow::bail!(
                "getblockheader returned invalid header length for hash {}: expected {}, got {}",
                hash,
                BTC_HEADER_HEX_LEN,
                header_hex.len()
            );
        }

        Ok(header_hex)
    }
}
