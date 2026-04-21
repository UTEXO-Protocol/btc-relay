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
        let payload = build_rpc_payload(method, params);

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

    /// Returns `true` while bitcoind is in initial block download (IBD).
    /// Uses `getblockchaininfo` → `initialblockdownload` (Bitcoin Core).
    pub fn initial_block_download(&self) -> Result<bool> {
        let result = self
            .rpc_call("getblockchaininfo", json!([]))
            .context("getblockchaininfo rpc call failed")?;

        let ibd = result
            .get("initialblockdownload")
            .and_then(|v| v.as_bool())
            .context("getblockchaininfo result missing initialblockdownload")?;

        Ok(ibd)
    }
}

fn build_rpc_payload(method: &str, params: Value) -> Value {
    json!({
        "jsonrpc": "1.0",
        "id": "btc-relayer",
        "method": method,
        "params": params,
    })
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::thread;

    fn spawn_test_server(response_body: &'static str) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind test server");
        let addr = listener.local_addr().expect("local addr");

        thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("accept");
            let mut req_buf = [0_u8; 2048];
            let _ = stream.read(&mut req_buf);
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                response_body.len(),
                response_body
            );
            stream
                .write_all(response.as_bytes())
                .expect("write response");
        });

        format!("http://{}", addr)
    }

    #[test]
    fn builds_expected_rpc_payload_shape() {
        let payload = build_rpc_payload("getblockhash", json!([42]));
        assert_eq!(payload["jsonrpc"], "1.0");
        assert_eq!(payload["id"], "btc-relayer");
        assert_eq!(payload["method"], "getblockhash");
        assert_eq!(payload["params"], json!([42]));
    }

    #[test]
    fn initial_block_download_reads_flag_from_blockchain_info() {
        let url = spawn_test_server(
            r#"{"result":{"chain":"regtest","blocks":1,"initialblockdownload":false},"error":null,"id":"btc-relayer"}"#,
        );
        let client = HttpBitcoinRpcClient::new(
            url,
            "user".to_string(),
            "pass".to_string(),
            Client::builder().build().expect("client"),
        );
        assert!(!client.initial_block_download().expect("ibd"));
    }

    #[test]
    fn rejects_header_with_invalid_length() {
        let url = spawn_test_server(r#"{"result":"abcd","error":null,"id":"btc-relayer"}"#);
        let client = HttpBitcoinRpcClient::new(
            url,
            "user".to_string(),
            "pass".to_string(),
            Client::builder().build().expect("client"),
        );

        let err = client
            .get_block_header_hex("0000000000000000000000000000000000000000000000000000000000000000")
            .expect_err("expected invalid header length");
        assert!(
            err.to_string().contains("invalid header length"),
            "unexpected error: {}",
            err
        );
    }
}
