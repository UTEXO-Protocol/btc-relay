// Copyright (C) 2026 Utexo.
// See LICENSE for copying information.

use btc_relayer::configs::AppConfig;
use btc_relayer::startup;
use std::collections::VecDeque;
use std::io::{Read, Write};
use std::net::TcpListener;
use std::sync::{Arc, Mutex};
use std::thread;

fn spawn_json_rpc_server(responses: Vec<String>) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind test server");
    let addr = listener.local_addr().expect("local addr");
    let queue = Arc::new(Mutex::new(VecDeque::from(responses)));
    let queue_ref = Arc::clone(&queue);

    thread::spawn(move || {
        while let Ok((mut stream, _)) = listener.accept() {
            let mut req_buf = [0_u8; 4096];
            let _ = stream.read(&mut req_buf);
            let body = {
                let mut guard = queue_ref.lock().expect("response queue lock");
                guard
                    .pop_front()
                    .unwrap_or_else(|| r#"{"result":null,"error":{"code":-1,"message":"no response configured"},"id":"test"}"#.to_string())
            };
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            );
            let _ = stream.write_all(response.as_bytes());
        }
    });

    format!("http://{}", addr)
}

fn test_config(bitcoin_url: String, evm_url: String) -> AppConfig {
    AppConfig {
        bitcoin_rpc_url: bitcoin_url,
        bitcoin_rpc_user: "user".to_string(),
        bitcoin_rpc_password: "pass".to_string(),
        bitcoin_rpc_timeout_secs: 5,
        bitcoin_ibd_poll_secs: 1,
        evm_rpc_url: evm_url,
        relay_contract_address: "0x1111111111111111111111111111111111111111".to_string(),
        relayer_private_key: "0x01".to_string(),
        evm_chain_id: 31337,
        evm_tx_confirmations: 1,
        evm_tx_timeout_secs: 10,
        evm_max_fee_gwei: None,
        evm_priority_fee_gwei: None,
        poll_interval_secs: 5,
        start_height: 0,
        catchup_batch_size: 16,
        live_lag_threshold: 2,
        state_file_path: "artifacts/relay-state.json".to_string(),
    }
}

#[test]
fn bitcoin_startup_smoke_check_succeeds_against_mock_rpc_server() {
    let best_hash = "00".repeat(32);
    let header_hex = "11".repeat(80);
    let bitcoin_url = spawn_json_rpc_server(vec![
        r#"{"result":{"initialblockdownload":false},"error":null,"id":"btc-relayer"}"#.to_string(),
        r#"{"result":123,"error":null,"id":"btc-relayer"}"#.to_string(),
        format!(r#"{{"result":"{}","error":null,"id":"btc-relayer"}}"#, best_hash),
        format!(r#"{{"result":"{}","error":null,"id":"btc-relayer"}}"#, header_hex),
    ]);
    let cfg = test_config(bitcoin_url, "http://127.0.0.1:8545".to_string());
    startup::run_bitcoin_rpc_smoke_check(&cfg).expect("bitcoin smoke check should pass");
}

#[test]
fn bitcoin_startup_smoke_check_fails_on_bad_header_response() {
    let best_hash = "00".repeat(32);
    let bitcoin_url = spawn_json_rpc_server(vec![
        r#"{"result":{"initialblockdownload":false},"error":null,"id":"btc-relayer"}"#.to_string(),
        r#"{"result":321,"error":null,"id":"btc-relayer"}"#.to_string(),
        format!(r#"{{"result":"{}","error":null,"id":"btc-relayer"}}"#, best_hash),
        r#"{"result":"abcd","error":null,"id":"btc-relayer"}"#.to_string(),
    ]);
    let cfg = test_config(bitcoin_url, "http://127.0.0.1:8545".to_string());
    let err = startup::run_bitcoin_rpc_smoke_check(&cfg).expect_err("invalid header must fail");
    assert!(err.to_string().contains("get_block_header_hex"));
}

#[test]
fn evm_startup_read_check_succeeds_against_mock_rpc_server() {
    let height_abi = format!("0x{:064x}", 42_u64);
    let commit_hash = format!("0x{}", "11".repeat(32));
    let evm_url = spawn_json_rpc_server(vec![
        format!(r#"{{"result":"{}","error":null,"id":1}}"#, height_abi),
        format!(r#"{{"result":"{}","error":null,"id":1}}"#, commit_hash),
    ]);
    let cfg = test_config("http://127.0.0.1:8332".to_string(), evm_url);
    startup::run_evm_relay_read_check(&cfg).expect("evm read check should pass");
}
