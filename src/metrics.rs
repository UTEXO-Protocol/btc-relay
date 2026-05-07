// Copyright (C) 2026 Utexo.
// See LICENSE for copying information.

use anyhow::Result;
use prometheus::{
    Encoder, Gauge, IntCounter, IntCounterVec, IntGauge, Registry, TextEncoder, opts,
    register_gauge_with_registry,
    register_int_counter_vec_with_registry, register_int_counter_with_registry,
    register_int_gauge_with_registry,
};
use std::sync::OnceLock;
use std::thread;

struct Metrics {
    startup_checks_total: IntCounterVec,
    startup_checks_failed_total: IntCounterVec,
    sync_poll_cycles_total: IntCounter,
    sync_poll_cycle_errors_total: IntCounter,
    sync_poll_up_to_date_total: IntCounter,
    sync_poll_progressed_total: IntCounter,
    sync_headers_submitted_total: IntCounter,
    sync_retries_total: IntCounter,
    relayer_tx_confirmed_total: IntCounter,
    relayer_tx_fee_wei_total: Gauge,
    relayer_tx_fee_eth_total: Gauge,
    bitcoin_tip_height: IntGauge,
    relay_tip_height: IntGauge,
    relay_lag_blocks: IntGauge,
    relayer_wallet_balance_wei: Gauge,
    relayer_wallet_balance_eth: Gauge,
    relayer_est_txs_left_at_current_fee: Gauge,
}

static REGISTRY: OnceLock<Registry> = OnceLock::new();
static METRICS: OnceLock<Metrics> = OnceLock::new();

fn registry() -> &'static Registry {
    REGISTRY.get_or_init(Registry::new)
}

fn metrics() -> &'static Metrics {
    METRICS.get_or_init(|| {
        let registry = registry();
        Metrics {
            startup_checks_total: register_int_counter_vec_with_registry!(
                opts!("startup_checks_total", "Startup checks attempted by check name"),
                &["check"],
                registry
            )
            .expect("register startup_checks_total"),
            startup_checks_failed_total: register_int_counter_vec_with_registry!(
                opts!(
                    "startup_checks_failed_total",
                    "Startup check failures by check name"
                ),
                &["check"],
                registry
            )
            .expect("register startup_checks_failed_total"),
            sync_poll_cycles_total: register_int_counter_with_registry!(
                opts!("sync_poll_cycles_total", "Sync poll cycles attempted"),
                registry
            )
            .expect("register sync_poll_cycles_total"),
            sync_poll_cycle_errors_total: register_int_counter_with_registry!(
                opts!("sync_poll_cycle_errors_total", "Sync poll cycles that ended in error"),
                registry
            )
            .expect("register sync_poll_cycle_errors_total"),
            sync_poll_up_to_date_total: register_int_counter_with_registry!(
                opts!("sync_poll_up_to_date_total", "Poll cycles where relay was up-to-date"),
                registry
            )
            .expect("register sync_poll_up_to_date_total"),
            sync_poll_progressed_total: register_int_counter_with_registry!(
                opts!("sync_poll_progressed_total", "Poll cycles that submitted progress"),
                registry
            )
            .expect("register sync_poll_progressed_total"),
            sync_headers_submitted_total: register_int_counter_with_registry!(
                opts!("sync_headers_submitted_total", "Headers submitted by sync engine"),
                registry
            )
            .expect("register sync_headers_submitted_total"),
            sync_retries_total: register_int_counter_with_registry!(
                opts!("sync_retries_total", "Retry attempts during sync catch-up"),
                registry
            )
            .expect("register sync_retries_total"),
            relayer_tx_confirmed_total: register_int_counter_with_registry!(
                opts!("relayer_tx_confirmed_total", "Confirmed relayer submission transactions"),
                registry
            )
            .expect("register relayer_tx_confirmed_total"),
            relayer_tx_fee_wei_total: register_gauge_with_registry!(
                opts!("relayer_tx_fee_wei_total", "Cumulative relayer transaction fees in wei"),
                registry
            )
            .expect("register relayer_tx_fee_wei_total"),
            relayer_tx_fee_eth_total: register_gauge_with_registry!(
                opts!("relayer_tx_fee_eth_total", "Cumulative relayer transaction fees in ETH"),
                registry
            )
            .expect("register relayer_tx_fee_eth_total"),
            bitcoin_tip_height: register_int_gauge_with_registry!(
                opts!("bitcoin_tip_height", "Latest Bitcoin tip height seen by sync loop"),
                registry
            )
            .expect("register bitcoin_tip_height"),
            relay_tip_height: register_int_gauge_with_registry!(
                opts!("relay_tip_height", "Latest relay tip height seen by sync loop"),
                registry
            )
            .expect("register relay_tip_height"),
            relay_lag_blocks: register_int_gauge_with_registry!(
                opts!("relay_lag_blocks", "Current lag between bitcoin tip and relay tip"),
                registry
            )
            .expect("register relay_lag_blocks"),
            relayer_wallet_balance_wei: register_gauge_with_registry!(
                opts!("relayer_wallet_balance_wei", "Relayer wallet balance in wei"),
                registry
            )
            .expect("register relayer_wallet_balance_wei"),
            relayer_wallet_balance_eth: register_gauge_with_registry!(
                opts!("relayer_wallet_balance_eth", "Relayer wallet balance in ETH"),
                registry
            )
            .expect("register relayer_wallet_balance_eth"),
            relayer_est_txs_left_at_current_fee: register_gauge_with_registry!(
                opts!(
                    "relayer_est_txs_left_at_current_fee",
                    "Estimated remaining tx count at recent fee level"
                ),
                registry
            )
            .expect("register relayer_est_txs_left_at_current_fee"),
        }
    })
}

pub fn observe_startup_check(check: &str, ok: bool) {
    let m = metrics();
    m.startup_checks_total.with_label_values(&[check]).inc();
    if !ok {
        m.startup_checks_failed_total
            .with_label_values(&[check])
            .inc();
    }
}

pub fn inc_sync_poll_cycle() {
    metrics().sync_poll_cycles_total.inc();
}

pub fn inc_sync_poll_cycle_error() {
    metrics().sync_poll_cycle_errors_total.inc();
}

pub fn inc_sync_poll_up_to_date() {
    metrics().sync_poll_up_to_date_total.inc();
}

pub fn inc_sync_poll_progressed() {
    metrics().sync_poll_progressed_total.inc();
}

pub fn add_sync_headers_submitted(value: u64) {
    metrics().sync_headers_submitted_total.inc_by(value);
}

pub fn add_sync_retries(value: u64) {
    metrics().sync_retries_total.inc_by(value);
}

pub fn set_tip_gauges(bitcoin_tip: u64, relay_tip: u64, lag: u64) {
    let m = metrics();
    m.bitcoin_tip_height.set(bitcoin_tip as i64);
    m.relay_tip_height.set(relay_tip as i64);
    m.relay_lag_blocks.set(lag as i64);
}

pub fn set_relayer_wallet_balance(balance_wei: f64, balance_eth: f64) {
    let m = metrics();
    m.relayer_wallet_balance_wei.set(balance_wei);
    m.relayer_wallet_balance_eth.set(balance_eth);
}

pub fn record_confirmed_tx_fee_wei(tx_fee_wei: f64) {
    let m = metrics();
    m.relayer_tx_confirmed_total.inc();
    m.relayer_tx_fee_wei_total
        .set(m.relayer_tx_fee_wei_total.get() + tx_fee_wei);
    m.relayer_tx_fee_eth_total
        .set(m.relayer_tx_fee_eth_total.get() + (tx_fee_wei / 1_000_000_000_000_000_000_f64));
}

pub fn set_estimated_txs_left(value: f64) {
    metrics().relayer_est_txs_left_at_current_fee.set(value);
}

pub fn start_exporter(bind_addr: &str) -> Result<()> {
    let addr = bind_addr.to_string();
    let server = tiny_http::Server::http(addr.as_str())
        .map_err(|e| anyhow::anyhow!("bind {}: {}", addr, e))?;
    thread::spawn(move || {
        let encoder = TextEncoder::new();
        for request in server.incoming_requests() {
            if request.url() != "/metrics" {
                let _ = request.respond(tiny_http::Response::empty(404));
                continue;
            }
            let metric_families = registry().gather();
            let mut buffer = Vec::new();
            if encoder.encode(&metric_families, &mut buffer).is_err() {
                let _ = request.respond(tiny_http::Response::empty(500));
                continue;
            }
            let response = tiny_http::Response::from_data(buffer);
            let _ = request.respond(response);
        }
    });
    Ok(())
}
