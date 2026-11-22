//! Human-readable printing for `poly-as sandbox` commands.

use std::path::Path;

use execution::{NexBook, NexDiscovery, NexGateReport, NexMarket, NexValidationOutcome};

use crate::theme::{self, Badge};

pub(crate) fn print_sandbox_header(action: &str, jsonl: &Path) {
    let t = theme::current();
    println!();
    println!(
        "{}",
        t.banner(
            "POLYMARKET NEX SANDBOX :: AMOY 80002 (ISOLATED BOOKS / TEST pUSD)",
            None
        )
    );
    println!("{}", t.kv("Action:", action));
    println!("{}", t.kv("JSONL Telemetry:", &jsonl.display().to_string()));
    println!(
        "{}",
        t.kv(
            "Origin Tag:",
            "sandbox (never mixed with production calibration)"
        )
    );
    println!("{}", t.divider());
}

pub(crate) fn print_gate(report: &NexGateReport) {
    let t = theme::current();
    println!("{}", t.section("(a) PUBLIC CONNECTION GATE"));
    println!(
        "{}",
        t.kv(
            "Egress Public IPv4:",
            report.egress_ip.as_deref().unwrap_or("<probe failed>")
        )
    );
    if let Some(err) = &report.egress_probe_error {
        println!("{}", t.kv("Egress Probe Detail:", &t.yellow(err)));
    }
    println!(
        "{}",
        t.kv(
            "Registered Allowlist:",
            report
                .allowlisted_egress_ip
                .as_deref()
                .unwrap_or("<not configured>")
        )
    );
    println!(
        "{}",
        t.kv(
            "Allowlist Match:",
            &if report.egress_allowlist_match {
                format!("{} YES", t.badge(Badge::Pass))
            } else {
                format!(
                    "{} NO (update the NEX allowlist registry if the egress changed)",
                    t.badge(Badge::Warn)
                )
            }
        )
    );
    println!(
        "{}",
        t.kv("Pinned Ingress IPv4:", &report.ingress_ip.to_string())
    );
    for pin in &report.dns_pins {
        println!(
            "{}",
            t.kv(
                "DNS Pin:",
                &format!(
                    "{} -> {:?} (pinned={})",
                    pin.host, pin.resolved, pin.pinned_to_ingress
                )
            )
        );
    }
    println!(
        "{}",
        t.kv(
            "Outbound TCP/443:",
            &format!("{} ({})", t.pass_fail(report.tcp_443_ok), report.tcp_443_ok)
        )
    );
    println!("{}", t.kv("TLS SNI Host:", &report.tls_sni_host));
    println!("{}", t.kv("HTTP Host Header:", &report.http_host_header));
    println!(
        "{}",
        t.kv(
            "GET /health:",
            &format!(
                "{} {:?} (ok={})",
                t.pass_fail(report.health_ok),
                report.health_status,
                report.health_ok
            )
        )
    );
    if let Some(body) = &report.health_body {
        println!("{}", t.kv("Health Body:", body));
    }
    if let Some(err) = &report.health_error {
        println!("{}", t.kv("Health Error:", &t.red(err)));
    }
    println!("{}", t.divider());
}

pub(crate) fn print_discovery(discovery: &NexDiscovery) {
    let t = theme::current();
    println!("{}", t.section("(b) SANDBOX MARKET DISCOVERY"));
    println!("{}", t.kv("Source:", &discovery.source));
    if let Some(err) = &discovery.gamma_error {
        println!("{}", t.kv("Gamma Preprod Error:", &t.yellow(err)));
    }
    println!(
        "{}",
        t.kv("Markets Returned:", &discovery.markets.len().to_string())
    );
    for (idx, m) in discovery.markets.iter().take(15).enumerate() {
        println!(
            "    [{idx}] {} tick={} min={} v={:?} neg_risk={} slug={}",
            m.condition_id,
            m.tick_size_micro.0,
            m.min_order_size_micro.0,
            m.version,
            m.neg_risk,
            t.cyan(&m.slug)
        );
        println!("         {} {}", t.muted("UP"), m.up_asset_id);
        println!("         {} {}", t.muted("DN"), m.dn_asset_id);
    }
    println!("{}", t.divider());
}

pub(crate) fn print_book(book: &NexBook) {
    let t = theme::current();
    println!("{}", t.section("(c) BOOK"));
    println!("{}", t.kv("Market:", &book.market));
    println!("{}", t.kv("Asset ID:", &book.asset_id));
    println!(
        "{}",
        t.kv(
            "Tick / Min:",
            &format!(
                "{} / {}",
                book.tick_size_micro.0, book.min_order_size_micro.0
            )
        )
    );
    println!(
        "{}",
        t.kv("Venue Timestamp (ms):", &book.timestamp_ms.to_string())
    );
    println!("{}", t.kv("Empty Book:", &book.is_empty().to_string()));
    let (bid, ask) = book.best();
    println!(
        "{}",
        t.kv(
            "Best Bid/Ask:",
            &format!(
                "{:?} / {:?}",
                bid.map(|l| (l.price.0, l.size.0)),
                ask.map(|l| (l.price.0, l.size.0))
            )
        )
    );
    println!("{}", t.kv("Bids (top 5):", ""));
    for level in book.bids.iter().take(5) {
        println!("        {} x {}", level.price.0, level.size.0);
    }
    println!("{}", t.kv("Asks (top 5):", ""));
    for level in book.asks.iter().take(5) {
        println!("        {} x {}", level.price.0, level.size.0);
    }
    println!("{}", t.divider());
}

pub(crate) fn print_validation(outcome: &NexValidationOutcome) {
    let t = theme::current();
    println!("{}", t.section("(d) VALIDATION ORDER"));
    println!(
        "{}",
        t.kv(
            "Chain / Wallet:",
            &format!("{} / {}", outcome.chain_id, outcome.wallet_mode)
        )
    );
    println!("{}", t.kv("Maker Address:", &outcome.maker_address));
    println!(
        "{}",
        t.kv(
            "Outcome / Asset:",
            &format!("{} / {}", outcome.outcome, outcome.asset_id)
        )
    );
    println!(
        "{}",
        t.kv(
            "Price x Size (micro):",
            &format!(
                "{} x {} (one tick / min size)",
                outcome.price_micro, outcome.size_micro
            )
        )
    );
    println!("{}", t.kv("Guards:", &format!("{:?}", outcome.guards)));
    println!(
        "{}",
        t.kv(
            "Placement Success:",
            &format!(
                "{} ({})",
                t.pass_fail(outcome.placement.success),
                outcome.placement.success
            )
        )
    );
    println!("{}", t.kv("Order ID:", &outcome.placement.order_id));
    println!("{}", t.kv("Placement Status:", &outcome.placement.status));
    if !outcome.placement.error_msg.is_empty() {
        println!(
            "{}",
            t.kv("Placement Error:", &t.red(&outcome.placement.error_msg))
        );
    }
    match &outcome.readback {
        Some(record) => {
            println!(
                "{}",
                t.kv(
                    "Read-Back:",
                    &format!(
                        "status={} price={} size={} matched={}",
                        record.status, record.price, record.original_size, record.size_matched
                    )
                )
            );
        }
        None => println!(
            "{}",
            t.kv(
                "Read-Back:",
                &t.muted("<not visible; reconcile via user feed>")
            )
        ),
    }
    println!(
        "{}",
        t.kv("Read-Back Missing:", &outcome.readback_missing.to_string())
    );
    if let Some(detail) = &outcome.cancel_detail {
        println!("{}", t.kv("Cancel/Reconcile:", detail));
    }
    println!("{}", t.kv("Settlement:", &outcome.settlement_note));
    println!("{}", t.divider());
}

pub(crate) fn print_market_summary(market: &NexMarket) {
    println!(
        "    Market: {} (tick={} min={} v={:?})",
        market.condition_id,
        market.tick_size_micro.0,
        market.min_order_size_micro.0,
        market.version
    );
}
