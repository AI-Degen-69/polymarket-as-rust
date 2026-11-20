//! Human-readable printing for `poly-as sandbox` commands.

use std::path::Path;

use execution::{NexBook, NexDiscovery, NexGateReport, NexMarket, NexValidationOutcome};

pub(crate) fn print_sandbox_header(action: &str, jsonl: &Path) {
    println!();
    println!("================================================================================");
    println!("  POLYMARKET NEX SANDBOX -- AMOY 80002 (ISOLATED BOOKS / TEST pUSD)");
    println!("================================================================================");
    println!("  Action:             {action}");
    println!("  JSONL Telemetry:    {}", jsonl.display());
    println!("  Origin Tag:         sandbox (never mixed with production calibration)");
    println!("--------------------------------------------------------------------------------");
}

pub(crate) fn print_gate(report: &NexGateReport) {
    println!("(a) PUBLIC CONNECTION GATE");
    println!(
        "    Egress public IPv4:      {}",
        report.egress_ip.as_deref().unwrap_or("<probe failed>")
    );
    if let Some(err) = &report.egress_probe_error {
        println!("    Egress probe detail:    {err}");
    }
    println!(
        "    Registered allowlist:   {}",
        report
            .allowlisted_egress_ip
            .as_deref()
            .unwrap_or("<not configured>")
    );
    println!(
        "    Allowlist match:        {}",
        if report.egress_allowlist_match {
            "YES"
        } else {
            "NO (update the NEX allowlist registry if the egress changed)"
        }
    );
    println!("    Pinned ingress IPv4:    {}", report.ingress_ip);
    for pin in &report.dns_pins {
        println!(
            "    Host {} -> {:?} (pinned={})",
            pin.host, pin.resolved, pin.pinned_to_ingress
        );
    }
    println!("    Outbound TCP/443:       {}", report.tcp_443_ok);
    println!("    TLS SNI host:           {}", report.tls_sni_host);
    println!("    HTTP Host header:       {}", report.http_host_header);
    println!(
        "    GET /health:            {:?} (ok={})",
        report.health_status, report.health_ok
    );
    if let Some(body) = &report.health_body {
        println!("    Health body:            {body}");
    }
    if let Some(err) = &report.health_error {
        println!("    Health error:           {err}");
    }
    println!("--------------------------------------------------------------------------------");
}

pub(crate) fn print_discovery(discovery: &NexDiscovery) {
    println!("(b) SANDBOX MARKET DISCOVERY");
    println!("    Source:                 {}", discovery.source);
    if let Some(err) = &discovery.gamma_error {
        println!("    Gamma preprod error:    {err}");
    }
    println!("    Markets returned:       {}", discovery.markets.len());
    for (idx, m) in discovery.markets.iter().take(15).enumerate() {
        println!(
            "    [{idx}] {} tick={} min={} v={:?} neg_risk={} slug={}",
            m.condition_id,
            m.tick_size_micro.0,
            m.min_order_size_micro.0,
            m.version,
            m.neg_risk,
            m.slug
        );
        println!("         UP  {}", m.up_asset_id);
        println!("         DN  {}", m.dn_asset_id);
    }
    println!("--------------------------------------------------------------------------------");
}

pub(crate) fn print_book(book: &NexBook) {
    println!("(c) BOOK");
    println!("    Market:                 {}", book.market);
    println!("    Asset ID:               {}", book.asset_id);
    println!(
        "    Tick / min:             {} / {}",
        book.tick_size_micro.0, book.min_order_size_micro.0
    );
    println!("    Venue timestamp (ms):   {}", book.timestamp_ms);
    println!("    Empty book:             {}", book.is_empty());
    let (bid, ask) = book.best();
    println!(
        "    Best bid/ask:           {:?} / {:?}",
        bid.map(|l| (l.price.0, l.size.0)),
        ask.map(|l| (l.price.0, l.size.0))
    );
    println!("    Bids (top 5):           ");
    for level in book.bids.iter().take(5) {
        println!("        {} x {}", level.price.0, level.size.0);
    }
    println!("    Asks (top 5):           ");
    for level in book.asks.iter().take(5) {
        println!("        {} x {}", level.price.0, level.size.0);
    }
    println!("--------------------------------------------------------------------------------");
}

pub(crate) fn print_validation(outcome: &NexValidationOutcome) {
    println!("(d) VALIDATION ORDER");
    println!(
        "    Chain / wallet:         {} / {}",
        outcome.chain_id, outcome.wallet_mode
    );
    println!("    Maker address:          {}", outcome.maker_address);
    println!(
        "    Outcome / asset:        {} / {}",
        outcome.outcome, outcome.asset_id
    );
    println!(
        "    Price x size (micro):   {} x {} (one tick / min size)",
        outcome.price_micro, outcome.size_micro
    );
    println!("    Guards:                 {:?}", outcome.guards);
    println!("    Placement success:      {}", outcome.placement.success);
    println!("    Order ID:               {}", outcome.placement.order_id);
    println!("    Placement status:       {}", outcome.placement.status);
    if !outcome.placement.error_msg.is_empty() {
        println!(
            "    Placement error:        {}",
            outcome.placement.error_msg
        );
    }
    match &outcome.readback {
        Some(record) => {
            println!(
                "    Read-back:              status={} price={} size={} matched={}",
                record.status, record.price, record.original_size, record.size_matched
            );
        }
        None => println!("    Read-back:              <not visible; reconcile via user feed>"),
    }
    println!("    Read-back missing:      {}", outcome.readback_missing);
    if let Some(detail) = &outcome.cancel_detail {
        println!("    Cancel/reconcile:       {detail}");
    }
    println!("    Settlement:             {}", outcome.settlement_note);
    println!("--------------------------------------------------------------------------------");
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
