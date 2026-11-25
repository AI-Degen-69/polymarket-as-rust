#![cfg_attr(
    test,
    allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        clippy::indexing_slicing
    )
)]

use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use arrow_array::builder::{Float64Builder, ListBuilder};
use arrow_array::{ArrayRef, Float64Array, RecordBatch, StringArray, TimestampMicrosecondArray};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use core_types::{
    BookLevel, FeeSchedule, FinitePosF64, FixedBook, MarketDescriptor, MicroUsdc, Outcome,
    ProtocolVersion, SharesMicro,
};
use historical_data::{
    BookQualityFilter, CanonicalEventKind, CatalogEntry, DataSource, FidelityVerdict,
    IngestionMode, OutcomeMapping, PmdataMarketFiles, QualityFlag, ResolutionSource, SourceFilter,
    canonical_event_matches_filters, convert_price_to_micro, convert_size_to_micro,
    historical_fee_schedule, normalize_live_jsonl, normalize_pmdata_market, read_canonical_jsonl,
    reconcile_live_vs_pmdata, verify_reconstructed_books_for_stream, write_canonical_jsonl,
};
use parquet::arrow::ArrowWriter;

static TEMP_SEQ: AtomicU64 = AtomicU64::new(1);

struct TestTempDir {
    path: PathBuf,
}

impl TestTempDir {
    fn new(prefix: &str) -> Self {
        let id = TEMP_SEQ.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!(
            "poly_as_norm_{prefix}_{}_{}",
            std::process::id(),
            id
        ));
        fs::create_dir_all(&dir).unwrap();
        Self { path: dir }
    }
}

impl Drop for TestTempDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

fn catalog_entry(slug: &str, yes: &str, no: &str) -> CatalogEntry {
    CatalogEntry {
        exchange: "polymarket".to_string(),
        event_slug: slug.to_string(),
        market_slug: slug.to_string(),
        record_start: "2026-10-28".to_string(),
        record_end: "2026-10-28".to_string(),
        market_start_ms: Some(1_793_145_900_000),
        market_end_ms: Some(1_793_146_200_000),
        question: Some("Bitcoin Up or Down - Test".to_string()),
        event_title: Some("BTC 5m".to_string()),
        yes_outcome: Some(yes.to_string()),
        no_outcome: Some(no.to_string()),
        series: Some("updown.5m.btc".to_string()),
    }
}

struct L2RowSpec<'a> {
    slug: &'a str,
    ts_us: i64,
    local_us: i64,
    event_type: &'a str,
    bids: &'a [(f64, f64)],
    asks: &'a [(f64, f64)],
    pc_price: Option<f64>,
    pc_size: Option<f64>,
    pc_side: Option<&'a str>,
    new_tick_str: Option<&'a str>,
    winning: Option<&'a str>,
}

fn write_l2_parquet(path: &Path, rows: &[L2RowSpec<'_>]) {
    let f64_item = Arc::new(Field::new("item", DataType::Float64, true));
    let schema = Arc::new(Schema::new(vec![
        Field::new("market_slug", DataType::Utf8, false),
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            false,
        ),
        Field::new(
            "local_timestamp",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            false,
        ),
        Field::new("event_type", DataType::Utf8, false),
        Field::new("ask_prices", DataType::List(Arc::clone(&f64_item)), true),
        Field::new("ask_sizes", DataType::List(Arc::clone(&f64_item)), true),
        Field::new("bid_prices", DataType::List(Arc::clone(&f64_item)), true),
        Field::new("bid_sizes", DataType::List(f64_item), true),
        Field::new("best_ask", DataType::Float64, true),
        Field::new("best_bid", DataType::Float64, true),
        Field::new("pc_price", DataType::Float64, true),
        Field::new("pc_size", DataType::Float64, true),
        Field::new("pc_side", DataType::Utf8, true),
        Field::new("new_tick_size", DataType::LargeUtf8, true),
        Field::new("winning_outcome", DataType::Utf8, true),
    ]));

    let slugs: Vec<&str> = rows.iter().map(|r| r.slug).collect();
    let ts: Vec<i64> = rows.iter().map(|r| r.ts_us).collect();
    let local_ts: Vec<i64> = rows.iter().map(|r| r.local_us).collect();
    let evs: Vec<&str> = rows.iter().map(|r| r.event_type).collect();
    let best_asks: Vec<Option<f64>> = rows
        .iter()
        .map(|r| r.asks.first().map(|(p, _)| *p))
        .collect();
    let best_bids: Vec<Option<f64>> = rows
        .iter()
        .map(|r| r.bids.first().map(|(p, _)| *p))
        .collect();
    let pc_px: Vec<Option<f64>> = rows.iter().map(|r| r.pc_price).collect();
    let pc_sz: Vec<Option<f64>> = rows.iter().map(|r| r.pc_size).collect();
    let pc_side: Vec<Option<&str>> = rows.iter().map(|r| r.pc_side).collect();
    let tick_s: Vec<Option<&str>> = rows.iter().map(|r| r.new_tick_str).collect();
    let wins: Vec<Option<&str>> = rows.iter().map(|r| r.winning).collect();

    let mut ap_b = ListBuilder::new(Float64Builder::new());
    let mut as_b = ListBuilder::new(Float64Builder::new());
    let mut bp_b = ListBuilder::new(Float64Builder::new());
    let mut bs_b = ListBuilder::new(Float64Builder::new());
    for r in rows {
        if r.event_type == "book" {
            for (p, s) in r.asks {
                ap_b.values().append_value(*p);
                as_b.values().append_value(*s);
            }
            ap_b.append(true);
            as_b.append(true);
            for (p, s) in r.bids {
                bp_b.values().append_value(*p);
                bs_b.values().append_value(*s);
            }
            bp_b.append(true);
            bs_b.append(true);
        } else {
            ap_b.append(false);
            as_b.append(false);
            bp_b.append(false);
            bs_b.append(false);
        }
    }

    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(StringArray::from(slugs)) as ArrayRef,
            Arc::new(TimestampMicrosecondArray::from(ts).with_timezone("UTC")),
            Arc::new(TimestampMicrosecondArray::from(local_ts).with_timezone("UTC")),
            Arc::new(StringArray::from(evs)),
            Arc::new(ap_b.finish()),
            Arc::new(as_b.finish()),
            Arc::new(bp_b.finish()),
            Arc::new(bs_b.finish()),
            Arc::new(Float64Array::from(best_asks)),
            Arc::new(Float64Array::from(best_bids)),
            Arc::new(Float64Array::from(pc_px)),
            Arc::new(Float64Array::from(pc_sz)),
            Arc::new(StringArray::from(pc_side)),
            Arc::new(arrow_array::LargeStringArray::from(tick_s)),
            Arc::new(StringArray::from(wins)),
        ],
    )
    .unwrap();

    let file = File::create(path).unwrap();
    let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
}

struct TradeRowSpec<'a> {
    slug: &'a str,
    ts_us: i64,
    local_us: i64,
    outcome: &'a str,
    price: f64,
    size: f64,
    side: &'a str,
    trade_id: Option<&'a str>,
}

fn write_trades_parquet(path: &Path, rows: &[TradeRowSpec<'_>], include_trade_id_col: bool) {
    let mut fields = vec![
        Field::new("market_slug", DataType::Utf8, false),
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            false,
        ),
        Field::new(
            "local_timestamp",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            false,
        ),
        Field::new("event_type", DataType::Utf8, false),
        Field::new("outcome", DataType::Utf8, false),
        Field::new("price", DataType::Float64, false),
        Field::new("size", DataType::Float64, false),
        Field::new("side", DataType::Utf8, false),
    ];
    let mut arrays: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(
            rows.iter().map(|r| r.slug).collect::<Vec<_>>(),
        )),
        Arc::new(
            TimestampMicrosecondArray::from(rows.iter().map(|r| r.ts_us).collect::<Vec<_>>())
                .with_timezone("UTC"),
        ),
        Arc::new(
            TimestampMicrosecondArray::from(rows.iter().map(|r| r.local_us).collect::<Vec<_>>())
                .with_timezone("UTC"),
        ),
        Arc::new(StringArray::from(vec!["last_trade_price"; rows.len()])),
        Arc::new(StringArray::from(
            rows.iter().map(|r| r.outcome).collect::<Vec<_>>(),
        )),
        Arc::new(Float64Array::from(
            rows.iter().map(|r| r.price).collect::<Vec<_>>(),
        )),
        Arc::new(Float64Array::from(
            rows.iter().map(|r| r.size).collect::<Vec<_>>(),
        )),
        Arc::new(StringArray::from(
            rows.iter().map(|r| r.side).collect::<Vec<_>>(),
        )),
    ];
    if include_trade_id_col {
        fields.push(Field::new("trade_id", DataType::Utf8, true));
        arrays.push(Arc::new(StringArray::from(
            rows.iter().map(|r| r.trade_id).collect::<Vec<_>>(),
        )));
    }
    let schema = Arc::new(Schema::new(fields));
    let batch = RecordBatch::try_new(Arc::clone(&schema), arrays).unwrap();
    let file = File::create(path).unwrap();
    let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
}

#[test]
fn float_micro_conversion_flags_rounding_and_sub_tick() {
    let tick = MicroUsdc(10_000);
    let (p1, f1) = convert_price_to_micro(0.55, tick, false, "test").unwrap();
    assert_eq!(p1, MicroUsdc(550_000));
    assert!(f1.is_empty());

    let (p2, f2) = convert_price_to_micro(0.555, tick, false, "test").unwrap();
    assert_eq!(p2, MicroUsdc(555_000));
    assert!(f2.contains(&QualityFlag::SubTickPrice));

    let (_p3, f3) = convert_price_to_micro(0.555_555_9, MicroUsdc(1), false, "test").unwrap();
    assert!(f3.contains(&QualityFlag::FloatConversionRounding));

    let (s1, sf1) = convert_size_to_micro(3.5, SharesMicro(5_000_000), false, "test").unwrap();
    assert_eq!(s1, SharesMicro(3_500_000));
    assert!(sf1.contains(&QualityFlag::BelowMinOrderSize));

    assert!(convert_price_to_micro(f64::NAN, tick, false, "test").is_err());
    assert!(convert_price_to_micro(1.01, tick, false, "test").is_err());
}

#[test]
fn outcome_mapping_requires_explicit_catalog_and_rejects_ambiguity() {
    assert!(OutcomeMapping::from_catalog(None, "btc-updown-5m-1793145900").is_err());
    let good = catalog_entry("btc-updown-5m-1793145900", "Up", "Down");
    let map = OutcomeMapping::from_catalog(Some(&good), "btc-updown-5m-1793145900").unwrap();
    assert_eq!(map.yes_maps_to, Outcome::Up);
    assert_eq!(map.no_maps_to, Outcome::Down);
    assert_eq!(map.map_pmdata_outcome("yes", "t").unwrap(), Outcome::Up);
    assert_eq!(map.map_pmdata_outcome("no", "t").unwrap(), Outcome::Down);

    let amb = catalog_entry("btc-updown-5m-1793145900", "Up", "Up");
    assert!(OutcomeMapping::from_catalog(Some(&amb), "btc-updown-5m-1793145900").is_err());
}

#[test]
fn normalize_pmdata_preserves_provenance_dedups_trades_and_handles_full_vs_quota_saving() {
    let tmp = TestTempDir::new("norm_modes");
    let slug = "btc-updown-5m-1793145900";
    let t0_us = 1_793_145_900_000_000_i64;
    let yes_path = tmp.path.join("yes_l2.parquet");
    let no_path = tmp.path.join("no_l2.parquet");
    let trades_path = tmp.path.join("trades.parquet");

    write_l2_parquet(
        &yes_path,
        &[
            L2RowSpec {
                slug,
                ts_us: t0_us,
                local_us: t0_us + 2_000,
                event_type: "book",
                bids: &[(0.51, 20.0), (0.50, 30.0)],
                asks: &[(0.53, 25.0), (0.54, 40.0)],
                pc_price: None,
                pc_size: None,
                pc_side: None,
                new_tick_str: None,
                winning: None,
            },
            L2RowSpec {
                slug,
                ts_us: t0_us + 500_000,
                local_us: t0_us + 502_000,
                event_type: "price_change",
                bids: &[],
                asks: &[],
                pc_price: Some(0.52),
                pc_size: Some(15.0),
                pc_side: Some("BUY"),
                new_tick_str: None,
                winning: None,
            },
            L2RowSpec {
                slug,
                ts_us: t0_us + 1_000_000,
                local_us: t0_us + 1_002_000,
                event_type: "tick_size_change",
                bids: &[],
                asks: &[],
                pc_price: None,
                pc_size: None,
                pc_side: None,
                new_tick_str: Some("0.001"),
                winning: None,
            },
        ],
    );
    write_l2_parquet(
        &no_path,
        &[L2RowSpec {
            slug,
            ts_us: t0_us,
            local_us: t0_us + 2_000,
            event_type: "book",
            bids: &[(0.47, 25.0), (0.46, 40.0)],
            asks: &[(0.49, 20.0), (0.50, 30.0)],
            pc_price: None,
            pc_size: None,
            pc_side: None,
            new_tick_str: None,
            winning: None,
        }],
    );
    // Include pre-Oct-12 missing trade_id column (omitted) + duplicate identical row!
    write_trades_parquet(
        &trades_path,
        &[
            TradeRowSpec {
                slug,
                ts_us: t0_us + 600_000,
                local_us: t0_us + 603_000,
                outcome: "yes",
                price: 0.53,
                size: 10.0,
                side: "BUY",
                trade_id: None,
            },
            TradeRowSpec {
                slug,
                ts_us: t0_us + 600_000,
                local_us: t0_us + 603_000,
                outcome: "yes",
                price: 0.53,
                size: 10.0,
                side: "BUY",
                trade_id: None,
            },
        ],
        false,
    );

    let meta = catalog_entry(slug, "Up", "Down");
    let files_full = PmdataMarketFiles {
        market_slug: slug.to_string(),
        yes_l2_path: yes_path.clone(),
        no_l2_path: Some(no_path),
        trades_path: Some(trades_path.clone()),
        onchain_fills_path: None,
    };
    let full = normalize_pmdata_market(
        &files_full,
        IngestionMode::Full,
        Some(&meta),
        Some(Outcome::Up),
    )
    .unwrap();
    assert_eq!(full.summary.duplicate_trades_filtered, 1);
    assert_eq!(full.summary.trade_prints, 1);
    assert_eq!(full.summary.tick_size_changes, 1);
    assert_eq!(full.summary.missing_trade_id_count, 1);
    assert_eq!(full.summary.derived_down_events, 0);
    assert!(full.summary.has_explicit_resolution);
    for ev in &full.events {
        ev.validate_provenance_invariants().unwrap();
        assert_eq!(ev.provenance.source, DataSource::Pmdata);
        assert!(ev.timestamp.mono_ns.is_none());
        assert!(ev.timestamp.clock_sync.is_none());
    }
    assert!(full.events.iter().any(|e| matches!(
        e.kind,
        CanonicalEventKind::MarketResolution {
            winning_outcome: Outcome::Up,
            resolution_source: ResolutionSource::ExternalOverride,
            ..
        }
    )));
    // Fee schedule pre-Nov-20 uses 0.07 rate (`70_000` e6); post-Nov-20 uses 0.05 (`50_000` e6).
    assert_eq!(
        historical_fee_schedule(1_793_145_900_000).fee_rate_e6,
        70_000
    );
    assert_eq!(
        historical_fee_schedule(1_795_300_000_000).fee_rate_e6,
        50_000
    );
    assert_eq!(FeeSchedule::btc_5m_default().fee_rate_e6, 50_000);

    let files_qs = PmdataMarketFiles {
        market_slug: slug.to_string(),
        yes_l2_path: yes_path,
        no_l2_path: None,
        trades_path: Some(trades_path),
        onchain_fills_path: None,
    };
    let qs =
        normalize_pmdata_market(&files_qs, IngestionMode::QuotaSaving, Some(&meta), None).unwrap();
    assert!(qs.summary.derived_down_events >= 2);
    assert!(!qs.summary.has_explicit_resolution);

    // Filter checks: DirectBothRequired rejects ComplementaryFromYesL2 Down events, allows Yes.
    let kept_both: usize = qs
        .events
        .iter()
        .filter(|e| {
            canonical_event_matches_filters(
                e,
                SourceFilter::PmdataOnly,
                BookQualityFilter::DirectBothRequired,
            )
        })
        .count();
    assert!(kept_both < qs.events.len());

    let out_jsonl = tmp.path.join("canonical.jsonl");
    write_canonical_jsonl(&out_jsonl, &qs.events).unwrap();
    let roundtrip = read_canonical_jsonl(&out_jsonl).unwrap();
    assert_eq!(roundtrip, qs.events);
    assert_eq!(
        verify_reconstructed_books_for_stream(&roundtrip).unwrap(),
        2
    );
}

#[test]
fn reconcile_live_vs_pmdata_identifies_equivalent_and_discrepant_streams() {
    let tmp = TestTempDir::new("reconcile_10");
    let slug = "btc-updown-5m-1793145900";
    let start_ms = 1_793_145_900_000_u64;
    let expiry_ms = start_ms + 300_000;
    let t0_us = (start_ms as i64) * 1_000;

    let yes_path = tmp.path.join("yes_l2.parquet");
    let no_path = tmp.path.join("no_l2.parquet");
    let trades_path = tmp.path.join("trades.parquet");
    let live_path = tmp.path.join("live.jsonl");

    write_l2_parquet(
        &yes_path,
        &[
            L2RowSpec {
                slug,
                ts_us: t0_us + 100_000,
                local_us: t0_us + 102_000,
                event_type: "book",
                bids: &[(0.51, 20.0)],
                asks: &[(0.53, 25.0)],
                pc_price: None,
                pc_size: None,
                pc_side: None,
                new_tick_str: None,
                winning: None,
            },
            L2RowSpec {
                slug,
                ts_us: t0_us + 200_000,
                local_us: t0_us + 202_000,
                event_type: "price_change",
                bids: &[],
                asks: &[],
                pc_price: Some(0.52),
                pc_size: Some(15.0),
                pc_side: Some("BUY"),
                new_tick_str: None,
                winning: None,
            },
        ],
    );
    write_l2_parquet(
        &no_path,
        &[L2RowSpec {
            slug,
            ts_us: t0_us + 100_000,
            local_us: t0_us + 102_000,
            event_type: "book",
            bids: &[(0.47, 25.0)],
            asks: &[(0.49, 20.0)],
            pc_price: None,
            pc_size: None,
            pc_side: None,
            new_tick_str: None,
            winning: None,
        }],
    );
    write_trades_parquet(
        &trades_path,
        &[TradeRowSpec {
            slug,
            ts_us: t0_us + 300_000,
            local_us: t0_us + 302_000,
            outcome: "yes",
            price: 0.53,
            size: 10.0,
            side: "BUY",
            trade_id: Some("t1"),
        }],
        true,
    );

    // Build matching live JSONL capture.
    let desc = MarketDescriptor {
        condition_id: "0xcid1".to_string(),
        slug: slug.to_string(),
        question: "BTC Up or Down".to_string(),
        version: ProtocolVersion::CtfV1,
        neg_risk: false,
        asset_id_up: "111".to_string(),
        asset_id_dn: "222".to_string(),
        tick_size_micro: MicroUsdc(10_000),
        min_order_size_micro: SharesMicro(5_000_000),
        taker_delay_ms: 50,
        fee_schedule: FeeSchedule::btc_5m_default(),
        start_ts_ms: start_ms,
        expiry_ts_ms: expiry_ms,
        strike_price_usd: Some(FinitePosF64::try_new(90_000.0).unwrap()),
    };
    let book_up = FixedBook::<10>::try_from_slices(
        Outcome::Up,
        &[BookLevel {
            price: MicroUsdc(510_000),
            size: SharesMicro(20_000_000),
        }],
        &[BookLevel {
            price: MicroUsdc(530_000),
            size: SharesMicro(25_000_000),
        }],
        1_000,
        start_ms + 100,
    )
    .unwrap();
    let book_dn = FixedBook::<10>::try_from_slices(
        Outcome::Down,
        &[BookLevel {
            price: MicroUsdc(470_000),
            size: SharesMicro(25_000_000),
        }],
        &[BookLevel {
            price: MicroUsdc(490_000),
            size: SharesMicro(20_000_000),
        }],
        1_000,
        start_ms + 100,
    )
    .unwrap();

    let mut f = File::create(&live_path).unwrap();
    let write_env = |f: &mut File, ts: u64, mono: u64, ev: &str, payload: serde_json::Value| {
        let line = serde_json::json!({
            "ts_unix_ms": ts,
            "mono_ns": mono,
            "event": ev,
            "payload": payload,
        });
        writeln!(f, "{line}").unwrap();
    };
    write_env(
        &mut f,
        start_ms,
        100,
        "market_discovered",
        serde_json::to_value(&desc).unwrap(),
    );
    write_env(
        &mut f,
        start_ms + 100,
        200,
        "poly_book",
        serde_json::json!({
            "BookSnapshot": {
                "outcome": "Up",
                "book": book_up,
                "rx_mono_ns": 200,
                "exch_ts_ms": start_ms + 100
            }
        }),
    );
    write_env(
        &mut f,
        start_ms + 100,
        250,
        "poly_book",
        serde_json::json!({
            "BookSnapshot": {
                "outcome": "Down",
                "book": book_dn,
                "rx_mono_ns": 250,
                "exch_ts_ms": start_ms + 100
            }
        }),
    );
    write_env(
        &mut f,
        start_ms + 200,
        300,
        "poly_price_change",
        serde_json::json!({
            "PriceChange": {
                "outcome": "Up",
                "side": "Buy",
                "price": 520_000_u64,
                "size": 15_000_000_u64,
                "best_bid": 520_000_u64,
                "best_ask": 530_000_u64,
                "rx_mono_ns": 300,
                "exch_ts_ms": start_ms + 200
            }
        }),
    );
    write_env(
        &mut f,
        start_ms + 300,
        400,
        "poly_trade",
        serde_json::json!({
            "LastTrade": {
                "outcome": "Up",
                "side": "Buy",
                "price": 530_000_u64,
                "size": 10_000_000_u64,
                "rx_mono_ns": 400,
                "exch_ts_ms": start_ms + 300
            }
        }),
    );

    // Check `normalize_live_jsonl` preserves `mono_ns` cleanly.
    let norm_live = normalize_live_jsonl(&live_path).unwrap();
    assert!(norm_live.iter().all(|e| e.timestamp.mono_ns.is_some()));

    let meta = catalog_entry(slug, "Up", "Down");
    let files = PmdataMarketFiles {
        market_slug: slug.to_string(),
        yes_l2_path: yes_path,
        no_l2_path: Some(no_path),
        trades_path: Some(trades_path.clone()),
        onchain_fills_path: None,
    };
    let eq_report =
        reconcile_live_vs_pmdata(&live_path, &files, Some(&meta), IngestionMode::Full).unwrap();
    assert_eq!(eq_report.matched_snapshots, 2);
    assert_eq!(eq_report.matched_price_changes, 1);
    assert_eq!(eq_report.matched_trades, 1);
    assert_eq!(eq_report.snapshot_bbo_mismatches, 0);
    assert_eq!(eq_report.trade_price_mismatches, 0);
    assert_eq!(
        eq_report.verdict,
        FidelityVerdict::CompatibleWithLimitations
    );

    // Discrepant case: PMData misses the trade (`trades` parquet empty/different price) -> NotEquivalent.
    write_trades_parquet(
        &trades_path,
        &[TradeRowSpec {
            slug,
            ts_us: t0_us + 300_000,
            local_us: t0_us + 302_000,
            outcome: "yes",
            price: 0.77,
            size: 10.0,
            side: "BUY",
            trade_id: Some("t_mismatch"),
        }],
        true,
    );
    let diff_report =
        reconcile_live_vs_pmdata(&live_path, &files, Some(&meta), IngestionMode::Full).unwrap();
    assert_eq!(diff_report.verdict, FidelityVerdict::NotEquivalent);
    assert!(diff_report.missing_in_pmdata_trades >= 1 || diff_report.trade_price_mismatches >= 1);
}

#[test]
fn normalizer_enforces_crossed_guard_coalescing_resolution_conflicts_and_phase0_audit() {
    use historical_data::{
        HistoricalPhase0Report, audit_pmdata_historical_market, render_historical_phase0_markdown,
    };

    let tmp = TestTempDir::new("reg_norm");
    let slug = "btc-updown-5m-1793145900";
    let t0_us = 1_793_145_900_000_000_i64;
    let yes_path = tmp.path.join("yes_l2.parquet");
    let no_path = tmp.path.join("no_l2.parquet");
    let trades_path = tmp.path.join("trades.parquet");

    // Duplicate rounded price levels + crossed book + price_change without BBO + market_resolved=Up.
    write_l2_parquet(
        &yes_path,
        &[
            L2RowSpec {
                slug,
                ts_us: t0_us,
                local_us: t0_us + 1_000,
                event_type: "book",
                bids: &[(0.51, 10.0), (0.510_000_1, 15.0)],
                asks: &[(0.53, 25.0)],
                pc_price: None,
                pc_size: None,
                pc_side: None,
                new_tick_str: None,
                winning: None,
            },
            L2RowSpec {
                slug,
                ts_us: t0_us + 100_000,
                local_us: t0_us + 101_000,
                event_type: "book",
                bids: &[(0.55, 10.0)],
                asks: &[(0.54, 10.0)],
                pc_price: None,
                pc_size: None,
                pc_side: None,
                new_tick_str: None,
                winning: None,
            },
            L2RowSpec {
                slug,
                ts_us: t0_us + 200_000,
                local_us: t0_us + 201_000,
                event_type: "price_change",
                bids: &[],
                asks: &[],
                pc_price: Some(0.52),
                pc_size: Some(5.0),
                pc_side: Some("BUY"),
                new_tick_str: None,
                winning: None,
            },
            L2RowSpec {
                slug,
                ts_us: t0_us + 300_000,
                local_us: t0_us + 301_000,
                event_type: "market_resolved",
                bids: &[],
                asks: &[],
                pc_price: None,
                pc_size: None,
                pc_side: None,
                new_tick_str: None,
                winning: Some("Yes"),
            },
        ],
    );
    write_l2_parquet(
        &no_path,
        &[L2RowSpec {
            slug,
            ts_us: t0_us,
            local_us: t0_us + 1_000,
            event_type: "book",
            bids: &[(0.47, 25.0)],
            asks: &[(0.49, 25.0)],
            pc_price: None,
            pc_size: None,
            pc_side: None,
            new_tick_str: None,
            winning: None,
        }],
    );
    write_trades_parquet(&trades_path, &[], true);

    let meta = catalog_entry(slug, "Up", "Down");
    // Missing explicit trades path must error instead of silently returning empty.
    let bad_files = PmdataMarketFiles {
        market_slug: slug.to_string(),
        yes_l2_path: yes_path.clone(),
        no_l2_path: None,
        trades_path: Some(tmp.path.join("does_not_exist.parquet")),
        onchain_fills_path: None,
    };
    assert!(
        normalize_pmdata_market(&bad_files, IngestionMode::QuotaSaving, Some(&meta), None).is_err()
    );

    // Conflicting resolution_override must error.
    let qs_files = PmdataMarketFiles {
        market_slug: slug.to_string(),
        yes_l2_path: yes_path.clone(),
        no_l2_path: None,
        trades_path: Some(trades_path.clone()),
        onchain_fills_path: None,
    };
    assert!(
        normalize_pmdata_market(
            &qs_files,
            IngestionMode::QuotaSaving,
            Some(&meta),
            Some(Outcome::Down)
        )
        .is_err()
    );
    assert!(
        normalize_pmdata_market(
            &qs_files,
            IngestionMode::QuotaSaving,
            Some(&meta),
            Some(Outcome::Up)
        )
        .is_ok()
    );

    // QuotaSaving only derives complementary Down book for the 1 uncrossed snapshot (not crossed snap or missing-BBO pc).
    let qs =
        normalize_pmdata_market(&qs_files, IngestionMode::QuotaSaving, Some(&meta), None).unwrap();
    assert_eq!(qs.summary.derived_down_events, 1);
    assert_eq!(qs.summary.crossed_books_observed, 1);
    assert!(qs.summary.has_explicit_resolution);

    // Duplicate rounded bid levels coalesced to 1 level (`25.0` shares), allowing FixedBook conversion.
    let first_snap = qs
        .events
        .iter()
        .find(|e| matches!(e.kind, CanonicalEventKind::BookSnapshot { .. }))
        .unwrap();
    if let CanonicalEventKind::BookSnapshot { bids, .. } = &first_snap.kind {
        assert_eq!(bids.len(), 1);
        assert_eq!(bids[0].size, SharesMicro(25_000_000));
    }

    // Phase 0 audit & dynamic markdown rendering.
    let full_files = PmdataMarketFiles {
        market_slug: slug.to_string(),
        yes_l2_path: yes_path,
        no_l2_path: Some(no_path),
        trades_path: Some(trades_path),
        onchain_fills_path: None,
    };
    let audit = audit_pmdata_historical_market(&full_files, Some(&meta)).unwrap();
    assert_eq!(audit.yes_no_exact_mirror_snapshots, 1);
    assert!(audit.has_explicit_market_resolved);
    let md = render_historical_phase0_markdown(&HistoricalPhase0Report {
        generated_utc: "2026-11-25T00:00:00Z".to_string(),
        quota_units_consumed: 3,
        verdict: FidelityVerdict::HistoricalSchemaVerifiedOnly,
        disclaimer: "test boundary".to_string(),
        markets: vec![audit],
    });
    assert!(md.contains("Yes/No exact snapshot mirrors=1/1"));
}

#[test]
fn no_rows_resolution_distinct_missing_id_trades_direct_both_validation_and_bipartite_match() {
    let tmp = TestTempDir::new("review_regressions");
    let slug = "btc-updown-5m-1793145900";
    let start_ms = 1_793_145_900_000_u64;
    let t0_us = (start_ms as i64) * 1_000;
    let expiry_us = t0_us + 300_000_000;
    let meta = catalog_entry(slug, "Up", "Down");

    // 1. Resolution only present on `no_rows` as "Yes" (`winning_outcome` label) maps to `Outcome::Up`.
    // 2. Two missing-ID trades with identical `(ts, outcome, price, size)` but distinct `local_timestamp_us` are both retained.
    let yes_path = tmp.path.join("yes_only.parquet");
    let no_path = tmp.path.join("no_resolved.parquet");
    let trades_path = tmp.path.join("no_id_trades.parquet");

    write_l2_parquet(
        &yes_path,
        &[L2RowSpec {
            slug,
            ts_us: t0_us + 1_000_000,
            local_us: t0_us + 1_005_000,
            event_type: "book",
            bids: &[(0.48, 20.0)],
            asks: &[(0.52, 20.0)],
            pc_price: None,
            pc_size: None,
            pc_side: None,
            new_tick_str: None,
            winning: None,
        }],
    );
    write_l2_parquet(
        &no_path,
        &[
            L2RowSpec {
                slug,
                ts_us: t0_us + 1_000_000,
                local_us: t0_us + 1_005_000,
                event_type: "book",
                bids: &[(0.48, 20.0)],
                asks: &[(0.52, 20.0)],
                pc_price: None,
                pc_size: None,
                pc_side: None,
                new_tick_str: None,
                winning: None,
            },
            L2RowSpec {
                slug,
                ts_us: expiry_us + 200_000,
                local_us: expiry_us + 205_000,
                event_type: "market_resolved",
                bids: &[],
                asks: &[],
                pc_price: None,
                pc_size: None,
                pc_side: None,
                new_tick_str: None,
                winning: Some("Yes"),
            },
        ],
    );
    write_trades_parquet(
        &trades_path,
        &[
            TradeRowSpec {
                slug,
                ts_us: t0_us + 2_000_000,
                local_us: t0_us + 2_001_000,
                outcome: "yes",
                price: 0.50,
                size: 5.0,
                side: "buy",
                trade_id: None,
            },
            TradeRowSpec {
                slug,
                ts_us: t0_us + 2_000_000,
                local_us: t0_us + 2_002_000,
                outcome: "yes",
                price: 0.50,
                size: 5.0,
                side: "buy",
                trade_id: None,
            },
        ],
        false,
    );

    let qs_only = PmdataMarketFiles {
        market_slug: slug.to_string(),
        yes_l2_path: yes_path.clone(),
        no_l2_path: None,
        trades_path: Some(trades_path.clone()),
        onchain_fills_path: None,
    };
    let qs_norm =
        normalize_pmdata_market(&qs_only, IngestionMode::QuotaSaving, Some(&meta), None).unwrap();
    let retained_trades = qs_norm
        .events
        .iter()
        .filter(|e| matches!(e.kind, CanonicalEventKind::TradePrint { .. }))
        .count();
    assert_eq!(retained_trades, 2);
    assert!(
        historical_data::validate_canonical_replay_quality(
            &qs_norm.events,
            BookQualityFilter::DirectBothRequired
        )
        .is_err()
    );

    let full_files = PmdataMarketFiles {
        market_slug: slug.to_string(),
        yes_l2_path: yes_path,
        no_l2_path: Some(no_path),
        trades_path: Some(trades_path),
        onchain_fills_path: None,
    };
    let full_norm =
        normalize_pmdata_market(&full_files, IngestionMode::Full, Some(&meta), None).unwrap();
    assert!(full_norm.summary.has_explicit_resolution);
    assert!(full_norm.events.iter().any(|e| matches!(
        e.kind,
        CanonicalEventKind::MarketResolution {
            winning_outcome: Outcome::Up,
            ..
        }
    )));
    assert!(
        historical_data::validate_canonical_replay_quality(
            &full_norm.events,
            BookQualityFilter::DirectBothRequired
        )
        .is_ok()
    );
    let audit = historical_data::audit_pmdata_historical_market(&full_files, Some(&meta)).unwrap();
    assert!(audit.has_explicit_market_resolved);
    assert_eq!(audit.winning_outcome_label.as_deref(), Some("Yes"));

    // Bipartite matching: live trades at 2000ms and 2800ms, pmdata trades at 1100ms and 2000ms (all within 1000ms
    // cross-pairing except 2800 vs 1100) must match BOTH pairs (2000<->1100 and 2800<->2000) rather than greedily
    // matching 2000<->2000 first and leaving 2800 vs 1100 (diff=1700ms > 1000ms) unmatched!
    let bp_yes = tmp.path.join("bp_yes.parquet");
    let bp_no = tmp.path.join("bp_no.parquet");
    let bp_trades = tmp.path.join("bp_trades.parquet");
    let bp_live = tmp.path.join("bp_live.jsonl");
    write_l2_parquet(
        &bp_yes,
        &[L2RowSpec {
            slug,
            ts_us: t0_us + 1_000_000,
            local_us: t0_us + 1_001_000,
            event_type: "book",
            bids: &[(0.48, 20.0)],
            asks: &[(0.52, 20.0)],
            pc_price: None,
            pc_size: None,
            pc_side: None,
            new_tick_str: None,
            winning: None,
        }],
    );
    write_l2_parquet(
        &bp_no,
        &[L2RowSpec {
            slug,
            ts_us: t0_us + 1_000_000,
            local_us: t0_us + 1_001_000,
            event_type: "book",
            bids: &[(0.48, 20.0)],
            asks: &[(0.52, 20.0)],
            pc_price: None,
            pc_size: None,
            pc_side: None,
            new_tick_str: None,
            winning: None,
        }],
    );
    write_trades_parquet(
        &bp_trades,
        &[
            TradeRowSpec {
                slug,
                ts_us: t0_us + 1_100_000,
                local_us: t0_us + 1_101_000,
                outcome: "yes",
                price: 0.50,
                size: 5.0,
                side: "buy",
                trade_id: Some("pm-1"),
            },
            TradeRowSpec {
                slug,
                ts_us: t0_us + 2_000_000,
                local_us: t0_us + 2_001_000,
                outcome: "yes",
                price: 0.50,
                size: 5.0,
                side: "buy",
                trade_id: Some("pm-2"),
            },
        ],
        true,
    );
    let desc = MarketDescriptor {
        slug: slug.to_string(),
        condition_id: "0xcond_bp".to_string(),
        question: "BTC Up or Down".to_string(),
        version: ProtocolVersion::CtfV1,
        neg_risk: false,
        asset_id_up: "0xup".to_string(),
        asset_id_dn: "0xdn".to_string(),
        tick_size_micro: MicroUsdc(10_000),
        min_order_size_micro: SharesMicro(5_000_000),
        taker_delay_ms: 50,
        fee_schedule: FeeSchedule::btc_5m_default(),
        start_ts_ms: start_ms,
        expiry_ts_ms: start_ms + 300_000,
        strike_price_usd: None,
    };
    let book_up = FixedBook::<10>::try_from_slices(
        Outcome::Up,
        &[BookLevel {
            price: MicroUsdc(480_000),
            size: SharesMicro(20_000_000),
        }],
        &[BookLevel {
            price: MicroUsdc(520_000),
            size: SharesMicro(20_000_000),
        }],
        1_000,
        start_ms + 1_000,
    )
    .unwrap();
    let book_dn = FixedBook::<10>::try_from_slices(
        Outcome::Down,
        &[BookLevel {
            price: MicroUsdc(480_000),
            size: SharesMicro(20_000_000),
        }],
        &[BookLevel {
            price: MicroUsdc(520_000),
            size: SharesMicro(20_000_000),
        }],
        1_000,
        start_ms + 1_000,
    )
    .unwrap();
    let live_lines = vec![
        serde_json::json!({
            "ts_unix_ms": start_ms,
            "mono_ns": 100_u64,
            "event": "market_discovered",
            "payload": desc,
        }),
        serde_json::json!({
            "ts_unix_ms": start_ms + 1_000,
            "mono_ns": 1_000_u64,
            "event": "poly_book",
            "payload": {
                "BookSnapshot": {
                    "outcome": "Up",
                    "book": book_up,
                    "rx_mono_ns": 1_000_u64,
                    "exch_ts_ms": start_ms + 1_000,
                }
            }
        }),
        serde_json::json!({
            "ts_unix_ms": start_ms + 1_000,
            "mono_ns": 1_200_u64,
            "event": "poly_book",
            "payload": {
                "BookSnapshot": {
                    "outcome": "Down",
                    "book": book_dn,
                    "rx_mono_ns": 1_200_u64,
                    "exch_ts_ms": start_ms + 1_000,
                }
            }
        }),
        serde_json::json!({
            "ts_unix_ms": start_ms + 2_000,
            "mono_ns": 2_000_u64,
            "event": "poly_trade",
            "payload": {
                "LastTrade": {
                    "outcome": "Up",
                    "side": "Buy",
                    "price": 500_000_u64,
                    "size": 5_000_000_u64,
                    "rx_mono_ns": 2_000_u64,
                    "exch_ts_ms": start_ms + 2_000,
                }
            }
        }),
        serde_json::json!({
            "ts_unix_ms": start_ms + 2_800,
            "mono_ns": 2_800_u64,
            "event": "poly_trade",
            "payload": {
                "LastTrade": {
                    "outcome": "Up",
                    "side": "Buy",
                    "price": 500_000_u64,
                    "size": 5_000_000_u64,
                    "rx_mono_ns": 2_800_u64,
                    "exch_ts_ms": start_ms + 2_800,
                }
            }
        }),
    ];
    let mut lf = File::create(&bp_live).unwrap();
    for line in live_lines {
        writeln!(lf, "{}", serde_json::to_string(&line).unwrap()).unwrap();
    }
    drop(lf);

    let bp_files = PmdataMarketFiles {
        market_slug: slug.to_string(),
        yes_l2_path: bp_yes,
        no_l2_path: Some(bp_no),
        trades_path: Some(bp_trades),
        onchain_fills_path: None,
    };
    let rec =
        reconcile_live_vs_pmdata(&bp_live, &bp_files, Some(&meta), IngestionMode::Full).unwrap();
    assert_eq!(rec.matched_trades, 2);
    assert_eq!(rec.verdict, FidelityVerdict::CompatibleWithLimitations);
}
