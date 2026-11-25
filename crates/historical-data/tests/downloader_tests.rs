#![cfg_attr(
    test,
    allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        clippy::indexing_slicing
    )
)]

use std::fs;
use std::io::{Cursor, Write};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use arrow_array::{ArrayRef, RecordBatch, StringArray, TimestampMicrosecondArray};
use arrow_schema::{DataType, Field, Schema};
use chrono::NaiveDate;
use historical_data::{
    DownloadOutcome, EndpointKind, HARD_MAX_QUOTA_CAP, HistoricalError, ManifestStore,
    PmdataClient, PmdataConfig, PmdataDataset, PmdataL2Outcome, QuotaManifest, ReserveMetadata,
    ReserveOutcome, UpDownInterval, bundle_entry_key, read_l2_parquet,
};
use parquet::arrow::ArrowWriter;
use secrecy::SecretString;
use wiremock::matchers::{header, method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};
use zip::write::SimpleFileOptions;
use zip::{CompressionMethod, ZipWriter};

static TEMP_SEQ: AtomicU64 = AtomicU64::new(1);

struct TestTempDir {
    path: PathBuf,
}

impl TestTempDir {
    fn new(prefix: &str) -> Self {
        let id = TEMP_SEQ.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!(
            "poly_as_hist_{prefix}_{}_{}",
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

fn minimal_l2_parquet_bytes(slug: &str) -> Vec<u8> {
    let f64_item = Arc::new(Field::new("item", DataType::Float64, true));
    let schema = Arc::new(Schema::new(vec![
        Field::new("market_slug", DataType::Utf8, false),
        Field::new(
            "timestamp",
            DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, Some("UTC".into())),
            false,
        ),
        Field::new(
            "local_timestamp",
            DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, Some("UTC".into())),
            false,
        ),
        Field::new("event_type", DataType::Utf8, false),
        Field::new("ask_prices", DataType::List(Arc::clone(&f64_item)), true),
        Field::new("ask_sizes", DataType::List(Arc::clone(&f64_item)), true),
        Field::new("bid_prices", DataType::List(Arc::clone(&f64_item)), true),
        Field::new("bid_sizes", DataType::List(f64_item), true),
    ]));

    let mut ask_p_builder =
        arrow_array::builder::ListBuilder::new(arrow_array::builder::Float64Builder::new());
    ask_p_builder.values().append_value(0.55);
    ask_p_builder.append(true);
    let mut ask_s_builder =
        arrow_array::builder::ListBuilder::new(arrow_array::builder::Float64Builder::new());
    ask_s_builder.values().append_value(20.0);
    ask_s_builder.append(true);
    let mut bid_p_builder =
        arrow_array::builder::ListBuilder::new(arrow_array::builder::Float64Builder::new());
    bid_p_builder.values().append_value(0.50);
    bid_p_builder.append(true);
    let mut bid_s_builder =
        arrow_array::builder::ListBuilder::new(arrow_array::builder::Float64Builder::new());
    bid_s_builder.values().append_value(25.0);
    bid_s_builder.append(true);

    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(StringArray::from(vec![slug])) as ArrayRef,
            Arc::new(
                TimestampMicrosecondArray::from(vec![1_793_145_900_000_000_i64])
                    .with_timezone("UTC"),
            ),
            Arc::new(
                TimestampMicrosecondArray::from(vec![1_793_145_900_010_000_i64])
                    .with_timezone("UTC"),
            ),
            Arc::new(StringArray::from(vec!["book"])),
            Arc::new(ask_p_builder.finish()),
            Arc::new(ask_s_builder.finish()),
            Arc::new(bid_p_builder.finish()),
            Arc::new(bid_s_builder.finish()),
        ],
    )
    .unwrap();

    let mut out = Vec::new();
    {
        let mut writer = ArrowWriter::try_new(&mut out, schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }
    out
}

fn zip_bytes_with_entries(entries: &[(&str, Vec<u8>)]) -> Vec<u8> {
    let mut buf = Cursor::new(Vec::new());
    {
        let mut zw = ZipWriter::new(&mut buf);
        let opts = SimpleFileOptions::default().compression_method(CompressionMethod::Deflated);
        for (name, bytes) in entries {
            zw.start_file(name, opts).unwrap();
            zw.write_all(bytes).unwrap();
        }
        zw.finish().unwrap();
    }
    buf.into_inner()
}

fn test_config(tmp: &TestTempDir, base_url: &str, secret: Option<&str>) -> PmdataConfig {
    PmdataConfig {
        api_key: secret.map(|s| SecretString::from(s.to_string())),
        base_url: base_url.to_string(),
        catalog_url: format!("{base_url}/markets.parquet"),
        cache_dir: tmp.path.join("cache"),
        manifest_path: tmp.path.join("manifest.json"),
        quota_cap: HARD_MAX_QUOTA_CAP,
        max_retries: 1,
        request_timeout_secs: 10,
    }
}

#[test]
fn config_enforces_650_cap_and_redacts_api_key_everywhere() {
    let tmp = TestTempDir::new("cfg_redact");
    let secret = "sk-super-secret-do-not-leak-999999";
    let cfg = test_config(&tmp, "http://127.0.0.1:9999", Some(secret));
    let dbg = format!("{cfg:?}");
    assert!(dbg.contains("[REDACTED]"));
    assert!(!dbg.contains(secret));

    let client = PmdataClient::new(cfg.clone()).unwrap();
    let client_dbg = format!("{client:?}");
    assert!(!client_dbg.contains(secret));

    let over_cap = PmdataConfig {
        quota_cap: HARD_MAX_QUOTA_CAP + 1,
        ..cfg
    };
    assert!(over_cap.validate().is_err());

    assert!(QuotaManifest::new(HARD_MAX_QUOTA_CAP + 1).is_err());
    assert!(ManifestStore::new(tmp.path.join("m.json"), HARD_MAX_QUOTA_CAP + 1).is_err());
}

#[test]
fn url_builders_validate_inputs_and_outcome_query_param() {
    let tmp = TestTempDir::new("urls");
    let cfg = test_config(&tmp, "https://api.pmdata.dev/v1", None);
    let client = PmdataClient::new(cfg).unwrap();
    let d = NaiveDate::from_ymd_opt(2026, 10, 28).unwrap();

    assert_eq!(
        client
            .build_market_url(
                PmdataDataset::L2,
                d,
                "btc-updown-5m-1793145900",
                Some(PmdataL2Outcome::Yes)
            )
            .unwrap(),
        "https://api.pmdata.dev/v1/polymarket/l2/2026/10/28/btc-updown-5m-1793145900.parquet"
    );
    assert_eq!(
        client
            .build_market_url(
                PmdataDataset::L2,
                d,
                "btc-updown-5m-1793145900",
                Some(PmdataL2Outcome::No)
            )
            .unwrap(),
        "https://api.pmdata.dev/v1/polymarket/l2/2026/10/28/btc-updown-5m-1793145900.parquet?outcome=No"
    );
    assert!(
        client
            .build_market_url(
                PmdataDataset::Trades,
                d,
                "btc-updown-5m-1793145900",
                Some(PmdataL2Outcome::No)
            )
            .is_err()
    );
    assert!(
        client
            .build_bundle_url(
                PmdataDataset::Trades,
                d,
                "../etc",
                UpDownInterval::M15,
                None
            )
            .is_err()
    );
}

#[tokio::test]
async fn single_market_download_auth_caches_and_prevents_duplicates() {
    let server = MockServer::start().await;
    let tmp = TestTempDir::new("single_cache");
    let secret = "sk-test-key-abc123";
    let cfg = test_config(&tmp, &server.uri(), Some(secret));
    let client = PmdataClient::new(cfg).unwrap();
    let d = NaiveDate::from_ymd_opt(2026, 10, 28).unwrap();
    let slug = "btc-updown-5m-1793145900";
    let pq = minimal_l2_parquet_bytes(slug);

    Mock::given(method("GET"))
        .and(path(
            "/polymarket/l2/2026/10/28/btc-updown-5m-1793145900.parquet",
        ))
        .and(query_param("outcome", "No"))
        .and(header("api_key", secret))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(pq))
        .expect(1)
        .mount(&server)
        .await;

    // Dry run hits 0 times and charges 0 quota.
    let dry = client
        .download_market_file(PmdataDataset::L2, d, slug, Some(PmdataL2Outcome::No), true)
        .await
        .unwrap();
    assert!(matches!(dry, DownloadOutcome::DryRun(p) if p.quota_cost == 1 && !p.already_cached));
    assert_eq!(client.load_manifest().unwrap().charged_units, 0);

    // Real fetch hits once and charges 1 unit.
    let out1 = client
        .download_market_file(PmdataDataset::L2, d, slug, Some(PmdataL2Outcome::No), false)
        .await
        .unwrap();
    let cache_path = match out1 {
        DownloadOutcome::Downloaded { entry, .. } => {
            assert_eq!(entry.quota_units_charged, 1);
            assert_eq!(entry.remaining_quota_after, 649);
            entry.local_cache_path.unwrap()
        }
        other => panic!("expected Downloaded, got {other:?}"),
    };
    let rows = read_l2_parquet(&cache_path).unwrap();
    assert_eq!(rows.len(), 1);

    // Second request uses manifest + SHA256-verified cache with ZERO network calls.
    let out2 = client
        .download_market_file(PmdataDataset::L2, d, slug, Some(PmdataL2Outcome::No), false)
        .await
        .unwrap();
    assert!(matches!(out2, DownloadOutcome::CacheHit(_)));
    assert_eq!(client.load_manifest().unwrap().charged_units, 1);
}

#[tokio::test]
async fn uncharged_http_statuses_release_quota_while_corrupt_200_commits_ambiguous() {
    let server = MockServer::start().await;
    let tmp = TestTempDir::new("http_err");
    let cfg = test_config(&tmp, &server.uri(), Some("sk-secret-xyz"));
    let client = PmdataClient::new(cfg).unwrap();
    let d = NaiveDate::from_ymd_opt(2026, 10, 28).unwrap();

    Mock::given(method("GET"))
        .and(path("/polymarket/trades/2026/10/28/missing.parquet"))
        .respond_with(ResponseTemplate::new(404).set_body_string("not found"))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/polymarket/trades/2026/10/28/blocked.parquet"))
        .respond_with(ResponseTemplate::new(403).set_body_string("plus required"))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/polymarket/trades/2026/10/28/corrupt.parquet"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(b"NOT_PARQUET".to_vec()))
        .mount(&server)
        .await;

    let res_404 = client
        .download_market_file(PmdataDataset::Trades, d, "missing", None, false)
        .await
        .unwrap();
    assert!(
        matches!(res_404, DownloadOutcome::NotFound { entry } if entry.quota_units_charged == 0)
    );
    assert_eq!(client.load_manifest().unwrap().charged_units, 0);

    let err_403 = client
        .download_market_file(PmdataDataset::Trades, d, "blocked", None, false)
        .await
        .unwrap_err();
    assert!(matches!(
        err_403,
        HistoricalError::HttpStatus { status: 403, .. }
    ));
    let m = client.load_manifest().unwrap();
    assert_eq!(m.charged_units, 0);
    assert_eq!(m.reserved_units, 0);

    let err_corrupt = client
        .download_market_file(PmdataDataset::Trades, d, "corrupt", None, false)
        .await
        .unwrap_err();
    assert!(matches!(
        err_corrupt,
        HistoricalError::InvalidParquet { .. }
    ));
    let m2 = client.load_manifest().unwrap();
    assert_eq!(m2.charged_units, 1);
    assert_eq!(m2.reserved_units, 0);
}

#[tokio::test]
async fn day_bundle_zip_extraction_zip_slip_guard_and_range_resume_without_double_charge() {
    let server = MockServer::start().await;
    let tmp = TestTempDir::new("bundle_resume");
    let cfg = test_config(&tmp, &server.uri(), Some("sk-key-1"));
    let client = PmdataClient::new(cfg).unwrap();
    let d = NaiveDate::from_ymd_opt(2026, 10, 28).unwrap();
    let slug = "btc-updown-15m-1793146500";
    let valid_pq = minimal_l2_parquet_bytes(slug);
    let good_zip = zip_bytes_with_entries(&[("btc-updown-15m-1793146500.parquet", valid_pq)]);

    // 1. Seed a `.part` file + `FailedAmbiguous` entry (24 units already charged) and resume via Range.
    let entry_key = bundle_entry_key(
        PmdataDataset::L2,
        d,
        "btc",
        UpDownInterval::M15,
        Some(PmdataL2Outcome::Yes),
    );
    let split = good_zip.len() / 2;
    let cache_path = client.bundle_cache_path(
        PmdataDataset::L2,
        d,
        "btc",
        UpDownInterval::M15,
        Some(PmdataL2Outcome::Yes),
    );
    let part_path = PathBuf::from(format!("{}.part", cache_path.display()));
    fs::create_dir_all(part_path.parent().unwrap()).unwrap();
    fs::write(&part_path, &good_zip[..split]).unwrap();

    client
        .manifest()
        .reserve(ReserveMetadata {
            entry_key: entry_key.clone(),
            endpoint_kind: EndpointKind::DayBundle,
            dataset_type: "l2".to_string(),
            outcome_param: None,
            asset: "btc".to_string(),
            interval: "15m".to_string(),
            utc_date: "2026-10-28".to_string(),
            market_slug: None,
            requested_url: "http://example/zip".to_string(),
            quota_cost: 24,
        })
        .unwrap();
    client
        .manifest()
        .commit_ambiguous_failure(
            &entry_key,
            24,
            Some(200),
            "simulated mid-stream drop".into(),
        )
        .unwrap();

    Mock::given(method("GET"))
        .and(path("/polymarket/l2/2026/10/28/btc-15m.zip"))
        .and(header("range", format!("bytes={split}-").as_str()))
        .respond_with(
            ResponseTemplate::new(206)
                .insert_header(
                    "content-range",
                    format!("bytes {split}-{}/{}", good_zip.len() - 1, good_zip.len()).as_str(),
                )
                .set_body_bytes(good_zip[split..].to_vec()),
        )
        .expect(1)
        .mount(&server)
        .await;

    let resumed = client
        .download_day_bundle(
            PmdataDataset::L2,
            d,
            "btc",
            UpDownInterval::M15,
            Some(PmdataL2Outcome::Yes),
            false,
        )
        .await
        .unwrap();
    match resumed {
        DownloadOutcome::Downloaded {
            entry,
            extracted_files,
        } => {
            assert_eq!(entry.quota_units_charged, 24);
            assert_eq!(extracted_files.len(), 1);
            assert_eq!(read_l2_parquet(&extracted_files[0]).unwrap().len(), 1);
        }
        other => panic!("expected Downloaded, got {other:?}"),
    }
    // Total manifest charge must still be 24 (NOT 48!).
    let m = client.load_manifest().unwrap();
    assert_eq!(m.charged_units, 24);
    assert_eq!(m.reserved_units, 0);

    // 2. Zip-slip malicious path inside archive must fail cleanly.
    let bad_zip = zip_bytes_with_entries(&[("../evil.parquet", b"PAR1bad".to_vec())]);
    Mock::given(method("GET"))
        .and(path("/polymarket/l2/2026/10/28/btc-15m.zip"))
        .and(query_param("outcome", "No"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(bad_zip))
        .mount(&server)
        .await;
    let err = client
        .download_day_bundle(
            PmdataDataset::L2,
            d,
            "btc",
            UpDownInterval::M15,
            Some(PmdataL2Outcome::No),
            false,
        )
        .await
        .unwrap_err();
    assert!(matches!(err, HistoricalError::InvalidZip { .. }));
}

#[test]
fn concurrent_manifest_reserves_respect_lock_and_650_cap() {
    let tmp = TestTempDir::new("concurrent_manifest");
    let store = ManifestStore::new(tmp.path.join("manifest.json"), 10).unwrap();

    let mut handles = Vec::new();
    for i in 0..16_u32 {
        let s = store.clone();
        handles.push(std::thread::spawn(move || {
            let meta = ReserveMetadata {
                entry_key: format!("key:{i}"),
                endpoint_kind: EndpointKind::SingleMarket,
                dataset_type: "trades".to_string(),
                outcome_param: None,
                asset: "btc".to_string(),
                interval: "5m".to_string(),
                utc_date: "2026-10-28".to_string(),
                market_slug: Some(format!("slug-{i}")),
                requested_url: format!("http://example/{i}"),
                quota_cost: 1,
            };
            s.reserve(meta)
                .map(|o| matches!(o, ReserveOutcome::Reserved { .. }))
        }));
    }

    let mut reserved_ok = 0_u32;
    let mut quota_exceeded = 0_u32;
    for h in handles {
        match h.join().unwrap() {
            Ok(true) => reserved_ok += 1,
            Err(HistoricalError::QuotaExceeded { .. }) => quota_exceeded += 1,
            other => panic!("unexpected result: {other:?}"),
        }
    }
    assert_eq!(reserved_ok, 10);
    assert_eq!(quota_exceeded, 6);
    let m = store.load().unwrap();
    assert_eq!(m.reserved_units, 10);
    assert_eq!(m.remaining_quota(), 0);
}

#[tokio::test]
async fn resume_rejects_mismatched_content_range_and_preserves_prior_credit_on_429() {
    let server = MockServer::start().await;
    let tmp = TestTempDir::new("bad_content_range");
    let cfg = test_config(&tmp, &server.uri(), Some("sk-key-cr"));
    let client = PmdataClient::new(cfg).unwrap();
    let d = NaiveDate::from_ymd_opt(2026, 10, 28).unwrap();
    let entry_key = bundle_entry_key(
        PmdataDataset::L2,
        d,
        "btc",
        UpDownInterval::M15,
        Some(PmdataL2Outcome::Yes),
    );
    let cache_path = client.bundle_cache_path(
        PmdataDataset::L2,
        d,
        "btc",
        UpDownInterval::M15,
        Some(PmdataL2Outcome::Yes),
    );
    let part_path = PathBuf::from(format!("{}.part", cache_path.display()));
    fs::create_dir_all(part_path.parent().unwrap()).unwrap();
    fs::write(&part_path, b"PARTIAL_BYTES").unwrap();

    client
        .manifest()
        .reserve(ReserveMetadata {
            entry_key: entry_key.clone(),
            endpoint_kind: EndpointKind::DayBundle,
            dataset_type: "l2".to_string(),
            outcome_param: None,
            asset: "btc".to_string(),
            interval: "15m".to_string(),
            utc_date: "2026-10-28".to_string(),
            market_slug: None,
            requested_url: "http://example/zip".to_string(),
            quota_cost: 24,
        })
        .unwrap();
    client
        .manifest()
        .commit_ambiguous_failure(&entry_key, 24, Some(200), "drop".into())
        .unwrap();

    // First retry hits 429; must release at 0 additional cost AND keep 24-unit credit!
    Mock::given(method("GET"))
        .and(path("/polymarket/l2/2026/10/28/btc-15m.zip"))
        .respond_with(ResponseTemplate::new(429).set_body_string("rate limited"))
        .expect(1)
        .mount(&server)
        .await;
    let err_429 = client
        .download_day_bundle(
            PmdataDataset::L2,
            d,
            "btc",
            UpDownInterval::M15,
            Some(PmdataL2Outcome::Yes),
            false,
        )
        .await
        .unwrap_err();
    assert!(matches!(
        err_429,
        HistoricalError::HttpStatus { status: 429, .. }
    ));

    // Subsequent retry returns 206 with wrong Content-Range start -> rejected without corrupting `.part`.
    let server2 = MockServer::start().await;
    let cfg2 = PmdataConfig {
        base_url: server2.uri(),
        catalog_url: format!("{}/markets.parquet", server2.uri()),
        ..client.config().clone()
    };
    let client2 = PmdataClient::new(cfg2).unwrap();
    Mock::given(method("GET"))
        .and(path("/polymarket/l2/2026/10/28/btc-15m.zip"))
        .respond_with(
            ResponseTemplate::new(206)
                .insert_header("content-range", "bytes 0-5/100")
                .set_body_bytes(b"WRONG!".to_vec()),
        )
        .mount(&server2)
        .await;
    let err_cr = client2
        .download_day_bundle(
            PmdataDataset::L2,
            d,
            "btc",
            UpDownInterval::M15,
            Some(PmdataL2Outcome::Yes),
            false,
        )
        .await
        .unwrap_err();
    assert!(err_cr.to_string().contains("Content-Range"));
    assert_eq!(fs::read(&part_path).unwrap(), b"PARTIAL_BYTES");
    assert_eq!(client2.load_manifest().unwrap().charged_units, 24);
}

#[tokio::test]
async fn partial_200_stream_saves_prefix_to_part_and_resumes_without_double_charge() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    let tmp = TestTempDir::new("partial_stream_resume");
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let zip_payload = zip_bytes_with_entries(&[(
        "polymarket/l2/2026/10/28/btc-15m_yes_0.parquet",
        minimal_l2_parquet_bytes("btc-updown-15m-1793146500"),
    )]);
    let split = zip_payload.len() / 2;
    let first_half = zip_payload[..split].to_vec();
    let total_len = zip_payload.len();

    let accept_task = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut req_buf = [0_u8; 1024];
        let _ = socket.read(&mut req_buf).await.unwrap();
        let headers =
            format!("HTTP/1.1 200 OK\r\nContent-Length: {total_len}\r\nConnection: close\r\n\r\n");
        socket.write_all(headers.as_bytes()).await.unwrap();
        socket.write_all(&first_half).await.unwrap();
        socket.flush().await.unwrap();
        drop(socket);
    });

    let cfg = test_config(&tmp, &format!("http://{addr}"), Some("sk-stream-1"));
    let client = PmdataClient::new(cfg).unwrap();
    let d = NaiveDate::from_ymd_opt(2026, 10, 28).unwrap();
    let err = client
        .download_day_bundle(
            PmdataDataset::L2,
            d,
            "btc",
            UpDownInterval::M15,
            Some(PmdataL2Outcome::Yes),
            false,
        )
        .await
        .unwrap_err();
    accept_task.await.unwrap();
    assert!(!matches!(err, HistoricalError::QuotaExceeded { .. }));

    let cache_path = client.bundle_cache_path(
        PmdataDataset::L2,
        d,
        "btc",
        UpDownInterval::M15,
        Some(PmdataL2Outcome::Yes),
    );
    let part_path = PathBuf::from(format!("{}.part", cache_path.display()));
    assert_eq!(fs::read(&part_path).unwrap(), zip_payload[..split]);
    assert_eq!(client.load_manifest().unwrap().charged_units, 24);

    let server2 = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/polymarket/l2/2026/10/28/btc-15m.zip"))
        .and(header("range", format!("bytes={split}-")))
        .respond_with(
            ResponseTemplate::new(206)
                .insert_header(
                    "content-range",
                    format!("bytes {split}-{}/{total_len}", total_len - 1),
                )
                .set_body_bytes(zip_payload[split..].to_vec()),
        )
        .expect(1)
        .mount(&server2)
        .await;
    let cfg2 = PmdataConfig {
        base_url: server2.uri(),
        catalog_url: format!("{}/markets.parquet", server2.uri()),
        ..client.config().clone()
    };
    let client2 = PmdataClient::new(cfg2).unwrap();
    let plan = client2
        .plan_day_bundles(
            "btc",
            UpDownInterval::M15,
            &[d],
            historical_data::IngestionMode::QuotaSaving,
            false,
        )
        .unwrap();
    assert_eq!(plan.items[0].dataset, PmdataDataset::L2);
    assert_eq!(plan.items[0].quota_cost, 0);

    let outcome = client2
        .download_day_bundle(
            PmdataDataset::L2,
            d,
            "btc",
            UpDownInterval::M15,
            Some(PmdataL2Outcome::Yes),
            false,
        )
        .await
        .unwrap();
    assert!(matches!(outcome, DownloadOutcome::Downloaded { .. }));
    assert_eq!(client2.load_manifest().unwrap().charged_units, 24);
    assert_eq!(fs::read(&cache_path).unwrap(), zip_payload);
}

#[tokio::test]
async fn redirect_is_rejected_without_forwarding_api_key() {
    let primary = MockServer::start().await;
    let evil_target = MockServer::start().await;
    let tmp = TestTempDir::new("reject_redirect");
    let cfg = test_config(&tmp, &primary.uri(), Some("sk-secret-redirect"));
    let client = PmdataClient::new(cfg).unwrap();
    let d = NaiveDate::from_ymd_opt(2026, 10, 28).unwrap();

    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(200))
        .expect(0)
        .mount(&evil_target)
        .await;

    Mock::given(method("GET"))
        .and(path("/polymarket/l2/2026/10/28/btc-15m.zip"))
        .respond_with(
            ResponseTemplate::new(302)
                .insert_header("location", format!("{}/steal", evil_target.uri())),
        )
        .expect(1)
        .mount(&primary)
        .await;

    let err = client
        .download_day_bundle(
            PmdataDataset::L2,
            d,
            "btc",
            UpDownInterval::M15,
            Some(PmdataL2Outcome::Yes),
            false,
        )
        .await
        .unwrap_err();
    assert!(matches!(
        err,
        HistoricalError::HttpStatus { status: 302, .. }
    ));
    assert_eq!(client.load_manifest().unwrap().charged_units, 0);
}
