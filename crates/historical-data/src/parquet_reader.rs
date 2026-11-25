use std::fs::File;
use std::io::Read;
use std::path::Path;

use arrow_array::{
    Array, ArrayRef, Date32Array, Date64Array, Float32Array, Float64Array, Int32Array, Int64Array,
    LargeListArray, LargeStringArray, ListArray, RecordBatch, StringArray, StringViewArray,
    TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
    TimestampSecondArray, UInt32Array, UInt64Array,
};
use arrow_schema::SchemaRef;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use crate::error::HistoricalError;

/// Summary metadata returned by `verify_parquet_readable`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParquetSummary {
    /// Total rows scanned across all record batches.
    pub row_count: usize,
    /// Schema column names in file order.
    pub columns: Vec<String>,
}

/// Raw row from `https://public-data.pmdata.dev/v1/polymarket/markets.parquet`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CatalogEntry {
    pub exchange: String,
    pub event_slug: String,
    pub market_slug: String,
    /// First UTC date PMData has L2 data (`YYYY-MM-DD`).
    pub record_start: String,
    /// Last UTC date PMData has L2 data (`YYYY-MM-DD`).
    pub record_end: String,
    /// Market start timestamp in Unix milliseconds if available.
    pub market_start_ms: Option<i64>,
    /// Market end timestamp in Unix milliseconds if available.
    pub market_end_ms: Option<i64>,
    pub question: Option<String>,
    pub event_title: Option<String>,
    pub yes_outcome: Option<String>,
    pub no_outcome: Option<String>,
    pub series: Option<String>,
}

/// Raw row from PMData Polymarket `l2` Parquet (`timestamp`, `local_timestamp` preserved in UTC `us`).
#[derive(Debug, Clone, PartialEq)]
pub struct RawPmdataL2Row {
    pub market_slug: String,
    pub timestamp_us: i64,
    pub local_timestamp_us: i64,
    pub event_type: String,
    pub ask_prices: Vec<f64>,
    pub ask_sizes: Vec<f64>,
    pub bid_prices: Vec<f64>,
    pub bid_sizes: Vec<f64>,
    pub best_ask: Option<f64>,
    pub best_bid: Option<f64>,
    pub pc_price: Option<f64>,
    pub pc_size: Option<f64>,
    pub pc_side: Option<String>,
    pub new_tick_size: Option<f64>,
    pub winning_outcome: Option<String>,
}

/// Raw row from PMData Polymarket `trades` Parquet.
#[derive(Debug, Clone, PartialEq)]
pub struct RawPmdataTradeRow {
    pub market_slug: String,
    pub timestamp_us: i64,
    pub local_timestamp_us: i64,
    pub event_type: String,
    pub outcome: String,
    pub price: f64,
    pub size: f64,
    pub side: String,
    pub trade_id: Option<String>,
}

/// Raw row from PMData Polymarket `onchain_fills` Parquet.
#[derive(Debug, Clone, PartialEq)]
pub struct RawPmdataOnchainFillRow {
    pub timestamp_us: i64,
    pub maker: String,
    pub taker: String,
    pub outcome: String,
    pub maker_side: String,
    pub taker_side: String,
    pub price: f64,
    pub usd_amount: f64,
    pub token_amount: f64,
    pub transaction_hash: String,
    pub log_index: Option<i64>,
}

/// Verifies `PAR1` magic bytes, Arrow schema decoding, and full batch readability.
pub fn verify_parquet_readable(path: &Path) -> Result<ParquetSummary, HistoricalError> {
    check_par1_magic(path)?;
    let file = File::open(path).map_err(|source| HistoricalError::Io {
        context: format!("open parquet `{}`", path.display()),
        source,
    })?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file).map_err(|err| {
        HistoricalError::InvalidParquet {
            path: path.to_path_buf(),
            detail: err.to_string(),
        }
    })?;
    let columns: Vec<String> = builder
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect();
    let reader =
        builder
            .with_batch_size(8192)
            .build()
            .map_err(|err| HistoricalError::InvalidParquet {
                path: path.to_path_buf(),
                detail: err.to_string(),
            })?;
    let mut row_count = 0_usize;
    for batch_res in reader {
        let batch = batch_res.map_err(|err| HistoricalError::InvalidParquet {
            path: path.to_path_buf(),
            detail: err.to_string(),
        })?;
        row_count = row_count.saturating_add(batch.num_rows());
    }
    Ok(ParquetSummary { row_count, columns })
}

/// Reads the PMData Polymarket markets catalog Parquet into typed `CatalogEntry` rows.
pub fn read_catalog_parquet(path: &Path) -> Result<Vec<CatalogEntry>, HistoricalError> {
    read_catalog_parquet_filtered(path, None)
}

/// Reads catalog entries matching an optional `series` or slug prefix filter to keep memory small.
pub fn read_catalog_parquet_filtered(
    path: &Path,
    series_or_slug_prefix: Option<&str>,
) -> Result<Vec<CatalogEntry>, HistoricalError> {
    let reader = open_parquet_reader(path)?;
    let mut out = Vec::new();
    for batch_res in reader {
        let batch = batch_res.map_err(|err| HistoricalError::InvalidParquet {
            path: path.to_path_buf(),
            detail: err.to_string(),
        })?;
        parse_catalog_batch(path, &batch, series_or_slug_prefix, &mut out)?;
    }
    Ok(out)
}

/// Reads PMData `l2` Parquet rows preserving exact top-to-bottom file order.
pub fn read_l2_parquet(path: &Path) -> Result<Vec<RawPmdataL2Row>, HistoricalError> {
    let reader = open_parquet_reader(path)?;
    let mut out = Vec::new();
    for batch_res in reader {
        let batch = batch_res.map_err(|err| HistoricalError::InvalidParquet {
            path: path.to_path_buf(),
            detail: err.to_string(),
        })?;
        parse_l2_batch(path, &batch, &mut out)?;
    }
    Ok(out)
}

/// Reads PMData `trades` Parquet rows (`trade_id` is treated as optional for pre-2026-10-12 files).
pub fn read_trades_parquet(path: &Path) -> Result<Vec<RawPmdataTradeRow>, HistoricalError> {
    let reader = open_parquet_reader(path)?;
    let mut out = Vec::new();
    for batch_res in reader {
        let batch = batch_res.map_err(|err| HistoricalError::InvalidParquet {
            path: path.to_path_buf(),
            detail: err.to_string(),
        })?;
        parse_trades_batch(path, &batch, &mut out)?;
    }
    Ok(out)
}

/// Reads PMData `onchain_fills` Parquet rows (`_log_index` is optional).
pub fn read_onchain_fills_parquet(
    path: &Path,
) -> Result<Vec<RawPmdataOnchainFillRow>, HistoricalError> {
    let reader = open_parquet_reader(path)?;
    let mut out = Vec::new();
    for batch_res in reader {
        let batch = batch_res.map_err(|err| HistoricalError::InvalidParquet {
            path: path.to_path_buf(),
            detail: err.to_string(),
        })?;
        parse_onchain_batch(path, &batch, &mut out)?;
    }
    Ok(out)
}

fn check_par1_magic(path: &Path) -> Result<(), HistoricalError> {
    let mut file = File::open(path).map_err(|source| HistoricalError::Io {
        context: format!("open parquet `{}`", path.display()),
        source,
    })?;
    let meta = file.metadata().map_err(|source| HistoricalError::Io {
        context: format!("stat parquet `{}`", path.display()),
        source,
    })?;
    if meta.len() < 12 {
        return Err(HistoricalError::InvalidParquet {
            path: path.to_path_buf(),
            detail: format!("file too small ({} bytes) for Parquet footer", meta.len()),
        });
    }
    let mut header = [0_u8; 4];
    file.read_exact(&mut header)
        .map_err(|source| HistoricalError::Io {
            context: format!("read Parquet header `{}`", path.display()),
            source,
        })?;
    if &header != b"PAR1" {
        return Err(HistoricalError::InvalidParquet {
            path: path.to_path_buf(),
            detail: "missing PAR1 header magic".to_string(),
        });
    }
    Ok(())
}

fn open_parquet_reader(
    path: &Path,
) -> Result<parquet::arrow::arrow_reader::ParquetRecordBatchReader, HistoricalError> {
    check_par1_magic(path)?;
    let file = File::open(path).map_err(|source| HistoricalError::Io {
        context: format!("open parquet `{}`", path.display()),
        source,
    })?;
    ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(|err| HistoricalError::InvalidParquet {
            path: path.to_path_buf(),
            detail: err.to_string(),
        })?
        .with_batch_size(8192)
        .build()
        .map_err(|err| HistoricalError::InvalidParquet {
            path: path.to_path_buf(),
            detail: err.to_string(),
        })
}

fn parse_catalog_batch(
    path: &Path,
    batch: &RecordBatch,
    filter: Option<&str>,
    out: &mut Vec<CatalogEntry>,
) -> Result<(), HistoricalError> {
    let schema = batch.schema();
    let col_exchange = req_col(path, &schema, batch, "exchange")?;
    let col_event_slug = req_col(path, &schema, batch, "event_slug")?;
    let col_market_slug = req_col(path, &schema, batch, "market_slug")?;
    let col_record_start = req_col(path, &schema, batch, "record_start")?;
    let col_record_end = req_col(path, &schema, batch, "record_end")?;
    let col_market_start = opt_col(&schema, batch, "market_start");
    let col_market_end = opt_col(&schema, batch, "market_end");
    let col_question = opt_col(&schema, batch, "question");
    let col_event_title = opt_col(&schema, batch, "event_title");
    let col_yes = opt_col(&schema, batch, "yes_outcome");
    let col_no = opt_col(&schema, batch, "no_outcome");
    let col_series = opt_col(&schema, batch, "series");

    for row in 0..batch.num_rows() {
        let market_slug = get_req_str(path, col_market_slug, "market_slug", row)?;
        let series = col_series.and_then(|c| get_opt_str(path, c, "series", row).ok().flatten());
        if let Some(f) = filter {
            let f_lc = f.to_ascii_lowercase();
            let matches_series = series
                .as_deref()
                .is_some_and(|s| s.to_ascii_lowercase().contains(&f_lc));
            let matches_slug = market_slug.to_ascii_lowercase().contains(&f_lc);
            if !matches_series && !matches_slug {
                continue;
            }
        }
        let rec_start_us = get_req_timestamp_us(path, col_record_start, "record_start", row)?;
        let rec_end_us = get_req_timestamp_us(path, col_record_end, "record_end", row)?;
        out.push(CatalogEntry {
            exchange: get_req_str(path, col_exchange, "exchange", row)?,
            event_slug: get_req_str(path, col_event_slug, "event_slug", row)?,
            market_slug,
            record_start: us_to_utc_date_str(rec_start_us)?,
            record_end: us_to_utc_date_str(rec_end_us)?,
            market_start_ms: match col_market_start {
                Some(c) => get_opt_timestamp_us(path, c, "market_start", row)?
                    .map(|us| us.div_euclid(1_000)),
                None => None,
            },
            market_end_ms: match col_market_end {
                Some(c) => {
                    get_opt_timestamp_us(path, c, "market_end", row)?.map(|us| us.div_euclid(1_000))
                }
                None => None,
            },
            question: match col_question {
                Some(c) => get_opt_str(path, c, "question", row)?,
                None => None,
            },
            event_title: match col_event_title {
                Some(c) => get_opt_str(path, c, "event_title", row)?,
                None => None,
            },
            yes_outcome: match col_yes {
                Some(c) => get_opt_str(path, c, "yes_outcome", row)?,
                None => None,
            },
            no_outcome: match col_no {
                Some(c) => get_opt_str(path, c, "no_outcome", row)?,
                None => None,
            },
            series,
        });
    }
    Ok(())
}

fn parse_l2_batch(
    path: &Path,
    batch: &RecordBatch,
    out: &mut Vec<RawPmdataL2Row>,
) -> Result<(), HistoricalError> {
    let schema = batch.schema();
    let col_slug = req_col(path, &schema, batch, "market_slug")?;
    let col_ts = req_col(path, &schema, batch, "timestamp")?;
    let col_local_ts = req_col(path, &schema, batch, "local_timestamp")?;
    let col_ev = req_col(path, &schema, batch, "event_type")?;
    let col_ask_px = opt_col(&schema, batch, "ask_prices");
    let col_ask_sz = opt_col(&schema, batch, "ask_sizes");
    let col_bid_px = opt_col(&schema, batch, "bid_prices");
    let col_bid_sz = opt_col(&schema, batch, "bid_sizes");
    let col_best_ask = opt_col(&schema, batch, "best_ask");
    let col_best_bid = opt_col(&schema, batch, "best_bid");
    let col_pc_px = opt_col(&schema, batch, "pc_price");
    let col_pc_sz = opt_col(&schema, batch, "pc_size");
    let col_pc_side = opt_col(&schema, batch, "pc_side");
    let col_tick = opt_col(&schema, batch, "new_tick_size");
    let col_win = opt_col(&schema, batch, "winning_outcome");

    for row in 0..batch.num_rows() {
        out.push(RawPmdataL2Row {
            market_slug: get_req_str(path, col_slug, "market_slug", row)?,
            timestamp_us: get_req_timestamp_us(path, col_ts, "timestamp", row)?,
            local_timestamp_us: get_req_timestamp_us(path, col_local_ts, "local_timestamp", row)?,
            event_type: get_req_str(path, col_ev, "event_type", row)?,
            ask_prices: match col_ask_px {
                Some(c) => get_opt_f64_list(path, c, "ask_prices", row)?,
                None => Vec::new(),
            },
            ask_sizes: match col_ask_sz {
                Some(c) => get_opt_f64_list(path, c, "ask_sizes", row)?,
                None => Vec::new(),
            },
            bid_prices: match col_bid_px {
                Some(c) => get_opt_f64_list(path, c, "bid_prices", row)?,
                None => Vec::new(),
            },
            bid_sizes: match col_bid_sz {
                Some(c) => get_opt_f64_list(path, c, "bid_sizes", row)?,
                None => Vec::new(),
            },
            best_ask: match col_best_ask {
                Some(c) => get_opt_f64(path, c, "best_ask", row)?,
                None => None,
            },
            best_bid: match col_best_bid {
                Some(c) => get_opt_f64(path, c, "best_bid", row)?,
                None => None,
            },
            pc_price: match col_pc_px {
                Some(c) => get_opt_f64(path, c, "pc_price", row)?,
                None => None,
            },
            pc_size: match col_pc_sz {
                Some(c) => get_opt_f64(path, c, "pc_size", row)?,
                None => None,
            },
            pc_side: match col_pc_side {
                Some(c) => get_opt_str(path, c, "pc_side", row)?,
                None => None,
            },
            new_tick_size: match col_tick {
                Some(c) => get_opt_f64(path, c, "new_tick_size", row)?,
                None => None,
            },
            winning_outcome: match col_win {
                Some(c) => get_opt_str(path, c, "winning_outcome", row)?,
                None => None,
            },
        });
    }
    Ok(())
}

fn parse_trades_batch(
    path: &Path,
    batch: &RecordBatch,
    out: &mut Vec<RawPmdataTradeRow>,
) -> Result<(), HistoricalError> {
    let schema = batch.schema();
    let col_slug = req_col(path, &schema, batch, "market_slug")?;
    let col_ts = req_col(path, &schema, batch, "timestamp")?;
    let col_local_ts = req_col(path, &schema, batch, "local_timestamp")?;
    let col_ev = req_col(path, &schema, batch, "event_type")?;
    let col_outcome = req_col(path, &schema, batch, "outcome")?;
    let col_price = req_col(path, &schema, batch, "price")?;
    let col_size = req_col(path, &schema, batch, "size")?;
    let col_side = req_col(path, &schema, batch, "side")?;
    // `trade_id` was added on 2026-10-12 and is absent in earlier files.
    let col_trade_id = opt_col(&schema, batch, "trade_id");

    for row in 0..batch.num_rows() {
        out.push(RawPmdataTradeRow {
            market_slug: get_req_str(path, col_slug, "market_slug", row)?,
            timestamp_us: get_req_timestamp_us(path, col_ts, "timestamp", row)?,
            local_timestamp_us: get_req_timestamp_us(path, col_local_ts, "local_timestamp", row)?,
            event_type: get_req_str(path, col_ev, "event_type", row)?,
            outcome: get_req_str(path, col_outcome, "outcome", row)?,
            price: get_req_f64(path, col_price, "price", row)?,
            size: get_req_f64(path, col_size, "size", row)?,
            side: get_req_str(path, col_side, "side", row)?,
            trade_id: match col_trade_id {
                Some(c) => get_opt_str(path, c, "trade_id", row)?,
                None => None,
            },
        });
    }
    Ok(())
}

fn parse_onchain_batch(
    path: &Path,
    batch: &RecordBatch,
    out: &mut Vec<RawPmdataOnchainFillRow>,
) -> Result<(), HistoricalError> {
    let schema = batch.schema();
    let col_ts = req_col(path, &schema, batch, "timestamp")?;
    let col_maker = req_col(path, &schema, batch, "maker")?;
    let col_taker = req_col(path, &schema, batch, "taker")?;
    let col_outcome = req_col(path, &schema, batch, "outcome")?;
    let col_maker_side = req_col(path, &schema, batch, "maker_side")?;
    let col_taker_side = req_col(path, &schema, batch, "taker_side")?;
    let col_price = req_col(path, &schema, batch, "price")?;
    let col_usd = req_col(path, &schema, batch, "usd_amount")?;
    let col_tok = req_col(path, &schema, batch, "token_amount")?;
    let col_tx = req_col(path, &schema, batch, "transactionHash")?;
    let col_log_idx = opt_col(&schema, batch, "_log_index");

    for row in 0..batch.num_rows() {
        out.push(RawPmdataOnchainFillRow {
            timestamp_us: get_req_timestamp_us(path, col_ts, "timestamp", row)?,
            maker: get_req_str(path, col_maker, "maker", row)?,
            taker: get_req_str(path, col_taker, "taker", row)?,
            outcome: get_req_str(path, col_outcome, "outcome", row)?,
            maker_side: get_req_str(path, col_maker_side, "maker_side", row)?,
            taker_side: get_req_str(path, col_taker_side, "taker_side", row)?,
            price: get_req_f64(path, col_price, "price", row)?,
            usd_amount: get_req_f64(path, col_usd, "usd_amount", row)?,
            token_amount: get_req_f64(path, col_tok, "token_amount", row)?,
            transaction_hash: get_req_str(path, col_tx, "transactionHash", row)?,
            log_index: match col_log_idx {
                Some(c) => get_opt_i64(path, c, "_log_index", row)?,
                None => None,
            },
        });
    }
    Ok(())
}

fn req_col<'a>(
    path: &Path,
    schema: &SchemaRef,
    batch: &'a RecordBatch,
    name: &'static str,
) -> Result<&'a ArrayRef, HistoricalError> {
    let idx = schema
        .index_of(name)
        .map_err(|_| HistoricalError::SchemaMismatch {
            path: path.to_path_buf(),
            detail: format!("missing required column `{name}`"),
        })?;
    batch
        .columns()
        .get(idx)
        .ok_or_else(|| HistoricalError::SchemaMismatch {
            path: path.to_path_buf(),
            detail: format!("column index {idx} out of bounds for `{name}`"),
        })
}

fn opt_col<'a>(
    schema: &SchemaRef,
    batch: &'a RecordBatch,
    name: &'static str,
) -> Option<&'a ArrayRef> {
    schema
        .index_of(name)
        .ok()
        .and_then(|idx| batch.columns().get(idx))
}

fn get_opt_str(
    path: &Path,
    col: &ArrayRef,
    name: &'static str,
    row: usize,
) -> Result<Option<String>, HistoricalError> {
    if col.is_null(row) {
        return Ok(None);
    }
    if let Some(arr) = col.as_any().downcast_ref::<StringArray>() {
        return Ok(Some(arr.value(row).to_string()));
    }
    if let Some(arr) = col.as_any().downcast_ref::<LargeStringArray>() {
        return Ok(Some(arr.value(row).to_string()));
    }
    if let Some(arr) = col.as_any().downcast_ref::<StringViewArray>() {
        return Ok(Some(arr.value(row).to_string()));
    }
    Err(HistoricalError::SchemaMismatch {
        path: path.to_path_buf(),
        detail: format!(
            "column `{name}` has unsupported string type {:?}",
            col.data_type()
        ),
    })
}

fn get_req_str(
    path: &Path,
    col: &ArrayRef,
    name: &'static str,
    row: usize,
) -> Result<String, HistoricalError> {
    get_opt_str(path, col, name, row)?.ok_or_else(|| HistoricalError::SchemaMismatch {
        path: path.to_path_buf(),
        detail: format!("required string column `{name}` is null at row {row}"),
    })
}

fn get_opt_f64(
    path: &Path,
    col: &ArrayRef,
    name: &'static str,
    row: usize,
) -> Result<Option<f64>, HistoricalError> {
    if col.is_null(row) {
        return Ok(None);
    }
    if let Some(arr) = col.as_any().downcast_ref::<Float64Array>() {
        return Ok(Some(arr.value(row)));
    }
    if let Some(arr) = col.as_any().downcast_ref::<Float32Array>() {
        return Ok(Some(f64::from(arr.value(row))));
    }
    if let Some(arr) = col.as_any().downcast_ref::<Int64Array>() {
        return Ok(Some(arr.value(row) as f64));
    }
    if let Some(arr) = col.as_any().downcast_ref::<StringArray>() {
        return parse_opt_f64_str(path, name, row, arr.value(row));
    }
    if let Some(arr) = col.as_any().downcast_ref::<LargeStringArray>() {
        return parse_opt_f64_str(path, name, row, arr.value(row));
    }
    if let Some(arr) = col.as_any().downcast_ref::<StringViewArray>() {
        return parse_opt_f64_str(path, name, row, arr.value(row));
    }
    Err(HistoricalError::SchemaMismatch {
        path: path.to_path_buf(),
        detail: format!(
            "column `{name}` has unsupported numeric type {:?}",
            col.data_type()
        ),
    })
}

fn parse_opt_f64_str(
    path: &Path,
    name: &'static str,
    row: usize,
    raw: &str,
) -> Result<Option<f64>, HistoricalError> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }
    let val = trimmed
        .parse::<f64>()
        .map_err(|err| HistoricalError::SchemaMismatch {
            path: path.to_path_buf(),
            detail: format!("column `{name}` invalid float string `{trimmed}` at row {row}: {err}"),
        })?;
    Ok(Some(val))
}

fn get_req_f64(
    path: &Path,
    col: &ArrayRef,
    name: &'static str,
    row: usize,
) -> Result<f64, HistoricalError> {
    get_opt_f64(path, col, name, row)?.ok_or_else(|| HistoricalError::SchemaMismatch {
        path: path.to_path_buf(),
        detail: format!("required float column `{name}` is null at row {row}"),
    })
}

fn get_opt_i64(
    path: &Path,
    col: &ArrayRef,
    name: &'static str,
    row: usize,
) -> Result<Option<i64>, HistoricalError> {
    if col.is_null(row) {
        return Ok(None);
    }
    if let Some(arr) = col.as_any().downcast_ref::<Int64Array>() {
        return Ok(Some(arr.value(row)));
    }
    if let Some(arr) = col.as_any().downcast_ref::<Int32Array>() {
        return Ok(Some(i64::from(arr.value(row))));
    }
    if let Some(arr) = col.as_any().downcast_ref::<UInt64Array>() {
        let v = i64::try_from(arr.value(row)).map_err(|_| HistoricalError::SchemaMismatch {
            path: path.to_path_buf(),
            detail: format!("column `{name}` u64 overflows i64 at row {row}"),
        })?;
        return Ok(Some(v));
    }
    if let Some(arr) = col.as_any().downcast_ref::<UInt32Array>() {
        return Ok(Some(i64::from(arr.value(row))));
    }
    Err(HistoricalError::SchemaMismatch {
        path: path.to_path_buf(),
        detail: format!(
            "column `{name}` has unsupported integer type {:?}",
            col.data_type()
        ),
    })
}

fn get_opt_timestamp_us(
    path: &Path,
    col: &ArrayRef,
    name: &'static str,
    row: usize,
) -> Result<Option<i64>, HistoricalError> {
    if col.is_null(row) {
        return Ok(None);
    }
    if let Some(arr) = col.as_any().downcast_ref::<TimestampMicrosecondArray>() {
        return Ok(Some(arr.value(row)));
    }
    if let Some(arr) = col.as_any().downcast_ref::<TimestampMillisecondArray>() {
        return Ok(Some(arr.value(row).saturating_mul(1_000)));
    }
    if let Some(arr) = col.as_any().downcast_ref::<TimestampSecondArray>() {
        return Ok(Some(arr.value(row).saturating_mul(1_000_000)));
    }
    if let Some(arr) = col.as_any().downcast_ref::<TimestampNanosecondArray>() {
        return Ok(Some(arr.value(row).div_euclid(1_000)));
    }
    if let Some(arr) = col.as_any().downcast_ref::<Date32Array>() {
        return Ok(Some(
            i64::from(arr.value(row)).saturating_mul(86_400_000_000),
        ));
    }
    if let Some(arr) = col.as_any().downcast_ref::<Date64Array>() {
        return Ok(Some(arr.value(row).saturating_mul(1_000)));
    }
    if let Some(arr) = col.as_any().downcast_ref::<Int64Array>() {
        return Ok(Some(arr.value(row)));
    }
    Err(HistoricalError::SchemaMismatch {
        path: path.to_path_buf(),
        detail: format!(
            "column `{name}` has unsupported timestamp type {:?}",
            col.data_type()
        ),
    })
}

fn get_req_timestamp_us(
    path: &Path,
    col: &ArrayRef,
    name: &'static str,
    row: usize,
) -> Result<i64, HistoricalError> {
    get_opt_timestamp_us(path, col, name, row)?.ok_or_else(|| HistoricalError::SchemaMismatch {
        path: path.to_path_buf(),
        detail: format!("required timestamp column `{name}` is null at row {row}"),
    })
}

fn get_opt_f64_list(
    path: &Path,
    col: &ArrayRef,
    name: &'static str,
    row: usize,
) -> Result<Vec<f64>, HistoricalError> {
    if col.is_null(row) {
        return Ok(Vec::new());
    }
    if let Some(list) = col.as_any().downcast_ref::<ListArray>() {
        let slice = list.value(row);
        return extract_f64_slice(path, &slice, name);
    }
    if let Some(list) = col.as_any().downcast_ref::<LargeListArray>() {
        let slice = list.value(row);
        return extract_f64_slice(path, &slice, name);
    }
    Err(HistoricalError::SchemaMismatch {
        path: path.to_path_buf(),
        detail: format!(
            "column `{name}` has unsupported list type {:?}",
            col.data_type()
        ),
    })
}

fn extract_f64_slice(
    path: &Path,
    values: &ArrayRef,
    name: &'static str,
) -> Result<Vec<f64>, HistoricalError> {
    if let Some(arr) = values.as_any().downcast_ref::<Float64Array>() {
        let mut out = Vec::with_capacity(arr.len());
        for i in 0..arr.len() {
            if arr.is_null(i) {
                return Err(HistoricalError::SchemaMismatch {
                    path: path.to_path_buf(),
                    detail: format!("null element inside float list `{name}`"),
                });
            }
            out.push(arr.value(i));
        }
        return Ok(out);
    }
    if let Some(arr) = values.as_any().downcast_ref::<Float32Array>() {
        let mut out = Vec::with_capacity(arr.len());
        for i in 0..arr.len() {
            if arr.is_null(i) {
                return Err(HistoricalError::SchemaMismatch {
                    path: path.to_path_buf(),
                    detail: format!("null element inside float32 list `{name}`"),
                });
            }
            out.push(f64::from(arr.value(i)));
        }
        return Ok(out);
    }
    Err(HistoricalError::SchemaMismatch {
        path: path.to_path_buf(),
        detail: format!(
            "list `{name}` has unsupported element type {:?}",
            values.data_type()
        ),
    })
}

fn us_to_utc_date_str(us: i64) -> Result<String, HistoricalError> {
    let secs = us.div_euclid(1_000_000);
    let dt = chrono::DateTime::from_timestamp(secs, 0).ok_or_else(|| {
        HistoricalError::Normalization {
            context: "catalog_date".to_string(),
            detail: format!("timestamp_us {us} out of UTC date range"),
        }
    })?;
    Ok(dt.format("%Y-%m-%d").to_string())
}
