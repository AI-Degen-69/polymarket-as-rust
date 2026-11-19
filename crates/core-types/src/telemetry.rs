use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};

use hdrhistogram::Histogram;
use serde::Serialize;

use crate::error::TelemetryError;

/// Percentiles snapshot extracted from `LatencyTracker` (values in nanoseconds).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct LatencySummary {
    /// Total recorded samples.
    pub count: u64,
    /// Samples whose elapsed measurement resolved to `0 ns` and were clamped to `1 ns`.
    pub zero_ns_samples: u64,
    /// Minimum recorded nanoseconds (`1` if any `0 ns` samples were clamped).
    pub min_ns: u64,
    /// 50th percentile (median) nanoseconds.
    pub p50_ns: u64,
    /// 95th percentile nanoseconds.
    pub p95_ns: u64,
    /// 99th percentile nanoseconds.
    pub p99_ns: u64,
    /// 99.9th percentile nanoseconds.
    pub p999_ns: u64,
    /// Maximum recorded nanoseconds.
    pub max_ns: u64,
}

/// High-dynamic-range latency histogram tracking `[1 ns .. 60 s]` at 3 significant digits.
pub struct LatencyTracker {
    hist: Histogram<u64>,
    zero_ns_samples: u64,
}

impl fmt::Debug for LatencyTracker {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LatencyTracker")
            .field("count", &self.hist.len())
            .field("zero_ns_samples", &self.zero_ns_samples)
            .field("bounds_ns", &"[1..=60_000_000_000]")
            .finish()
    }
}

impl LatencyTracker {
    /// Allocates a bounded HDR histogram (`1 ns` to `60s`, 3 significant digits).
    pub fn new() -> Result<Self, TelemetryError> {
        let hist = Histogram::<u64>::new_with_bounds(1, 60_000_000_000, 3)
            .map_err(|e| TelemetryError::HistogramInit(e.to_string()))?;
        Ok(Self {
            hist,
            zero_ns_samples: 0,
        })
    }

    /// Records a sample in nanoseconds (`0` tracked in `zero_ns_samples` and clamped to `1ns`).
    pub fn record_ns(&mut self, nanos: u64) -> Result<(), TelemetryError> {
        let bounded = nanos.max(1);
        self.hist
            .record(bounded)
            .map_err(|e| TelemetryError::HistogramRecord(e.to_string()))?;
        if nanos == 0 {
            self.zero_ns_samples = self.zero_ns_samples.saturating_add(1);
        }
        Ok(())
    }

    /// Computes percentile statistics (`min, p50, p95, p99, p999, max`) in nanoseconds.
    #[must_use]
    pub fn summary(&self) -> LatencySummary {
        if self.hist.is_empty() {
            return LatencySummary {
                count: 0,
                zero_ns_samples: 0,
                min_ns: 0,
                p50_ns: 0,
                p95_ns: 0,
                p99_ns: 0,
                p999_ns: 0,
                max_ns: 0,
            };
        }
        LatencySummary {
            count: self.hist.len(),
            zero_ns_samples: self.zero_ns_samples,
            min_ns: self.hist.min(),
            p50_ns: self.hist.value_at_quantile(0.50),
            p95_ns: self.hist.value_at_quantile(0.95),
            p99_ns: self.hist.value_at_quantile(0.99),
            p999_ns: self.hist.value_at_quantile(0.999),
            max_ns: self.hist.max(),
        }
    }
}

#[derive(Serialize)]
struct JsonlEnvelope<'a, T: Serialize> {
    ts_unix_ms: u64,
    mono_ns: u64,
    event: &'a str,
    payload: &'a T,
}

/// Buffered JSON Lines writer outputting atomic newline-delimited telemetry events to disk.
pub struct JsonlWriter {
    path: PathBuf,
    writer: BufWriter<File>,
    line_buf: Vec<u8>,
    dirty: bool,
}

impl fmt::Debug for JsonlWriter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("JsonlWriter")
            .field("path", &self.path)
            .field("dirty", &self.dirty)
            .field("line_buf_cap", &self.line_buf.capacity())
            .finish()
    }
}

impl JsonlWriter {
    /// Creates or truncates a JSONL log file, creating missing parent directories with path context.
    pub fn create(path: &Path) -> Result<Self, TelemetryError> {
        if let Some(parent) = path.parent()
            && !parent.as_os_str().is_empty()
        {
            fs::create_dir_all(parent).map_err(|source| TelemetryError::Io {
                path: parent.to_path_buf(),
                source,
            })?;
        }
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(path)
            .map_err(|source| TelemetryError::Io {
                path: path.to_path_buf(),
                source,
            })?;
        Ok(Self {
            path: path.to_path_buf(),
            writer: BufWriter::with_capacity(64 * 1024, file),
            line_buf: Vec::with_capacity(512),
            dirty: false,
        })
    }

    /// Serializes one JSON record into a reusable line buffer before writing atomically with `\n`.
    pub fn write_event<T: Serialize>(
        &mut self,
        ts_unix_ms: u64,
        mono_ns: u64,
        event: &str,
        payload: &T,
    ) -> Result<(), TelemetryError> {
        let envelope = JsonlEnvelope {
            ts_unix_ms,
            mono_ns,
            event,
            payload,
        };
        self.line_buf.clear();
        serde_json::to_writer(&mut self.line_buf, &envelope)?;
        self.line_buf.push(b'\n');
        self.writer
            .write_all(&self.line_buf)
            .map_err(|source| TelemetryError::Io {
                path: self.path.clone(),
                source,
            })?;
        self.dirty = true;
        Ok(())
    }

    /// Flushes buffered JSONL bytes to disk, propagating any OS I/O error with path context.
    pub fn flush(&mut self) -> Result<(), TelemetryError> {
        self.writer.flush().map_err(|source| TelemetryError::Io {
            path: self.path.clone(),
            source,
        })?;
        self.dirty = false;
        Ok(())
    }

    /// Flushes and closes the JSONL writer explicitly so write errors are never lost on drop.
    pub fn close(mut self) -> Result<(), TelemetryError> {
        let res = self.writer.flush().map_err(|source| TelemetryError::Io {
            path: self.path.clone(),
            source,
        });
        // Caller takes ownership of any flush error here; suppress duplicate flush in `Drop`.
        self.dirty = false;
        res
    }
}

impl Drop for JsonlWriter {
    fn drop(&mut self) {
        if self.dirty {
            if let Err(err) = self.writer.flush() {
                let _ = writeln!(
                    std::io::stderr(),
                    "WARN: failed to flush JSONL telemetry for {} on drop: {err}",
                    self.path.display()
                );
            }
            self.dirty = false;
        }
    }
}
