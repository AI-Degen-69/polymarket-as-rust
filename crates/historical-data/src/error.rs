use std::path::PathBuf;

use core_types::{MathError, WireError};
use thiserror::Error;

/// Typed errors for PMData downloading, persistent quota tracking, Parquet/ZIP validation,
/// canonical normalization, and live-vs-PMData reconciliation.
#[derive(Debug, Error)]
pub enum HistoricalError {
    /// Invalid or missing configuration value.
    #[error("historical pipeline config error ({key}): {detail}")]
    Config {
        /// Config key or invariant name.
        key: &'static str,
        /// Safe error detail (never includes credentials).
        detail: String,
    },

    /// Hard download quota cap (`<= 650`) would be exceeded.
    #[error(
        "quota protection blocked request: needed={needed_units}, used={charged_units}, reserved={reserved_units}, cap={quota_cap}"
    )]
    QuotaExceeded {
        /// Units required by the planned request.
        needed_units: u32,
        /// Already charged units in manifest.
        charged_units: u32,
        /// Currently reserved in-flight units in manifest.
        reserved_units: u32,
        /// Configured hard maximum cap (`<= 650`).
        quota_cap: u32,
    },

    /// Manifest file lock could not be acquired within bounded wait.
    #[error("manifest lock unavailable at `{path}`: {detail}")]
    ManifestLocked {
        /// Lock file path.
        path: PathBuf,
        /// Failure detail.
        detail: String,
    },

    /// Persistent manifest read, parse, or atomic write failure.
    #[error("manifest error at `{path}`: {detail}")]
    ManifestIo {
        /// Manifest path.
        path: PathBuf,
        /// Error detail.
        detail: String,
    },

    /// Network/transport HTTP client error (redacted endpoint, zero credentials).
    #[error("HTTP request failed on {endpoint}: {detail}")]
    Http {
        /// Credential-free endpoint URL.
        endpoint: String,
        /// Sanitized transport error detail.
        detail: String,
    },

    /// Non-success HTTP response status from PMData API.
    #[error("HTTP {status} from {endpoint}: {message}")]
    HttpStatus {
        /// HTTP status code.
        status: u16,
        /// Credential-free endpoint URL.
        endpoint: String,
        /// Sanitized server error message.
        message: String,
    },

    /// Corrupted or unsafe ZIP day bundle archive.
    #[error("invalid ZIP archive `{path}`: {detail}")]
    InvalidZip {
        /// Archive path.
        path: PathBuf,
        /// Integrity or path-safety failure detail.
        detail: String,
    },

    /// Corrupted, unreadable, or empty Parquet file.
    #[error("invalid Parquet file `{path}`: {detail}")]
    InvalidParquet {
        /// Parquet file path.
        path: PathBuf,
        /// Failure detail.
        detail: String,
    },

    /// Parquet schema or column type mismatch.
    #[error("schema mismatch in `{path}`: {detail}")]
    SchemaMismatch {
        /// Parquet file path.
        path: PathBuf,
        /// Schema mismatch detail.
        detail: String,
    },

    /// Canonical normalization or validation error.
    #[error("normalization error ({context}): {detail}")]
    Normalization {
        /// Market slug or normalization stage.
        context: String,
        /// Detail of violation.
        detail: String,
    },

    /// Live-vs-PMData reconciliation error.
    #[error("reconciliation error: {detail}")]
    Reconcile {
        /// Detail of failure.
        detail: String,
    },

    /// Core math invariant error.
    #[error("math invariant error: {0}")]
    Math(#[from] MathError),

    /// Core wire/fee invariant error.
    #[error("wire invariant error: {0}")]
    Wire(#[from] WireError),

    /// Underlying filesystem or JSON serialization error.
    #[error("I/O error on {context}: {source}")]
    Io {
        /// Path or operation label.
        context: String,
        /// OS I/O error.
        #[source]
        source: std::io::Error,
    },

    /// JSON serialization/deserialization error.
    #[error("JSON error in {context}: {source}")]
    Json {
        /// Operation label.
        context: String,
        /// Serde error.
        #[source]
        source: serde_json::Error,
    },
}

/// Sanitizes error strings so response bodies are bounded and never leak credentials.
pub(crate) fn sanitize_snippet(raw: &str, secret: Option<&str>) -> String {
    let mut cleaned = raw.trim().to_string();
    if let Some(sec) = secret
        && !sec.is_empty()
    {
        cleaned = cleaned.replace(sec, "[REDACTED]");
    }
    const MAX_LEN: usize = 280;
    if cleaned.chars().count() <= MAX_LEN {
        cleaned
    } else {
        let mut out: String = cleaned.chars().take(MAX_LEN).collect();
        out.push_str("...");
        out
    }
}
