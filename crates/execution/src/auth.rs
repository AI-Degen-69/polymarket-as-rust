use base64::Engine;
use base64::engine::general_purpose::{STANDARD, URL_SAFE};
use hmac::{Hmac, Mac};
use secrecy::{ExposeSecret, SecretString};
use sha2::Sha256;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::error::ExecutionError;

type HmacSha256 = Hmac<Sha256>;

/// L2 authenticated HTTP headers required by Polymarket CLOB endpoints.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct L2Headers {
    /// Ethereum address associated with the API key (`POLY_ADDRESS`).
    pub poly_address: String,
    /// CLOB API Key UUID (`POLY_API_KEY`).
    pub poly_api_key: String,
    /// API Passphrase string (`POLY_PASSPHRASE`).
    pub poly_passphrase: String,
    /// Unix timestamp in seconds (`POLY_TIMESTAMP`).
    pub poly_timestamp: String,
    /// HMAC-SHA256 signature encoded as urlsafeBase64WithPadding (`POLY_SIGNATURE`).
    pub poly_signature: String,
}

impl L2Headers {
    /// Inserts these L2 headers into a `reqwest::RequestBuilder`.
    pub fn apply_to_request(self, builder: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        builder
            .header("POLY_ADDRESS", self.poly_address)
            .header("POLY_API_KEY", self.poly_api_key)
            .header("POLY_PASSPHRASE", self.poly_passphrase)
            .header("POLY_TIMESTAMP", self.poly_timestamp)
            .header("POLY_SIGNATURE", self.poly_signature)
    }
}

/// Helper to decode base64 API secret, tolerating URL_SAFE with or without padding and STANDARD base64.
fn decode_base64_secret(secret: &str) -> Result<Vec<u8>, ExecutionError> {
    let trimmed = secret.trim();
    if let Ok(bytes) = URL_SAFE.decode(trimmed) {
        return Ok(bytes);
    }
    // Try padding URL_SAFE if length was unpadded
    let rem = trimmed.len() % 4;
    if rem != 0 {
        let missing = 4 - rem;
        let mut padded = trimmed.to_string();
        for _ in 0..missing {
            padded.push('=');
        }
        if let Ok(bytes) = URL_SAFE.decode(&padded) {
            return Ok(bytes);
        }
    }
    // Fallback to STANDARD base64
    STANDARD.decode(trimmed).map_err(ExecutionError::Base64)
}

/// Strips query string from path so e.g. `/data/orders?market=0x123` becomes `/data/orders`.
#[must_use]
pub fn strip_query_string(path: &str) -> &str {
    match path.split_once('?') {
        Some((clean, _)) => clean,
        None => path,
    }
}

/// Computes HMAC-SHA256 signature for Polymarket L2 authentication.
///
/// Message format: `timestamp_secs + UPPER_METHOD + clean_path + (body if present)`
/// Signature encoding: `urlsafeBase64WithPadding`.
pub fn compute_l2_signature(
    secret_bytes: &[u8],
    method: &str,
    path: &str,
    body: Option<&str>,
    timestamp_secs: u64,
) -> Result<String, ExecutionError> {
    let clean_path = strip_query_string(path);
    let upper_method = method.to_ascii_uppercase();
    let body_str = body.unwrap_or("");
    let message = format!("{timestamp_secs}{upper_method}{clean_path}{body_str}");

    let mut mac = HmacSha256::new_from_slice(secret_bytes)
        .map_err(|err| ExecutionError::HmacKey(err.to_string()))?;
    mac.update(message.as_bytes());
    let result = mac.finalize();
    Ok(URL_SAFE.encode(result.into_bytes()))
}

/// Signer holding credentials to produce L2 authentication headers for Polymarket CLOB.
#[derive(Clone)]
pub struct L2HmacSigner {
    address: String,
    api_key: String,
    secret_bytes: Vec<u8>,
    passphrase: SecretString,
}

impl L2HmacSigner {
    /// Creates a dry-run signer with fresh random credentials (no hardcoded secrets).
    ///
    /// Generates 32 random secret bytes (URL-safe base64) and a random 16-byte
    /// hex passphrase. The address/api-key labels are synthetic trace-only
    /// values; signing output verifies locally but never touches the CLOB.
    #[must_use]
    pub fn new_ephemeral_dry_run(address: impl Into<String>, api_key: impl Into<String>) -> Self {
        use rand::RngCore;
        let mut rng = rand::rng();
        let mut secret_raw = [0u8; 32];
        rng.fill_bytes(&mut secret_raw);
        let mut pass_raw = [0u8; 16];
        rng.fill_bytes(&mut pass_raw);
        Self {
            address: address.into(),
            api_key: api_key.into(),
            secret_bytes: secret_raw.to_vec(),
            passphrase: secrecy::SecretString::new(hex::encode(pass_raw).into()),
        }
    }

    /// Creates a new `L2HmacSigner`, validating and decoding the base64 secret.
    pub fn new(
        address: impl Into<String>,
        api_key: impl Into<String>,
        secret: &SecretString,
        passphrase: SecretString,
    ) -> Result<Self, ExecutionError> {
        let secret_bytes = decode_base64_secret(secret.expose_secret())?;
        Ok(Self {
            address: address.into(),
            api_key: api_key.into(),
            secret_bytes,
            passphrase,
        })
    }

    /// Sign an L2 request with an explicit timestamp in seconds.
    pub fn sign_request(
        &self,
        method: &str,
        path: &str,
        body: Option<&str>,
        timestamp_secs: u64,
    ) -> Result<L2Headers, ExecutionError> {
        let signature =
            compute_l2_signature(&self.secret_bytes, method, path, body, timestamp_secs)?;

        Ok(L2Headers {
            poly_address: self.address.clone(),
            poly_api_key: self.api_key.clone(),
            poly_passphrase: self.passphrase.expose_secret().to_string(),
            poly_timestamp: timestamp_secs.to_string(),
            poly_signature: signature,
        })
    }

    /// Sign an L2 request with the current system time in seconds.
    pub fn sign_request_now(
        &self,
        method: &str,
        path: &str,
        body: Option<&str>,
    ) -> Result<L2Headers, ExecutionError> {
        let timestamp_secs = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|err| {
                ExecutionError::Config(format!("System clock before UNIX epoch: {err}"))
            })?
            .as_secs();

        self.sign_request(method, path, body, timestamp_secs)
    }

    /// Returns the signer's address (`POLY_ADDRESS`).
    #[must_use]
    pub fn address(&self) -> &str {
        &self.address
    }

    /// Returns the signer's API key (`POLY_API_KEY`).
    #[must_use]
    pub fn api_key(&self) -> &str {
        &self.api_key
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_url_safe_secret_decoding_with_special_chars() -> Result<(), Box<dyn std::error::Error>>
    {
        // Base64 with '_' and '-' (URL-safe)
        let raw_key = b"polymarket-secret-key-with-bytes";
        let url_safe_b64 = URL_SAFE.encode(raw_key);
        let secret = SecretString::new(url_safe_b64.into());
        let signer = L2HmacSigner::new(
            "0x1234567890123456789012345678901234567890",
            "test-api-key-uuid",
            &secret,
            SecretString::new("test-passphrase".into()),
        )?;

        assert_eq!(signer.secret_bytes, raw_key);
        Ok(())
    }

    #[test]
    fn test_l2_hmac_test_vector_and_query_strip() -> Result<(), Box<dyn std::error::Error>> {
        // Secret encoded in URL-safe base64: "aGVsbG8td29ybGRfc2VjcmV0" ("hello-world_secret")
        let secret = SecretString::new("aGVsbG8td29ybGRfc2VjcmV0".into());
        let signer = L2HmacSigner::new(
            "0xeE3B778A783510BC833384919F709e3d2fee1624",
            "api-key-1234",
            &secret,
            SecretString::new("pass-5678".into()),
        )?;

        let ts = 1700000000;
        let headers = signer.sign_request("post", "/order", Some(r#"{"salt":123}"#), ts)?;

        assert_eq!(
            headers.poly_address,
            "0xeE3B778A783510BC833384919F709e3d2fee1624"
        );
        assert_eq!(headers.poly_api_key, "api-key-1234");
        assert_eq!(headers.poly_passphrase, "pass-5678");
        assert_eq!(headers.poly_timestamp, "1700000000");
        assert!(!headers.poly_signature.is_empty());

        // Verify query stripping: GET /data/orders?market=0x123 must match GET /data/orders
        let h1 = signer.sign_request("GET", "/data/orders?market=0x123", None, ts)?;
        let h2 = signer.sign_request("GET", "/data/orders", None, ts)?;

        assert_eq!(h1.poly_signature, h2.poly_signature);
        Ok(())
    }

    #[test]
    fn test_method_case_insensitivity() -> Result<(), Box<dyn std::error::Error>> {
        let secret = SecretString::new("c2VjcmV0".into());
        let signer = L2HmacSigner::new("0xaddr", "key", &secret, SecretString::new("pass".into()))?;

        let h_lower = signer.sign_request("delete", "/order", None, 100)?;
        let h_upper = signer.sign_request("DELETE", "/order", None, 100)?;
        assert_eq!(h_lower.poly_signature, h_upper.poly_signature);
        Ok(())
    }
}
