use reqwest::{Client, StatusCode};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::auth::L2HmacSigner;
use crate::eip712::Order;
use crate::error::ExecutionError;

pub const DEFAULT_CLOB_REST_URL: &str = "https://clob.polymarket.com";
pub const MAX_BATCH_SIZE: usize = 15;

/// Venue trading mode detected from HTTP 503 error responses or health endpoints.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VenueMode {
    Normal,
    CancelOnly,
    PostOnly,
    Disabled,
}

/// Typed CLOB API domain error for structured recovery.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ClobApiError {
    #[error("Post-only order crossed the book")]
    OrderCrossesBook,

    #[error("Order not found: {order_id}")]
    OrderNotFound { order_id: String },

    #[error("Rate limited (429): retry after {retry_after_ms}ms")]
    RateLimited { retry_after_ms: u64 },

    #[error("Venue mode active: {0:?}")]
    VenueModeActive(VenueMode),

    #[error("Circuit breaker tripped: {0}")]
    CircuitBreakerTripped(String),

    #[error("CLOB rejected: {code:?}: {message}")]
    Rejected { code: Option<u16>, message: String },
}

/// Request body format for a single order placement (`POST /order`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlaceOrderPayload {
    #[serde(rename = "deferExec")]
    pub defer_exec: bool,
    pub order: WireOrder,
    #[serde(rename = "orderType")]
    pub order_type: String,
    pub owner: String,
    #[serde(rename = "postOnly")]
    pub post_only: bool,
}

/// Inner wire order serialized inside `PlaceOrderPayload`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WireOrder {
    pub salt: u64,
    pub maker: String,
    pub signer: String,
    #[serde(rename = "tokenId")]
    pub token_id: String,
    #[serde(rename = "makerAmount")]
    pub maker_amount: String,
    #[serde(rename = "takerAmount")]
    pub taker_amount: String,
    pub side: String,
    #[serde(rename = "signatureType")]
    pub signature_type: u8,
    pub signature: String,
    pub timestamp: String,
    pub expiration: String,
    pub metadata: String,
    pub builder: String,
}

impl WireOrder {
    /// Constructs a `WireOrder` from an alloy `Order` and its computed signature hex.
    #[must_use]
    pub fn from_order(order: &Order, signature_hex: String) -> Self {
        let side_str = if order.side == 0 { "BUY" } else { "SELL" };
        let metadata_str = format!("0x{}", hex::encode(order.metadata.as_slice()));
        let builder_str = format!("0x{}", hex::encode(order.builder.as_slice()));

        Self {
            salt: order.salt.to::<u64>(),
            maker: format!("{:#x}", order.maker),
            signer: format!("{:#x}", order.signer),
            token_id: order.tokenId.to_string(),
            maker_amount: order.makerAmount.to_string(),
            taker_amount: order.takerAmount.to_string(),
            side: side_str.to_string(),
            signature_type: order.signatureType,
            signature: signature_hex,
            timestamp: order.timestamp.to_string(),
            expiration: "0".to_string(),
            metadata: metadata_str,
            builder: builder_str,
        }
    }
}

/// Response returned from `POST /order` or elements in `POST /orders`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrderPlacementResponse {
    #[serde(alias = "ok", default)]
    pub success: bool,
    #[serde(alias = "errorMsg", alias = "error_msg", alias = "error", default)]
    pub error_msg: String,
    #[serde(alias = "orderID", alias = "orderId", alias = "order_id", default)]
    pub order_id: String,
    #[serde(default)]
    pub status: String,
    #[serde(alias = "makingAmount", alias = "making_amount", default)]
    pub making_amount: String,
    #[serde(alias = "takingAmount", alias = "taking_amount", default)]
    pub taking_amount: String,
    #[serde(alias = "tradeIDs", alias = "tradeIds", alias = "trade_ids", default)]
    pub trade_ids: Vec<String>,
    #[serde(
        alias = "transactionsHashes",
        alias = "transactionHashes",
        alias = "transactions_hashes",
        default
    )]
    pub transactions_hashes: Vec<String>,
}

/// Response returned from cancellation endpoints (`DELETE /order`, `/orders`, `/cancel-market-orders`, `/cancel-all`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CancelResponse {
    pub canceled: Vec<String>,
    #[serde(default)]
    pub not_canceled: HashMap<String, String>,
}

/// Open order representation from `GET /data/orders`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OpenOrderRecord {
    pub id: String,
    pub status: String,
    pub owner: String,
    pub maker_address: String,
    pub market: String,
    pub asset_id: String,
    pub side: String,
    pub original_size: String,
    pub size_matched: String,
    pub price: String,
    #[serde(default)]
    pub outcome: Option<String>,
    #[serde(default)]
    pub expiration: Option<String>,
    #[serde(default)]
    pub order_type: Option<String>,
    #[serde(default)]
    pub created_at: Option<u64>,
}

/// Response from `GET /data/orders`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OpenOrdersResponse {
    #[serde(default)]
    pub count: usize,
    #[serde(default)]
    pub next_cursor: String,
    pub data: Vec<OpenOrderRecord>,
}

/// Trade maker fill match element inside a trade payload.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TradeMakerFill {
    #[serde(alias = "orderId")]
    pub order_id: String,
    pub owner: String,
    #[serde(default)]
    pub maker_address: Option<String>,
    #[serde(alias = "matchedAmount")]
    pub matched_amount: String,
    pub price: String,
    pub asset_id: String,
    #[serde(default)]
    pub side: String,
}

/// Trade record from `GET /data/trades`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TradeRecord {
    pub id: String,
    #[serde(alias = "takerOrderId", default)]
    pub taker_order_id: String,
    #[serde(default)]
    pub market: String,
    #[serde(alias = "assetId", default)]
    pub asset_id: String,
    pub side: String,
    pub size: String,
    pub price: String,
    pub status: String,
    #[serde(alias = "makerOrders", default, deserialize_with = "null_as_empty_vec")]
    pub maker_orders: Vec<TradeMakerFill>,
    #[serde(alias = "traderSide", default)]
    pub trader_side: Option<String>,
    #[serde(alias = "transactionHash", default)]
    pub transaction_hash: Option<String>,
}

/// Deserializes `null` or a missing array as an empty `Vec` (the user WS sends `"maker_orders": null`).
fn null_as_empty_vec<'de, D, T>(deserializer: D) -> Result<Vec<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Deserialize<'de>,
{
    Ok(Option::<Vec<T>>::deserialize(deserializer)?.unwrap_or_default())
}

/// High-performance keep-alive CLOB REST client.
#[derive(Clone)]
pub struct ClobRestClient {
    base_url: String,
    http: Client,
    signer: L2HmacSigner,
    consecutive_auth_errors: Arc<AtomicU32>,
    last_auth_error_ts: Arc<AtomicU64>,
    circuit_breaker_tripped: Arc<AtomicBool>,
}

impl ClobRestClient {
    /// Creates a new `ClobRestClient` with optimized HTTP keep-alive settings.
    pub fn new(base_url: impl Into<String>, signer: L2HmacSigner) -> Result<Self, ExecutionError> {
        let http = Client::builder()
            .tcp_nodelay(true)
            .connect_timeout(Duration::from_millis(2000))
            .timeout(Duration::from_millis(4000))
            .build()?;

        Ok(Self {
            base_url: base_url.into().trim_end_matches('/').to_string(),
            http,
            signer,
            consecutive_auth_errors: Arc::new(AtomicU32::new(0)),
            last_auth_error_ts: Arc::new(AtomicU64::new(0)),
            circuit_breaker_tripped: Arc::new(AtomicBool::new(false)),
        })
    }

    /// Creates a client reusing a pre-configured `reqwest::Client` (e.g. pinned NEX ingress DNS).
    #[must_use]
    pub fn with_http_client(
        base_url: impl Into<String>,
        signer: L2HmacSigner,
        http: Client,
    ) -> Self {
        Self {
            base_url: base_url.into().trim_end_matches('/').to_string(),
            http,
            signer,
            consecutive_auth_errors: Arc::new(AtomicU32::new(0)),
            last_auth_error_ts: Arc::new(AtomicU64::new(0)),
            circuit_breaker_tripped: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Retrieves one order by ID, including canceled or fully matched orders (`GET /data/order/{id}`).
    ///
    /// # Errors
    /// Returns a 404 `order_not_found` detail when the venue has no record of the ID.
    pub async fn get_order(&self, order_id: &str) -> Result<OpenOrderRecord, ExecutionError> {
        let path = format!("/data/order/{order_id}");
        let headers = self.signer.sign_request_now("GET", &path, None)?;
        let url = format!("{}{path}", self.base_url);
        let req = headers.apply_to_request(self.http.get(&url));
        let resp = req.send().await?;
        let status = resp.status();
        let text = resp.text().await?;

        if status == StatusCode::NOT_FOUND {
            return Err(ExecutionError::Rest {
                status_code: Some(404),
                detail: format!("order_not_found: {order_id}"),
            });
        }
        if !status.is_success() {
            self.record_error_status(status)?;
            return Err(ExecutionError::Rest {
                status_code: Some(status.as_u16()),
                detail: format!("get order request failed: '{text}'"),
            });
        }
        self.record_error_status(status)?;
        serde_json::from_str(&text).map_err(|err| ExecutionError::Rest {
            status_code: Some(status.as_u16()),
            detail: format!("Failed to parse single order '{text}': {err}"),
        })
    }

    /// Checks and updates circuit breaker state on authentication/bad request errors (400/401).
    /// Trips if >= 5 consecutive errors occur within 10 seconds.
    fn record_error_status(&self, status: StatusCode) -> Result<(), ExecutionError> {
        if status == StatusCode::UNAUTHORIZED || status == StatusCode::BAD_REQUEST {
            let now_secs = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or(0);

            let last_ts = self.last_auth_error_ts.swap(now_secs, Ordering::SeqCst);
            let count = if now_secs.saturating_sub(last_ts) > 10 {
                // Reset counter if window expired
                self.consecutive_auth_errors.store(1, Ordering::SeqCst);
                1
            } else {
                self.consecutive_auth_errors.fetch_add(1, Ordering::SeqCst) + 1
            };

            if count >= 5 {
                self.circuit_breaker_tripped.store(true, Ordering::SeqCst);
                return Err(ExecutionError::CircuitBreaker(format!(
                    "Tripped by {count} consecutive {status} errors within 10s"
                )));
            }
        } else if status.is_success() {
            self.consecutive_auth_errors.store(0, Ordering::SeqCst);
        }
        Ok(())
    }

    /// Resets circuit breaker manually.
    pub fn reset_circuit_breaker(&self) {
        self.circuit_breaker_tripped.store(false, Ordering::SeqCst);
        self.consecutive_auth_errors.store(0, Ordering::SeqCst);
    }

    /// Checks whether the circuit breaker is currently tripped.
    #[must_use]
    pub fn is_circuit_breaker_tripped(&self) -> bool {
        self.circuit_breaker_tripped.load(Ordering::SeqCst)
    }

    /// Submits a single order (`POST /order`).
    ///
    /// For passive maker intents: `orderType: "GTC"`, `postOnly: true`, `expiration: "0"`.
    /// For flatten intents: `orderType: "FAK"`, `postOnly: false`, `expiration: "0"`.
    pub async fn post_order(
        &self,
        order: &Order,
        signature_hex: String,
        post_only: bool,
    ) -> Result<OrderPlacementResponse, ExecutionError> {
        if self.is_circuit_breaker_tripped() {
            return Err(ExecutionError::CircuitBreaker(
                "Cannot submit order while circuit breaker is tripped".to_string(),
            ));
        }

        let wire_order = WireOrder::from_order(order, signature_hex);
        let order_type = if post_only { "GTC" } else { "FAK" };

        let payload = PlaceOrderPayload {
            defer_exec: false,
            order: wire_order,
            order_type: order_type.to_string(),
            owner: self.signer.api_key().to_string(),
            post_only,
        };

        let body_str = serde_json::to_string(&payload)?;
        let path = "/order";
        let headers = self
            .signer
            .sign_request_now("POST", path, Some(&body_str))?;

        let url = format!("{}{path}", self.base_url);
        let req = self
            .http
            .post(&url)
            .header("Content-Type", "application/json");
        let req = headers.apply_to_request(req).body(body_str);

        let resp = req.send().await?;
        let status = resp.status();

        if status == StatusCode::TOO_MANY_REQUESTS {
            let retry_after_ms = parse_retry_after(&resp);
            return Err(ExecutionError::Rest {
                status_code: Some(429),
                detail: format!("Rate limited, retry_after={retry_after_ms}ms"),
            });
        }

        if status == StatusCode::SERVICE_UNAVAILABLE {
            return Err(ExecutionError::Rest {
                status_code: Some(503),
                detail: "Venue 503 service unavailable (cancel-only / maintenance)".to_string(),
            });
        }

        let text = resp.text().await?;
        let crossed_book = text.contains("order crosses book");
        if !(status == StatusCode::BAD_REQUEST && crossed_book) {
            self.record_error_status(status)?;
        }

        let parsed: OrderPlacementResponse =
            serde_json::from_str(&text).map_err(|err| ExecutionError::Rest {
                status_code: Some(status.as_u16()),
                detail: format!("Failed to parse response '{text}': {err}"),
            })?;

        // Check for specific cross book condition
        if !parsed.success && (crossed_book || parsed.error_msg.contains("order crosses book")) {
            return Err(ExecutionError::Rest {
                status_code: Some(status.as_u16()),
                detail: "invalid post-only order: order crosses book".to_string(),
            });
        }

        Ok(parsed)
    }

    /// Submits a batch of orders (`POST /orders`, max 15 orders).
    ///
    /// Atomically sends UP and DOWN passive bids to eliminate inter-leg network RTT.
    pub async fn post_orders_batch(
        &self,
        orders_with_sigs: Vec<(&Order, String, bool)>,
    ) -> Result<Vec<OrderPlacementResponse>, ExecutionError> {
        if orders_with_sigs.is_empty() {
            return Ok(Vec::new());
        }
        if orders_with_sigs.len() > MAX_BATCH_SIZE {
            return Err(ExecutionError::Rest {
                status_code: Some(400),
                detail: format!(
                    "Batch size {} exceeds maximum of {MAX_BATCH_SIZE}",
                    orders_with_sigs.len()
                ),
            });
        }
        if self.is_circuit_breaker_tripped() {
            return Err(ExecutionError::CircuitBreaker(
                "Cannot submit batch while circuit breaker is tripped".to_string(),
            ));
        }

        let mut payloads = Vec::with_capacity(orders_with_sigs.len());
        for (order, sig, post_only) in orders_with_sigs {
            let order_type = if post_only { "GTC" } else { "FAK" };
            payloads.push(PlaceOrderPayload {
                defer_exec: false,
                order: WireOrder::from_order(order, sig),
                order_type: order_type.to_string(),
                owner: self.signer.api_key().to_string(),
                post_only,
            });
        }

        let body_str = serde_json::to_string(&payloads)?;
        let path = "/orders";
        let headers = self
            .signer
            .sign_request_now("POST", path, Some(&body_str))?;

        let url = format!("{}{path}", self.base_url);
        let req = self
            .http
            .post(&url)
            .header("Content-Type", "application/json");
        let req = headers.apply_to_request(req).body(body_str);

        let resp = req.send().await?;
        let status = resp.status();

        if status == StatusCode::TOO_MANY_REQUESTS {
            let retry_after_ms = parse_retry_after(&resp);
            return Err(ExecutionError::Rest {
                status_code: Some(429),
                detail: format!("Rate limited on batch: retry_after={retry_after_ms}ms"),
            });
        }

        if status == StatusCode::SERVICE_UNAVAILABLE {
            return Err(ExecutionError::Rest {
                status_code: Some(503),
                detail: "Venue 503 service unavailable on batch (cancel-only / maintenance)"
                    .to_string(),
            });
        }

        let text = resp.text().await?;
        let crossed_book = text.contains("order crosses book");
        if !(status == StatusCode::BAD_REQUEST && crossed_book) {
            self.record_error_status(status)?;
        }

        let responses: Vec<OrderPlacementResponse> =
            serde_json::from_str(&text).map_err(|err| ExecutionError::Rest {
                status_code: Some(status.as_u16()),
                detail: format!("Failed to parse batch response '{text}': {err}"),
            })?;

        Ok(responses)
    }

    /// Cancels a single order by ID (`DELETE /order`).
    ///
    /// If `order_not_found` is returned, returns a structured error to trigger FSM `PendingReconcile`.
    pub async fn cancel_order(&self, order_id: &str) -> Result<CancelResponse, ExecutionError> {
        let path = "/order";
        let body = serde_json::json!({ "orderID": order_id });
        let body_str = serde_json::to_string(&body)?;

        let headers = self
            .signer
            .sign_request_now("DELETE", path, Some(&body_str))?;
        let url = format!("{}{path}", self.base_url);
        let req = self
            .http
            .delete(&url)
            .header("Content-Type", "application/json");
        let req = headers.apply_to_request(req).body(body_str);

        let resp = req.send().await?;
        let status = resp.status();
        let _ = self.record_error_status(status);

        let text = resp.text().await?;

        // Detect order_not_found
        if text.contains("order_not_found") || text.contains("Order not found") {
            return Err(ExecutionError::Rest {
                status_code: Some(status.as_u16()),
                detail: format!("order_not_found: {order_id}"),
            });
        }

        if !status.is_success() {
            return Err(ExecutionError::Rest {
                status_code: Some(status.as_u16()),
                detail: format!("cancel order request failed: '{text}'"),
            });
        }

        let parsed: CancelResponse =
            serde_json::from_str(&text).map_err(|err| ExecutionError::Rest {
                status_code: Some(status.as_u16()),
                detail: format!("Failed to parse cancel response '{text}': {err}"),
            })?;

        Ok(parsed)
    }

    /// Cancels multiple orders by IDs (`DELETE /orders`).
    pub async fn cancel_orders(
        &self,
        order_ids: &[&str],
    ) -> Result<CancelResponse, ExecutionError> {
        let path = "/orders";
        let body_str = serde_json::to_string(&order_ids)?;

        let headers = self
            .signer
            .sign_request_now("DELETE", path, Some(&body_str))?;
        let url = format!("{}{path}", self.base_url);
        let req = self
            .http
            .delete(&url)
            .header("Content-Type", "application/json");
        let req = headers.apply_to_request(req).body(body_str);

        let resp = req.send().await?;
        let status = resp.status();
        let _ = self.record_error_status(status);

        let text = resp.text().await?;
        if !status.is_success() {
            return Err(ExecutionError::Rest {
                status_code: Some(status.as_u16()),
                detail: format!("cancel batch request failed: '{text}'"),
            });
        }

        let parsed: CancelResponse =
            serde_json::from_str(&text).map_err(|err| ExecutionError::Rest {
                status_code: Some(status.as_u16()),
                detail: format!("Failed to parse cancel batch response '{text}': {err}"),
            })?;

        Ok(parsed)
    }

    /// Cancels orders scoped to a specific condition ID (`DELETE /cancel-market-orders`).
    pub async fn cancel_market_orders(
        &self,
        condition_id: &str,
    ) -> Result<CancelResponse, ExecutionError> {
        let path = "/cancel-market-orders";
        let body = serde_json::json!({ "market": condition_id });
        let body_str = serde_json::to_string(&body)?;

        let headers = self
            .signer
            .sign_request_now("DELETE", path, Some(&body_str))?;
        let url = format!("{}{path}", self.base_url);
        let req = self
            .http
            .delete(&url)
            .header("Content-Type", "application/json");
        let req = headers.apply_to_request(req).body(body_str);

        let resp = req.send().await?;
        let status = resp.status();
        let _ = self.record_error_status(status);

        let text = resp.text().await?;
        if !status.is_success() {
            return Err(ExecutionError::Rest {
                status_code: Some(status.as_u16()),
                detail: format!("cancel market request failed: '{text}'"),
            });
        }

        let parsed: CancelResponse =
            serde_json::from_str(&text).map_err(|err| ExecutionError::Rest {
                status_code: Some(status.as_u16()),
                detail: format!("Failed to parse cancel market response '{text}': {err}"),
            })?;

        Ok(parsed)
    }

    /// Cancels all open orders owned by the API key across all markets (`DELETE /cancel-all`).
    /// Works even in venue `cancel-only` mode.
    pub async fn cancel_all(&self) -> Result<CancelResponse, ExecutionError> {
        let path = "/cancel-all";
        let headers = self.signer.sign_request_now("DELETE", path, None)?;

        let url = format!("{}{path}", self.base_url);
        let req = self.http.delete(&url);
        let req = headers.apply_to_request(req);

        let resp = req.send().await?;
        let status = resp.status();
        let _ = self.record_error_status(status);

        let text = resp.text().await?;
        if !status.is_success() {
            return Err(ExecutionError::Rest {
                status_code: Some(status.as_u16()),
                detail: format!("cancel-all request failed: '{text}'"),
            });
        }

        let parsed: CancelResponse =
            serde_json::from_str(&text).map_err(|err| ExecutionError::Rest {
                status_code: Some(status.as_u16()),
                detail: format!("Failed to parse cancel-all response '{text}': {err}"),
            })?;

        Ok(parsed)
    }

    /// Retrieves open resting orders for the authenticated user (`GET /data/orders`), paginating
    /// until the end of the list.
    pub async fn get_open_orders(
        &self,
        market: Option<&str>,
        asset_id: Option<&str>,
    ) -> Result<Vec<OpenOrderRecord>, ExecutionError> {
        let mut all_orders = Vec::new();
        let mut cursor = "MA==".to_string();

        loop {
            let mut query_params = vec![format!("next_cursor={cursor}")];
            if let Some(m) = market {
                query_params.push(format!("market={m}"));
            }
            if let Some(a) = asset_id {
                query_params.push(format!("asset_id={a}"));
            }

            let query_str = format!("?{}", query_params.join("&"));
            let request_path = format!("/data/orders{query_str}");
            let headers = self.signer.sign_request_now("GET", &request_path, None)?;

            let url = format!("{}{request_path}", self.base_url);
            let req = self.http.get(&url);
            let req = headers.apply_to_request(req);

            let resp = req.send().await?;
            let status = resp.status();
            let _ = self.record_error_status(status);

            let text = resp.text().await?;
            if !status.is_success() {
                return Err(ExecutionError::Rest {
                    status_code: Some(status.as_u16()),
                    detail: format!("open orders request failed: '{text}'"),
                });
            }

            let parsed: OpenOrdersResponse =
                serde_json::from_str(&text).map_err(|err| ExecutionError::Rest {
                    status_code: Some(status.as_u16()),
                    detail: format!("Failed to parse open orders '{text}': {err}"),
                })?;

            all_orders.extend(parsed.data);

            if parsed.next_cursor.is_empty()
                || parsed.next_cursor == "LTE="
                || parsed.next_cursor == cursor
            {
                break;
            }
            cursor = parsed.next_cursor;
        }

        Ok(all_orders)
    }

    /// Retrieves filled/matched trades for the authenticated maker (`GET /data/trades`), paginating
    /// until the end of the list.
    pub async fn get_trades(
        &self,
        maker_address: &str,
        market: Option<&str>,
    ) -> Result<Vec<TradeRecord>, ExecutionError> {
        let mut all_trades = Vec::new();
        let mut cursor = "MA==".to_string();

        #[derive(Deserialize)]
        struct TradesEnvelope {
            #[serde(default)]
            next_cursor: String,
            data: Vec<TradeRecord>,
        }

        loop {
            let mut query_params = vec![
                format!("maker_address={maker_address}"),
                format!("next_cursor={cursor}"),
            ];
            if let Some(m) = market {
                query_params.push(format!("market={m}"));
            }

            let query_str = format!("?{}", query_params.join("&"));
            let request_path = format!("/data/trades{query_str}");
            let headers = self.signer.sign_request_now("GET", &request_path, None)?;

            let url = format!("{}{request_path}", self.base_url);
            let req = self.http.get(&url);
            let req = headers.apply_to_request(req);

            let resp = req.send().await?;
            let status = resp.status();
            let _ = self.record_error_status(status);

            let text = resp.text().await?;
            if !status.is_success() {
                return Err(ExecutionError::Rest {
                    status_code: Some(status.as_u16()),
                    detail: format!("trades request failed: '{text}'"),
                });
            }

            // API returns array of trade records or wrapped in data
            if let Ok(records) = serde_json::from_str::<Vec<TradeRecord>>(&text) {
                all_trades.extend(records);
                break;
            }

            let envelope: TradesEnvelope =
                serde_json::from_str(&text).map_err(|err| ExecutionError::Rest {
                    status_code: Some(status.as_u16()),
                    detail: format!("Failed to parse trades '{text}': {err}"),
                })?;

            all_trades.extend(envelope.data);

            if envelope.next_cursor.is_empty()
                || envelope.next_cursor == "LTE="
                || envelope.next_cursor == cursor
            {
                break;
            }
            cursor = envelope.next_cursor;
        }

        Ok(all_trades)
    }
}

/// Helper to parse `Retry-After` header value into milliseconds.
fn parse_retry_after(resp: &reqwest::Response) -> u64 {
    resp.headers()
        .get("Retry-After")
        .and_then(|val| val.to_str().ok())
        .and_then(|s| s.parse::<u64>().ok())
        .map(|secs| secs * 1000)
        .unwrap_or(1000)
}

#[cfg(test)]
mod tests {
    use super::*;
    use core_types::market::{MarketDescriptor, ProtocolVersion, WalletMode};
    use core_types::money::{FeeSchedule, MicroUsdc, SharesMicro};
    use secrecy::SecretString;
    use wiremock::matchers::{header, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn make_test_signer() -> Result<
        (
            L2HmacSigner,
            crate::eip712::OrderSigner,
            crate::eip712::MarketDomain,
        ),
        ExecutionError,
    > {
        let secret = SecretString::new("aGVsbG8td29ybGRfc2VjcmV0".into());
        let l2 = L2HmacSigner::new(
            "0xeE3B778A783510BC833384919F709e3d2fee1624",
            "api-key-1234",
            &secret,
            SecretString::new("synthetic_test_passphrase_fixture".into()),
        )?;

        // No hardcoded private key: use a fresh ephemeral signer so no secret
        // material ever enters version control (GitGuardian gate).
        let order_signer = crate::eip712::OrderSigner::new_ephemeral(WalletMode::Eoa, None)?;

        let market = MarketDescriptor {
            condition_id: "0x1234".to_string(),
            slug: "btc-5m".to_string(),
            question: "Will BTC go up?".to_string(),
            version: ProtocolVersion::PolyV2,
            neg_risk: false,
            asset_id_up: "111".to_string(),
            asset_id_dn: "222".to_string(),
            tick_size_micro: MicroUsdc::from_raw(10_000),
            min_order_size_micro: SharesMicro::from_raw(5_000_000),
            taker_delay_ms: 50,
            fee_schedule: FeeSchedule::btc_5m_default(),
            start_ts_ms: 1000,
            expiry_ts_ms: 2000,
            strike_price_usd: None,
        };
        let domain = crate::eip712::MarketDomain::for_market(&market);

        Ok((l2, order_signer, domain))
    }

    #[tokio::test]
    async fn test_mock_clob_post_order_and_batch() -> Result<(), Box<dyn std::error::Error>> {
        let server = MockServer::start().await;
        let (l2, order_signer, domain) = make_test_signer()?;
        let client = ClobRestClient::new(server.uri(), l2)?;

        let order = order_signer.build_order(12345, "111", 5_200_000, 10_000_000, true, 1000)?;
        let sig = order_signer.sign_order(&order, &domain)?;

        // Mock POST /order
        Mock::given(method("POST"))
            .and(path("/order"))
            .and(header("POLY_API_KEY", "api-key-1234"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "success": true,
                "orderID": "0xorder123",
                "status": "live",
                "makingAmount": "5200000",
                "takingAmount": "10000000"
            })))
            .mount(&server)
            .await;

        let resp = client.post_order(&order, sig.clone(), true).await?;
        assert!(resp.success);
        assert_eq!(resp.order_id, "0xorder123");

        // Mock POST /orders batch
        Mock::given(method("POST"))
            .and(path("/orders"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!([
                {
                    "success": true,
                    "orderID": "0xorder1",
                    "status": "live"
                },
                {
                    "success": true,
                    "orderID": "0xorder2",
                    "status": "live"
                }
            ])))
            .mount(&server)
            .await;

        let batch_resp = client
            .post_orders_batch(vec![(&order, sig.clone(), true), (&order, sig, true)])
            .await?;
        assert_eq!(batch_resp.len(), 2);
        assert_eq!(
            batch_resp.first().map(|r| r.order_id.as_str()),
            Some("0xorder1")
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_mock_clob_cancel_all_and_circuit_breaker()
    -> Result<(), Box<dyn std::error::Error>> {
        let server = MockServer::start().await;
        let (l2, _, _) = make_test_signer()?;
        let client = ClobRestClient::new(server.uri(), l2)?;

        // Mock DELETE /cancel-all
        Mock::given(method("DELETE"))
            .and(path("/cancel-all"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "canceled": ["0x1", "0x2"],
                "not_canceled": {}
            })))
            .mount(&server)
            .await;

        let cancel_res = client.cancel_all().await?;
        assert_eq!(cancel_res.canceled.len(), 2);

        // Test 5 consecutive 401s trip circuit breaker
        Mock::given(method("DELETE"))
            .and(path("/order"))
            .respond_with(ResponseTemplate::new(401).set_body_string("Unauthorized"))
            .mount(&server)
            .await;

        for _ in 0..4 {
            let _ = client.cancel_order("0xbad").await;
            assert!(!client.is_circuit_breaker_tripped());
        }

        // 5th error must trip
        let res5 = client.cancel_order("0xbad").await;
        assert!(res5.is_err());
        assert!(client.is_circuit_breaker_tripped());

        Ok(())
    }

    #[tokio::test]
    async fn test_mock_clob_order_not_found_and_cross_book()
    -> Result<(), Box<dyn std::error::Error>> {
        let server = MockServer::start().await;
        let (l2, order_signer, domain) = make_test_signer()?;
        let client = ClobRestClient::new(server.uri(), l2)?;

        // Mock order_not_found
        Mock::given(method("DELETE"))
            .and(path("/order"))
            .respond_with(ResponseTemplate::new(404).set_body_json(serde_json::json!({
                "error": "order_not_found"
            })))
            .mount(&server)
            .await;

        let cancel_err = client.cancel_order("0xmissing").await;
        match cancel_err {
            Err(e) => assert!(e.to_string().contains("order_not_found")),
            Ok(_) => return Err("Expected cancel_order to fail with order_not_found".into()),
        }

        // Mock order crosses book
        let order = order_signer.build_order(111, "111", 5_200_000, 10_000_000, true, 1000)?;
        let sig = order_signer.sign_order(&order, &domain)?;

        Mock::given(method("POST"))
            .and(path("/order"))
            .respond_with(ResponseTemplate::new(400).set_body_json(serde_json::json!({
                "success": false,
                "errorMsg": "invalid post-only order: order crosses book"
            })))
            .mount(&server)
            .await;

        let post_err = client.post_order(&order, sig, true).await;
        match post_err {
            Err(e) => assert!(e.to_string().contains("order crosses book")),
            Ok(_) => return Err("Expected post_order to fail with crosses book".into()),
        }

        Ok(())
    }

    #[test]
    fn test_wire_order_timestamp_is_millisecond_string() {
        let (_, order_signer, _) = make_test_signer().expect("test signer");
        let ts_ms = 1_700_000_000_123u64;
        let order = order_signer
            .build_order(1, "111", 10_000, 10_000, true, ts_ms)
            .expect("build order");

        assert_eq!(order.timestamp.to_string(), "1700000000123");

        let wire = WireOrder::from_order(&order, "0xsig".to_string());
        let json = serde_json::to_value(&wire).expect("serialize");
        assert_eq!(
            json.get("timestamp").and_then(|v| v.as_str()),
            Some("1700000000123"),
            "CLOB wire timestamp must keep the signed millisecond value as a string"
        );
    }
}
