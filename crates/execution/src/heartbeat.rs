use core_types::market::ExecutionMode;
use reqwest::{Client, StatusCode};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::sync::watch;

use crate::auth::L2HmacSigner;
use crate::clob_rest::ClobRestClient;
use crate::error::ExecutionError;

pub const DEFAULT_HEARTBEAT_INTERVAL: Duration = Duration::from_millis(3500);

/// Heartbeat request body sent to `POST /v1/heartbeats`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct HeartbeatRequest {
    pub heartbeat_id: String,
}

/// Heartbeat response returned by CLOB on success (`200 OK`) or mismatch (`400 Bad Request`).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct HeartbeatResponse {
    #[serde(default)]
    pub heartbeat_id: Option<String>,
    #[serde(alias = "errorMsg", alias = "error", default)]
    pub error_msg: Option<String>,
}

/// Actor managing the 3.5s CLOB order cancellation heartbeat loop.
pub struct OrderHeartbeatActor {
    base_url: String,
    http: Client,
    signer: L2HmacSigner,
    execution_mode: ExecutionMode,
    interval: Duration,
    current_heartbeat_id: String,
    is_running: Arc<AtomicBool>,
}

impl OrderHeartbeatActor {
    /// Creates a new `OrderHeartbeatActor`.
    ///
    /// Safety Latch: `execution_mode` is strictly checked. Heartbeats are ONLY dispatched
    /// over the wire if `execution_mode == ExecutionMode::CanaryLive`.
    pub fn new(
        base_url: impl Into<String>,
        signer: L2HmacSigner,
        execution_mode: ExecutionMode,
    ) -> Result<Self, ExecutionError> {
        let http = Client::builder()
            .tcp_nodelay(true)
            .connect_timeout(Duration::from_millis(1500))
            .timeout(Duration::from_millis(3000))
            .build()?;

        Ok(Self {
            base_url: base_url.into().trim_end_matches('/').to_string(),
            http,
            signer,
            execution_mode,
            interval: DEFAULT_HEARTBEAT_INTERVAL,
            current_heartbeat_id: String::new(),
            is_running: Arc::new(AtomicBool::new(false)),
        })
    }

    /// Sets custom heartbeat interval (default is 3,500 ms).
    pub fn with_interval(mut self, interval: Duration) -> Self {
        self.interval = interval;
        self
    }

    /// Returns the currently active heartbeat ID.
    #[must_use]
    pub fn current_heartbeat_id(&self) -> &str {
        &self.current_heartbeat_id
    }

    /// Returns whether the actor is currently running.
    #[must_use]
    pub fn is_running(&self) -> bool {
        self.is_running.load(Ordering::SeqCst)
    }

    /// Performs one single heartbeat step.
    ///
    /// If server responds with 400 Bad Request containing expected `heartbeat_id`,
    /// immediately retries with the server's expected ID.
    pub async fn send_heartbeat_step(&mut self) -> Result<String, ExecutionError> {
        // Enforce safety latch: only send live heartbeat if in CanaryLive mode
        if self.execution_mode != ExecutionMode::CanaryLive {
            return Ok(self.current_heartbeat_id.clone());
        }

        let path = "/v1/heartbeats";
        let payload = HeartbeatRequest {
            heartbeat_id: self.current_heartbeat_id.clone(),
        };
        let body_str = serde_json::to_string(&payload)?;

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
        let text = resp.text().await?;

        if status == StatusCode::OK {
            let parsed: HeartbeatResponse =
                serde_json::from_str(&text).map_err(|err| ExecutionError::Rest {
                    status_code: Some(status.as_u16()),
                    detail: format!("Failed to parse 200 heartbeat response '{text}': {err}"),
                })?;

            if let Some(new_id) = parsed.heartbeat_id {
                self.current_heartbeat_id = new_id.clone();
                return Ok(new_id);
            }
            return Err(ExecutionError::Rest {
                status_code: Some(status.as_u16()),
                detail: format!("Heartbeat 200 OK did not contain heartbeat_id: '{text}'"),
            });
        }

        if status == StatusCode::BAD_REQUEST {
            // Check for Invalid Heartbeat ID recovery payload
            if let Ok(parsed) = serde_json::from_str::<HeartbeatResponse>(&text)
                && let Some(expected_id) = parsed.heartbeat_id
            {
                // Update heartbeat_id to expected value and immediately retry
                self.current_heartbeat_id = expected_id.clone();

                let retry_payload = HeartbeatRequest {
                    heartbeat_id: expected_id,
                };
                let retry_body = serde_json::to_string(&retry_payload)?;
                let retry_headers =
                    self.signer
                        .sign_request_now("POST", path, Some(&retry_body))?;

                let retry_req = self
                    .http
                    .post(&url)
                    .header("Content-Type", "application/json");
                let retry_req = retry_headers.apply_to_request(retry_req).body(retry_body);

                let retry_resp = retry_req.send().await?;
                let retry_status = retry_resp.status();
                let retry_text = retry_resp.text().await?;

                if retry_status == StatusCode::OK {
                    let retry_parsed: HeartbeatResponse = serde_json::from_str(&retry_text)
                        .map_err(|err| ExecutionError::Rest {
                            status_code: Some(retry_status.as_u16()),
                            detail: format!("Failed to parse retry response '{retry_text}': {err}"),
                        })?;

                    if let Some(new_id) = retry_parsed.heartbeat_id {
                        self.current_heartbeat_id = new_id.clone();
                        return Ok(new_id);
                    }
                }

                return Err(ExecutionError::Rest {
                    status_code: Some(retry_status.as_u16()),
                    detail: format!("Heartbeat 400 recovery retry failed: '{retry_text}'"),
                });
            }
        }

        Err(ExecutionError::Rest {
            status_code: Some(status.as_u16()),
            detail: format!("Heartbeat failed ({status}): '{text}'"),
        })
    }

    /// Spawns the background heartbeat loop, running every `interval` until `shutdown_rx` triggers.
    pub fn spawn_loop(
        mut self,
        mut shutdown_rx: watch::Receiver<bool>,
    ) -> tokio::task::JoinHandle<Result<String, ExecutionError>> {
        self.is_running.store(true, Ordering::SeqCst);
        let is_running = self.is_running.clone();

        tokio::spawn(async move {
            if *shutdown_rx.borrow() {
                is_running.store(false, Ordering::SeqCst);
                return Ok(self.current_heartbeat_id);
            }

            let mut ticker = tokio::time::interval(self.interval);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

            loop {
                tokio::select! {
                    res = shutdown_rx.changed() => {
                        if res.is_err() || *shutdown_rx.borrow() {
                            break;
                        }
                    }
                    _ = ticker.tick() => {
                        if let Err(err) = self.send_heartbeat_step().await {
                            // If circuit breaker or network fails, log and exit loop so timeout cancellation can handle
                            is_running.store(false, Ordering::SeqCst);
                            return Err(err);
                        }
                    }
                }
            }

            is_running.store(false, Ordering::SeqCst);
            Ok(self.current_heartbeat_id)
        })
    }
}

/// Executes clean shutdown order cancellation sequence:
/// 1. Issues `DELETE /cancel-all`
/// 2. Queries `GET /data/orders` confirming all resting orders are cleared.
pub async fn execute_shutdown_cancel(clob_client: &ClobRestClient) -> Result<(), ExecutionError> {
    let _ = clob_client.cancel_all().await?;

    for _ in 0..5 {
        let open_orders = clob_client.get_open_orders(None, None).await?;
        if open_orders.is_empty() {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
        let _ = clob_client.cancel_all().await;
    }

    let remaining = clob_client.get_open_orders(None, None).await?;
    if !remaining.is_empty() {
        return Err(ExecutionError::Rest {
            status_code: None,
            detail: format!(
                "Shutdown cancel verification failed: {} orders still open",
                remaining.len()
            ),
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use secrecy::SecretString;
    use wiremock::matchers::{body_json, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn make_test_signer() -> Result<L2HmacSigner, ExecutionError> {
        let secret = SecretString::new("aGVsbG8td29ybGRfc2VjcmV0".into());
        L2HmacSigner::new(
            "0xeE3B778A783510BC833384919F709e3d2fee1624",
            "api-key-1234",
            &secret,
            SecretString::new("synthetic_test_passphrase_fixture".into()),
        )
    }

    #[tokio::test]
    async fn test_heartbeat_cycles_and_400_recovery() -> Result<(), Box<dyn std::error::Error>> {
        let server = MockServer::start().await;
        let signer = make_test_signer()?;
        let mut actor = OrderHeartbeatActor::new(server.uri(), signer, ExecutionMode::CanaryLive)?;

        // 1. Initial request with heartbeat_id = "" -> returns "hb-1"
        Mock::given(method("POST"))
            .and(path("/v1/heartbeats"))
            .and(body_json(serde_json::json!({ "heartbeat_id": "" })))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "heartbeat_id": "hb-1"
            })))
            .expect(1)
            .mount(&server)
            .await;

        let hb1 = actor.send_heartbeat_step().await?;
        assert_eq!(hb1, "hb-1");
        assert_eq!(actor.current_heartbeat_id(), "hb-1");

        // 2. Next request with heartbeat_id = "hb-1" -> returns "hb-2"
        Mock::given(method("POST"))
            .and(path("/v1/heartbeats"))
            .and(body_json(serde_json::json!({ "heartbeat_id": "hb-1" })))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "heartbeat_id": "hb-2"
            })))
            .expect(1)
            .mount(&server)
            .await;

        let hb2 = actor.send_heartbeat_step().await?;
        assert_eq!(hb2, "hb-2");
        assert_eq!(actor.current_heartbeat_id(), "hb-2");

        // 3. Test 400 Mismatch recovery:
        // Actor sends "hb-2", server responds 400 with expected "hb-99"
        Mock::given(method("POST"))
            .and(path("/v1/heartbeats"))
            .and(body_json(serde_json::json!({ "heartbeat_id": "hb-2" })))
            .respond_with(ResponseTemplate::new(400).set_body_json(serde_json::json!({
                "error_msg": "Invalid Heartbeat ID",
                "heartbeat_id": "hb-99"
            })))
            .expect(1)
            .mount(&server)
            .await;

        // And server expects immediate retry with "hb-99" -> returns "hb-100"
        Mock::given(method("POST"))
            .and(path("/v1/heartbeats"))
            .and(body_json(serde_json::json!({ "heartbeat_id": "hb-99" })))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "heartbeat_id": "hb-100"
            })))
            .expect(1)
            .mount(&server)
            .await;

        let hb_recovered = actor.send_heartbeat_step().await?;
        assert_eq!(hb_recovered, "hb-100");
        assert_eq!(actor.current_heartbeat_id(), "hb-100");

        Ok(())
    }

    #[tokio::test]
    async fn test_safety_latch_dry_run_never_sends_heartbeats()
    -> Result<(), Box<dyn std::error::Error>> {
        let server = MockServer::start().await;
        let signer = make_test_signer()?;

        // DryRun mode
        let mut actor = OrderHeartbeatActor::new(server.uri(), signer, ExecutionMode::DryRun)?;

        // Server has 0 mocks mounted, if any request arrives it will fail with 404
        let res = actor.send_heartbeat_step().await?;
        assert_eq!(res, "");
        assert_eq!(actor.current_heartbeat_id(), "");

        Ok(())
    }

    #[tokio::test]
    async fn test_execute_shutdown_cancel_sequence() -> Result<(), Box<dyn std::error::Error>> {
        let server = MockServer::start().await;
        let signer = make_test_signer()?;
        let client = ClobRestClient::new(server.uri(), signer)?;

        // Mock DELETE /cancel-all
        Mock::given(method("DELETE"))
            .and(path("/cancel-all"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "canceled": ["0x1", "0x2"],
                "not_canceled": {}
            })))
            .mount(&server)
            .await;

        // Mock GET /data/orders -> returns empty array
        Mock::given(method("GET"))
            .and(path("/data/orders"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "count": 0,
                "data": []
            })))
            .mount(&server)
            .await;

        execute_shutdown_cancel(&client).await?;
        Ok(())
    }
}
