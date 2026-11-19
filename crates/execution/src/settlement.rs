//! Settlement worker: confirmed-balance merge dispatch, gasless relayer path,
//! pre-flight approval checks, and wallet-type verification.
//!
//! Plan refs: Phase 5 Slice 6 (`src/settlement.rs`, relayer + approval + wallet
//! checks, merge-before-CONFIRMED guard).

use std::time::Duration;

use core_types::market::{MarketDescriptor, ProtocolVersion, WalletMode};
use secrecy::{ExposeSecret, SecretString};
use serde::{Deserialize, Serialize};

use crate::error::ExecutionError;
use crate::user_ws::OrderRegistrySnapshot;

/// Canonical Polygon contract addresses (plan §1.1, finding C12).
pub const ROUTER_ADDRESS: &str = "0x12121212006e4CD160D18e3f00711DA5c3372600";
/// Legacy CTF exchange contract.
pub const CTF_ADDRESS: &str = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045";
/// pUSD (Polygon) ERC-20 used by CTF `mergePositions`.
pub const PUSD_ADDRESS: &str = "0xC011a7E12a19f7B1f670d46F03B03f3342E82DFB";
/// Gasless relayer v2 base URL.
pub const DEFAULT_RELAYER_URL: &str = "https://relayer-v2.polymarket.com";
/// Minimum merge batch in micro-shares (5 shares default).
pub const DEFAULT_MIN_BATCH_MICRO: u64 = 5_000_000;

/// Merge dispatch target derived from [`MarketDescriptor::version`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MergeTarget {
    /// Protocol V2: `Router.merge(conditionId, amount)`.
    PolyV2Router {
        condition_id: String,
        amount_micro: u64,
    },
    /// Legacy CTF: `CTF.mergePositions(pUSD, 0, conditionId, [1,2], amount)`.
    CtfV1Merge {
        condition_id: String,
        amount_micro: u64,
    },
}

/// Selects the on-chain merge entrypoint for `market`.
#[must_use]
pub fn merge_target_for_market(market: &MarketDescriptor, amount_micro: u64) -> MergeTarget {
    match market.version {
        ProtocolVersion::PolyV2 => MergeTarget::PolyV2Router {
            condition_id: market.condition_id.clone(),
            amount_micro,
        },
        ProtocolVersion::CtfV1 => MergeTarget::CtfV1Merge {
            condition_id: market.condition_id.clone(),
            amount_micro,
        },
    }
}

/// Builds deterministic human-readable merge calldata for dry-run previews.
///
/// Real submission ABI-encodes the same `(conditionId, amount)` pair; this
/// preview keeps the exact pair visible without requiring an RPC node.
#[must_use]
pub fn build_merge_calldata(target: &MergeTarget) -> String {
    match target {
        MergeTarget::PolyV2Router {
            condition_id,
            amount_micro,
        } => {
            format!(
                "Router.merge(conditionId={condition_id}, amount={amount_micro}) [{ROUTER_ADDRESS}]"
            )
        }
        MergeTarget::CtfV1Merge {
            condition_id,
            amount_micro,
        } => {
            format!(
                "CTF.mergePositions(pUSD={PUSD_ADDRESS}, conditionId={condition_id}, indexSets=[1,2], amount={amount_micro}) [{CTF_ADDRESS}]"
            )
        }
    }
}

/// Decision emitted by [`decide_settlement`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SettlementDecision {
    /// `true` when a merge may proceed now.
    pub should_settle: bool,
    /// Pairable `CONFIRMED` amount in micro-shares.
    pub amount_micro: u64,
    /// Machine-readable reason (`ready`, `below-threshold`, `no-pairs`).
    pub reason: &'static str,
}

/// Decides whether confirmed balances are mergeable.
///
/// Only `CONFIRMED` fills count (`snapshot.pairable_confirmed()`), so a fill
/// stuck at `MATCHED`/`MINED` defers automatically (adversarial test 4).
#[must_use]
pub fn decide_settlement(
    snapshot: &OrderRegistrySnapshot,
    min_batch_micro: u64,
) -> SettlementDecision {
    let pairable = snapshot.pairable_confirmed();
    if pairable == 0 {
        return SettlementDecision {
            should_settle: false,
            amount_micro: 0,
            reason: "no-pairs",
        };
    }
    if pairable < min_batch_micro {
        return SettlementDecision {
            should_settle: false,
            amount_micro: pairable,
            reason: "below-threshold",
        };
    }
    SettlementDecision {
        should_settle: true,
        amount_micro: pairable,
        reason: "ready",
    }
}

/// Dynamic gas-batching floor: `N_min = ceil(10 * TxCost_gas / s_star)`.
///
/// Prevents dust merges where gas dwarfs the captured spread.
pub fn min_batch_for_gas(
    tx_cost_usdc_micro: u64,
    spread_star_usd: f64,
) -> Result<u64, ExecutionError> {
    if !spread_star_usd.is_finite() || spread_star_usd <= 0.0 {
        return Err(ExecutionError::Settlement(format!(
            "invalid s_star {spread_star_usd}"
        )));
    }
    let need = (10.0 * tx_cost_usdc_micro as f64 / spread_star_usd).ceil();
    if !need.is_finite() || need < 0.0 || need > u64::MAX as f64 {
        return Err(ExecutionError::Settlement(
            "gas batch floor out of range".to_string(),
        ));
    }
    Ok(need as u64)
}

/// Pre-flight ERC-20 / ERC-1155 approval state observed at startup.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ApprovalState {
    /// Observed pUSD allowance for the settlement spender (micro-USDC).
    pub pusd_allowance_micro: u64,
    /// Allowance required to cover the merge batch (micro-USDC).
    pub pusd_needed_micro: u64,
    /// Result of ERC-1155 `isApprovedForAll(operator)`.
    pub erc1155_approved: bool,
}

/// Verifies approvals before live quoting is enabled.
pub fn check_preflight(state: &ApprovalState) -> Result<(), ExecutionError> {
    if !state.erc1155_approved {
        return Err(ExecutionError::Settlement(
            "pre-flight: ERC-1155 isApprovedForAll == false".to_string(),
        ));
    }
    if state.pusd_allowance_micro < state.pusd_needed_micro {
        return Err(ExecutionError::Settlement(format!(
            "pre-flight: pUSD allowance {} < needed {}",
            state.pusd_allowance_micro, state.pusd_needed_micro
        )));
    }
    Ok(())
}

/// Verifies the configured [`WalletMode`] against read-only on-chain/profile data.
///
/// `eth_code_len` is the byte length returned by `eth_getCode(wallet)`;
/// `profile_sig_type` is the `sig_type` reported by the Polymarket profile API.
pub fn verify_wallet_type(
    expected: WalletMode,
    maker_address: &str,
    eth_code_len: usize,
    profile_sig_type: Option<u8>,
) -> Result<(), ExecutionError> {
    if maker_address.trim().is_empty() {
        return Err(ExecutionError::Settlement(
            "wallet verification: empty maker address".to_string(),
        ));
    }
    let is_contract = eth_code_len > 0;
    let expected_sig = expected.as_u8();

    match expected {
        WalletMode::Eoa => {
            if is_contract {
                return Err(ExecutionError::Settlement(
                    "wallet verification: EOA mode but eth_getCode is non-empty".to_string(),
                ));
            }
            if let Some(reported) = profile_sig_type
                && reported != expected_sig
            {
                return Err(ExecutionError::Settlement(format!(
                    "wallet type mismatch: configured EOA sig_type=0 but profile reports {reported}"
                )));
            }
        }
        WalletMode::PolyProxy | WalletMode::GnosisSafe | WalletMode::DepositWallet1271 => {
            if !is_contract {
                return Err(ExecutionError::Settlement(format!(
                    "wallet verification: {expected:?} expects contract code, got empty eth_getCode"
                )));
            }
            let Some(reported) = profile_sig_type else {
                return Err(ExecutionError::Settlement(format!(
                    "wallet verification: profile sig_type required for contract wallet {expected:?}"
                )));
            };
            if reported != expected_sig {
                return Err(ExecutionError::Settlement(format!(
                    "wallet type mismatch: configured sig_type={expected_sig} but profile reports {reported}"
                )));
            }
        }
    }
    Ok(())
}

/// Relayer transaction parameters fetched from
/// `GET /v1/account/transactions/params`.
#[derive(Debug, Clone, Deserialize)]
struct TxParamsResponse {
    #[serde(default)]
    nonce: String,
}

/// Relayer submit response (`submit` returns a tracking ID + state).
#[derive(Debug, Clone, Deserialize, Serialize)]
struct RelayerSubmitResponse {
    #[serde(
        alias = "transactionID",
        alias = "transactionId",
        alias = "id",
        default
    )]
    transaction_id: String,
    #[serde(alias = "state", default)]
    state: String,
}

/// Relayer transaction record from `GET /transaction?id=<transactionID>`.
#[derive(Debug, Clone, Deserialize)]
struct RelayerTxRecord {
    #[serde(default, alias = "transactionID", alias = "transactionId")]
    transaction_id: String,
    #[serde(
        default,
        alias = "transactionHash",
        alias = "txHash",
        alias = "tx_hash"
    )]
    transaction_hash: Option<String>,
    #[serde(default)]
    state: String,
}

/// Minimal gasless relayer v2 client (base URL is mockable in tests).
#[derive(Clone)]
pub struct RelayerClient {
    base_url: String,
    http: reqwest::Client,
    api_key: SecretString,
    api_key_address: String,
}

impl std::fmt::Debug for RelayerClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("RelayerClient([REDACTED])")
    }
}

impl RelayerClient {
    /// Creates a relayer client. Secrets are held in [`SecretString`].
    pub fn new(
        base_url: impl Into<String>,
        api_key: SecretString,
        api_key_address: impl Into<String>,
    ) -> Result<Self, ExecutionError> {
        let http = reqwest::Client::builder()
            .tcp_nodelay(true)
            .connect_timeout(Duration::from_millis(2000))
            .timeout(Duration::from_millis(5000))
            .build()?;
        Ok(Self {
            base_url: base_url.into().trim_end_matches('/').to_string(),
            http,
            api_key,
            api_key_address: api_key_address.into(),
        })
    }

    async fn fetch_nonce(&self, wallet: &str) -> Result<String, ExecutionError> {
        let url = format!(
            "{}/v1/account/transactions/params?address={wallet}",
            self.base_url
        );
        let resp = self.http.get(&url).send().await?;
        let status = resp.status();
        let text = resp.text().await?;
        if !status.is_success() {
            return Err(ExecutionError::Rest {
                status_code: Some(status.as_u16()),
                detail: format!("relayer params failed: {text}"),
            });
        }
        let parsed: TxParamsResponse =
            serde_json::from_str(&text).map_err(|err| ExecutionError::Rest {
                status_code: Some(status.as_u16()),
                detail: format!("relayer params parse '{text}': {err}"),
            })?;
        Ok(parsed.nonce)
    }

    /// Submits a merge via the gasless relayer and returns the tracking transaction ID.
    pub async fn submit_merge(
        &self,
        wallet: &str,
        target: &MergeTarget,
        calldata_preview: &str,
    ) -> Result<String, ExecutionError> {
        let nonce = self.fetch_nonce(wallet).await?;
        let body = serde_json::json!({
            "wallet": wallet,
            "nonce": nonce,
            "calldataPreview": calldata_preview,
            "conditionId": match target {
                MergeTarget::PolyV2Router { condition_id, .. } => condition_id,
                MergeTarget::CtfV1Merge { condition_id, .. } => condition_id,
            },
            "amount": match target {
                MergeTarget::PolyV2Router { amount_micro, .. } => amount_micro,
                MergeTarget::CtfV1Merge { amount_micro, .. } => amount_micro,
            },
        });
        let url = format!("{}/submit", self.base_url);
        let resp = self
            .http
            .post(&url)
            .header("RELAYER_API_KEY", self.api_key.expose_secret())
            .header("RELAYER_API_KEY_ADDRESS", &self.api_key_address)
            .json(&body)
            .send()
            .await?;
        let status = resp.status();
        let text = resp.text().await?;
        if !status.is_success() {
            return Err(ExecutionError::Rest {
                status_code: Some(status.as_u16()),
                detail: format!("relayer submit failed: {text}"),
            });
        }
        let parsed: RelayerSubmitResponse =
            serde_json::from_str(&text).map_err(|err| ExecutionError::Rest {
                status_code: Some(status.as_u16()),
                detail: format!("relayer submit parse '{text}': {err}"),
            })?;
        if parsed.transaction_id.trim().is_empty() {
            return Err(ExecutionError::Rest {
                status_code: Some(status.as_u16()),
                detail: "relayer submit returned empty transaction ID".to_string(),
            });
        }
        Ok(parsed.transaction_id)
    }

    /// Polls `GET /transaction?id=<transaction_id>` until `STATE_CONFIRMED` or timeout.
    /// Returns the onchain transaction hash if available.
    pub async fn poll_until_confirmed(
        &self,
        transaction_id: &str,
        max_wait: Duration,
    ) -> Result<String, ExecutionError> {
        let deadline = std::time::Instant::now() + max_wait;
        loop {
            let url = format!("{}/transaction?id={transaction_id}", self.base_url);
            let resp = self.http.get(&url).send().await?;
            let status = resp.status();
            let text = resp.text().await?;
            if status.is_success() {
                let records = if let Ok(list) = serde_json::from_str::<Vec<RelayerTxRecord>>(&text)
                {
                    list
                } else if let Ok(single) = serde_json::from_str::<RelayerTxRecord>(&text) {
                    vec![single]
                } else {
                    Vec::new()
                };

                for rec in records {
                    if !rec.transaction_id.is_empty() && rec.transaction_id != transaction_id {
                        continue;
                    }
                    let st = rec.state.trim().to_ascii_uppercase();
                    if st == "STATE_CONFIRMED" || st == "CONFIRMED" {
                        return Ok(rec
                            .transaction_hash
                            .unwrap_or_else(|| transaction_id.to_string()));
                    }
                    if st == "STATE_FAILED" || st == "STATE_INVALID" || st == "FAILED" {
                        return Err(ExecutionError::Settlement(format!(
                            "relayer tx {transaction_id} terminal failure: {st} ({text})"
                        )));
                    }
                }
            }
            if std::time::Instant::now() >= deadline {
                return Err(ExecutionError::Settlement(format!(
                    "relayer tx {transaction_id} not confirmed within {max_wait:?}"
                )));
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    }
}

/// Settlement worker configuration.
#[derive(Debug, Clone)]
pub struct SettlementConfig {
    /// Minimum `CONFIRMED` pairable micro-shares before a merge is attempted.
    pub min_batch_micro: u64,
    /// Maximum relayer confirmation wait.
    pub max_poll: Duration,
    /// When true, enables direct settlement fallback checking dynamic gas floor.
    pub direct_mode: bool,
    /// Dynamic gas floor in micro-shares: N_min = ceil(10 * TxCost_gas / s*).
    pub min_batch_for_gas: u64,
}

impl Default for SettlementConfig {
    fn default() -> Self {
        Self {
            min_batch_micro: DEFAULT_MIN_BATCH_MICRO,
            max_poll: Duration::from_secs(30),
            direct_mode: false,
            min_batch_for_gas: DEFAULT_MIN_BATCH_MICRO,
        }
    }
}

/// Confirmed settlement receipt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SettlementReceipt {
    /// Relayer tracking hash / transaction hash.
    pub tx_hash: String,
    /// Merged amount in micro-shares.
    pub amount_micro: u64,
    /// Human-readable merge target.
    pub target: String,
}

/// Background worker merging only `CONFIRMED` pairable balances.
pub struct SettlementWorker {
    config: SettlementConfig,
    market: MarketDescriptor,
    wallet: String,
    relayer: Option<RelayerClient>,
    in_flight_micro: std::sync::atomic::AtomicU64,
}

impl SettlementWorker {
    /// Creates a worker. `relayer` is `None` for dry-run previews.
    #[must_use]
    pub fn new(
        config: SettlementConfig,
        market: MarketDescriptor,
        wallet: String,
        relayer: Option<RelayerClient>,
    ) -> Self {
        Self {
            config,
            market,
            wallet,
            relayer,
            in_flight_micro: std::sync::atomic::AtomicU64::new(0),
        }
    }

    /// Current decision for `snapshot` (never settles `MATCHED`-only fills).
    /// Subtracts any in-flight merge amounts that have not yet confirmed.
    #[must_use]
    pub fn decision(&self, snapshot: &OrderRegistrySnapshot) -> SettlementDecision {
        let in_flight = self
            .in_flight_micro
            .load(std::sync::atomic::Ordering::SeqCst);
        let pairable = snapshot.pairable_confirmed();
        let available = pairable.saturating_sub(in_flight);
        if available < self.config.min_batch_micro {
            return SettlementDecision {
                should_settle: false,
                amount_micro: 0,
                reason: if in_flight > 0 && pairable >= self.config.min_batch_micro {
                    "in-flight-reservation"
                } else if pairable == 0 {
                    "no-pairs"
                } else {
                    "below-threshold"
                },
            };
        }
        SettlementDecision {
            should_settle: true,
            amount_micro: available,
            reason: "confirmed-threshold-met",
        }
    }

    /// Deterministic calldata preview for `amount_micro`.
    #[must_use]
    pub fn preview_calldata(&self, amount_micro: u64) -> String {
        build_merge_calldata(&merge_target_for_market(&self.market, amount_micro))
    }

    /// Attempts one merge if [`Self::decision`] is ready.
    ///
    /// Returns `Ok(None)` when below threshold (defer, including `MATCHED`-only
    /// fills). Dry-run workers without a relayer return a synthetic receipt so
    /// the exec trace can verify calldata without spending gas.
    pub async fn settle_once(
        &self,
        snapshot: &OrderRegistrySnapshot,
    ) -> Result<Option<SettlementReceipt>, ExecutionError> {
        let decision = self.decision(snapshot);
        if !decision.should_settle {
            return Ok(None);
        }

        if self.config.direct_mode && decision.amount_micro < self.config.min_batch_for_gas {
            return Err(ExecutionError::Settlement(format!(
                "Direct settlement batch {} < dynamic gas floor {}",
                decision.amount_micro, self.config.min_batch_for_gas
            )));
        }

        let amount = decision.amount_micro;
        self.in_flight_micro
            .fetch_add(amount, std::sync::atomic::Ordering::SeqCst);

        let target = merge_target_for_market(&self.market, amount);
        let preview = build_merge_calldata(&target);
        let Some(relayer) = self.relayer.as_ref() else {
            self.in_flight_micro
                .fetch_sub(amount, std::sync::atomic::Ordering::SeqCst);
            return Ok(Some(SettlementReceipt {
                tx_hash: "dry-run-no-relayer".to_string(),
                amount_micro: amount,
                target: preview,
            }));
        };

        let tx_id = match relayer.submit_merge(&self.wallet, &target, &preview).await {
            Ok(id) => id,
            Err(e) => {
                self.in_flight_micro
                    .fetch_sub(amount, std::sync::atomic::Ordering::SeqCst);
                return Err(e);
            }
        };

        let tx_hash = match relayer
            .poll_until_confirmed(&tx_id, self.config.max_poll)
            .await
        {
            Ok(hash) => {
                self.in_flight_micro
                    .fetch_sub(amount, std::sync::atomic::Ordering::SeqCst);
                hash
            }
            Err(e) => {
                self.in_flight_micro
                    .fetch_sub(amount, std::sync::atomic::Ordering::SeqCst);
                return Err(e);
            }
        };

        Ok(Some(SettlementReceipt {
            tx_hash,
            amount_micro: amount,
            target: preview,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use core_types::money::{FeeSchedule, MicroUsdc, SharesMicro};
    use secrecy::SecretString;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    type TestResult = Result<(), Box<dyn std::error::Error>>;

    fn test_market(version: ProtocolVersion) -> MarketDescriptor {
        MarketDescriptor {
            condition_id: "0xabc".to_string(),
            slug: "btc-5m".to_string(),
            question: "q".to_string(),
            version,
            neg_risk: false,
            asset_id_up: "111".to_string(),
            asset_id_dn: "222".to_string(),
            tick_size_micro: MicroUsdc::from_raw(10_000),
            min_order_size_micro: SharesMicro::from_raw(5_000_000),
            taker_delay_ms: 50,
            fee_schedule: FeeSchedule::btc_5m_default(),
            start_ts_ms: 1000,
            expiry_ts_ms: 301_000,
            strike_price_usd: None,
        }
    }

    fn confirmed_snapshot(up: i64, dn: i64) -> OrderRegistrySnapshot {
        OrderRegistrySnapshot {
            confirmed_up: up,
            confirmed_dn: dn,
            ..OrderRegistrySnapshot::default()
        }
    }

    #[test]
    fn merge_dispatches_router_vs_ctf() -> TestResult {
        let v2 = merge_target_for_market(&test_market(ProtocolVersion::PolyV2), 7_000_000);
        let ctf = merge_target_for_market(&test_market(ProtocolVersion::CtfV1), 7_000_000);
        assert!(matches!(v2, MergeTarget::PolyV2Router { .. }));
        assert!(matches!(ctf, MergeTarget::CtfV1Merge { .. }));
        let v2_calldata = build_merge_calldata(&v2);
        let ctf_calldata = build_merge_calldata(&ctf);
        assert!(v2_calldata.contains("Router.merge"));
        assert!(ctf_calldata.contains("mergePositions"));
        assert!(v2_calldata.contains("0xabc"));
        Ok(())
    }

    #[test]
    fn matched_only_defers_until_confirmed() -> TestResult {
        // MATCHED fills land in unconfirmed_* and must NOT settle.
        let matched_only = OrderRegistrySnapshot {
            unconfirmed_up: 10_000_000,
            unconfirmed_dn: 10_000_000,
            ..OrderRegistrySnapshot::default()
        };
        let d = decide_settlement(&matched_only, DEFAULT_MIN_BATCH_MICRO);
        assert!(!d.should_settle);
        assert_eq!(d.reason, "no-pairs");

        let dust = confirmed_snapshot(1_000_000, 1_000_000);
        let d2 = decide_settlement(&dust, DEFAULT_MIN_BATCH_MICRO);
        assert!(!d2.should_settle);
        assert_eq!(d2.reason, "below-threshold");

        let ready = confirmed_snapshot(6_000_000, 8_000_000);
        let d3 = decide_settlement(&ready, DEFAULT_MIN_BATCH_MICRO);
        assert!(d3.should_settle);
        assert_eq!(d3.amount_micro, 6_000_000);
        Ok(())
    }

    #[test]
    fn gas_floor_and_preflight_and_wallet_checks() -> TestResult {
        let n = min_batch_for_gas(500_000, 0.04)?;
        assert!(n > 0);
        assert!(min_batch_for_gas(500_000, 0.0).is_err());
        assert!(min_batch_for_gas(500_000, f64::NAN).is_err());

        assert!(
            check_preflight(&ApprovalState {
                pusd_allowance_micro: 10_000_000,
                pusd_needed_micro: 5_000_000,
                erc1155_approved: true,
            })
            .is_ok()
        );
        assert!(
            check_preflight(&ApprovalState {
                pusd_allowance_micro: 1,
                pusd_needed_micro: 5_000_000,
                erc1155_approved: true,
            })
            .is_err()
        );
        assert!(
            check_preflight(&ApprovalState {
                pusd_allowance_micro: 10_000_000,
                pusd_needed_micro: 5_000_000,
                erc1155_approved: false,
            })
            .is_err()
        );

        assert!(
            verify_wallet_type(
                WalletMode::DepositWallet1271,
                "0xeE3B778A783510BC833384919F709e3d2fee1624",
                128,
                Some(3)
            )
            .is_ok()
        );
        assert!(
            verify_wallet_type(
                WalletMode::DepositWallet1271,
                "0xeE3B778A783510BC833384919F709e3d2fee1624",
                128,
                Some(2)
            )
            .is_err()
        );
        assert!(
            verify_wallet_type(
                WalletMode::DepositWallet1271,
                "0xeE3B778A783510BC833384919F709e3d2fee1624",
                128,
                None
            )
            .is_err()
        );
        assert!(verify_wallet_type(WalletMode::Eoa, "0xabc", 10, Some(0)).is_err());
        assert!(verify_wallet_type(WalletMode::Eoa, "0xabc", 0, Some(0)).is_ok());
        Ok(())
    }

    #[tokio::test]
    async fn relayer_submit_and_confirm_flow() -> TestResult {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v1/account/transactions/params"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "nonce": "7"
            })))
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/submit"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "transactionID": "tx-relayer-123",
                "state": "STATE_NEW"
            })))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/transaction"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!([
                {
                    "transactionID": "tx-relayer-123",
                    "transactionHash": "0xrelay123",
                    "state": "STATE_CONFIRMED"
                }
            ])))
            .mount(&server)
            .await;

        let relayer = RelayerClient::new(
            server.uri(),
            SecretString::new("relay-key".into()),
            "0xaddr",
        )?;
        let worker = SettlementWorker::new(
            SettlementConfig::default(),
            test_market(ProtocolVersion::PolyV2),
            "0xwallet".to_string(),
            Some(relayer),
        );
        let snap = confirmed_snapshot(6_000_000, 6_000_000);
        let receipt = worker.settle_once(&snap).await?.ok_or("expected receipt")?;
        assert_eq!(receipt.tx_hash, "0xrelay123");
        assert_eq!(receipt.amount_micro, 6_000_000);

        // Verify in-flight reservation prevents double submission
        let worker_res = SettlementWorker::new(
            SettlementConfig::default(),
            test_market(ProtocolVersion::PolyV2),
            "0xwallet".to_string(),
            None,
        );
        worker_res
            .in_flight_micro
            .store(6_000_000, std::sync::atomic::Ordering::SeqCst);
        let d = worker_res.decision(&snap);
        assert!(!d.should_settle);
        assert_eq!(d.reason, "in-flight-reservation");

        // Verify direct_mode gas floor check
        let worker_direct = SettlementWorker::new(
            SettlementConfig {
                direct_mode: true,
                min_batch_for_gas: 10_000_000,
                ..SettlementConfig::default()
            },
            test_market(ProtocolVersion::PolyV2),
            "0xwallet".to_string(),
            None,
        );
        assert!(worker_direct.settle_once(&snap).await.is_err());

        let dry = SettlementWorker::new(
            SettlementConfig::default(),
            test_market(ProtocolVersion::CtfV1),
            "0xwallet".to_string(),
            None,
        );
        let deferred = dry.settle_once(&OrderRegistrySnapshot::default()).await?;
        assert!(deferred.is_none());
        Ok(())
    }

    #[test]
    fn relayer_client_debug_redacts_secret() -> TestResult {
        let client = RelayerClient::new(
            "http://localhost",
            SecretString::new("SECRET_SENTINEL_RELAY".into()),
            "0xaddr",
        )?;
        let rendered = format!("{client:?}");
        assert!(!rendered.contains("SECRET_SENTINEL_RELAY"));
        Ok(())
    }
}
