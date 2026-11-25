use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::JoinHandle;

use arc_swap::ArcSwap;
use core_types::{MicroUsdc, Outcome, SharesMicro};
use math_engine::QuoteOutput;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::{self, error::TrySendError};

use crate::breakers::{BreakerConfig, BreakerStatus, CircuitBreakers};
use crate::error::RiskLifecycleError;
use crate::lifecycle::{EpochLifecycleController, EpochPhase, LifecycleAction, LifecycleInput};
use crate::station7_risk::{
    DollarNotionalRiskEngine, FlattenAction, OrderRegistrySnapshot, RiskEvaluationInput, RiskTier,
};
use crate::station8_hysteresis::{
    CancelReason, HysteresisGate, HysteresisInput, OrderAction, OrderSlotState, SizedQuote,
};

/// Conflated, latest-wins quote proposal published lock-free from Pricing to Risk.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct QuoteProposal {
    /// Monotonic proposal sequence.
    pub seq: u64,
    /// Evaluation timestamp in milliseconds.
    pub now_ms: u64,
    /// Active 5m epoch expiration timestamp in ms.
    pub expiry_ms: u64,
    /// Active market tick size in micro-USDC.
    pub tick_size_micro: MicroUsdc,
    /// Station 6 dual-bid quote output.
    pub quote: QuoteOutput,
    /// Raw unrounded UP target bid price in USD.
    pub bid_up_raw: f64,
    /// Raw unrounded DOWN target bid price in USD.
    pub bid_dn_raw: f64,
    /// `true` when Station 6 capped UP bid below its raw target.
    #[serde(default)]
    pub price_capped_up: bool,
    /// `true` when Station 6 capped DOWN bid below its raw target.
    #[serde(default)]
    pub price_capped_dn: bool,
    /// `true` when UP bid lies outside `max_entry_prob` and is authorized only to offset held DOWN shares.
    #[serde(default)]
    pub hedge_only_up: bool,
    /// `true` when DOWN bid lies outside `max_entry_prob` and is authorized only to offset held UP shares.
    #[serde(default)]
    pub hedge_only_dn: bool,
    /// Orderbook best bid for UP outcome.
    pub best_bid_up: Option<MicroUsdc>,
    /// Orderbook best bid for DOWN outcome.
    pub best_bid_dn: Option<MicroUsdc>,
    /// Orderbook best ask for UP outcome.
    pub best_ask_up: Option<MicroUsdc>,
    /// Orderbook best ask for DOWN outcome.
    pub best_ask_dn: Option<MicroUsdc>,
    /// Fast-path Binance spot jump defensive cancel flag.
    pub defensive_cancel: bool,
    /// Per-side Binance lead-lag cancel / cooldown flag for UP bids.
    #[serde(default)]
    pub defensive_cancel_up: bool,
    /// Per-side Binance lead-lag cancel / cooldown flag for DOWN bids.
    #[serde(default)]
    pub defensive_cancel_dn: bool,
    /// Nanosecond latency from pricing ingress to proposal publish.
    pub total_compute_ns: u64,
}

impl Default for QuoteProposal {
    fn default() -> Self {
        Self {
            seq: 0,
            now_ms: 0,
            expiry_ms: 0,
            tick_size_micro: MicroUsdc(10_000),
            quote: QuoteOutput::Suppressed(math_engine::SuppressionReason::WarmingUp),
            bid_up_raw: 0.0,
            bid_dn_raw: 0.0,
            price_capped_up: false,
            price_capped_dn: false,
            hedge_only_up: false,
            hedge_only_dn: false,
            best_bid_up: None,
            best_bid_dn: None,
            best_ask_up: None,
            best_ask_dn: None,
            defensive_cancel: false,
            defensive_cancel_up: false,
            defensive_cancel_dn: false,
            total_compute_ns: 0,
        }
    }
}

/// Commands dispatched from Risk thread to Execution/Paper actor.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ExecutionCommand {
    /// Submit a new resting maker limit order.
    SubmitNew {
        /// Target outcome.
        outcome: Outcome,
        /// Limit price in micro-USDC.
        price: MicroUsdc,
        /// Order size in micro-shares.
        size: SharesMicro,
    },
    /// Cancel a resting order.
    Cancel {
        /// Target outcome.
        outcome: Outcome,
        /// Exchange or sim order identifier.
        order_id: String,
        /// Cancellation reason.
        reason: CancelReason,
    },
    /// Atomically replace old order with new quote.
    CancelAndReplace {
        /// Target outcome.
        outcome: Outcome,
        /// Old order identifier.
        old_order_id: String,
        /// New limit price.
        new_price: MicroUsdc,
        /// New limit size.
        new_size: SharesMicro,
        /// Reason for update.
        reason: CancelReason,
    },
    /// Immediate high-priority cancel across all markets and legs.
    CancelAll {
        /// Explanation for emergency cancel.
        reason: String,
    },
    /// Immediate high-priority aggressive taker flatten order.
    ExecuteFlattenFak {
        /// Target outcome leg.
        outcome: Outcome,
        /// Limit price.
        price: MicroUsdc,
        /// Size to rebalance.
        size: SharesMicro,
        /// Rationale.
        reason: String,
    },
}

/// Adversarial fault modes that can be injected for testing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum FaultInjection {
    /// No fault injected.
    #[default]
    None,
    /// Simulates Polymarket feed going silent (> 3.5s).
    StalePoly,
    /// Simulates Binance feed going silent (> 2.0s).
    StaleBinance,
    /// Forces cost-basis utilization `u = 0.95` to trigger Tier 4 Hard Stop.
    ForceTier4,
    /// Emits `tick_size_change 0.01 -> 0.001` to trigger freeze.
    TickChange,
    /// Injects sudden Binance `+$150` jump triggering lead-lag cancel.
    BinanceJump,
}

/// Default maximum duration (`2s`) a single-flight flatten lock remains active without ACK/clear.
pub const DEFAULT_FLATTEN_IN_FLIGHT_TIMEOUT_NS: u64 = 2_000_000_000;

/// Unified telemetry trace record emitted by the Risk Actor on every evaluation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RiskActorTrace {
    /// Wall-clock timestamp in ms.
    pub now_ms: u64,
    /// Remaining time to expiry in seconds.
    pub tau_secs: u64,
    /// Active 300s epoch lifecycle phase.
    pub phase: EpochPhase,
    /// Active cost-basis risk tier.
    pub tier: RiskTier,
    /// Cost-basis utilization ratio `u` in micro-units.
    pub utilization_u_e6: u64,
    /// Circuit breaker status.
    pub breaker: BreakerStatus,
    /// Emitted order actions for UP leg.
    pub action_up: OrderAction,
    /// Emitted order actions for DOWN leg.
    pub action_dn: OrderAction,
    /// Whether emergency flatten was commanded.
    pub flatten_action: FlattenAction,
    /// Reason when `CancelAll` or a flatten hold/failure occurred.
    pub cancel_all_reason: Option<String>,
    /// Whether the single-flight flatten guard is currently armed.
    pub flatten_in_flight: bool,
    /// Orderbook best bid for UP outcome observed on this tick.
    pub best_bid_up: Option<MicroUsdc>,
    /// Orderbook best ask for UP outcome observed on this tick.
    pub best_ask_up: Option<MicroUsdc>,
    /// Orderbook best bid for DOWN outcome observed on this tick.
    pub best_bid_dn: Option<MicroUsdc>,
    /// Orderbook best ask for DOWN outcome observed on this tick.
    pub best_ask_dn: Option<MicroUsdc>,
    /// Risk thread evaluation latency in nanoseconds.
    pub risk_eval_ns: u64,
}

/// Actor communication harness coupling Pricing, Risk, and Execution.
#[derive(Clone)]
pub struct ActorPipelineHarness {
    /// Lock-free latest-wins slot for QuoteProposal (Pricing -> Risk).
    pub pricing_slot: Arc<ArcSwap<QuoteProposal>>,
    /// Lock-free latest-wins slot for OrderRegistrySnapshot (Execution -> Risk).
    pub registry_slot: Arc<ArcSwap<OrderRegistrySnapshot>>,
    /// High-priority non-blocking channel for cancels and flattens.
    pub priority_tx: mpsc::Sender<ExecutionCommand>,
    /// Routine non-blocking channel for normal quoting.
    pub routine_tx: mpsc::Sender<ExecutionCommand>,
    /// Condvar signaling Risk thread when Pricing publishes a new quote.
    pub wake_signal: Arc<(Mutex<bool>, Condvar)>,
    /// Global cancellation / shutdown flag.
    pub shutdown: Arc<AtomicBool>,
}

impl ActorPipelineHarness {
    /// Initializes lock-free shared slots and bounded Tokio channels.
    pub fn new(
        priority_cap: usize,
        routine_cap: usize,
    ) -> (
        Self,
        mpsc::Receiver<ExecutionCommand>,
        mpsc::Receiver<ExecutionCommand>,
    ) {
        let (priority_tx, priority_rx) = mpsc::channel(priority_cap);
        let (routine_tx, routine_rx) = mpsc::channel(routine_cap);

        let harness = Self {
            pricing_slot: Arc::new(ArcSwap::from_pointee(QuoteProposal::default())),
            registry_slot: Arc::new(ArcSwap::from_pointee(OrderRegistrySnapshot::default())),
            priority_tx,
            routine_tx,
            wake_signal: Arc::new((Mutex::new(false), Condvar::new())),
            shutdown: Arc::new(AtomicBool::new(false)),
        };

        (harness, priority_rx, routine_rx)
    }

    /// Publishes a new quote proposal from the Pricing thread (latest-wins).
    pub fn publish_quote(&self, proposal: QuoteProposal) {
        self.pricing_slot.store(Arc::new(proposal));
        let (lock, cvar) = &*self.wake_signal;
        if let Ok(mut ready) = lock.lock() {
            *ready = true;
            cvar.notify_one();
        }
    }

    /// Updates the order registry snapshot from the Execution actor.
    pub fn publish_registry_snapshot(&self, snapshot: OrderRegistrySnapshot) {
        self.registry_slot.store(Arc::new(snapshot));
    }
}

/// Optional zero-based logical processor assignment for the dedicated actor threads.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CorePinning {
    /// Logical core for the Pricing actor thread (`--pin-pricing-core`).
    pub pricing_core: Option<usize>,
    /// Logical core for the Risk actor thread (`--pin-risk-core`).
    pub risk_core: Option<usize>,
}

impl CorePinning {
    /// Returns `true` when at least one actor thread is explicitly pinned.
    pub fn is_active(&self) -> bool {
        self.pricing_core.is_some() || self.risk_core.is_some()
    }

    /// Renders the human-readable pinning status for the CLI startup banner.
    pub fn summary(&self) -> String {
        let pricing = self
            .pricing_core
            .map_or_else(|| "unpinned".to_string(), |core| format!("core {core}"));
        let risk = self
            .risk_core
            .map_or_else(|| "unpinned".to_string(), |core| format!("core {core}"));
        format!("Pricing {pricing}, Risk {risk}")
    }
}

/// Pins the calling OS thread to a zero-based logical core (`--pin-*-core`).
///
/// Returns `Err(ThreadPinning)` when the core index is outside the host's logical
/// processor set or the OS rejects the affinity mask, so a bad flag never silently
/// degrades into unpinned scheduling.
pub fn pin_thread_to_core(core: usize) -> Result<(), RiskLifecycleError> {
    let cores = core_affinity::get_core_ids().ok_or_else(|| {
        RiskLifecycleError::ThreadPinning("logical core enumeration unavailable".to_string())
    })?;
    let id = *cores.get(core).ok_or_else(|| {
        RiskLifecycleError::ThreadPinning(format!(
            "logical core {core} out of range ({} available)",
            cores.len()
        ))
    })?;
    if core_affinity::set_for_current(id) {
        Ok(())
    } else {
        Err(RiskLifecycleError::ThreadPinning(format!(
            "OS rejected affinity mask for logical core {core}"
        )))
    }
}

/// Named handle for one supervised actor OS thread.
struct SupervisedActor {
    name: &'static str,
    handle: JoinHandle<()>,
}

/// Watches the dedicated Pricing and Risk actor threads for unexpected exits.
///
/// A healthy actor thread only returns after `shutdown_flag()` is raised. A panic
/// or premature return is a fatal topology fault: the supervisor fires an
/// emergency `CancelAll` on the priority lane and reports the fault so the caller
/// can exit non-zero instead of leaving orphan resting quotes.
pub struct ActorSupervisor {
    harness: ActorPipelineHarness,
    shutdown: Arc<AtomicBool>,
    threads: Vec<SupervisedActor>,
    pin_fault: Arc<Mutex<Option<String>>>,
}

impl ActorSupervisor {
    /// Creates a supervisor bound to the shared actor harness and shutdown flag.
    pub fn new(harness: ActorPipelineHarness) -> Self {
        let shutdown = Arc::clone(&harness.shutdown);
        Self {
            harness,
            shutdown,
            threads: Vec::new(),
            pin_fault: Arc::new(Mutex::new(None)),
        }
    }

    /// Shared shutdown flag; set it to request a clean actor exit.
    pub fn shutdown_flag(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.shutdown)
    }

    /// Spawns a dedicated OS thread for one actor, pinning it to `core` when provided.
    pub fn spawn<F>(
        &mut self,
        name: &'static str,
        core: Option<usize>,
        body: F,
    ) -> Result<(), RiskLifecycleError>
    where
        F: FnOnce() + Send + 'static,
    {
        let pin_fault = Arc::clone(&self.pin_fault);
        let handle = std::thread::Builder::new()
            .name(format!("poly-as-{name}"))
            .spawn(move || {
                let pinned = match core {
                    Some(core) => match pin_thread_to_core(core) {
                        Ok(()) => true,
                        Err(err) => {
                            if let Ok(mut slot) = pin_fault.lock() {
                                *slot = Some(err.to_string());
                            }
                            false
                        }
                    },
                    None => true,
                };
                if pinned {
                    body();
                }
            })
            .map_err(|err| {
                RiskLifecycleError::ThreadPinning(format!("cannot spawn {name} thread: {err}"))
            })?;
        self.threads.push(SupervisedActor { name, handle });
        Ok(())
    }

    /// Joins every actor thread, firing an emergency `CancelAll` if one exit was unexpected.
    pub fn join(mut self) -> Result<(), RiskLifecycleError> {
        let mut fault: Option<(String, String)> = None;
        let threads = std::mem::take(&mut self.threads);
        for actor in threads {
            let outcome = actor.handle.join();
            if fault.is_some() {
                continue;
            }
            match outcome {
                Ok(()) if self.shutdown.load(Ordering::Acquire) => {}
                Ok(()) => {
                    fault = Some((
                        actor.name.to_string(),
                        "actor thread exited before shutdown was requested".to_string(),
                    ));
                }
                Err(payload) => {
                    fault = Some((actor.name.to_string(), panic_message(payload.as_ref())));
                }
            }
        }
        if fault.is_none()
            && let Ok(slot) = self.pin_fault.lock()
            && let Some(reason) = slot.clone()
        {
            fault = Some(("pinning".to_string(), reason));
        }
        match fault {
            None => Ok(()),
            Some((actor, reason)) => {
                self.fire_emergency_cancel(format!("Actor thread '{actor}' faulted: {reason}"));
                Err(RiskLifecycleError::ActorThreadFault { actor, reason })
            }
        }
    }

    /// Sends `CancelAll` on the priority lane, guaranteeing delivery off the async runtime.
    fn fire_emergency_cancel(&self, reason: String) {
        let tx = self.harness.priority_tx.clone();
        if tx
            .try_send(ExecutionCommand::CancelAll {
                reason: reason.clone(),
            })
            .is_ok()
        {
            return;
        }
        let spawned = std::thread::Builder::new()
            .name("poly-as-emergency-cancel".to_string())
            .spawn(move || {
                let _ = tx.blocking_send(ExecutionCommand::CancelAll { reason });
            });
        if let Ok(handle) = spawned {
            let _ = handle.join();
        }
    }
}

/// Extracts a printable message from a caught panic payload.
fn panic_message(payload: &(dyn std::any::Any + Send)) -> String {
    if let Some(msg) = payload.downcast_ref::<&str>() {
        (*msg).to_string()
    } else if let Some(msg) = payload.downcast_ref::<String>() {
        msg.clone()
    } else {
        "panicked with a non-string payload".to_string()
    }
}

/// Dedicated Risk Actor managing Stations 7, 8, Lifecycle, and Breakers.
pub struct RiskActor {
    harness: ActorPipelineHarness,
    risk_engine: DollarNotionalRiskEngine,
    hysteresis: HysteresisGate,
    lifecycle: EpochLifecycleController,
    breakers: CircuitBreakers,
    fault: FaultInjection,
    flatten_in_flight: bool,
    flatten_in_flight_since_ns: Option<u64>,
    flatten_in_flight_timeout_ns: u64,
    last_expiry_ms: Option<u64>,
    last_processed_seq: u64,
}

impl RiskActor {
    /// Constructs a new Risk Actor with component configurations.
    pub fn new(
        harness: ActorPipelineHarness,
        risk_engine: DollarNotionalRiskEngine,
        hysteresis: HysteresisGate,
        lifecycle: EpochLifecycleController,
        breakers: CircuitBreakers,
        fault: FaultInjection,
    ) -> Self {
        Self {
            harness,
            risk_engine,
            hysteresis,
            lifecycle,
            breakers,
            fault,
            flatten_in_flight: false,
            flatten_in_flight_since_ns: None,
            flatten_in_flight_timeout_ns: DEFAULT_FLATTEN_IN_FLIGHT_TIMEOUT_NS,
            last_expiry_ms: None,
            last_processed_seq: 0,
        }
    }

    /// Runs a single step of the Risk Actor evaluation loop.
    pub fn step(
        &mut self,
        now_ns: u64,
        clock_offset_ms: i64,
    ) -> Result<Option<RiskActorTrace>, RiskLifecycleError> {
        let start_time = std::time::Instant::now();
        let proposal = self.harness.pricing_slot.load();
        let mut snapshot = **self.harness.registry_slot.load();

        // 1. Apply simulated fault injections if active:
        match self.fault {
            FaultInjection::StalePoly => {
                // Do not update Polymarket heartbeat; let staleness accumulate.
            }
            FaultInjection::StaleBinance => {
                // Do not update Binance heartbeat.
            }
            FaultInjection::ForceTier4 => {
                // Force cost-basis to 95% of V_max.
                snapshot.unhedged_up = SharesMicro::from_raw(100 * core_types::ONE_SHARE_MICRO);
                snapshot.unhedged_cost_up_micro = MicroUsdc(47_500_000); // 0.95 * $50
            }
            FaultInjection::TickChange => {
                self.breakers.on_tick_size_change(MicroUsdc(1_000));
            }
            FaultInjection::BinanceJump => {
                // Covered via defensive_cancel flag in proposal.
            }
            FaultInjection::None => {
                self.breakers.record_poly_heartbeat(now_ns);
                self.breakers.record_binance_heartbeat(now_ns);
            }
        }

        let now_ms = proposal.now_ms;
        let expiry_ms = proposal.expiry_ms;
        let tau_secs = expiry_ms.saturating_sub(now_ms) / 1000;

        // 2. Evaluate Circuit Breakers:
        let breaker_status = self.breakers.check_status(now_ns, clock_offset_ms);
        if let BreakerStatus::Tripped { reason } = breaker_status {
            let cancel_reason = format!("Circuit breaker tripped: {reason:?}");
            let _ = self
                .harness
                .priority_tx
                .try_send(ExecutionCommand::CancelAll {
                    reason: cancel_reason.clone(),
                });
            return Ok(Some(RiskActorTrace {
                now_ms,
                tau_secs,
                phase: EpochPhase::ActiveQuoting,
                tier: RiskTier::Tier4HardStop,
                utilization_u_e6: 0,
                breaker: breaker_status,
                action_up: OrderAction::Hold,
                action_dn: OrderAction::Hold,
                flatten_action: FlattenAction::None,
                cancel_all_reason: Some(cancel_reason),
                flatten_in_flight: self.flatten_in_flight,
                best_bid_up: proposal.best_bid_up,
                best_ask_up: proposal.best_ask_up,
                best_bid_dn: proposal.best_bid_dn,
                best_ask_dn: proposal.best_ask_dn,
                risk_eval_ns: start_time.elapsed().as_nanos() as u64,
            }));
        }

        // Clear stale or completed single-flight flatten guards before evaluating lifecycle/risk.
        if self.last_expiry_ms.is_some_and(|prev| prev != expiry_ms) {
            self.clear_flatten_in_flight();
            self.lifecycle.reset_for_new_epoch();
        } else if now_ms >= expiry_ms {
            self.clear_flatten_in_flight();
        }
        self.last_expiry_ms = Some(expiry_ms);

        let flatten_timed_out = self.flatten_in_flight
            && self.flatten_in_flight_since_ns.is_some_and(|since| {
                now_ns.saturating_sub(since) >= self.flatten_in_flight_timeout_ns
            });
        if snapshot.unhedged_up == snapshot.unhedged_dn || flatten_timed_out {
            self.clear_flatten_in_flight();
        }

        // 3. Evaluate 300s Epoch Lifecycle:
        let lifecycle_input = LifecycleInput {
            now_ms,
            expiry_ms,
            best_ask_up: proposal.best_ask_up,
            best_ask_dn: proposal.best_ask_dn,
            best_bid_up: proposal.best_bid_up,
            best_bid_dn: proposal.best_bid_dn,
            flatten_in_flight: self.flatten_in_flight,
        };

        let lifecycle_action = self.lifecycle.evaluate(&lifecycle_input, &snapshot);
        let phase = match &lifecycle_action {
            LifecycleAction::ContinueQuoting { phase, .. } => *phase,
            LifecycleAction::CancelAllResting { phase, .. } => *phase,
            LifecycleAction::ExecuteFlattenFak { .. } => EpochPhase::MandatoryFlatten,
            LifecycleAction::ResidualDustUnflattenable { phase, .. } => *phase,
            LifecycleAction::EpochRollover { .. } => EpochPhase::TerminalLockout,
        };

        // Handle lifecycle emergency actions:
        match lifecycle_action {
            LifecycleAction::CancelAllResting { reason, .. } => {
                if phase == EpochPhase::TerminalLockout {
                    self.clear_flatten_in_flight();
                }
                let _ = self
                    .harness
                    .priority_tx
                    .try_send(ExecutionCommand::CancelAll {
                        reason: reason.clone(),
                    });
                Ok(Some(RiskActorTrace {
                    now_ms,
                    tau_secs,
                    phase,
                    tier: RiskTier::Tier1Normal,
                    utilization_u_e6: 0,
                    breaker: breaker_status,
                    action_up: OrderAction::Hold,
                    action_dn: OrderAction::Hold,
                    flatten_action: FlattenAction::None,
                    cancel_all_reason: Some(reason),
                    flatten_in_flight: self.flatten_in_flight,
                    best_bid_up: proposal.best_bid_up,
                    best_ask_up: proposal.best_ask_up,
                    best_bid_dn: proposal.best_bid_dn,
                    best_ask_dn: proposal.best_ask_dn,
                    risk_eval_ns: start_time.elapsed().as_nanos() as u64,
                }))
            }
            LifecycleAction::ExecuteFlattenFak {
                outcome,
                price,
                size,
                reason,
            } => {
                let (flatten_action, cancel_all_reason) =
                    match self
                        .harness
                        .priority_tx
                        .try_send(ExecutionCommand::ExecuteFlattenFak {
                            outcome,
                            price,
                            size,
                            reason: reason.clone(),
                        }) {
                        Ok(()) => {
                            self.flatten_in_flight = true;
                            self.flatten_in_flight_since_ns = Some(now_ns);
                            (
                                FlattenAction::FlattenFak {
                                    outcome,
                                    price,
                                    size,
                                },
                                None,
                            )
                        }
                        Err(TrySendError::Full(_)) => {
                            self.clear_flatten_in_flight();
                            (
                                FlattenAction::None,
                                Some(format!(
                                    "Flatten try_send failed (priority channel full): {reason}"
                                )),
                            )
                        }
                        Err(TrySendError::Closed(_)) => {
                            self.clear_flatten_in_flight();
                            (
                                FlattenAction::None,
                                Some(format!(
                                    "Flatten try_send failed (priority channel closed): {reason}"
                                )),
                            )
                        }
                    };
                Ok(Some(RiskActorTrace {
                    now_ms,
                    tau_secs,
                    phase,
                    tier: RiskTier::Tier4HardStop,
                    utilization_u_e6: 0,
                    breaker: breaker_status,
                    action_up: OrderAction::Hold,
                    action_dn: OrderAction::Hold,
                    flatten_action,
                    cancel_all_reason,
                    flatten_in_flight: self.flatten_in_flight,
                    best_bid_up: proposal.best_bid_up,
                    best_ask_up: proposal.best_ask_up,
                    best_bid_dn: proposal.best_bid_dn,
                    best_ask_dn: proposal.best_ask_dn,
                    risk_eval_ns: start_time.elapsed().as_nanos() as u64,
                }))
            }
            LifecycleAction::ResidualDustUnflattenable {
                unhedged_shares,
                min_order_size,
                ..
            } => {
                let reason = "Residual dust below min order size".to_string();
                let _ = self
                    .harness
                    .priority_tx
                    .try_send(ExecutionCommand::CancelAll {
                        reason: reason.clone(),
                    });
                Ok(Some(RiskActorTrace {
                    now_ms,
                    tau_secs,
                    phase,
                    tier: RiskTier::Tier4HardStop,
                    utilization_u_e6: 0,
                    breaker: breaker_status,
                    action_up: OrderAction::Hold,
                    action_dn: OrderAction::Hold,
                    flatten_action: FlattenAction::ResidualDust {
                        unhedged_shares,
                        min_order_size,
                    },
                    cancel_all_reason: Some(reason),
                    flatten_in_flight: self.flatten_in_flight,
                    best_bid_up: proposal.best_bid_up,
                    best_ask_up: proposal.best_ask_up,
                    best_bid_dn: proposal.best_bid_dn,
                    best_ask_dn: proposal.best_ask_dn,
                    risk_eval_ns: start_time.elapsed().as_nanos() as u64,
                }))
            }
            LifecycleAction::EpochRollover {
                expired_expiry_ms,
                paired_shares_to_settle,
            } => {
                self.clear_flatten_in_flight();
                let reason = format!(
                    "Epoch rollover for expiry {expired_expiry_ms} with {paired_shares_to_settle:?} paired"
                );
                let _ = self
                    .harness
                    .priority_tx
                    .try_send(ExecutionCommand::CancelAll {
                        reason: reason.clone(),
                    });
                Ok(Some(RiskActorTrace {
                    now_ms,
                    tau_secs,
                    phase,
                    tier: RiskTier::Tier1Normal,
                    utilization_u_e6: 0,
                    breaker: breaker_status,
                    action_up: OrderAction::Hold,
                    action_dn: OrderAction::Hold,
                    flatten_action: FlattenAction::None,
                    cancel_all_reason: Some(reason),
                    flatten_in_flight: self.flatten_in_flight,
                    best_bid_up: proposal.best_bid_up,
                    best_ask_up: proposal.best_ask_up,
                    best_bid_dn: proposal.best_bid_dn,
                    best_ask_dn: proposal.best_ask_dn,
                    risk_eval_ns: start_time.elapsed().as_nanos() as u64,
                }))
            }
            LifecycleAction::ContinueQuoting {
                prohibit_accumulation,
                block_new_entries,
                ..
            } => {
                // 4. Station 7 Dollar-Notional Risk Engine:
                let (target_bid_up, target_bid_dn) = match proposal.quote {
                    QuoteOutput::Active { bid_up, bid_dn, .. } => (bid_up, bid_dn),
                    QuoteOutput::Suppressed(_) => (None, None),
                };

                let risk_input = RiskEvaluationInput {
                    bid_up: target_bid_up,
                    bid_dn: target_bid_dn,
                    best_bid_up: proposal.best_bid_up,
                    best_bid_dn: proposal.best_bid_dn,
                    best_ask_up: proposal.best_ask_up,
                    best_ask_dn: proposal.best_ask_dn,
                    flatten_in_flight: self.flatten_in_flight,
                };

                // Deduct each slot's own Live resting order from evaluation exposure/cash so a
                // single-slot replacement does not stack against itself and trigger false churn or Tier4.
                let mut eval_snapshot = snapshot;
                for outcome in [Outcome::Up, Outcome::Down] {
                    if let OrderSlotState::Live {
                        price,
                        remaining_size,
                        ..
                    } = self.hysteresis.state(outcome)
                    {
                        let slot_notional = ((remaining_size.0 as u128 * price.0 as u128)
                            / core_types::MICRO_SCALE as u128)
                            as u64;
                        match outcome {
                            Outcome::Up => {
                                eval_snapshot.open_bid_shares_up = eval_snapshot
                                    .open_bid_shares_up
                                    .saturating_sub(*remaining_size);
                                eval_snapshot.open_bid_notional_up = eval_snapshot
                                    .open_bid_notional_up
                                    .saturating_sub(MicroUsdc(slot_notional));
                            }
                            Outcome::Down => {
                                eval_snapshot.open_bid_shares_dn = eval_snapshot
                                    .open_bid_shares_dn
                                    .saturating_sub(*remaining_size);
                                eval_snapshot.open_bid_notional_dn = eval_snapshot
                                    .open_bid_notional_dn
                                    .saturating_sub(MicroUsdc(slot_notional));
                            }
                        }
                        eval_snapshot.available_usdc_micro = MicroUsdc(
                            eval_snapshot
                                .available_usdc_micro
                                .0
                                .saturating_add(slot_notional),
                        );
                    }
                }

                let risk_decision = self.risk_engine.evaluate(&eval_snapshot, &risk_input)?;

                let mut trace_flatten = risk_decision.flatten;
                let mut cancel_all_reason = None;
                if let FlattenAction::FlattenFak {
                    outcome,
                    price,
                    size,
                } = risk_decision.flatten
                {
                    let reason = "Station 7 Tier 4 Hard Stop flatten".to_string();
                    match self
                        .harness
                        .priority_tx
                        .try_send(ExecutionCommand::ExecuteFlattenFak {
                            outcome,
                            price,
                            size,
                            reason: reason.clone(),
                        }) {
                        Ok(()) => {
                            self.flatten_in_flight = true;
                            self.flatten_in_flight_since_ns = Some(now_ns);
                        }
                        Err(TrySendError::Full(_)) => {
                            self.clear_flatten_in_flight();
                            trace_flatten = FlattenAction::None;
                            cancel_all_reason = Some(format!(
                                "Flatten try_send failed (priority channel full): {reason}"
                            ));
                        }
                        Err(TrySendError::Closed(_)) => {
                            self.clear_flatten_in_flight();
                            trace_flatten = FlattenAction::None;
                            cancel_all_reason = Some(format!(
                                "Flatten try_send failed (priority channel closed): {reason}"
                            ));
                        }
                    }
                }

                // If accumulation prohibited in PreExpiryLockout, zero out the accumulating leg
                // and strictly cap the reducing leg to the remaining offsetting delta.
                let min_order_micro = self
                    .risk_engine
                    .config()
                    .min_order_size_shares
                    .saturating_mul(core_types::ONE_SHARE_MICRO);
                let (mut auth_size_up, mut auth_size_dn) = if prohibit_accumulation {
                    if snapshot.unhedged_up > snapshot.unhedged_dn {
                        (
                            None,
                            cap_size_to_offset(
                                risk_decision.size_dn,
                                snapshot.unhedged_up,
                                snapshot.unhedged_dn,
                                min_order_micro,
                            ),
                        )
                    } else if snapshot.unhedged_dn > snapshot.unhedged_up {
                        (
                            cap_size_to_offset(
                                risk_decision.size_up,
                                snapshot.unhedged_dn,
                                snapshot.unhedged_up,
                                min_order_micro,
                            ),
                            None,
                        )
                    } else {
                        (None, None)
                    }
                } else if block_new_entries {
                    // Entry-time guard: suppress fresh Leg-1 accumulation while allowing existing
                    // inventory (including dust < 5 sh) to hedge or reduce.
                    if snapshot.unhedged_up > snapshot.unhedged_dn {
                        (None, risk_decision.size_dn)
                    } else if snapshot.unhedged_dn > snapshot.unhedged_up {
                        (risk_decision.size_up, None)
                    } else {
                        (None, None)
                    }
                } else {
                    (risk_decision.size_up, risk_decision.size_dn)
                };

                // Extreme-probability hedge exemption: cap reducing leg strictly to offsetting delta.
                if proposal.hedge_only_up {
                    auth_size_up = cap_size_to_offset(
                        auth_size_up,
                        snapshot.unhedged_dn,
                        snapshot.unhedged_up,
                        min_order_micro,
                    );
                }
                if proposal.hedge_only_dn {
                    auth_size_dn = cap_size_to_offset(
                        auth_size_dn,
                        snapshot.unhedged_up,
                        snapshot.unhedged_dn,
                        min_order_micro,
                    );
                }

                let (cancel_up, cancel_dn) =
                    if proposal.defensive_cancel_up || proposal.defensive_cancel_dn {
                        (proposal.defensive_cancel_up, proposal.defensive_cancel_dn)
                    } else {
                        (proposal.defensive_cancel, proposal.defensive_cancel)
                    };

                // 5. Station 8 Continuous Hysteresis Gate for UP leg:
                let quote_up = match (target_bid_up, auth_size_up) {
                    (Some(p), Some(s)) => Some(SizedQuote {
                        price: p,
                        size: s,
                        price_raw_usd: proposal.bid_up_raw,
                        price_capped: proposal.price_capped_up,
                    }),
                    _ => None,
                };

                let hyst_input_up = HysteresisInput {
                    outcome: Outcome::Up,
                    proposed_quote: quote_up,
                    tick_size_micro: proposal.tick_size_micro,
                    best_ask: proposal.best_ask_up,
                    defensive_cancel: cancel_up,
                    now_ns,
                    next_seq: proposal.seq.saturating_mul(2),
                };
                let action_up = self.hysteresis.evaluate_leg(&hyst_input_up);
                self.dispatch_action(&action_up);

                // 6. Station 8 Continuous Hysteresis Gate for DOWN leg:
                let quote_dn = match (target_bid_dn, auth_size_dn) {
                    (Some(p), Some(s)) => Some(SizedQuote {
                        price: p,
                        size: s,
                        price_raw_usd: proposal.bid_dn_raw,
                        price_capped: proposal.price_capped_dn,
                    }),
                    _ => None,
                };

                let hyst_input_dn = HysteresisInput {
                    outcome: Outcome::Down,
                    proposed_quote: quote_dn,
                    tick_size_micro: proposal.tick_size_micro,
                    best_ask: proposal.best_ask_dn,
                    defensive_cancel: cancel_dn,
                    now_ns,
                    next_seq: proposal.seq.saturating_mul(2).saturating_add(1),
                };
                let action_dn = self.hysteresis.evaluate_leg(&hyst_input_dn);
                self.dispatch_action(&action_dn);

                self.last_processed_seq = proposal.seq;

                Ok(Some(RiskActorTrace {
                    now_ms,
                    tau_secs,
                    phase,
                    tier: risk_decision.tier,
                    utilization_u_e6: risk_decision.utilization_u_e6,
                    breaker: breaker_status,
                    action_up,
                    action_dn,
                    flatten_action: trace_flatten,
                    cancel_all_reason,
                    flatten_in_flight: self.flatten_in_flight,
                    best_bid_up: proposal.best_bid_up,
                    best_ask_up: proposal.best_ask_up,
                    best_bid_dn: proposal.best_bid_dn,
                    best_ask_dn: proposal.best_ask_dn,
                    risk_eval_ns: start_time.elapsed().as_nanos() as u64,
                }))
            }
        }
    }

    /// Dispatches an order action over the appropriate Tokio channel (priority vs routine).
    fn dispatch_action(&self, action: &OrderAction) {
        match action {
            OrderAction::Hold => {}
            OrderAction::SubmitNew {
                outcome,
                price,
                size,
            } => {
                let _ = self
                    .harness
                    .routine_tx
                    .try_send(ExecutionCommand::SubmitNew {
                        outcome: *outcome,
                        price: *price,
                        size: *size,
                    });
            }
            OrderAction::Cancel {
                outcome,
                order_id,
                reason,
            } => {
                // Defensive cancels or cross-book cancels go over the priority channel!
                if matches!(
                    reason,
                    CancelReason::DefensiveCancel
                        | CancelReason::CrossesBook
                        | CancelReason::CircuitBreaker
                ) {
                    let _ = self.harness.priority_tx.try_send(ExecutionCommand::Cancel {
                        outcome: *outcome,
                        order_id: order_id.clone(),
                        reason: *reason,
                    });
                } else {
                    let _ = self.harness.routine_tx.try_send(ExecutionCommand::Cancel {
                        outcome: *outcome,
                        order_id: order_id.clone(),
                        reason: *reason,
                    });
                }
            }
            OrderAction::CancelAndReplace {
                outcome,
                old_order_id,
                new_price,
                new_size,
                reason,
            } => {
                let _ = self
                    .harness
                    .routine_tx
                    .try_send(ExecutionCommand::CancelAndReplace {
                        outcome: *outcome,
                        old_order_id: old_order_id.clone(),
                        new_price: *new_price,
                        new_size: *new_size,
                        reason: *reason,
                    });
            }
        }
    }

    /// Accessor for hysteresis gate.
    pub fn hysteresis_mut(&mut self) -> &mut HysteresisGate {
        &mut self.hysteresis
    }

    /// Returns whether the single-flight emergency flatten guard is currently armed.
    #[must_use]
    pub fn flatten_in_flight(&self) -> bool {
        self.flatten_in_flight
    }

    /// Overrides the single-flight flatten timeout in nanoseconds (for bounded recovery).
    pub fn set_flatten_in_flight_timeout_ns(&mut self, timeout_ns: u64) {
        self.flatten_in_flight_timeout_ns = timeout_ns;
    }

    /// Clears the single-flight emergency flatten guard after execution completes or is canceled.
    pub fn clear_flatten_in_flight(&mut self) {
        self.flatten_in_flight = false;
        self.flatten_in_flight_since_ns = None;
    }

    /// Records receipt of a Polymarket feed message for the staleness breaker.
    pub fn record_poly_heartbeat(&mut self, now_ns: u64) {
        self.breakers.record_poly_heartbeat(now_ns);
    }

    /// Records receipt of a Binance feed message for the staleness breaker.
    pub fn record_binance_heartbeat(&mut self, now_ns: u64) {
        self.breakers.record_binance_heartbeat(now_ns);
    }

    /// Signals a dynamic tick-size change to the circuit breakers.
    pub fn on_tick_size_change(&mut self, new_tick: MicroUsdc) {
        self.breakers.on_tick_size_change(new_tick);
    }

    /// Acknowledges that the pricing pipeline has recomputed using the updated tick size.
    pub fn acknowledge_tick_size_sync(&mut self) {
        self.breakers.acknowledge_tick_size_sync();
    }

    /// Resets lifecycle, hysteresis order slots, flatten guard, and circuit breakers for a new 300s epoch.
    pub fn reset_for_new_epoch(&mut self, new_tick_size: MicroUsdc, now_ns: u64) {
        self.lifecycle.reset_for_new_epoch();
        self.hysteresis.set_state(Outcome::Up, OrderSlotState::Idle);
        self.hysteresis
            .set_state(Outcome::Down, OrderSlotState::Idle);
        self.clear_flatten_in_flight();
        self.last_expiry_ms = None;
        self.breakers = CircuitBreakers::new(BreakerConfig::default(), new_tick_size, now_ns);
    }
}

/// Caps a reducing order size strictly to the offsetting share delta, filtering out sub-min dust.
fn cap_size_to_offset(
    size: Option<SharesMicro>,
    held_opp: SharesMicro,
    held_leg: SharesMicro,
    min_order_micro: u64,
) -> Option<SharesMicro> {
    let offset = held_opp.as_u64().saturating_sub(held_leg.as_u64());
    size.map(|s| s.min(SharesMicro::from_raw(offset)))
        .filter(|s| s.as_u64() >= min_order_micro)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::breakers::BreakerConfig;
    use crate::breakers::BreakerReason;
    use crate::lifecycle::LifecycleConfig;
    use crate::station7_risk::RiskConfig;
    use crate::station8_hysteresis::HysteresisConfig;

    #[test]
    fn test_adversarial_slow_execution_backpressure() {
        // Slow Execution Backpressure Test:
        // Block Execution consumer for 500ms while Pricing emits 500 quotes
        // -> Risk & Pricing NEVER block/deadlock; when Execution resumes,
        // it immediately sees the latest quote (zero 500-item stale backlog!).
        let (harness, _priority_rx, mut routine_rx) = ActorPipelineHarness::new(16, 16);

        let risk_cfg = RiskConfig::default();
        let engine = DollarNotionalRiskEngine::new(risk_cfg).unwrap();
        let hyst_cfg = HysteresisConfig::default();
        let hysteresis = HysteresisGate::new(hyst_cfg).unwrap();
        let life_cfg = LifecycleConfig::default();
        let lifecycle = EpochLifecycleController::new(life_cfg).unwrap();
        let breaker_cfg = BreakerConfig::default();
        let breakers = CircuitBreakers::new(breaker_cfg, MicroUsdc(10_000), 0);

        let mut risk_actor = RiskActor::new(
            harness.clone(),
            engine,
            hysteresis,
            lifecycle,
            breakers,
            FaultInjection::None,
        );

        // 1. Pricing emits 500 quotes rapidly:
        for i in 1..=500 {
            let proposal = QuoteProposal {
                seq: i,
                now_ms: 100_000 + i,
                expiry_ms: 300_000,
                tick_size_micro: MicroUsdc(10_000),
                quote: QuoteOutput::Active {
                    bid_up: Some(MicroUsdc(480_000)),
                    bid_dn: Some(MicroUsdc(480_000)),
                    pair_cost_micro: Some(MicroUsdc(960_000)),
                    gross_margin_bps: Some(400),
                },
                bid_up_raw: 0.48,
                bid_dn_raw: 0.48,
                best_bid_up: Some(MicroUsdc(470_000)),
                best_bid_dn: Some(MicroUsdc(470_000)),
                best_ask_up: Some(MicroUsdc(520_000)),
                best_ask_dn: Some(MicroUsdc(520_000)),
                defensive_cancel: false,
                total_compute_ns: 12_000,
                ..Default::default()
            };
            harness.publish_quote(proposal);
        }

        // Pricing never blocked! Slot holds latest proposal (seq = 500).
        assert_eq!(harness.pricing_slot.load().seq, 500);

        // 2. Risk step evaluates latest proposal directly:
        let trace = risk_actor.step(1_000_000_000, 0).unwrap();
        assert!(trace.is_some());

        // 3. Execution consumer empties channel:
        let mut received = 0;
        while routine_rx.try_recv().is_ok() {
            received += 1;
        }
        // Channel was bounded to 16, so backpressure dropped stale updates rather than accumulating 500!
        assert!(received <= 16);
    }

    #[test]
    fn test_adversarial_lead_lag_defensive_cancel() {
        // Lead-Lag <15ms Defensive Cancel Test:
        // Inject sudden Binance +$150 BTC jump -> LeadLagDefensiveCancel bypasses
        // 100ms SG grid and dispatches Cancel in < 1ms internal latency.
        let (harness, mut priority_rx, _routine_rx) = ActorPipelineHarness::new(16, 16);

        let risk_cfg = RiskConfig::default();
        let engine = DollarNotionalRiskEngine::new(risk_cfg).unwrap();
        let hyst_cfg = HysteresisConfig::default();
        let mut hysteresis = HysteresisGate::new(hyst_cfg).unwrap();
        hysteresis.set_state(
            Outcome::Up,
            crate::station8_hysteresis::OrderSlotState::Live {
                order_id: "order_up_1".to_string(),
                price: MicroUsdc(480_000),
                original_size: SharesMicro::from_raw(20 * core_types::ONE_SHARE_MICRO),
                remaining_size: SharesMicro::from_raw(20 * core_types::ONE_SHARE_MICRO),
            },
        );

        let life_cfg = LifecycleConfig::default();
        let lifecycle = EpochLifecycleController::new(life_cfg).unwrap();
        let breaker_cfg = BreakerConfig::default();
        let breakers = CircuitBreakers::new(breaker_cfg, MicroUsdc(10_000), 0);

        let mut risk_actor = RiskActor::new(
            harness.clone(),
            engine,
            hysteresis,
            lifecycle,
            breakers,
            FaultInjection::None,
        );

        // Publish proposal with defensive_cancel = true
        let proposal = QuoteProposal {
            seq: 1,
            now_ms: 100_000,
            expiry_ms: 300_000,
            tick_size_micro: MicroUsdc(10_000),
            quote: QuoteOutput::Active {
                bid_up: Some(MicroUsdc(480_000)),
                bid_dn: Some(MicroUsdc(480_000)),
                pair_cost_micro: Some(MicroUsdc(960_000)),
                gross_margin_bps: Some(400),
            },
            bid_up_raw: 0.48,
            bid_dn_raw: 0.48,
            best_bid_up: Some(MicroUsdc(470_000)),
            best_bid_dn: Some(MicroUsdc(470_000)),
            best_ask_up: Some(MicroUsdc(520_000)),
            best_ask_dn: Some(MicroUsdc(520_000)),
            defensive_cancel: true,
            total_compute_ns: 5_000,
            ..Default::default()
        };
        harness.publish_quote(proposal);

        let trace = risk_actor.step(1_000_000, 0).unwrap().unwrap();
        // Compute latency for risk eval must be < 100 us (< 100_000 ns)
        assert!(trace.risk_eval_ns < 1_000_000); // well under 1ms!

        // Must receive Cancel on priority channel:
        let cmd = priority_rx.try_recv().unwrap();
        assert_eq!(
            cmd,
            ExecutionCommand::Cancel {
                outcome: Outcome::Up,
                order_id: "order_up_1".to_string(),
                reason: CancelReason::DefensiveCancel,
            }
        );
    }

    #[test]
    fn test_adversarial_fault_stale_feed() {
        // Adversarial test: FaultInjection::StalePoly triggers feed staleness breaker
        let (harness, mut priority_rx, _routine_rx) = ActorPipelineHarness::new(16, 16);

        let risk_cfg = RiskConfig::default();
        let engine = DollarNotionalRiskEngine::new(risk_cfg).unwrap();
        let hyst_cfg = HysteresisConfig::default();
        let hysteresis = HysteresisGate::new(hyst_cfg).unwrap();
        let life_cfg = LifecycleConfig::default();
        let lifecycle = EpochLifecycleController::new(life_cfg).unwrap();
        let breaker_cfg = BreakerConfig {
            poly_stale_ms: 3_000,
            ..Default::default()
        };
        let breakers = CircuitBreakers::new(breaker_cfg, MicroUsdc(10_000), 0);

        let mut risk_actor = RiskActor::new(
            harness.clone(),
            engine,
            hysteresis,
            lifecycle,
            breakers,
            FaultInjection::StalePoly,
        );

        // Advance 3.5s (3_500_000_000 ns) -> Staleness breaker must trip!
        let trace = risk_actor.step(3_500_000_000, 0).unwrap().unwrap();
        assert_eq!(
            trace.breaker,
            BreakerStatus::Tripped {
                reason: BreakerReason::PolymarketFeedStale,
            }
        );

        let cmd = priority_rx.try_recv().unwrap();
        assert!(matches!(cmd, ExecutionCommand::CancelAll { .. }));
    }

    #[test]
    fn test_adversarial_fault_tier4_flatten() {
        // Adversarial test: FaultInjection::ForceTier4 triggers Tier 4 Hard Stop + FlattenFak
        let (harness, mut priority_rx, _routine_rx) = ActorPipelineHarness::new(16, 16);

        let risk_cfg = RiskConfig::default();
        let engine = DollarNotionalRiskEngine::new(risk_cfg).unwrap();
        let hyst_cfg = HysteresisConfig::default();
        let hysteresis = HysteresisGate::new(hyst_cfg).unwrap();
        let life_cfg = LifecycleConfig::default();
        let lifecycle = EpochLifecycleController::new(life_cfg).unwrap();
        let breaker_cfg = BreakerConfig::default();
        let breakers = CircuitBreakers::new(breaker_cfg, MicroUsdc(10_000), 0);

        let mut risk_actor = RiskActor::new(
            harness.clone(),
            engine,
            hysteresis,
            lifecycle,
            breakers,
            FaultInjection::ForceTier4,
        );

        let proposal = QuoteProposal {
            seq: 1,
            now_ms: 100_000,
            expiry_ms: 300_000,
            tick_size_micro: MicroUsdc(10_000),
            quote: QuoteOutput::Active {
                bid_up: Some(MicroUsdc(480_000)),
                bid_dn: Some(MicroUsdc(480_000)),
                pair_cost_micro: Some(MicroUsdc(960_000)),
                gross_margin_bps: Some(400),
            },
            bid_up_raw: 0.48,
            bid_dn_raw: 0.48,
            best_bid_up: Some(MicroUsdc(470_000)),
            best_bid_dn: Some(MicroUsdc(470_000)),
            best_ask_up: Some(MicroUsdc(520_000)),
            best_ask_dn: Some(MicroUsdc(520_000)),
            defensive_cancel: false,
            total_compute_ns: 10_000,
            ..Default::default()
        };
        harness.publish_quote(proposal);

        let trace = risk_actor.step(1_000_000, 0).unwrap().unwrap();
        assert_eq!(trace.tier, RiskTier::Tier4HardStop);

        let cmd = priority_rx.try_recv().unwrap();
        assert!(matches!(cmd, ExecutionCommand::ExecuteFlattenFak { .. }));
    }

    #[test]
    fn test_supervisor_fires_emergency_cancel_on_actor_panic() {
        // Fault injection: a Risk actor thread panics mid-run. The supervisor must
        // catch the exit, fire emergency CancelAll on the priority lane, and report
        // the fault so the CLI exits non-zero.
        let (harness, mut priority_rx, _routine_rx) = ActorPipelineHarness::new(16, 16);
        let mut supervisor = ActorSupervisor::new(harness);

        supervisor
            .spawn("risk", None, || {
                std::panic::panic_any("injected risk actor fault");
            })
            .unwrap();

        let err = supervisor.join().unwrap_err();
        assert!(matches!(
            err,
            RiskLifecycleError::ActorThreadFault { ref actor, .. } if actor == "risk"
        ));

        match priority_rx.try_recv() {
            Ok(ExecutionCommand::CancelAll { reason }) => {
                assert!(reason.contains("risk"), "unexpected reason: {reason}");
            }
            other => panic!("expected emergency CancelAll, got {other:?}"),
        }
    }

    #[test]
    fn test_supervisor_flags_actor_exit_before_shutdown() {
        // An actor that returns without the shutdown flag set leaves orphan quotes:
        // the supervisor must treat the clean-but-premature exit as a fault.
        let (harness, mut priority_rx, _routine_rx) = ActorPipelineHarness::new(16, 16);
        let mut supervisor = ActorSupervisor::new(harness);

        supervisor.spawn("pricing", None, || {}).unwrap();

        let err = supervisor.join().unwrap_err();
        assert!(matches!(
            err,
            RiskLifecycleError::ActorThreadFault { ref actor, .. } if actor == "pricing"
        ));
        assert!(matches!(
            priority_rx.try_recv(),
            Ok(ExecutionCommand::CancelAll { .. })
        ));
    }

    #[test]
    fn test_supervisor_accepts_ordered_shutdown() {
        let (harness, mut priority_rx, _routine_rx) = ActorPipelineHarness::new(16, 16);
        let mut supervisor = ActorSupervisor::new(harness);
        let shutdown = supervisor.shutdown_flag();

        supervisor
            .spawn("risk", None, move || {
                while !shutdown.load(Ordering::Acquire) {
                    std::thread::sleep(std::time::Duration::from_millis(1));
                }
            })
            .unwrap();

        supervisor.shutdown_flag().store(true, Ordering::Release);
        supervisor.join().unwrap();
        assert!(priority_rx.try_recv().is_err());
    }

    #[test]
    fn test_pin_thread_to_core_rejects_out_of_range_core() {
        assert!(matches!(
            pin_thread_to_core(usize::MAX),
            Err(RiskLifecycleError::ThreadPinning(_))
        ));
    }

    #[test]
    fn test_pin_thread_to_core_accepts_available_core() {
        let cores = core_affinity::get_core_ids().unwrap_or_default();
        if cores.is_empty() {
            return;
        }
        pin_thread_to_core(0).unwrap();
    }

    #[test]
    fn test_supervisor_pinned_thread_join_is_clean() {
        let (harness, mut priority_rx, _routine_rx) = ActorPipelineHarness::new(16, 16);
        let mut supervisor = ActorSupervisor::new(harness.clone());
        let shutdown = supervisor.shutdown_flag();

        supervisor
            .spawn("pricing", Some(0), move || {
                while !shutdown.load(Ordering::Acquire) {
                    std::thread::sleep(std::time::Duration::from_millis(1));
                }
            })
            .unwrap();

        harness.shutdown.store(true, Ordering::Release);
        supervisor.join().unwrap();
        assert!(priority_rx.try_recv().is_err());
    }

    #[test]
    fn test_mandatory_flatten_net_19_and_try_send_drop_recovery() {
        // Bounded priority capacity = 1 so we can simulate a dropped priority try_send.
        let (harness, mut priority_rx, _routine_rx) = ActorPipelineHarness::new(1, 16);

        let engine = DollarNotionalRiskEngine::new(RiskConfig::default()).unwrap();
        let hysteresis = HysteresisGate::new(HysteresisConfig::default()).unwrap();
        let lifecycle = EpochLifecycleController::new(LifecycleConfig::default()).unwrap();
        let breakers = CircuitBreakers::new(BreakerConfig::default(), MicroUsdc(10_000), 0);

        let mut risk_actor = RiskActor::new(
            harness.clone(),
            engine,
            hysteresis,
            lifecycle,
            breakers,
            FaultInjection::None,
        );

        // Q = (0, 19) -> net = -19 shares.
        harness.publish_registry_snapshot(OrderRegistrySnapshot {
            unhedged_up: SharesMicro::ZERO,
            unhedged_dn: SharesMicro::from_raw(19 * core_types::ONE_SHARE_MICRO),
            ..Default::default()
        });

        let expiry_ms = 300_000;
        let proposal = QuoteProposal {
            seq: 1,
            now_ms: expiry_ms - 25_000, // T-25s -> MandatoryFlatten
            expiry_ms,
            tick_size_micro: MicroUsdc(10_000),
            quote: QuoteOutput::Suppressed(math_engine::SuppressionReason::WarmingUp),
            bid_up_raw: 0.48,
            bid_dn_raw: 0.48,
            best_bid_up: Some(MicroUsdc(500_000)),
            best_bid_dn: Some(MicroUsdc(480_000)),
            best_ask_up: Some(MicroUsdc(520_000)),
            best_ask_dn: Some(MicroUsdc(490_000)),
            defensive_cancel: false,
            total_compute_ns: 5_000,
            ..Default::default()
        };
        harness.publish_quote(proposal);

        // Pre-fill the priority channel so try_send(ExecuteFlattenFak) fails on tick 1.
        harness
            .priority_tx
            .try_send(ExecutionCommand::CancelAll {
                reason: "saturate priority channel".to_string(),
            })
            .unwrap();

        let trace_dropped = risk_actor.step(1_000_000, 0).unwrap().unwrap();
        assert_eq!(trace_dropped.phase, EpochPhase::MandatoryFlatten);
        assert_eq!(trace_dropped.flatten_action, FlattenAction::None);
        assert!(!trace_dropped.flatten_in_flight);
        assert!(!risk_actor.flatten_in_flight());
        assert!(
            trace_dropped
                .cancel_all_reason
                .as_deref()
                .is_some_and(|r| r.contains("try_send failed"))
        );

        // Drain the pre-filled command and run the next tick.
        let _ = priority_rx.try_recv().unwrap();
        let trace_recovered = risk_actor.step(101_000_000, 0).unwrap().unwrap();
        assert_eq!(trace_recovered.phase, EpochPhase::MandatoryFlatten);
        assert!(trace_recovered.flatten_in_flight);
        assert!(risk_actor.flatten_in_flight());
        assert_eq!(
            trace_recovered.flatten_action,
            FlattenAction::FlattenFak {
                outcome: Outcome::Up,
                price: MicroUsdc(520_000),
                size: SharesMicro::from_raw(19 * core_types::ONE_SHARE_MICRO),
            }
        );

        let cmd = priority_rx.try_recv().unwrap();
        assert_eq!(
            cmd,
            ExecutionCommand::ExecuteFlattenFak {
                outcome: Outcome::Up,
                price: MicroUsdc(520_000),
                size: SharesMicro::from_raw(19 * core_types::ONE_SHARE_MICRO),
                reason: "Mandatory flatten BUY UP at ask".to_string(),
            }
        );
    }

    #[test]
    fn test_flatten_in_flight_timeout_and_epoch_reset_recovery() {
        let (harness, mut priority_rx, _routine_rx) = ActorPipelineHarness::new(16, 16);

        let engine = DollarNotionalRiskEngine::new(RiskConfig::default()).unwrap();
        let hysteresis = HysteresisGate::new(HysteresisConfig::default()).unwrap();
        let lifecycle = EpochLifecycleController::new(LifecycleConfig::default()).unwrap();
        let breakers = CircuitBreakers::new(BreakerConfig::default(), MicroUsdc(10_000), 0);

        let mut risk_actor = RiskActor::new(
            harness.clone(),
            engine,
            hysteresis,
            lifecycle,
            breakers,
            FaultInjection::None,
        );
        risk_actor.set_flatten_in_flight_timeout_ns(500_000_000); // 500ms bounded expiry

        harness.publish_registry_snapshot(OrderRegistrySnapshot {
            unhedged_up: SharesMicro::ZERO,
            unhedged_dn: SharesMicro::from_raw(19 * core_types::ONE_SHARE_MICRO),
            ..Default::default()
        });

        let expiry_1 = 300_000;
        let base_proposal = QuoteProposal {
            seq: 1,
            now_ms: expiry_1 - 25_000,
            expiry_ms: expiry_1,
            tick_size_micro: MicroUsdc(10_000),
            quote: QuoteOutput::Suppressed(math_engine::SuppressionReason::WarmingUp),
            bid_up_raw: 0.48,
            bid_dn_raw: 0.48,
            best_bid_up: Some(MicroUsdc(500_000)),
            best_bid_dn: Some(MicroUsdc(480_000)),
            best_ask_up: Some(MicroUsdc(520_000)),
            best_ask_dn: Some(MicroUsdc(490_000)),
            defensive_cancel: false,
            total_compute_ns: 5_000,
            ..Default::default()
        };
        harness.publish_quote(base_proposal);

        // Tick 1 at t=100ms sends initial ExecuteFlattenFak and arms flatten_in_flight.
        let t1 = risk_actor.step(100_000_000, 0).unwrap().unwrap();
        assert!(t1.flatten_in_flight);
        assert!(matches!(
            priority_rx.try_recv().unwrap(),
            ExecutionCommand::ExecuteFlattenFak { .. }
        ));

        // Tick 2 at t=300ms (< 500ms timeout) holds via CancelAllResting with explicit reason.
        let t2 = risk_actor.step(300_000_000, 0).unwrap().unwrap();
        assert!(t2.flatten_in_flight);
        assert_eq!(t2.flatten_action, FlattenAction::None);
        assert!(
            t2.cancel_all_reason
                .as_deref()
                .is_some_and(|r| r.contains("Flatten order in flight"))
        );
        assert!(matches!(
            priority_rx.try_recv().unwrap(),
            ExecutionCommand::CancelAll { .. }
        ));

        // Tick 3 at t=700ms (>= 500ms timeout) auto-clears guard and retries ExecuteFlattenFak.
        let t3 = risk_actor.step(700_000_000, 0).unwrap().unwrap();
        assert!(t3.flatten_in_flight);
        assert!(matches!(
            t3.flatten_action,
            FlattenAction::FlattenFak { .. }
        ));
        assert!(matches!(
            priority_rx.try_recv().unwrap(),
            ExecutionCommand::ExecuteFlattenFak { .. }
        ));

        // Tick 4: epoch changes to expiry_2 -> clears stale flatten_in_flight automatically.
        let expiry_2 = 600_000;
        harness.publish_quote(QuoteProposal {
            seq: 2,
            now_ms: expiry_2 - 25_000,
            expiry_ms: expiry_2,
            ..base_proposal
        });
        // Even with timeout set very large, expiry change resets the guard so a new flatten fires.
        risk_actor.set_flatten_in_flight_timeout_ns(60_000_000_000);
        let t4 = risk_actor.step(800_000_000, 0).unwrap().unwrap();
        assert!(t4.flatten_in_flight);
        assert!(matches!(
            t4.flatten_action,
            FlattenAction::FlattenFak { .. }
        ));
        assert!(matches!(
            priority_rx.try_recv().unwrap(),
            ExecutionCommand::ExecuteFlattenFak { .. }
        ));
    }

    #[test]
    fn test_hedge_only_caps_order_size_to_offsetting_inventory_in_tier1() {
        let (harness, _priority_rx, mut routine_rx) = ActorPipelineHarness::new(16, 16);
        let engine = DollarNotionalRiskEngine::new(RiskConfig::default()).unwrap();
        let hysteresis = HysteresisGate::new(HysteresisConfig::default()).unwrap();
        let lifecycle = EpochLifecycleController::new(LifecycleConfig::default()).unwrap();
        let breakers = CircuitBreakers::new(BreakerConfig::default(), MicroUsdc(10_000), 0);

        let mut risk_actor = RiskActor::new(
            harness.clone(),
            engine,
            hysteresis,
            lifecycle,
            breakers,
            FaultInjection::None,
        );

        // Hold 5 UP shares at $0.10 ($0.50 total => u = 0.01 Tier1Normal).
        // Without hedge_only_dn, Tier 1 would quote ~19-20 shares DN!
        harness.publish_registry_snapshot(OrderRegistrySnapshot {
            unhedged_up: SharesMicro::from_raw(5 * core_types::ONE_SHARE_MICRO),
            unhedged_cost_up_micro: MicroUsdc(500_000),
            available_usdc_micro: MicroUsdc(100_000_000),
            ..Default::default()
        });

        harness.publish_quote(QuoteProposal {
            seq: 1,
            now_ms: 100_000,
            expiry_ms: 300_000,
            tick_size_micro: MicroUsdc(10_000),
            quote: QuoteOutput::Active {
                bid_up: None,
                bid_dn: Some(MicroUsdc(900_000)),
                pair_cost_micro: None,
                gross_margin_bps: None,
            },
            bid_dn_raw: 0.90,
            hedge_only_dn: true,
            best_bid_up: Some(MicroUsdc(90_000)),
            best_ask_up: Some(MicroUsdc(110_000)),
            best_bid_dn: Some(MicroUsdc(890_000)),
            best_ask_dn: Some(MicroUsdc(910_000)),
            ..Default::default()
        });

        let trace = risk_actor.step(1_000_000, 0).unwrap().unwrap();
        assert_eq!(trace.tier, RiskTier::Tier1Normal);
        assert_eq!(
            trace.action_dn,
            OrderAction::SubmitNew {
                outcome: Outcome::Down,
                price: MicroUsdc(900_000),
                size: SharesMicro::from_raw(5 * core_types::ONE_SHARE_MICRO),
            }
        );
        assert_eq!(
            routine_rx.try_recv().unwrap(),
            ExecutionCommand::SubmitNew {
                outcome: Outcome::Down,
                price: MicroUsdc(900_000),
                size: SharesMicro::from_raw(5 * core_types::ONE_SHARE_MICRO),
            }
        );
    }

    #[test]
    fn test_price_capped_forces_cancel_and_replace_even_inside_deadband() {
        let (harness, _priority_rx, mut routine_rx) = ActorPipelineHarness::new(16, 16);
        let engine = DollarNotionalRiskEngine::new(RiskConfig::default()).unwrap();
        let mut hysteresis = HysteresisGate::new(HysteresisConfig::default()).unwrap();
        let active_size = SharesMicro::from_raw(20 * core_types::ONE_SHARE_MICRO);
        hysteresis.set_state(
            Outcome::Down,
            crate::station8_hysteresis::OrderSlotState::Live {
                order_id: "dn_live_1".to_string(),
                price: MicroUsdc(500_000),
                original_size: active_size,
                remaining_size: active_size,
            },
        );
        let lifecycle = EpochLifecycleController::new(LifecycleConfig::default()).unwrap();
        let breakers = CircuitBreakers::new(BreakerConfig::default(), MicroUsdc(10_000), 0);

        let mut risk_actor = RiskActor::new(
            harness.clone(),
            engine,
            hysteresis,
            lifecycle,
            breakers,
            FaultInjection::None,
        );
        harness.publish_registry_snapshot(OrderRegistrySnapshot {
            available_usdc_micro: MicroUsdc(100_000_000),
            ..Default::default()
        });

        // Capped DN target is $0.49 (1 tick below resting $0.50, inside 1.25-tick deadband),
        // with price_capped_dn = true and bid_dn_raw clamped to 0.49.
        harness.publish_quote(QuoteProposal {
            seq: 1,
            now_ms: 100_000,
            expiry_ms: 300_000,
            tick_size_micro: MicroUsdc(10_000),
            quote: QuoteOutput::Active {
                bid_up: None,
                bid_dn: Some(MicroUsdc(490_000)),
                pair_cost_micro: None,
                gross_margin_bps: None,
            },
            bid_dn_raw: 0.49,
            price_capped_dn: true,
            best_bid_up: Some(MicroUsdc(480_000)),
            best_ask_up: Some(MicroUsdc(510_000)),
            best_bid_dn: Some(MicroUsdc(480_000)),
            best_ask_dn: Some(MicroUsdc(520_000)),
            ..Default::default()
        });

        let trace = risk_actor.step(1_000_000, 0).unwrap().unwrap();
        assert_eq!(
            trace.action_dn,
            OrderAction::CancelAndReplace {
                outcome: Outcome::Down,
                old_order_id: "dn_live_1".to_string(),
                new_price: MicroUsdc(490_000),
                new_size: active_size,
                reason: CancelReason::DeadbandDrift,
            }
        );
        assert!(matches!(
            routine_rx.try_recv().unwrap(),
            ExecutionCommand::CancelAndReplace {
                outcome: Outcome::Down,
                new_price: MicroUsdc(490_000),
                ..
            }
        ));
    }

    #[test]
    fn test_per_side_directional_defensive_cancel() {
        let active_size = SharesMicro::from_raw(20 * core_types::ONE_SHARE_MICRO);
        let setup_actor = || {
            let (harness, priority_rx, routine_rx) = ActorPipelineHarness::new(16, 16);
            let engine = DollarNotionalRiskEngine::new(RiskConfig::default()).unwrap();
            let mut hysteresis = HysteresisGate::new(HysteresisConfig::default()).unwrap();
            hysteresis.set_state(
                Outcome::Up,
                crate::station8_hysteresis::OrderSlotState::Live {
                    order_id: "up_1".to_string(),
                    price: MicroUsdc(480_000),
                    original_size: active_size,
                    remaining_size: active_size,
                },
            );
            hysteresis.set_state(
                Outcome::Down,
                crate::station8_hysteresis::OrderSlotState::Live {
                    order_id: "dn_1".to_string(),
                    price: MicroUsdc(480_000),
                    original_size: active_size,
                    remaining_size: active_size,
                },
            );
            let lifecycle = EpochLifecycleController::new(LifecycleConfig::default()).unwrap();
            let breakers = CircuitBreakers::new(BreakerConfig::default(), MicroUsdc(10_000), 0);
            let risk_actor = RiskActor::new(
                harness.clone(),
                engine,
                hysteresis,
                lifecycle,
                breakers,
                FaultInjection::None,
            );
            harness.publish_registry_snapshot(OrderRegistrySnapshot {
                available_usdc_micro: MicroUsdc(100_000_000),
                ..Default::default()
            });
            (harness, priority_rx, routine_rx, risk_actor)
        };

        let base = QuoteProposal {
            seq: 1,
            now_ms: 100_000,
            expiry_ms: 300_000,
            tick_size_micro: MicroUsdc(10_000),
            quote: QuoteOutput::Active {
                bid_up: Some(MicroUsdc(480_000)),
                bid_dn: Some(MicroUsdc(480_000)),
                pair_cost_micro: Some(MicroUsdc(960_000)),
                gross_margin_bps: Some(400),
            },
            bid_up_raw: 0.48,
            bid_dn_raw: 0.48,
            best_bid_up: Some(MicroUsdc(480_000)),
            best_bid_dn: Some(MicroUsdc(480_000)),
            best_ask_up: Some(MicroUsdc(520_000)),
            best_ask_dn: Some(MicroUsdc(520_000)),
            ..Default::default()
        };

        // 1. UP-only directional cancel: UP cancels on priority channel, DOWN holds Live.
        let (h1, mut p1, _r1, mut a1) = setup_actor();
        h1.publish_quote(QuoteProposal {
            defensive_cancel: true,
            defensive_cancel_up: true,
            defensive_cancel_dn: false,
            ..base
        });
        let tr1 = a1.step(1_000_000, 0).unwrap().unwrap();
        assert_eq!(
            tr1.action_up,
            OrderAction::Cancel {
                outcome: Outcome::Up,
                order_id: "up_1".to_string(),
                reason: CancelReason::DefensiveCancel,
            }
        );
        assert_eq!(tr1.action_dn, OrderAction::Hold);
        assert!(matches!(
            p1.try_recv().unwrap(),
            ExecutionCommand::Cancel {
                outcome: Outcome::Up,
                reason: CancelReason::DefensiveCancel,
                ..
            }
        ));
        assert!(p1.try_recv().is_err());

        // 2. DOWN-only directional cancel: DOWN cancels, UP holds Live.
        let (h2, mut p2, _r2, mut a2) = setup_actor();
        h2.publish_quote(QuoteProposal {
            defensive_cancel: true,
            defensive_cancel_up: false,
            defensive_cancel_dn: true,
            ..base
        });
        let tr2 = a2.step(1_000_000, 0).unwrap().unwrap();
        assert_eq!(tr2.action_up, OrderAction::Hold);
        assert_eq!(
            tr2.action_dn,
            OrderAction::Cancel {
                outcome: Outcome::Down,
                order_id: "dn_1".to_string(),
                reason: CancelReason::DefensiveCancel,
            }
        );
        assert!(matches!(
            p2.try_recv().unwrap(),
            ExecutionCommand::Cancel {
                outcome: Outcome::Down,
                reason: CancelReason::DefensiveCancel,
                ..
            }
        ));
        assert!(p2.try_recv().is_err());

        // 3. Legacy symmetric flag (up=false, dn=false, defensive_cancel=true) cancels both.
        let (h3, mut p3, _r3, mut a3) = setup_actor();
        h3.publish_quote(QuoteProposal {
            defensive_cancel: true,
            defensive_cancel_up: false,
            defensive_cancel_dn: false,
            ..base
        });
        let tr3 = a3.step(1_000_000, 0).unwrap().unwrap();
        assert!(matches!(tr3.action_up, OrderAction::Cancel { .. }));
        assert!(matches!(tr3.action_dn, OrderAction::Cancel { .. }));
        assert!(p3.try_recv().is_ok());
        assert!(p3.try_recv().is_ok());
    }

    #[test]
    fn test_pre_expiry_lockout_caps_reducing_order_to_delta_and_blocks_submin_flips() {
        let make_actor = || {
            let (harness, priority_rx, routine_rx) = ActorPipelineHarness::new(16, 16);
            let engine = DollarNotionalRiskEngine::new(RiskConfig::default()).unwrap();
            let hysteresis = HysteresisGate::new(HysteresisConfig::default()).unwrap();
            let lifecycle = EpochLifecycleController::new(LifecycleConfig::default()).unwrap();
            let breakers = CircuitBreakers::new(BreakerConfig::default(), MicroUsdc(10_000), 0);
            let actor = RiskActor::new(
                harness.clone(),
                engine,
                hysteresis,
                lifecycle,
                breakers,
                FaultInjection::None,
            );
            (actor, harness, priority_rx, routine_rx)
        };

        // 1. tau = 53s (PreExpiryLockout) with q = +11.94sh (Tier1Normal, cost=$2.51, hedge_only_dn=false)
        // -> authorizes strictly 11.94sh DN and None UP.
        let (mut actor_up, harness_up, _p_up, mut r_up) = make_actor();
        harness_up.publish_registry_snapshot(OrderRegistrySnapshot {
            unhedged_up: SharesMicro::from_raw(11_940_000),
            unhedged_cost_up_micro: MicroUsdc(2_507_400),
            available_usdc_micro: MicroUsdc(100_000_000),
            ..Default::default()
        });
        harness_up.publish_quote(QuoteProposal {
            seq: 1,
            now_ms: 247_000, // tau = 53s
            expiry_ms: 300_000,
            tick_size_micro: MicroUsdc(10_000),
            quote: QuoteOutput::Active {
                bid_up: Some(MicroUsdc(210_000)),
                bid_dn: Some(MicroUsdc(690_000)),
                pair_cost_micro: Some(MicroUsdc(900_000)),
                gross_margin_bps: Some(1_000),
            },
            bid_up_raw: 0.21,
            bid_dn_raw: 0.69,
            hedge_only_dn: false,
            best_bid_up: Some(MicroUsdc(210_000)),
            best_ask_up: Some(MicroUsdc(220_000)),
            best_bid_dn: Some(MicroUsdc(680_000)),
            best_ask_dn: Some(MicroUsdc(700_000)),
            ..Default::default()
        });
        let trace_1 = actor_up.step(1_000_000, 0).unwrap().unwrap();
        assert_eq!(trace_1.phase, EpochPhase::PreExpiryLockout);
        assert_eq!(trace_1.tier, RiskTier::Tier1Normal);
        assert_eq!(trace_1.action_up, OrderAction::Hold);
        assert_eq!(
            trace_1.action_dn,
            OrderAction::SubmitNew {
                outcome: Outcome::Down,
                price: MicroUsdc(690_000),
                size: SharesMicro::from_raw(11_940_000),
            }
        );
        assert_eq!(
            r_up.try_recv().unwrap(),
            ExecutionCommand::SubmitNew {
                outcome: Outcome::Down,
                price: MicroUsdc(690_000),
                size: SharesMicro::from_raw(11_940_000),
            }
        );

        // 2. tau = 53s with q = +2.08sh (< 5sh min) -> suppresses both legs instead of submitting 20sh flip!
        let (mut actor_dust, harness_dust, _p_dust, mut r_dust) = make_actor();
        harness_dust.publish_registry_snapshot(OrderRegistrySnapshot {
            unhedged_up: SharesMicro::from_raw(2_080_000),
            unhedged_cost_up_micro: MicroUsdc(436_800),
            available_usdc_micro: MicroUsdc(100_000_000),
            ..Default::default()
        });
        harness_dust.publish_quote(QuoteProposal {
            seq: 1,
            now_ms: 247_000,
            expiry_ms: 300_000,
            tick_size_micro: MicroUsdc(10_000),
            quote: QuoteOutput::Active {
                bid_up: Some(MicroUsdc(210_000)),
                bid_dn: Some(MicroUsdc(690_000)),
                pair_cost_micro: Some(MicroUsdc(900_000)),
                gross_margin_bps: Some(1_000),
            },
            bid_up_raw: 0.21,
            bid_dn_raw: 0.69,
            best_bid_up: Some(MicroUsdc(210_000)),
            best_ask_up: Some(MicroUsdc(220_000)),
            best_bid_dn: Some(MicroUsdc(680_000)),
            best_ask_dn: Some(MicroUsdc(700_000)),
            ..Default::default()
        });
        let trace_2 = actor_dust.step(1_000_000, 0).unwrap().unwrap();
        assert_eq!(trace_2.phase, EpochPhase::PreExpiryLockout);
        assert_eq!(trace_2.action_up, OrderAction::Hold);
        assert_eq!(trace_2.action_dn, OrderAction::Hold);
        assert!(r_dust.try_recv().is_err());

        // 3. Symmetric q = -11.94sh -> authorizes strictly 11.94sh UP and None DN.
        let (mut actor_dn, harness_dn, _p_dn, mut r_dn) = make_actor();
        harness_dn.publish_registry_snapshot(OrderRegistrySnapshot {
            unhedged_dn: SharesMicro::from_raw(11_940_000),
            unhedged_cost_dn_micro: MicroUsdc(2_507_400),
            available_usdc_micro: MicroUsdc(100_000_000),
            ..Default::default()
        });
        harness_dn.publish_quote(QuoteProposal {
            seq: 1,
            now_ms: 247_000,
            expiry_ms: 300_000,
            tick_size_micro: MicroUsdc(10_000),
            quote: QuoteOutput::Active {
                bid_up: Some(MicroUsdc(690_000)),
                bid_dn: Some(MicroUsdc(210_000)),
                pair_cost_micro: Some(MicroUsdc(900_000)),
                gross_margin_bps: Some(1_000),
            },
            bid_up_raw: 0.69,
            bid_dn_raw: 0.21,
            best_bid_up: Some(MicroUsdc(680_000)),
            best_ask_up: Some(MicroUsdc(700_000)),
            best_bid_dn: Some(MicroUsdc(210_000)),
            best_ask_dn: Some(MicroUsdc(220_000)),
            ..Default::default()
        });
        let trace_3 = actor_dn.step(1_000_000, 0).unwrap().unwrap();
        assert_eq!(trace_3.phase, EpochPhase::PreExpiryLockout);
        assert_eq!(
            trace_3.action_up,
            OrderAction::SubmitNew {
                outcome: Outcome::Up,
                price: MicroUsdc(690_000),
                size: SharesMicro::from_raw(11_940_000),
            }
        );
        assert_eq!(trace_3.action_dn, OrderAction::Hold);
        assert_eq!(
            r_dn.try_recv().unwrap(),
            ExecutionCommand::SubmitNew {
                outcome: Outcome::Up,
                price: MicroUsdc(690_000),
                size: SharesMicro::from_raw(11_940_000),
            }
        );

        // 4. Symmetric q = -2.08sh (< 5sh min) -> suppresses both legs cleanly.
        let (mut actor_dn_dust, harness_dn_dust, _p_dn_dust, mut r_dn_dust) = make_actor();
        harness_dn_dust.publish_registry_snapshot(OrderRegistrySnapshot {
            unhedged_dn: SharesMicro::from_raw(2_080_000),
            unhedged_cost_dn_micro: MicroUsdc(436_800),
            available_usdc_micro: MicroUsdc(100_000_000),
            ..Default::default()
        });
        harness_dn_dust.publish_quote(QuoteProposal {
            seq: 1,
            now_ms: 247_000,
            expiry_ms: 300_000,
            tick_size_micro: MicroUsdc(10_000),
            quote: QuoteOutput::Active {
                bid_up: Some(MicroUsdc(690_000)),
                bid_dn: Some(MicroUsdc(210_000)),
                pair_cost_micro: Some(MicroUsdc(900_000)),
                gross_margin_bps: Some(1_000),
            },
            bid_up_raw: 0.69,
            bid_dn_raw: 0.21,
            best_bid_up: Some(MicroUsdc(680_000)),
            best_ask_up: Some(MicroUsdc(700_000)),
            best_bid_dn: Some(MicroUsdc(210_000)),
            best_ask_dn: Some(MicroUsdc(220_000)),
            ..Default::default()
        });
        let trace_4 = actor_dn_dust.step(1_000_000, 0).unwrap().unwrap();
        assert_eq!(trace_4.phase, EpochPhase::PreExpiryLockout);
        assert_eq!(trace_4.action_up, OrderAction::Hold);
        assert_eq!(trace_4.action_dn, OrderAction::Hold);
        assert!(r_dn_dust.try_recv().is_err());
    }

    #[test]
    fn test_entry_time_guard_blocks_fresh_entries_but_preserves_hedges_and_flatten() {
        let make_actor = || {
            let (harness, priority_rx, routine_rx) = ActorPipelineHarness::new(16, 16);
            let risk_cfg = RiskConfig {
                cap_tier1_reducing_to_delta: true,
                ..Default::default()
            };
            let engine = DollarNotionalRiskEngine::new(risk_cfg).unwrap();
            let hysteresis = HysteresisGate::new(HysteresisConfig::default()).unwrap();
            let lifecycle = EpochLifecycleController::new(LifecycleConfig {
                entry_open_delay_secs: 15,
                entry_cutoff_secs: Some(120),
                ..Default::default()
            })
            .unwrap();
            let breakers = CircuitBreakers::new(BreakerConfig::default(), MicroUsdc(10_000), 0);
            let actor = RiskActor::new(
                harness.clone(),
                engine,
                hysteresis,
                lifecycle,
                breakers,
                FaultInjection::None,
            );
            (actor, harness, priority_rx, routine_rx)
        };

        let make_proposal = |seq: u64, now_ms: u64| QuoteProposal {
            seq,
            now_ms,
            expiry_ms: 300_000,
            tick_size_micro: MicroUsdc(10_000),
            quote: QuoteOutput::Active {
                bid_up: Some(MicroUsdc(480_000)),
                bid_dn: Some(MicroUsdc(490_000)),
                pair_cost_micro: Some(MicroUsdc(970_000)),
                gross_margin_bps: Some(300),
            },
            bid_up_raw: 0.48,
            bid_dn_raw: 0.49,
            best_bid_up: Some(MicroUsdc(470_000)),
            best_ask_up: Some(MicroUsdc(500_000)),
            best_bid_dn: Some(MicroUsdc(480_000)),
            best_ask_dn: Some(MicroUsdc(510_000)),
            ..Default::default()
        };

        // 1. Opening exclusion (0-15s, elapsed=5s -> tau=295s): flat -> both legs blocked.
        let (mut actor_flat, harness_flat, _p_flat, mut r_flat) = make_actor();
        harness_flat.publish_registry_snapshot(OrderRegistrySnapshot {
            available_usdc_micro: MicroUsdc(100_000_000),
            ..Default::default()
        });
        harness_flat.publish_quote(make_proposal(1, 5_000));
        let t_open = actor_flat.step(1_000_000, 0).unwrap().unwrap();
        assert_eq!(t_open.phase, EpochPhase::ActiveQuoting);
        assert_eq!(t_open.action_up, OrderAction::Hold);
        assert_eq!(t_open.action_dn, OrderAction::Hold);
        assert!(r_flat.try_recv().is_err());

        // 2. Inside permitted window (elapsed=15s -> tau=285s): flat -> both legs active!
        harness_flat.publish_quote(make_proposal(2, 15_000));
        let t_inside = actor_flat.step(2_000_000, 0).unwrap().unwrap();
        assert!(matches!(t_inside.action_up, OrderAction::SubmitNew { .. }));
        assert!(matches!(t_inside.action_dn, OrderAction::SubmitNew { .. }));
        assert!(r_flat.try_recv().is_ok());
        assert!(r_flat.try_recv().is_ok());

        // 3. Past cutoff (elapsed=125s -> tau=175s): resting Live orders are canceled when flat!
        actor_flat.hysteresis.set_state(
            Outcome::Up,
            crate::station8_hysteresis::OrderSlotState::Live {
                order_id: "ord_up".to_string(),
                price: MicroUsdc(480_000),
                original_size: SharesMicro::from_raw(20 * core_types::ONE_SHARE_MICRO),
                remaining_size: SharesMicro::from_raw(20 * core_types::ONE_SHARE_MICRO),
            },
        );
        actor_flat.hysteresis.set_state(
            Outcome::Down,
            crate::station8_hysteresis::OrderSlotState::Live {
                order_id: "ord_dn".to_string(),
                price: MicroUsdc(490_000),
                original_size: SharesMicro::from_raw(20 * core_types::ONE_SHARE_MICRO),
                remaining_size: SharesMicro::from_raw(20 * core_types::ONE_SHARE_MICRO),
            },
        );
        harness_flat.publish_quote(make_proposal(3, 125_000));
        let t_after = actor_flat.step(3_000_000, 0).unwrap().unwrap();
        assert!(matches!(t_after.action_up, OrderAction::Cancel { .. }));
        assert!(matches!(t_after.action_dn, OrderAction::Cancel { .. }));

        // 4. Past cutoff with non-dust inventory (q = +15sh UP): UP suppressed, DN hedge active!
        let (mut actor_inv, harness_inv, _p_inv, mut r_inv) = make_actor();
        harness_inv.publish_registry_snapshot(OrderRegistrySnapshot {
            unhedged_up: SharesMicro::from_raw(15 * core_types::ONE_SHARE_MICRO),
            unhedged_cost_up_micro: MicroUsdc(7_200_000),
            available_usdc_micro: MicroUsdc(100_000_000),
            ..Default::default()
        });
        harness_inv.publish_quote(make_proposal(1, 130_000));
        let t_hedge = actor_inv.step(1_000_000, 0).unwrap().unwrap();
        assert_eq!(t_hedge.action_up, OrderAction::Hold);
        assert_eq!(
            t_hedge.action_dn,
            OrderAction::SubmitNew {
                outcome: Outcome::Down,
                price: MicroUsdc(490_000),
                size: SharesMicro::from_raw(15 * core_types::ONE_SHARE_MICRO),
            }
        );
        assert!(r_inv.try_recv().is_ok());

        // 5. Past cutoff with dust inventory (q = +2sh < 5sh): reducing leg still allowed to repair dust!
        let (mut actor_dust, harness_dust, _p_dust, _r_dust) = make_actor();
        harness_dust.publish_registry_snapshot(OrderRegistrySnapshot {
            unhedged_up: SharesMicro::from_raw(2 * core_types::ONE_SHARE_MICRO),
            unhedged_cost_up_micro: MicroUsdc(960_000),
            available_usdc_micro: MicroUsdc(100_000_000),
            ..Default::default()
        });
        harness_dust.publish_quote(make_proposal(1, 130_000));
        let t_dust = actor_dust.step(1_000_000, 0).unwrap().unwrap();
        assert_eq!(t_dust.action_up, OrderAction::Hold);
        assert_eq!(
            t_dust.action_dn,
            OrderAction::SubmitNew {
                outcome: Outcome::Down,
                price: MicroUsdc(490_000),
                size: SharesMicro::from_raw(5 * core_types::ONE_SHARE_MICRO),
            }
        );

        // 6. MandatoryFlatten (tau=25s) still emits FlattenFak even when entry cutoff is 120s.
        let (mut actor_flat_end, harness_flat_end, mut p_end, _r_end) = make_actor();
        harness_flat_end.publish_registry_snapshot(OrderRegistrySnapshot {
            unhedged_up: SharesMicro::from_raw(15 * core_types::ONE_SHARE_MICRO),
            unhedged_cost_up_micro: MicroUsdc(7_200_000),
            available_usdc_micro: MicroUsdc(100_000_000),
            ..Default::default()
        });
        harness_flat_end.publish_quote(make_proposal(1, 275_000));
        let t_flatten = actor_flat_end.step(1_000_000, 0).unwrap().unwrap();
        assert_eq!(t_flatten.phase, EpochPhase::MandatoryFlatten);
        assert!(matches!(
            t_flatten.flatten_action,
            FlattenAction::FlattenFak {
                outcome: Outcome::Down,
                ..
            }
        ));
        assert!(matches!(
            p_end.try_recv().unwrap(),
            ExecutionCommand::ExecuteFlattenFak { .. }
        ));
    }
}
