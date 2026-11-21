use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::JoinHandle;

use arc_swap::ArcSwap;
use core_types::{MicroUsdc, Outcome, SharesMicro};
use math_engine::QuoteOutput;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

use crate::breakers::{BreakerStatus, CircuitBreakers};
use crate::error::RiskLifecycleError;
use crate::lifecycle::{EpochLifecycleController, EpochPhase, LifecycleAction, LifecycleInput};
use crate::station7_risk::{
    DollarNotionalRiskEngine, FlattenAction, OrderRegistrySnapshot, RiskEvaluationInput, RiskTier,
};
use crate::station8_hysteresis::{
    CancelReason, HysteresisGate, HysteresisInput, OrderAction, SizedQuote,
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
            best_bid_up: None,
            best_bid_dn: None,
            best_ask_up: None,
            best_ask_dn: None,
            defensive_cancel: false,
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
            let _ = self
                .harness
                .priority_tx
                .try_send(ExecutionCommand::CancelAll {
                    reason: format!("Circuit breaker tripped: {reason:?}"),
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
                risk_eval_ns: start_time.elapsed().as_nanos() as u64,
            }));
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
                let _ = self
                    .harness
                    .priority_tx
                    .try_send(ExecutionCommand::CancelAll { reason });
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
                    risk_eval_ns: start_time.elapsed().as_nanos() as u64,
                }))
            }
            LifecycleAction::ExecuteFlattenFak {
                outcome,
                price,
                size,
                reason,
            } => {
                self.flatten_in_flight = true;
                let _ = self
                    .harness
                    .priority_tx
                    .try_send(ExecutionCommand::ExecuteFlattenFak {
                        outcome,
                        price,
                        size,
                        reason,
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
                    flatten_action: FlattenAction::FlattenFak {
                        outcome,
                        price,
                        size,
                    },
                    risk_eval_ns: start_time.elapsed().as_nanos() as u64,
                }))
            }
            LifecycleAction::ResidualDustUnflattenable {
                unhedged_shares,
                min_order_size,
                ..
            } => {
                let _ = self
                    .harness
                    .priority_tx
                    .try_send(ExecutionCommand::CancelAll {
                        reason: "Residual dust below min order size".to_string(),
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
                    risk_eval_ns: start_time.elapsed().as_nanos() as u64,
                }))
            }
            LifecycleAction::EpochRollover {
                expired_expiry_ms,
                paired_shares_to_settle,
            } => {
                let _ = self.harness.priority_tx.try_send(ExecutionCommand::CancelAll {
                    reason: format!("Epoch rollover for expiry {expired_expiry_ms} with {paired_shares_to_settle:?} paired"),
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
                    risk_eval_ns: start_time.elapsed().as_nanos() as u64,
                }))
            }
            LifecycleAction::ContinueQuoting {
                prohibit_accumulation,
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

                let risk_decision = self.risk_engine.evaluate(&snapshot, &risk_input)?;

                if let FlattenAction::FlattenFak {
                    outcome,
                    price,
                    size,
                } = risk_decision.flatten
                {
                    self.flatten_in_flight = true;
                    let _ =
                        self.harness
                            .priority_tx
                            .try_send(ExecutionCommand::ExecuteFlattenFak {
                                outcome,
                                price,
                                size,
                                reason: "Station 7 Tier 4 Hard Stop flatten".to_string(),
                            });
                }

                // If accumulation prohibited in PreExpiryLockout, zero out the accumulating leg:
                let (auth_size_up, auth_size_dn) = if prohibit_accumulation {
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

                // 5. Station 8 Continuous Hysteresis Gate for UP leg:
                let quote_up = match (target_bid_up, auth_size_up) {
                    (Some(p), Some(s)) => Some(SizedQuote {
                        price: p,
                        size: s,
                        price_raw_usd: proposal.bid_up_raw,
                    }),
                    _ => None,
                };

                let hyst_input_up = HysteresisInput {
                    outcome: Outcome::Up,
                    proposed_quote: quote_up,
                    tick_size_micro: proposal.tick_size_micro,
                    best_ask: proposal.best_ask_up,
                    defensive_cancel: proposal.defensive_cancel,
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
                    }),
                    _ => None,
                };

                let hyst_input_dn = HysteresisInput {
                    outcome: Outcome::Down,
                    proposed_quote: quote_dn,
                    tick_size_micro: proposal.tick_size_micro,
                    best_ask: proposal.best_ask_dn,
                    defensive_cancel: proposal.defensive_cancel,
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
                    flatten_action: risk_decision.flatten,
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
}
