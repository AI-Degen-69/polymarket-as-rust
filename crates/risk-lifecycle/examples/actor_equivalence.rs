//! Differential driver for `RiskActor::step`.
//!
//! `riskdiff.ps1` compares the replay's telemetry traces. Traces only describe what the
//! actor *decided*; they do not show the commands it actually put on the priority and
//! routine lanes, in order, with their payloads, nor the slot state it left behind.
//! This driver records exactly that, for one scripted scenario per exit path, so the same
//! file can be compiled against two commits and the two dumps diffed line for line.
//!
//! Usage: `cargo run -q -p risk-lifecycle --example actor_equivalence -- <out.jsonl>`
//!
//! Output is one JSON object per line, in the order the actor produced it:
//!   {"ev":"scenario","name":..}                          start of a scripted scenario
//!   {"ev":"trace","scenario":..,"label":..,"trace":{..}}  one `step` result, minus wall-clock latency
//!   {"ev":"cmd","scenario":..,"seq":N,"lane":"priority"|"routine","cmd":{..}}  drained in order
//!   {"ev":"no_trace",..} / {"ev":"step_error",..}         step returned None or an error
//!   {"ev":"state","scenario":..,"label":..,..}           flatten guard + both hysteresis slots
//!
//! Ordering is preserved: each `step` is followed by its own drained commands, priority
//! lane first, then routine, which is the order the execution actor consumes them.
//! Nothing else is sorted. The only field this file drops is `risk_eval_ns`, which is
//! real elapsed time and cannot be equal across two runs.

use std::error::Error;
use std::fs::File;
use std::io::Write as _;
use std::path::Path;

use core_types::{MicroUsdc, Outcome, SharesMicro};
use math_engine::{QuoteOutput, SuppressionReason};
use risk_lifecycle::{
    ActorPipelineHarness, BreakerConfig, CircuitBreakers, DollarNotionalRiskEngine,
    EpochLifecycleController, ExecutionCommand, FaultInjection, HysteresisConfig, HysteresisGate,
    LifecycleConfig, OrderRegistrySnapshot, OrderSlotState, QuoteProposal, RiskActor, RiskConfig,
};
use serde_json::{Value, json};
use tokio::sync::mpsc;

const EXP: u64 = 300_000;
const ONE: u64 = core_types::ONE_SHARE_MICRO;

fn us(micro: u64) -> MicroUsdc {
    MicroUsdc(micro)
}

fn sh(shares: u64) -> SharesMicro {
    SharesMicro::from_raw(shares * ONE)
}

struct Rig {
    actor: RiskActor,
    harness: ActorPipelineHarness,
    priority: mpsc::Receiver<ExecutionCommand>,
    routine: mpsc::Receiver<ExecutionCommand>,
    out: Vec<String>,
    seq: u64,
    scenario: &'static str,
}

impl Rig {
    fn new(
        priority_cap: usize,
        fault: FaultInjection,
        hysteresis: HysteresisGate,
        scenario: &'static str,
    ) -> Result<Self, Box<dyn Error>> {
        let (harness, priority, routine) = ActorPipelineHarness::new(priority_cap, 64);
        let engine = DollarNotionalRiskEngine::new(RiskConfig::default())?;
        let lifecycle = EpochLifecycleController::new(LifecycleConfig::default())?;
        let breakers = CircuitBreakers::new(BreakerConfig::default(), us(10_000), 0);
        let actor = RiskActor::new(
            harness.clone(),
            engine,
            hysteresis,
            lifecycle,
            breakers,
            fault,
        );
        let mut rig = Self {
            actor,
            harness,
            priority,
            routine,
            out: Vec::new(),
            seq: 0,
            scenario,
        };
        rig.rec(json!({ "ev": "scenario", "name": scenario }));
        Ok(rig)
    }

    fn rec(&mut self, value: Value) {
        self.out.push(value.to_string());
    }

    fn quote(&self, proposal: QuoteProposal) {
        self.harness.publish_quote(proposal);
    }

    fn snapshot(&self, snapshot: OrderRegistrySnapshot) {
        self.harness.publish_registry_snapshot(snapshot);
    }

    fn slot(&mut self, outcome: Outcome, state: OrderSlotState) {
        self.actor.hysteresis_mut().set_state(outcome, state);
    }

    /// One evaluation, then every command it left in either lane.
    fn step(&mut self, now_ns: u64, label: &'static str) {
        match self.actor.step(now_ns, 0) {
            Ok(Some(trace)) => {
                let mut value = match serde_json::to_value(&trace) {
                    Ok(value) => value,
                    Err(err) => {
                        self.rec(
                            json!({ "ev": "step_error", "label": label, "error": err.to_string() }),
                        );
                        return;
                    }
                };
                if let Value::Object(ref mut map) = value {
                    map.remove("risk_eval_ns");
                }
                self.rec(json!({
                    "ev": "trace",
                    "scenario": self.scenario,
                    "label": label,
                    "trace": value,
                }));
            }
            Ok(None) => {
                self.rec(json!({ "ev": "no_trace", "scenario": self.scenario, "label": label }));
            }
            Err(err) => {
                self.rec(json!({
                    "ev": "step_error",
                    "scenario": self.scenario,
                    "label": label,
                    "error": err.to_string(),
                }));
            }
        }
        self.drain();
    }

    fn drain(&mut self) {
        while let Ok(cmd) = self.priority.try_recv() {
            self.emit_cmd("priority", &cmd);
        }
        while let Ok(cmd) = self.routine.try_recv() {
            self.emit_cmd("routine", &cmd);
        }
    }

    fn emit_cmd(&mut self, lane: &str, cmd: &ExecutionCommand) {
        self.seq += 1;
        let seq = self.seq;
        let scenario = self.scenario;
        self.rec(json!({
            "ev": "cmd",
            "scenario": scenario,
            "seq": seq,
            "lane": lane,
            "cmd": encode(cmd),
        }));
    }

    fn state(&mut self, label: &'static str) {
        let up = encode_slot(self.actor.hysteresis_mut().state(Outcome::Up));
        let dn = encode_slot(self.actor.hysteresis_mut().state(Outcome::Down));
        let guard = self.actor.flatten_in_flight();
        self.rec(json!({
            "ev": "state",
            "scenario": self.scenario,
            "label": label,
            "flatten_in_flight": guard,
            "slot_up": up,
            "slot_dn": dn,
        }));
    }

    fn finish(self, out: &mut Vec<String>) {
        out.extend(self.out);
    }
}

fn encode(cmd: &ExecutionCommand) -> Value {
    match cmd {
        ExecutionCommand::SubmitNew {
            outcome,
            price,
            size,
        } => json!({
            "type": "SubmitNew",
            "outcome": format!("{outcome:?}"),
            "price": price.0,
            "size": size.0,
        }),
        ExecutionCommand::Cancel {
            outcome,
            order_id,
            reason,
        } => json!({
            "type": "Cancel",
            "outcome": format!("{outcome:?}"),
            "order_id": order_id,
            "reason": format!("{reason:?}"),
        }),
        ExecutionCommand::CancelAndReplace {
            outcome,
            old_order_id,
            new_price,
            new_size,
            reason,
        } => json!({
            "type": "CancelAndReplace",
            "outcome": format!("{outcome:?}"),
            "old_order_id": old_order_id,
            "new_price": new_price.0,
            "new_size": new_size.0,
            "reason": format!("{reason:?}"),
        }),
        ExecutionCommand::CancelAll { reason } => json!({
            "type": "CancelAll",
            "reason": reason,
        }),
        ExecutionCommand::ExecuteFlattenFak {
            outcome,
            price,
            size,
            reason,
        } => json!({
            "type": "ExecuteFlattenFak",
            "outcome": format!("{outcome:?}"),
            "price": price.0,
            "size": size.0,
            "reason": reason,
        }),
    }
}

/// The whole slot state, serialized as-is rather than copied field by field: the `sent_ns`
/// of an in-flight place or cancel and the raw/capped price of a queued next quote are all
/// deterministic here (the rig drives `step` with fixed stamps), so dropping them would let
/// two different slot states compare as identical.
fn encode_slot(state: &OrderSlotState) -> Value {
    match serde_json::to_value(state) {
        Ok(value) => value,
        Err(err) => json!({ "encode_error": err.to_string() }),
    }
}

fn gate() -> Result<HysteresisGate, Box<dyn Error>> {
    Ok(HysteresisGate::new(HysteresisConfig::default())?)
}

fn live(order_id: &str, price: u64, original: u64, remaining: u64) -> OrderSlotState {
    OrderSlotState::Live {
        order_id: order_id.to_string(),
        price: us(price),
        original_size: sh(original),
        remaining_size: sh(remaining),
    }
}

#[allow(clippy::too_many_arguments)]
fn proposal(
    seq: u64,
    now_ms: u64,
    expiry_ms: u64,
    bid_up: Option<u64>,
    bid_dn: Option<u64>,
    ask_up: Option<u64>,
    ask_dn: Option<u64>,
) -> QuoteProposal {
    QuoteProposal {
        seq,
        now_ms,
        expiry_ms,
        tick_size_micro: us(10_000),
        quote: QuoteOutput::Active {
            bid_up: bid_up.map(us),
            bid_dn: bid_dn.map(us),
            pair_cost_micro: match (bid_up, bid_dn) {
                (Some(up), Some(dn)) => Some(us(up + dn)),
                _ => None,
            },
            gross_margin_bps: Some(400),
        },
        bid_up_raw: bid_up.map_or(0.0, |p| p as f64 / 1_000_000.0),
        bid_dn_raw: bid_dn.map_or(0.0, |p| p as f64 / 1_000_000.0),
        best_bid_up: bid_up.map(us),
        best_bid_dn: bid_dn.map(us),
        best_ask_up: ask_up.map(us),
        best_ask_dn: ask_dn.map(us),
        defensive_cancel: false,
        total_compute_ns: 10_000,
        ..Default::default()
    }
}

// 1. Circuit breaker activation: the actor exits before measuring exposure.
fn scenario_breaker(out: &mut Vec<String>) -> Result<(), Box<dyn Error>> {
    let mut rig = Rig::new(16, FaultInjection::StalePoly, gate()?, "breaker")?;
    rig.snapshot(OrderRegistrySnapshot {
        unhedged_up: sh(10),
        unhedged_cost_up_micro: us(4_800_000),
        available_usdc_micro: us(100_000_000),
        ..Default::default()
    });
    rig.slot(Outcome::Up, live("up-1", 480_000, 20, 20));
    rig.quote(proposal(
        1,
        100_000,
        EXP,
        Some(480_000),
        Some(480_000),
        Some(520_000),
        Some(520_000),
    ));
    rig.step(3_500_000_000, "trip");
    rig.step(4_000_000_000, "still-tripped");
    rig.state("end");
    rig.finish(out);
    Ok(())
}

// 2. Terminal lockout: expiry is close but not reached, so no rollover yet.
fn scenario_terminal_lockout(out: &mut Vec<String>) -> Result<(), Box<dyn Error>> {
    let mut rig = Rig::new(16, FaultInjection::None, gate()?, "terminal_lockout")?;
    rig.snapshot(OrderRegistrySnapshot {
        unhedged_up: sh(8),
        unhedged_cost_up_micro: us(3_840_000),
        available_usdc_micro: us(100_000_000),
        ..Default::default()
    });
    rig.slot(Outcome::Up, live("up-1", 480_000, 20, 8));
    rig.quote(proposal(
        1,
        EXP - 20_000,
        EXP,
        Some(480_000),
        Some(480_000),
        Some(510_000),
        Some(520_000),
    ));
    rig.step(1_000_000, "t-20");
    rig.quote(proposal(
        2,
        EXP - 12_000,
        EXP,
        Some(480_000),
        Some(480_000),
        Some(510_000),
        Some(520_000),
    ));
    rig.step(2_000_000_000, "t-12");
    rig.quote(proposal(
        3,
        EXP - 5_000,
        EXP,
        Some(480_000),
        Some(480_000),
        Some(510_000),
        Some(520_000),
    ));
    rig.step(3_000_000_000, "t-5");
    rig.state("end");
    rig.finish(out);
    Ok(())
}

// 3. Tier-4 hard stop that flattens successfully.
fn scenario_tier4_flatten(out: &mut Vec<String>) -> Result<(), Box<dyn Error>> {
    let mut rig = Rig::new(16, FaultInjection::ForceTier4, gate()?, "tier4_flatten")?;
    rig.snapshot(OrderRegistrySnapshot {
        unhedged_up: sh(20),
        unhedged_cost_up_micro: us(9_500_000),
        available_usdc_micro: us(100_000_000),
        ..Default::default()
    });
    rig.quote(proposal(
        1,
        EXP - 200_000,
        EXP,
        Some(480_000),
        Some(480_000),
        Some(510_000),
        Some(520_000),
    ));
    rig.step(1_000_000, "hard-stop");
    rig.step(2_000_000, "in-flight");
    rig.quote(proposal(
        2,
        EXP - 199_000,
        EXP,
        Some(480_000),
        Some(480_000),
        Some(515_000),
        Some(520_000),
    ));
    rig.step(3_000_000, "in-flight-price-moved");
    rig.state("end");
    rig.finish(out);
    Ok(())
}

// 4. Tier-4 flatten whose priority send fails, then recovers.
fn scenario_flatten_send_failure(out: &mut Vec<String>) -> Result<(), Box<dyn Error>> {
    let mut rig = Rig::new(1, FaultInjection::None, gate()?, "flatten_send_failure")?;
    rig.snapshot(OrderRegistrySnapshot {
        unhedged_dn: sh(19),
        unhedged_cost_dn_micro: us(9_120_000),
        available_usdc_micro: us(100_000_000),
        ..Default::default()
    });
    rig.harness
        .priority_tx
        .try_send(ExecutionCommand::CancelAll {
            reason: "saturate priority channel".to_string(),
        })?;
    rig.quote(proposal(
        1,
        EXP - 25_000,
        EXP,
        Some(500_000),
        Some(480_000),
        Some(520_000),
        Some(490_000),
    ));
    rig.step(1_000_000, "send-dropped");
    rig.step(101_000_000, "send-recovered");
    rig.state("end");
    rig.finish(out);
    Ok(())
}

// 5. Residual dust below the market minimum cannot be flattened.
fn scenario_residual_dust(out: &mut Vec<String>) -> Result<(), Box<dyn Error>> {
    let mut rig = Rig::new(16, FaultInjection::None, gate()?, "residual_dust")?;
    rig.snapshot(OrderRegistrySnapshot {
        unhedged_up: sh(3),
        unhedged_cost_up_micro: us(1_500_000),
        available_usdc_micro: us(100_000_000),
        ..Default::default()
    });
    rig.quote(proposal(
        1,
        EXP - 20_000,
        EXP,
        Some(490_000),
        Some(460_000),
        Some(510_000),
        Some(480_000),
    ));
    rig.step(1_000_000, "dust");
    rig.step(2_000_000_000, "dust-again");
    rig.state("end");
    rig.finish(out);
    Ok(())
}

// 6. Epoch rollover: one-shot lockout cancel-all that clears the flatten guard,
//    followed by the first evaluation of the next epoch.
fn scenario_epoch_rollover(out: &mut Vec<String>) -> Result<(), Box<dyn Error>> {
    let mut rig = Rig::new(16, FaultInjection::None, gate()?, "epoch_rollover")?;
    rig.snapshot(OrderRegistrySnapshot {
        unhedged_up: sh(19),
        unhedged_cost_up_micro: us(9_120_000),
        available_usdc_micro: us(100_000_000),
        ..Default::default()
    });
    rig.slot(Outcome::Up, live("up-1", 480_000, 19, 19));
    rig.quote(proposal(
        1,
        EXP - 25_000,
        EXP,
        Some(480_000),
        Some(480_000),
        Some(510_000),
        Some(520_000),
    ));
    rig.step(1_000_000, "guard-set");
    rig.quote(proposal(
        2,
        EXP,
        EXP,
        Some(480_000),
        Some(480_000),
        Some(510_000),
        Some(520_000),
    ));
    rig.step(2_000_000_000, "rollover");
    rig.step(3_000_000_000, "lockout-repeat");
    rig.quote(proposal(
        3,
        EXP + 10_000,
        EXP + 300_000,
        Some(475_000),
        Some(475_000),
        Some(510_000),
        Some(525_000),
    ));
    rig.step(4_000_000_000, "next-epoch");
    rig.state("end");
    rig.finish(out);
    Ok(())
}

// 7. Normal Station 7 evaluation: quoting, replacement, and suppression.
fn scenario_normal_path(out: &mut Vec<String>) -> Result<(), Box<dyn Error>> {
    let mut rig = Rig::new(16, FaultInjection::None, gate()?, "normal_path")?;
    rig.snapshot(OrderRegistrySnapshot {
        available_usdc_micro: us(100_000_000),
        ..Default::default()
    });
    rig.quote(proposal(
        1,
        EXP - 250_000,
        EXP,
        Some(480_000),
        Some(480_000),
        Some(520_000),
        Some(520_000),
    ));
    rig.step(1_000_000, "fresh-entry");

    rig.slot(Outcome::Up, live("up-1", 480_000, 20, 20));
    rig.slot(Outcome::Down, live("dn-1", 480_000, 20, 20));
    rig.snapshot(OrderRegistrySnapshot {
        open_bid_shares_up: sh(20),
        open_bid_notional_up: us(9_600_000),
        open_bid_shares_dn: sh(20),
        open_bid_notional_dn: us(9_600_000),
        available_usdc_micro: us(100_000_000),
        ..Default::default()
    });
    rig.quote(proposal(
        2,
        EXP - 249_000,
        EXP,
        Some(480_000),
        Some(480_000),
        Some(520_000),
        Some(520_000),
    ));
    rig.step(2_000_000, "resting-within-deadband");

    rig.quote(proposal(
        3,
        EXP - 248_000,
        EXP,
        Some(495_000),
        Some(495_000),
        Some(525_000),
        Some(525_000),
    ));
    rig.step(3_000_000, "deadband-drift");

    rig.quote(QuoteProposal {
        seq: 4,
        now_ms: EXP - 247_000,
        expiry_ms: EXP,
        tick_size_micro: us(10_000),
        quote: QuoteOutput::Suppressed(SuppressionReason::WarmingUp),
        bid_up_raw: 0.0,
        bid_dn_raw: 0.0,
        best_bid_up: Some(us(480_000)),
        best_bid_dn: Some(us(480_000)),
        best_ask_up: Some(us(520_000)),
        best_ask_dn: Some(us(520_000)),
        total_compute_ns: 10_000,
        ..Default::default()
    });
    rig.step(4_000_000, "suppressed");
    rig.state("end");
    rig.finish(out);
    Ok(())
}

// 8. snapshot_without_own_slots: eight shapes of "my own order" vs "real exposure".
fn scenario_own_slots(out: &mut Vec<String>) -> Result<(), Box<dyn Error>> {
    let mut rig = Rig::new(16, FaultInjection::None, gate()?, "own_slots")?;

    // (a) Two resting orders, one per leg.
    rig.slot(Outcome::Up, live("up-1", 480_000, 20, 20));
    rig.slot(Outcome::Down, live("dn-1", 460_000, 10, 10));
    rig.snapshot(OrderRegistrySnapshot {
        open_bid_shares_up: sh(20),
        open_bid_notional_up: us(9_600_000),
        open_bid_shares_dn: sh(10),
        open_bid_notional_dn: us(4_600_000),
        available_usdc_micro: us(100_000_000),
        ..Default::default()
    });
    rig.quote(proposal(
        1,
        EXP - 250_000,
        EXP,
        Some(480_000),
        Some(460_000),
        Some(520_000),
        Some(500_000),
    ));
    rig.step(1_000_000, "two-live-slots");

    // (b) Partial fill: 20 placed, 6 filled, 14 remaining, plus the filled inventory.
    rig.slot(Outcome::Up, live("up-1", 480_000, 20, 14));
    rig.snapshot(OrderRegistrySnapshot {
        unhedged_up: sh(6),
        unhedged_cost_up_micro: us(2_880_000),
        open_bid_shares_up: sh(14),
        open_bid_notional_up: us(6_720_000),
        available_usdc_micro: us(100_000_000),
        ..Default::default()
    });
    rig.quote(proposal(
        2,
        EXP - 249_000,
        EXP,
        Some(480_000),
        Some(480_000),
        Some(520_000),
        Some(520_000),
    ));
    rig.step(2_000_000, "partial-fill");

    // (c) Cancelled order still listed in the snapshot: nothing is ours to subtract.
    rig.slot(Outcome::Up, OrderSlotState::Idle);
    rig.slot(Outcome::Down, OrderSlotState::Idle);
    rig.snapshot(OrderRegistrySnapshot {
        open_bid_shares_up: sh(20),
        open_bid_notional_up: us(9_600_000),
        available_usdc_micro: us(100_000_000),
        ..Default::default()
    });
    rig.quote(proposal(
        3,
        EXP - 248_000,
        EXP,
        Some(480_000),
        Some(480_000),
        Some(520_000),
        Some(520_000),
    ));
    rig.step(3_000_000, "cancelled-still-in-snapshot");

    // (d) Replacement: slot carries the new price, snapshot still carries the old order.
    rig.slot(Outcome::Up, live("up-2", 490_000, 20, 20));
    rig.snapshot(OrderRegistrySnapshot {
        open_bid_shares_up: sh(20),
        open_bid_notional_up: us(9_600_000),
        available_usdc_micro: us(100_000_000),
        ..Default::default()
    });
    rig.quote(proposal(
        4,
        EXP - 247_000,
        EXP,
        Some(490_000),
        Some(490_000),
        Some(520_000),
        Some(520_000),
    ));
    rig.step(4_000_000, "replacement-old-notional");

    // (e) Both legs filled and held, both slots idle.
    rig.slot(Outcome::Up, OrderSlotState::Idle);
    rig.slot(Outcome::Down, OrderSlotState::Idle);
    rig.snapshot(OrderRegistrySnapshot {
        unhedged_up: sh(12),
        unhedged_cost_up_micro: us(5_760_000),
        unhedged_dn: sh(4),
        unhedged_cost_dn_micro: us(1_840_000),
        available_usdc_micro: us(100_000_000),
        ..Default::default()
    });
    rig.quote(proposal(
        5,
        EXP - 246_000,
        EXP,
        Some(480_000),
        Some(480_000),
        Some(520_000),
        Some(520_000),
    ));
    rig.step(5_000_000, "two-sided-inventory");

    // (f) Zero-size resting order: subtracts nothing, releases nothing.
    rig.slot(Outcome::Up, live("up-3", 480_000, 0, 0));
    rig.snapshot(OrderRegistrySnapshot {
        open_bid_shares_up: sh(20),
        open_bid_notional_up: us(9_600_000),
        available_usdc_micro: us(100_000_000),
        ..Default::default()
    });
    rig.quote(proposal(
        6,
        EXP - 245_000,
        EXP,
        Some(480_000),
        Some(480_000),
        Some(520_000),
        Some(520_000),
    ));
    rig.step(6_000_000, "zero-size-order");

    // (g) Residual-size resting order (3 shares, below the 5-share minimum).
    rig.slot(Outcome::Up, live("up-4", 480_000, 20, 3));
    rig.snapshot(OrderRegistrySnapshot {
        open_bid_shares_up: sh(3),
        open_bid_notional_up: us(1_440_000),
        available_usdc_micro: us(100_000_000),
        ..Default::default()
    });
    rig.quote(proposal(
        7,
        EXP - 244_000,
        EXP,
        Some(480_000),
        Some(480_000),
        Some(520_000),
        Some(520_000),
    ));
    rig.step(7_000_000, "residual-size-order");

    // (h) Own order at a price the snapshot does not carry at all: the subtraction is
    //     by slot notional, so the released cash must still match the slot.
    rig.slot(Outcome::Down, live("dn-9", 310_000, 25, 25));
    rig.snapshot(OrderRegistrySnapshot {
        open_bid_shares_dn: sh(25),
        open_bid_notional_dn: us(7_750_000),
        available_usdc_micro: us(100_000_000),
        ..Default::default()
    });
    rig.quote(proposal(
        8,
        EXP - 243_000,
        EXP,
        Some(480_000),
        Some(310_000),
        Some(520_000),
        Some(360_000),
    ));
    rig.step(8_000_000, "slot-notional-vs-snapshot");
    rig.state("end");
    rig.finish(out);
    Ok(())
}

// 9. Expiry ladder: unbalanced inventory held into the last 30s, an empty flatten book,
//    a partial fill during flattening, then liquidity returning.
fn scenario_expiry_ladder(out: &mut Vec<String>) -> Result<(), Box<dyn Error>> {
    let mut rig = Rig::new(16, FaultInjection::None, gate()?, "expiry_ladder")?;
    rig.snapshot(OrderRegistrySnapshot {
        unhedged_up: sh(19),
        unhedged_cost_up_micro: us(9_120_000),
        available_usdc_micro: us(100_000_000),
        ..Default::default()
    });
    rig.slot(Outcome::Up, live("up-1", 480_000, 19, 19));
    rig.quote(proposal(
        1,
        EXP - 40_000,
        EXP,
        Some(480_000),
        Some(480_000),
        Some(500_000),
        Some(505_000),
    ));
    rig.step(1_000_000, "t-40");

    rig.quote(proposal(
        2,
        EXP - 28_000,
        EXP,
        Some(480_000),
        Some(480_000),
        Some(510_000),
        Some(512_000),
    ));
    rig.step(2_000_000_000, "t-28-flatten");

    // Book goes empty on the ask side while the flatten is in flight.
    rig.quote(proposal(
        3,
        EXP - 26_000,
        EXP,
        Some(480_000),
        Some(480_000),
        None,
        Some(512_000),
    ));
    rig.step(3_000_000_000, "t-26-empty-book");

    // Guard released while the down ask is still empty: liquidity is only on the UP bid,
    // so the fallback has to sell UP at the bid instead of buying DN at the ask.
    rig.actor.clear_flatten_in_flight();
    rig.quote(proposal(
        4,
        EXP - 24_000,
        EXP,
        Some(470_000),
        Some(470_000),
        None,
        None,
    ));
    rig.step(3_500_000_000, "t-24-empty-book-fallback");

    // Partial fill of the flatten: 6 of 19 done.
    rig.snapshot(OrderRegistrySnapshot {
        unhedged_up: sh(13),
        unhedged_cost_up_micro: us(6_240_000),
        available_usdc_micro: us(100_000_000),
        ..Default::default()
    });
    rig.actor.clear_flatten_in_flight();
    rig.quote(proposal(
        5,
        EXP - 20_000,
        EXP,
        Some(480_000),
        Some(480_000),
        Some(520_000),
        Some(522_000),
    ));
    rig.step(4_000_000_000, "t-20-partial-fill");

    // Liquidity returns, wider spread.
    rig.quote(proposal(
        6,
        EXP - 14_000,
        EXP,
        Some(480_000),
        Some(480_000),
        Some(540_000),
        Some(542_000),
    ));
    rig.step(5_000_000_000, "t-14-liquidity-back");
    rig.state("end");
    rig.finish(out);
    Ok(())
}

// 10. A partial fill lands while the flatten FAK is still in flight: the single-flight
//     guard must keep it armed (inventory changed, exposure is not flat), so the actor
//     cancels resting quotes instead of firing a second FAK; only the 2s guard timeout
//     releases it, and the refire is for the remaining size, not the original one.
fn scenario_flatten_partial_fill(out: &mut Vec<String>) -> Result<(), Box<dyn Error>> {
    let mut rig = Rig::new(16, FaultInjection::None, gate()?, "flatten_partial_fill")?;
    rig.snapshot(OrderRegistrySnapshot {
        unhedged_up: sh(19),
        unhedged_cost_up_micro: us(9_120_000),
        available_usdc_micro: us(100_000_000),
        ..Default::default()
    });
    rig.quote(proposal(
        1,
        EXP - 28_000,
        EXP,
        Some(480_000),
        Some(480_000),
        Some(510_000),
        Some(512_000),
    ));
    rig.step(1_000_000_000, "t-28-flatten");

    // 6 of 19 filled by the FAK: unhedged is now 13, so `unhedged_up != unhedged_dn`
    // and the guard is not cleared by the flat check.
    rig.snapshot(OrderRegistrySnapshot {
        unhedged_up: sh(13),
        unhedged_cost_up_micro: us(6_240_000),
        available_usdc_micro: us(100_000_000),
        ..Default::default()
    });
    rig.quote(proposal(
        2,
        EXP - 27_500,
        EXP,
        Some(480_000),
        Some(480_000),
        Some(510_000),
        Some(512_000),
    ));
    rig.step(1_500_000_000, "partial-fill-in-flight");

    // 2.5s after the FAK went out: the guard has timed out, so the refire is allowed,
    // and it sizes off the remaining 13 shares rather than the original 19.
    rig.quote(proposal(
        3,
        EXP - 20_000,
        EXP,
        Some(480_000),
        Some(480_000),
        Some(520_000),
        Some(522_000),
    ));
    rig.step(3_500_000_000, "guard-timeout-refire");

    // The refire filled the rest: equal unhedged legs clear the guard by the flat check.
    rig.snapshot(OrderRegistrySnapshot {
        available_usdc_micro: us(100_000_000),
        ..Default::default()
    });
    rig.quote(proposal(
        4,
        EXP - 19_000,
        EXP,
        Some(480_000),
        Some(480_000),
        Some(520_000),
        Some(522_000),
    ));
    rig.step(4_000_000_000, "flatten-complete");
    rig.state("end");
    rig.finish(out);
    Ok(())
}

// 11. Two flatten sends fail in a row on a full priority lane. A dropped FAK must leave
//     no armed guard, so the retry is not suppressed by single-flight, and the third
//     attempt is the one that actually reaches the lane.
fn scenario_flatten_retry_after_failure(out: &mut Vec<String>) -> Result<(), Box<dyn Error>> {
    let mut rig = Rig::new(
        1,
        FaultInjection::None,
        gate()?,
        "flatten_retry_after_failure",
    )?;
    rig.snapshot(OrderRegistrySnapshot {
        unhedged_dn: sh(19),
        unhedged_cost_dn_micro: us(9_120_000),
        available_usdc_micro: us(100_000_000),
        ..Default::default()
    });
    rig.harness
        .priority_tx
        .try_send(ExecutionCommand::CancelAll {
            reason: "saturate priority channel (attempt 1)".to_string(),
        })?;
    rig.quote(proposal(
        1,
        EXP - 27_000,
        EXP,
        Some(480_000),
        Some(470_000),
        Some(500_000),
        Some(490_000),
    ));
    rig.step(1_000_000, "attempt-1-dropped");
    rig.state("after-attempt-1");

    rig.harness
        .priority_tx
        .try_send(ExecutionCommand::CancelAll {
            reason: "saturate priority channel (attempt 2)".to_string(),
        })?;
    rig.quote(proposal(
        2,
        EXP - 26_500,
        EXP,
        Some(480_000),
        Some(470_000),
        Some(500_000),
        Some(490_000),
    ));
    rig.step(2_000_000, "attempt-2-dropped");
    rig.state("after-attempt-2");

    rig.quote(proposal(
        3,
        EXP - 26_000,
        EXP,
        Some(480_000),
        Some(470_000),
        Some(500_000),
        Some(490_000),
    ));
    rig.step(3_000_000, "attempt-3-sent");

    // Now that a FAK is genuinely in flight, the guard holds and only cancels are sent.
    rig.quote(proposal(
        4,
        EXP - 25_000,
        EXP,
        Some(480_000),
        Some(470_000),
        Some(500_000),
        Some(490_000),
    ));
    rig.step(3_100_000, "in-flight-after-retry");
    rig.state("end");
    rig.finish(out);
    Ok(())
}

fn main() -> Result<(), Box<dyn Error>> {
    let path = std::env::args()
        .nth(1)
        .ok_or("usage: actor_equivalence <out.jsonl>")?;
    let mut lines: Vec<String> = Vec::new();

    scenario_breaker(&mut lines)?;
    scenario_terminal_lockout(&mut lines)?;
    scenario_tier4_flatten(&mut lines)?;
    scenario_flatten_send_failure(&mut lines)?;
    scenario_residual_dust(&mut lines)?;
    scenario_epoch_rollover(&mut lines)?;
    scenario_normal_path(&mut lines)?;
    scenario_own_slots(&mut lines)?;
    scenario_expiry_ladder(&mut lines)?;
    scenario_flatten_partial_fill(&mut lines)?;
    scenario_flatten_retry_after_failure(&mut lines)?;

    let mut body = String::new();
    for line in &lines {
        body.push_str(line);
        body.push('\n');
    }
    let mut file = File::create(Path::new(&path))?;
    file.write_all(body.as_bytes())?;
    println!("wrote {} lines to {path}", lines.len());
    Ok(())
}
