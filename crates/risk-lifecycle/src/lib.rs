//! Risk, multi-tier inventory limits, continuous hysteresis, and 300s epoch lifecycle
//! actor pipeline for Polymarket 5-minute prediction markets.

#![forbid(unsafe_code)]
#![cfg_attr(
    test,
    allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        clippy::indexing_slicing
    )
)]

/// Circuit breakers monitoring feed staleness, API error rates, clock drift, and tick changes.
pub mod breakers;
/// Typed error definitions for risk, hysteresis, and lifecycle components.
pub mod error;
/// Deterministic 4-phase 300s prediction market epoch lifecycle controller.
pub mod lifecycle;
/// Multi-thread actor pipeline topology wiring Pricing, Risk, and Execution.
pub mod runtime;
/// Station 7: Absolute Dollar-Notional Risk Engine and multi-tier position limits.
pub mod station7_risk;
/// Station 8: Queue Preservation, Order Slot FSM, and Continuous Hysteresis Gate.
pub mod station8_hysteresis;

pub use breakers::{BreakerConfig, BreakerReason, BreakerStatus, CircuitBreakers};
pub use error::RiskLifecycleError;
pub use lifecycle::{
    EpochLifecycleController, EpochPhase, LifecycleAction, LifecycleConfig, LifecycleInput,
};
pub use runtime::{
    ActorPipelineHarness, ActorSupervisor, CorePinning, ExecutionCommand, FaultInjection,
    QuoteProposal, RiskActor, RiskActorTrace, pin_thread_to_core,
};
pub use station7_risk::{
    DollarNotionalRiskEngine, FlattenAction, OrderRegistrySnapshot, RiskConfig, RiskDecision,
    RiskEvaluationInput, RiskTier,
};
pub use station8_hysteresis::{
    CancelReason, HysteresisConfig, HysteresisGate, HysteresisInput, OrderAction, OrderSlotState,
    SizedQuote,
};
