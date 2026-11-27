# Documentation Index

Entry point for everything under `docs/`. Start here instead of guessing at a filename.

## Specification & verification

- [strategy-verification-spec.md](strategy-verification-spec.md) — the verification standard: sweep matrix, 70/30 in-sample / out-of-sample split, survival gates G1–G6, and the 400-fill validation bar.

## Strategy research

- [boundary-sweep-selection-report.md](boundary-sweep-selection-report.md) — current baseline: the `9`-configuration boundary sweep and why Config `#6/9` (`$0.65`, `120s`) was promoted as a provisional research baseline. Authoritative for the shipped defaults.
- [sweep-results-investigation.md](sweep-results-investigation.md) — Part I: accounting telemetry validation and runtime default-resolution audit; Part II: `21`-capture G3/G5 failure investigation (`25-11_21-55-50Z` vs. `26-11_07-55-07Z`); Part III: what that failure means for the shipped defaults (under-powered sample, values unchanged) and the `UNDERPOWERED` gate verdict.
- [merge-edge-retention-feasibility.md](merge-edge-retention-feasibility.md) — `21`-capture (`241`-window) accounting audit and statistical power study separating repeatable maker pair-merge edge (`+$65.24`) from adverse-selection settlement drag (`-$33.56`, `85%` loss rate, `0` flattens); recommends a complementary 3-gate framework and a sealed Holdout Vault.
- [mandatory-flatten-root-cause-investigation.md](mandatory-flatten-root-cause-investigation.md) — root-cause audit of why `0` mandatory flattens executed across `40` settlement-exposed windows (`36/40` sub-`5sh` dust, `4/40` books collapsing empty before `tau = 30s`) and counterfactuals showing early taker flattens hurt Net PnL.
- [passive-reducing-hedge-override-replay.md](passive-reducing-hedge-override-replay.md) — **NO-GO** exact L2 replay evaluation of dynamic passive reducing-leg cross-tick overrides: rescued the `4` large settlement windows (`+$9.52`) but destroyed `24%–75%` of Net PnL on mean-reverting windows; defaults left unchanged.

## Historical data (PMData)

- [hybrid-historical-pipeline.md](hybrid-historical-pipeline.md) — architecture, L2 ingestion modes, quota accounting, research-suitability matrix, and known provider limits.
- [pmdata/README.md](pmdata/README.md) — subcommand usage guide: [catalog](pmdata/catalog.md), [download](pmdata/download.md), [normalize](pmdata/normalize.md), [reconcile](pmdata/reconcile.md), [status](pmdata/status.md), [verify-historical](pmdata/verify-historical.md).
- [pmdata-phase0-historical-report.md](pmdata-phase0-historical-report.md) — Phase 0 historical verification audit and fidelity findings.

## Operations

- [poly-as-cheatsheet.md](poly-as-cheatsheet.md) — every `poly-as` subcommand, flag, and env override in one page.
- [nex-sandbox-runbook.md](nex-sandbox-runbook.md) — NEX sandbox (Amoy 80002) operator runbook for live-execution readiness.

## Explainers

- [explanations/project-glossary.html](explanations/project-glossary.html) — glossary of the project's domain terms.
- [explanations/phase-2-math-engine.html](explanations/phase-2-math-engine.html) — how Stations 1–6 price a quote.
- [explanations/phase-3-paper-sim.html](explanations/phase-3-paper-sim.html) — FIFO paper simulation and accounting.
- [explanations/verification-and-survival-gates.html](explanations/verification-and-survival-gates.html) — how strategy verification and the survival gates work.
- [explanations/risk-lifecycle-refactor.html](explanations/risk-lifecycle-refactor.html) — what splitting `RiskActor::step` into stages changed, and what the actor-level equivalence driver proves.
- [pr-presentations/5-presentation-risk-and-300s-lifecycle.html](pr-presentations/5-presentation-risk-and-300s-lifecycle.html) — visual showcase for the risk and 300s lifecycle work.
- [pr-presentations/16-presentation-offline-replay-and-sweep.html](pr-presentations/16-presentation-offline-replay-and-sweep.html) — visual showcase for offline replay and sweeping.
- [pr-presentations/58-presentation-winner-config.html](pr-presentations/58-presentation-winner-config.html) — interactive explainer of the promoted `$0.65` / `120s` configuration.

## Conventions

- PR presentations live in `docs/pr-presentations/<pr>-presentation-<slug>.html` as self-contained files (inline CSS/JS, no external assets).
- Question and design explainers live in `docs/explanations/`.
- Research reports state their dataset, run id, and whether a figure is Observed, Derived, or Interpretation.
