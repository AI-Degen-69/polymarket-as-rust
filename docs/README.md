# Documentation Index

Entry point for everything under `docs/`. Start here instead of guessing at a filename.

## Specification & verification

- [strategy-verification-spec.md](strategy-verification-spec.md) — the verification standard: sweep matrix, 70/30 in-sample / out-of-sample split, survival gates G1–G6, and the 400-fill validation bar.

## Strategy research

- [boundary-sweep-selection-report.md](boundary-sweep-selection-report.md) — current baseline: the `9`-configuration boundary sweep and why Config `#6/9` (`$0.65`, `120s`) was promoted. Authoritative for the shipped defaults.
- [sweep-results-investigation.md](sweep-results-investigation.md) — accounting telemetry validation (the five `summary` fields and their reconciliation identities) plus the runtime default-resolution audit.
- [post-run-analysis/README.md](post-run-analysis/README.md) — archived per-run statistical audits, indexed by run folder (`DD-MM_<N>w`), each with `REPORT.md`, `dashboard.html`, and `metrics.json`.

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
- [pr-presentations/5-presentation-risk-and-300s-lifecycle.html](pr-presentations/5-presentation-risk-and-300s-lifecycle.html) — visual showcase for the risk and 300s lifecycle work.
- [pr-presentations/16-presentation-offline-replay-and-sweep.html](pr-presentations/16-presentation-offline-replay-and-sweep.html) — visual showcase for offline replay and sweeping.
- [pr-presentations/58-presentation-winner-config.html](pr-presentations/58-presentation-winner-config.html) — interactive explainer of the promoted `$0.65` / `120s` configuration.

## Conventions

- PR presentations live in `docs/pr-presentations/<pr>-presentation-<slug>.html` as self-contained files (inline CSS/JS, no external assets).
- Question and design explainers live in `docs/explanations/`.
- Post-run audits live in `docs/post-run-analysis/<DD-MM>_<N>w/`.
- Research reports state their dataset, run id, and whether a figure is Observed, Derived, or Interpretation.
