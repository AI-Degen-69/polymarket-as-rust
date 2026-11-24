# Post-Run Statistical Analysis

Dedicated archive for post-run lifecycle performance audits, quantitative storytelling, Monte Carlo forward simulations, bootstrap confidence intervals, and stress tests.

## Directory Layout & Naming Convention

Every analyzed run gets its own dedicated folder named **`DD-MM_<N>w`** (UTC date `DD-MM` plus `<N>w` windows run; append `_HH-MMZ` if multiple runs share the same date and window count):

- `REPORT.md` — Plain-English performance narrative, accounting reconciliation, financial ratios, CIs, stress tests, and forward outlook.
- `dashboard.html` — Standalone offline Lieflat Charts interactive SVG dashboard (open directly in any browser).
- `metrics.json` — Full deterministic statistical snapshot and per-epoch ledger for cross-run comparison.

## Runs Index

| Folder | Run Window (UTC) | Source Log | Windows / Duration | All-In P&L | Trading ROI | Per-Ep Sharpe (`t`) | Profit Factor | Max DD | Files |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| `24-11_58w` | `2026-11-24 03:30–08:30Z` | `logs/lifecycle/24-11_03-27-55Z.jsonl` | 58w / 5.00h | **+$24.65** | +3.24% (+3.71% all-in) | 0.139 (`1.06`) | 1.56× | −$9.66 | [REPORT.md](24-11_58w/REPORT.md) · [dashboard.html](24-11_58w/dashboard.html) · [metrics.json](24-11_58w/metrics.json) |
