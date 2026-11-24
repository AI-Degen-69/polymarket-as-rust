# Lifecycle Post-Run Statistical Analysis — `2026-11-24 03:30–08:30 UTC`

- **Source telemetry:** `logs/lifecycle/24-11_03-27-55Z.jsonl` (live paper lifecycle, BTC 5-minute Up/Down markets)
- **Interactive dashboard:** [dashboard.html](dashboard.html) (Lieflat Charts R09 Porcelain · offline SVG)
- **Machine-readable ledger:** [metrics.json](metrics.json)
- **Reconciliation error:** `$0.00000000` across all 58 epochs (`merge_gain + settlement − fees = realized_pnl`)

---

## 1. Executive Story

Over 5.00 hours (58 completed 5-minute epochs, 180,000 ticks at 10 Hz), the Avellaneda–Stoikov engine generated **+$24.65 all-in net P&L** (**+$21.56** trading P&L ex-rebates plus **+$3.09** estimated maker rebates) on **$664.42** deployed notional (**+3.24%** trading ROI, **+3.71%** all-in ROI, **+24.65%** return on `$100` dual-leg working capital). Against the naive baseline (**−$32.72**), the strategy produced **+$57.37** of alpha edge (**+$54.28** ex-rebates) with zero circuit-breaker trips and **7.9 µs** median end-to-end loop latency.

The run reveals a clear structural engine and one clear vulnerability:

1. **The engine wins through payoff asymmetry, not win rate.** Decided epochs finished **18W / 19L (48.6% win rate)** alongside 21 flat/idle epochs. Positive expectancy (**+$0.425/epoch** all-in, **+$0.666/active epoch**) came from **1.64× payoff asymmetry** (average win **+$3.83** vs average loss **−$2.33**, positive skewness **+0.75**).
2. **Pair-merge spread carries the P&L.** Across 100 merges, the bot paired **659.28 pairs** (`89.8%` of all `1,467.67` shares bought) at a weighted average pair cost of **$0.9320** (**+679.8 bps** gross spread), earning **+$44.82** in gross merge gains.
3. **Unhedged expiry settlement is the primary leak.** The remaining `10.2%` of shares that reached expiry unhedged went **5W / 16L (23.8% win rate)**, costing **−$23.26 net** (`+$14.83` wins vs `−$38.09` losses) and erasing 52% of gross merge profit.
4. **Statistical significance is promising but not yet locked.** On `N = 58` epochs, the per-epoch Sharpe of **0.139** yields `t = 1.06` (`85.5%` bootstrap probability of positive true expectancy). Holding this edge requires **~141 epochs (~11.8h)** for one-sided `p < 0.05` and **~200 epochs (~16.7h)** for two-sided 95% confidence.

---

## 2. Accounting Waterfall & Execution Microstructure

| Line Item | Amount (USD) | Share / Rate | Notes |
| --- | ---: | ---: | --- |
| Gross Merge Spread Gains | +$44.8206 | 679.8 bps/pair | 100 merges · 659.28 pairs · avg pair `$0.9320` |
| Unhedged Expiry Settlement (Net) | −$23.2562 | 5W / 16L (23.8%) | Gross wins `+$14.83` · gross losses `−$38.09` |
| Taker Fees Paid | −$0.0080 | 0.001% deployed | 1 mandatory flatten (`btc-updown-5m-1795500300`) |
| **Net Trading P&L (Ex-Rebates)** | **+$21.5564** | **+3.24% ROI** | Reconciled to `$0.00000000` |
| Estimated Maker Rebates | +$3.0908 | 12.5% of all-in | 147 maker fills (72 Up / 75 Down) |
| **All-In Net P&L** | **+$24.6471** | **+3.71% ROI** | **+24.65%** on `$100` working capital (`$50/leg`) |
| Naive Baseline P&L | −$32.7217 | — | Passive reference benchmark |
| **Alpha Edge (All-In / Ex-Rebate)** | **+$57.3688** | **+$54.2781** | Strategy net minus baseline |

**Microstructure & Latency:**
- **Fill quality:** 1-second markout **+64.97 bps**, 5-second markout **−3.40 bps** (confirms fast queue/spread capture rather than directional drift). Fill ratio `0.553%` (`147` fills across `2,926` submits, `10,556` replaces, `2,868` cancels).
- **Risk tiers:** `Tier1Normal`: 23, `Tier2ModerateSkew`: 12, `Tier4HardStop`: 11, `breaker_trips`: 0.
- **Engine latency:** Total loop P50 **7.9 µs**, P95 **24.0 µs**, P99 **40.2 µs**, P99.9 **99.5 µs** (Pricing P50 `4.2 µs`, Risk P50 `3.6 µs`).

---

## 3. Financial Ratios & Bootstrap Confidence Intervals

Deterministic bootstrap: `10,000` resamples (`seed = 42`), verified against 3-epoch block bootstrap (`block_len = 3`).

| Metric | All-In (58 Ep) | Ex-Rebate (58 Ep) | 90% Boot CI (All-In) | 95% Boot CI (All-In) |
| --- | ---: | ---: | --- | --- |
| Total 58-Epoch P&L | **+$24.65** | +$21.56 | `[−$12.89, +$63.51]` | `[−$19.88, +$70.25]` |
| Expectancy / Epoch (Mean) | **+$0.425** | +$0.372 | `[−$0.222, +$1.095]` | `[−$0.343, +$1.211]` |
| Block-Boot Mean (3-Ep Blocks) | **+$0.425** | +$0.372 | `[−$0.206, +$0.993]` | `[−$0.306, +$1.126]` |
| Active-Epoch Expectancy (`N=37`) | **+$0.666** | +$0.583 | Median `−$0.24` | Std Dev `$3.84` |
| Sharpe Ratio (Per 5m Epoch) | **+0.139** | +0.122 | `[−0.080, +0.338]` | `[−0.130, +0.374]` |
| Sharpe Ratio (Annualized, `√105,120`)\* | **44.9** | 39.6 | `[−26.1, +109.7]` | `[−42.1, +121.2]` |
| Sortino Ratio (Per 5m Epoch) | **+0.259** | +0.223 | `[−0.117, +0.845]` | `[−0.179, +1.004]` |
| Sortino Ratio (Annualized)\* | **83.8** | 72.1 | `[−38.1, +273.9]` | `[−58.0, +325.4]` |
| Profit Factor / Omega(0) | **1.56×** | 1.47× | `[0.78×, 3.06×]` | `[0.66×, 3.53×]` |
| Payoff Ratio (`Avg Win / \|Avg Loss\|`) | **1.64×** | 1.56× | `+$3.83 / −$2.33` | `+$3.73 / −$2.40` ex-reb |
| Kelly Fraction (Full / Half) | **17.4% / 8.7%** | 15.6% / 7.8% | Decided WR `48.65%` | Lag-1 autocorr `−0.033` |
| Max Drawdown (`$ / % $100 Cap`) | **−$9.66 (9.7%)** | −$10.31 (10.3%) | Ep 13→29 (16 ep / 80m) | Recovery `2.55×` |
| Ulcer Index / Gain-to-Pain | **$4.05 / 0.56** | $4.29 / 0.47 | `4.05%` of `$100` cap | Streaks: 3W / 4L |
| Epoch VaR(95%) / CVaR(95%) | **−$4.26 / −$5.22** | −$4.38 / −$5.26 | Worst `−$6.53` (Ep 49) | Best `+$9.87` (Ep 43) |
| Tail Ratio (`P95 / \|P5\|`) / Skew | **1.53× / +0.75** | 1.45× / +0.72 | Excess kurtosis `+0.96` | SQN / `t`-stat `1.06` |
| `P(True Expectancy > $0)` | **85.5%** | 82.5% | — | Needs `N ≥ 141` for `p<0.05` |

*\*Annualized Sharpe/Sortino multiply per-epoch ratios by `√105,120 ≈ 324.2` assuming i.i.d. stationarity. Always evaluate alongside `t = 1.06` (`N = 58`).*

---

## 4. Hourly Rhythm (`03:30–08:30 UTC`)

| Hour (UTC) | Epochs (Active) | Fills | Pairs Merged | Deployed | Merge Gain | Settlement | Rebates | All-In P&L |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| H1 · `03:30–04:30` | 12 (9) | 32 | 141.77 | $137.42 | +$10.54 | −$6.20 | +$0.63 | **+$4.98** |
| H2 · `04:30–05:30` | 12 (7) | 36 | 149.46 | $152.61 | −$0.60 | −$1.74 | +$0.63 | **−$1.71** |
| H3 · `05:30–06:30` | 12 (8) | 25 | 127.19 | $123.01 | +$14.96 | −$10.78 | +$0.59 | **+$4.76** |
| H4 · `06:30–07:30` | 12 (6) | 31 | 137.82 | $133.22 | +$14.89 | −$0.29 | +$0.66 | **+$15.26** |
| H5 · `07:30–08:30` | 10 (7) | 23 | 103.04 | $118.15 | +$5.04 | −$4.25 | +$0.57 | **+$1.35** |

Four of five hours finished positive. H2 was the sole losing hour (`−$1.71`, inside the Ep 13→29 drawdown). H4 generated `+$15.26` when unhedged settlement drag dropped to nearly zero (`−$0.29`).

---

## 5. Forward Monte Carlo Simulations (`10,000` Paths, `seed = 1337`)

Projections assume fixed `$50/leg` sizing and stationarity of the sampled epoch distribution. **Live Haircut** deducts `50 bps` execution slippage on deployed notional and cuts maker rebates by `50%`.

| Horizon · Regime | P05 | P25 | Median (P50) | P75 | P95 | `P(> $0)` | Median MDD | P95 MDD | `P(MDD > $100)` |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 24h (288 ep) · Base | +$36 | +$87 | **+$122** | +$157 | +$208 | 99.2% | −$27 | −$49 | 0.03% |
| 7d (2,016 ep) · Base | +$635 | +$770 | **+$861** | +$951 | +$1,088 | 100.0% | −$45 | −$71 | 0.32% |
| 30d (8,640 ep) · Base | +$3,196 | +$3,478 | **+$3,671** | +$3,858 | +$4,125 | 100.0% | −$60 | −$86 | 1.27% |
| 24h · Live Haircut | +$15 | +$63 | **+$97** | +$132 | +$181 | 97.4% | −$30 | −$56 | 0.06% |
| 7d · Live Haircut | +$465 | +$596 | **+$686** | +$778 | +$913 | 100.0% | −$52 | −$83 | 1.18% |
| 30d · Live Haircut | +$2,482 | +$2,753 | **+$2,946** | +$3,130 | +$3,407 | 100.0% | −$70 | −$102 | 5.79% |

**Bankroll sizing takeaway:** At `$50/leg` (`$100` active working cap), funding **$250–$300** provides a `$150–$200` drawdown buffer above the 30-day P95 haircut drawdown (`−$102`).

---

## 6. Stress Tests & Breakeven Thresholds

| Scenario | Net P&L | Delta vs Base | Expectancy/Ep | Sharpe/Ep | Profit Factor | Max DD | Survives? |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | :---: |
| Base Run (All-In) | +$24.65 | $0.00 | +$0.425 | +0.139 | 1.56× | −$9.66 | Yes |
| Zero Rebates | +$21.56 | −$3.09 | +$0.372 | +0.122 | 1.47× | −$10.31 | Yes |
| Live Slippage −50 bps | +$21.33 | −$3.32 | +$0.368 | +0.121 | 1.47× | −$10.50 | Yes |
| 0% Rebate + −50 bps | +$18.23 | −$6.41 | +$0.314 | +0.104 | 1.39× | −$11.14 | Yes |
| Extra Tail Loss (2× Worst) | +$18.12 | −$6.53 | +$0.307 | +0.097 | 1.36× | −$9.66 | Yes |
| Live Slippage −100 bps | +$18.00 | −$6.64 | +$0.310 | +0.102 | 1.38× | −$11.33 | Yes |
| Ex-Best Epoch (drop Ep #43) | +$14.78 | −$9.87 | +$0.259 | +0.092 | 1.33× | −$9.66 | Yes |
| Live Slippage −200 bps | +$11.36 | −$13.29 | +$0.196 | +0.065 | 1.23× | −$12.99 | Yes |
| Settlement Losses +50% | +$5.60 | −$19.05 | +$0.097 | +0.028 | 1.09× | −$15.30 | Barely |
| Severe Combo (0% Reb, −100 bps, 1.25× Sett) | +$5.39 | −$19.26 | +$0.093 | +0.029 | 1.10× | −$14.57 | Barely |
| **Settlement Losses 2.0×** | **−$13.44** | **−$38.09** | **−$0.232** | **−0.059** | **0.82×** | **−$25.12** | **Fails** |

- **Breakeven execution slippage:** **371.0 bps** all-in (**324.4 bps** ex-rebates) on deployed notional.
- **Breakeven unhedged settlement loss multiplier:** **1.65×**.

---

## 7. Forward Action Plan

1. **Reduce unhedged expiry exposure first:** Audit the 16 losing settlement epochs (starting with Ep #49, `−$6.53`, 18.75 unhedged Down shares) before scaling capital.
2. **Collect 150–200+ out-of-sample live windows across multiple UTC regimes:** Use chunked 15-window captures across Asia/London/US sessions with a strict train/holdout split rather than sweeping repeatedly on one contiguous block.
3. **Require statistical clearance before mainnet sizing:** Target `t ≥ 1.65` (`p < 0.05` one-sided) on untouched holdout/forward windows with `$250–$300` allocated per `$50/leg` sizing slot.
