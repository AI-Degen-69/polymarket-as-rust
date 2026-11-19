# Polymarket 5-Minute BTC Avellaneda-Stoikov HFT Bot (Greenfield Rust) — Master Implementation Plan

> **For agentic workers:** Implement strictly one phase at a time following Section 4 (Strict Gating Rule). Use `skills/verification-before-completion/SKILL.md` on every gate and `skills/doubt-driven-development/SKILL.md` before locking any non-trivial invariant. Mandatory phase reviewers: `C:\Users\Tiger\.agents\agents\rust-reviewer.md`, `C:\Users\Tiger\.agents\agents\silent-failure-hunter.md`, and `C:\Users\Tiger\.agents\agents\rust-build-resolver.md` (on build failures).

**Goal:** Build a pure-Rust, terminal-first, zero-UI, zero-allocation-hot-path Avellaneda-Stoikov dual-bid market maker for Polymarket's 5-minute Bitcoin UP/DOWN binary prediction series, progressing cleanly from typed foundations through live feeds, 8-station math, FIFO paper simulation, risk/lifecycle actors, and live Polymarket CLOB execution with on-chain collateral recycling.

**Architecture:** Modular 7-crate Cargo workspace (`core-types`, `feeds`, `math-engine`, `paper-sim`, `risk-lifecycle`, `execution`, `cli`) running a hybrid Tokio I/O + pinned OS-thread actor pipeline. Per-feed `rtrb` SPSC ringbuffers feed a pinned Pricing thread; Pricing publishes latest-wins conflated quotes (`ArcSwap` / triple-buffer) to a pinned Risk thread; Risk sends non-blocking intents over bounded `tokio::sync::mpsc` to the Execution Tokio actor; Execution publishes lock-free `OrderRegistrySnapshot` updates back via `ArcSwap` to eliminate cyclic deadlocks. Every phase ships a runnable `poly-as` CLI subcommand printing live human-readable traces to stdout and structured JSONL telemetry to `logs/`.

**Tech Stack:** Rust `1.99.1` (`edition = "2024"`, target `x86_64-pc-windows-msvc`), `tokio`, `tokio-tungstenite` (`TCP_NODELAY`), `reqwest` (rustls), `rtrb`, `arc-swap`, `alloy` (`sol!`, `eip712_domain!`, `PrivateKeySigner`), `hmac`/`sha2`/`base64` (`URL_SAFE`), `hdrhistogram`, `secrecy`, `thiserror`, `clap`, `serde`/`serde_json`, `windows-sys` (`timeBeginPeriod`/`CreateWaitableTimerExW`).

## Global Constraints

1. **Zero UI / Pure Rust / Terminal-First:** No Python runtime, no web server, no React/Vite frontend. Every phase produces a runnable `poly-as <subcommand>` printing stdout telemetry and writing JSONL logs to disk.
2. **Zero Hardcoded Secrets:** Never commit or embed private keys, API keys, secrets, passphrases, or relayer keys in source code or `Default` impls. Load strictly from `.env` / environment variables wrapped in `secrecy::SecretString`, and never print secrets to stdout or JSONL.
3. **Zero Silent Failures & Zero Hot-Path Panics:** Workspace Cargo lints enforce `#![deny(clippy::unwrap_used, clippy::expect_used, clippy::panic)]` in library/production code (`cfg_attr(test, allow(...))`). All fallbacks emit explicit typed flags and telemetry counters.
4. **Fixed-Point Monetary Exactness:** All wire prices, order sizes, ticks, fees, and hysteresis thresholds use integer fixed-point newtypes (`MicroUsdc(u64)`, `SharesMicro(u64)`, `PriceTicks(u32)`). Floating-point (`f64`) is confined strictly to analytical math (Stations 1–5) and explicitly finite-checked before integer quantization.
5. **Zero-Allocation Hot Path:** Intern 77-digit token IDs to `#[repr(u8)] enum Outcome { Up = 0, Down = 1 }` at ingress. Parse price/size decimal slices directly into integer micro-units without allocating `String`/`Vec` inside `rtrb` slots.
6. **Official Docs Grounding:** Verified against `C:\Users\Tiger\Agents\Projects\AI Trading\Library\`, read-only legacy docs `C:\Users\Tiger\Agents\Projects\AI Trading\dangerously-skip-permissions-bot\docs\avellaneda-stoikov\`, live Polymarket official docs (`polymarket-docs` MCP, Nov 2026), and Context7 (`rtrb`, `tokio-tungstenite`, `hdrhistogram`, `alloy`).
7. **Windows 11 Platform Discipline:** Commands use Windows PowerShell (`pwsh`) syntax; use `rtk cargo ...` / `rtk git ...` in agent tool calls. Mitigate Windows 15.625ms timer granularity and `w32time` clock drift explicitly in `core-types`.
8. **Git Workflow & Delivery Discipline:** Follow [AGENTS.md](AGENTS.md) for all Phase 2–5 work: branch from synced `main` (`feat/phase-<N>-<slug>`), make atomic conventional commits per verified slice (separating prior-phase polish from new phase features), push to `origin`, open a PR with the Verification Scorecard, wait for green CI, merge cleanly (`--merge --delete-branch`), fast-forward local `main`, and push annotated tag `phase-<N>-complete`. Never commit feature work directly to `main`.

---

## 1. Library Audit & Invariant Contract

### 1.1 Canonical Formulas, Units & Protocol Rules (Cited)

| Domain | Invariant / Formula | Exact Units | Primary Source Citation | Ground-Truth / Cross-Check Status |
| :--- | :--- | :--- | :--- | :--- |
| **5m Binary Contract** | Outcomes `UP` (Yes) + `DOWN` (No); $p_{\text{UP}} + p_{\text{DOWN}} = 1.00$; horizon $T = 300\text{ s}$; resolution payoff $\$1.00$ or $\$0.00$. | `p`: USD/share (`MicroUsdc`, `$1.00 = 1_000_000`); `T`: seconds (`300s`). | `Library/01-foundations-and-models/polymarket-5m-btc-microstructure-model.md:36-42` | Verified. |
| **Settlement Oracle** | 5m BTC resolves on **60-second Chainlink TWAP** over $[T-60\text{s}, T]$ vs inception 60s TWAP strike $K = S(t_0)$ (`UP` if $\text{TWAP} \ge K$). | `S(u), K`: USD/BTC (`f64`); window: `60s`. | `Library/01-foundations-and-models/microstructure-calibration-engine.md:36-43`; `polymarket-docs:/changelog/predictions#aug-14-2026` | Verified (Aug 7 was 30s; Aug 14, 2026 moved 5m markets to 60s). Note: Nov 30, 2026 PolyBolt Chainlink requires explicit Chainlink API key (`changelog/predictions#nov-16-2026`). |
| **Dynamic Taker Fee** | $\text{Fee (USDC)} = \text{shares} \times \text{feeRate} \times p \times (1 - p)$, rounded to 5 decimals, min `0.00001 USDC`. Makers pay `0.00%`. | `fee`: USDC; `feeRate`: dimensionless (`0.05` post-Nov 20, 2026; `0.07` pre-Nov 20, 2026). | `Library/03-exchanges-and-protocols/polymarket/fees-and-maker-rebates.md:33-55,63`; `polymarket-docs:/programs/fee-and-rebate-changes-november-2026` | **DYNAMIC READ REQUIRED:** Read `feeSchedule` from market metadata; fallback `0.05` (max drag `1.25%` = `$0.0125/share` at `p = 0.50`). |
| **Maker Rebates** | Rolling 30d weighted volume $wV = \sum \text{shares}_k \cdot p_k (1-p_k)$; tiers: `<$200k: 0%`, `$200k–$1M: 3%`, `$1M–$4M: 7%`, `$4M–$10M: 10%`, `>=$10M: 15%`. | `wV`: USD-equivalent shares; rebate paid daily in `pUSD`. | `Library/03-exchanges-and-protocols/polymarket/fees-and-maker-rebates.md:106-124` | Effective Nov 20, 2026 23:59 UTC (replaces legacy flat 20% Crypto pool in `binary-spread-capture.md:59`). |
| **Station 1: SG Denoising** | 5-pt quadratic Savitzky-Golay (`m=2, d=2`) on 100ms grid. Causal right-endpoint (default): $c_{\text{causal}} = [3,-5,-3,9,31]/35$; Symmetric (A/B): $c_{\text{sym}} = [-3,12,17,12,-3]/35$. | `p_mid, s_i`: USD/share in `[tick, 1 - tick]`. | `Library/01-foundations-and-models/microstructure-calibration-engine.md:27-34`; Legacy `03-rust-engine-implementation.md:59-83` | **CONTRADICTION FIXED** (see §1.2 #1): Library uses non-causal centered weights; legacy code has wrong comment but exact causal array `[3,-5,-3,9,31]/35`. |
| **Station 2: TWAP & Variance** | Running 60s TWAP $\frac{1}{W}\int_{t-W}^{t} S(u)\,du$. Effective variance with explicit mode: default `BinaryCara` $V_{\text{rem}} = p(1-p)\cdot\max(\tau/300, \tau_{\text{floor}})$; A/B `LibraryStrict` $\sigma_{\text{eff}}^2(t) = \sigma_0^2((T-t)/T)^2$. | `tau`: seconds in `[0, 300]`; `V_rem`: `USD^2`; `sigma_0^2`: `USD^2/s` (or `USD^2/epoch`). | `Library/01-foundations-and-models/microstructure-calibration-engine.md:36-43`; `polymarket-5m-btc-microstructure-model.md:75`; `binary-spread-capture.md:39-45` | **CONTRADICTION FIXED** (see §1.2 #2): Avoids $\tau^3$ skew collapse and annualized-vol dimensional blow-up. |
| **Station 3: Arrival Intensity $\kappa$** | $\lambda(\delta) = A e^{-\kappa\delta}$. Default `TradeNormalizedDepth`: $\kappa_{\text{emp}} = \min(\kappa_{\text{touch}}, \frac{\sum \delta_j (V_{\text{cum},j}/\bar{V})}{\sum \delta_j^2})$ on UP book only; A/B `LibraryLogSlope`: $\frac{\ln(V_k) - \ln(V_1)}{\delta_k - \delta_1}$. Clamped `[15.0, 200.0]`, explicit fallback `50.0`. | `delta`: USD; `kappa`: `USD^-1`; `V_cum, V_bar`: shares. | `Library/01-foundations-and-models/microstructure-calibration-engine.md:45-51`; `avellaneda-stoikov-foundations.md:46-50` | **FATAL SIGN & SCALE BUGS FIXED** (see §1.2 #3): Library formula `(ln(V_1)-ln(V_k))/(delta_k-delta_1)` is ALWAYS `<= 0` for cumulative depth! |
| **Station 4: Optimal Spread $s^*$** | $s^* = (\text{include\_var} ? \gamma V_{\text{rem}} : 0) + \frac{2}{\gamma}\ln(1 + \frac{\gamma}{\kappa})$, floored by $s_{\min} = \max(2\cdot\text{tick}, \text{fee\_floor})$. | `gamma`: `USD^-1`; `s*`: USD. | `Library/01-foundations-and-models/microstructure-calibration-engine.md:53-59`; `avellaneda-stoikov-foundations.md:72,82` | Supports both full A-S spread (with variance term) and pure Station 4 spread via config flag. |
| **Station 5: Reservation Price $r_i$** | $r_i = s_i - q \cdot \gamma \cdot V_{\text{rem}}$ where $q = q_{\text{UP}} - q_{\text{DOWN}}$ (net unhedged shares). Clamped to `[tick, 1 - tick]`. | `q`: signed shares (`i64`); `r_i`: USD/share. | `Library/01-foundations-and-models/microstructure-calibration-engine.md:61-67`; `avellaneda-stoikov-foundations.md:59` | **INVERTED SIGN BUG IN LIBRARY REJECTED** (`avellaneda-stoikov-foundations.md:79-80` flipped signs of `q`!). |
| **Station 6: Dual-Bid Matrix** | $p_{\text{bid,raw}}^{\text{UP}} = r_i - s^*/2$, $p_{\text{bid,raw}}^{\text{DN}} = 1.00 - (r_i + s^*/2)$. Quantized via `floor_to_tick`. Invariant: `bid_up + bid_dn <= max_combined_bid` (`990_000` micro-USDC at `0.01` tick). 3-way local Post-Only guard. | `bid_up, bid_dn`: `MicroUsdc(u64)`. | `Library/01-foundations-and-models/microstructure-calibration-engine.md:69-76`; Legacy `01-pipeline-overview.md:64-66` | Uses conservative `floor_to_tick` (not `.round()`), graceful widening/suppression (never hot-path `assert!`). |
| **Station 7: Risk & Lifecycle** | Cost-basis exposure `u = unhedged_cost_basis / V_max`; $Q_{\max}(p) = \lfloor V_{\max} / p \rfloor$; linear size throttle + 4 Risk Tiers (`<=20%`, `20–60%`, `60–90%`, `>90%` FAK flatten) + 4 Expiry Phases (`>60s`, `60–30s`, `30–15s`, `<=15s`). | `V_max`: `MicroUsdc`; `u`: dimensionless ratio; `tau`: seconds. | `Library/01-foundations-and-models/microstructure-calibration-engine.md:78-84`; `inventory-skew-risk-management.md:69-85`; `polymarket-5m-btc-microstructure-model.md:91-111` | **MARTINGALE BUG & UNDERFLOW FIXED** (see §1.2 #7): Uses cost-basis exposure, includes open bids, caps reducing leg at `<= |q|`, handles dust `< min_order_size`. |
| **Station 8: Hysteresis Gate** | Continuous deadband on unrounded target: `|p_target_raw - active_price| >= (1.0 + h_buf) * tick` (`h_buf = 0.25` default), OR `active_remaining > allowed_size`, OR urgent cancel. Strict `Idle/PendingPlace/Live/PendingCancel/PendingReconcile` FSM. | `MicroUsdc(u64)` + `f64` deadband threshold. | `Library/01-foundations-and-models/microstructure-calibration-engine.md:86-92`; Legacy `03-rust-engine-implementation.md:302-362` | **ZERO-HYSTERESIS & IEEE 754 BUGS FIXED** (see §1.2 #8): Avoids `0.52-0.51 < 0.01` float bug and tick-boundary churn. |
| **Cross-Venue Lead-Lag** | $p_t^* = \Phi(d_2)$, $d_2 = \frac{\ln(S_t/K) - \frac{1}{2}\sigma_{\text{bin}}^2 \tau_{\text{yr}}}{\sigma_{\text{bin}}\sqrt{\tau_{\text{yr}}}}$, $\Delta p^* \approx \frac{\phi(d_2)}{S_t \sigma_{\text{bin}}\sqrt{\tau_{\text{yr}}}}\Delta S_t$. Defensive maker cancel `< 15ms` when adverse $|\Delta p^*| \ge \theta_{\text{cancel}}$. | `tau_yr = tau_secs / 31_536_000.0`; `sigma_bin`: annualized vol. | `Library/02-strategies-and-alphas/cross-venue-basis-arbitrage.md:34-57` | Wired into fast-path adverse-selection cancel bypassing 100ms SG grid. |
| **CLOB Wire Encoding** | 6-decimal integer strings (`× 10^6`): BUY `makerAmount = price*size`, `takerAmount = size`; SELL `makerAmount = size`, `takerAmount = price*size`. Tick matrix (`0.01 -> 2p/2s/4a`, `0.001 -> 3p/2s/5a`, `0.0001 -> 4p/2s/6a`). | `u128` exact intermediate -> `u64` micro-units -> ASCII string. | `Library/03-exchanges-and-protocols/polymarket/clob-rest-api.md:83-107`; `polymarket-docs:/trading/place-orders` | Exact integer rounding avoids float drift (`0.29 * 10.0`). |
| **EIP-712 & `signatureType`** | 11-field V2/V3 `Order(salt,maker,signer,tokenId,makerAmount,takerAmount,side,signatureType,timestamp,metadata,builder)`. Domain selected per `GET /book`: `version:"v2"` -> v3 `ExchangeV3 (0xe333...)`; CTF -> v2 `0xE111...` (`neg_risk:false`) / `0xe222...` (`neg_risk:true`). `0=EOA`, `1=Proxy`, `2=Safe`, `3=POLY_1271` (ERC-7739 `TypedDataSign`). | `timestamp`: Unix **ms** in `Order`; `POLY_TIMESTAMP`: Unix **s** in L2 header; `salt <= 2^53 - 1`. | `polymarket-docs:/trading/place-orders`, `/api-reference/trade/post-a-new-order` | **LIBRARY V1 STRUCT & `sig_type=3` LABEL REJECTED** (see §1.2 #9–10). |
| **L2 HMAC Auth** | `urlsafeBase64WithPadding(HMAC-SHA256(urlSafeBase64Decode(secret), ts + METHOD + path_no_query + exact_body))`. | `POLY_TIMESTAMP`: Unix seconds; secret decoded via `URL_SAFE`. | `polymarket-docs:/trading/manage-orders`, `/trading/combos/requesters` | **CRITICAL:** Operator secret contains `_` (URL-safe base64); standard base64 fails! |
| **CLOB Dead Man's Switch** | `POST https://clob.polymarket.com/v1/heartbeats` with L2 HMAC + `{"heartbeat_id":"<prev>"}` every **3.5s** (10s expiry, 5s server scan). Recover `400 Invalid Heartbeat ID` via returned ID. **NO DISARM ENDPOINT** — call `DELETE /cancel-all` on shutdown. | Interval: `3,500ms`; expiry: `10,000ms`. | `polymarket-docs:/trading/manage-orders#order-heartbeats` | **LIBRARY `PATCH /v1/trade/auto-cancel` REJECTED** (that endpoint is Perps-only, `api.perpetuals.polymarket.com`!). |
| **WebSocket Wire Schemas** | CLOB Market (`wss://ws-subscriptions-clob.polymarket.com/ws/market`) & User (`/ws/user`): client sends text `"PING"` every 10s -> `"PONG"`. Flat `event_type` JSON (`book`, `price_change` with `price_changes[]`, `last_trade_price`, `tick_size_change`, `order`, `trade`). `/ws/user` auth: `{"auth":{"apiKey","secret","passphrase"},"type":"user"}`. PolyBolt (`wss://ws-live-v2.polymarket.com/ws`): server RFC6455 pings 25s, `op:"auth"`, reference prices/TWAP only. | Timestamps: Unix ms strings/ints. | `polymarket-docs:/market-data/realtime-data`, `/trading/realtime-order-updates`, `/migrate/rtds-to-polybolt` | **LIBRARY WS SCHEMAS REJECTED** (Library copied TS SDK wrapper `topic/type/payload` and wrong `/ws/user` auth!). |
| **On-Chain Contracts & Merge** | Polygon `Chain ID 137`. CTF: `0x4D97DCd97eC945f40cF65F87097ACe5EA0476045`; CTF Exchange: `0xE111180000d2663C0091e4f400237545B87B996B`; ExchangeV3: `0xe3333700cA9d93003F00f0F71f8515005F6c00Aa`; PositionManager: `0x006F54F7f9A22e0000CC2AB60031000000ae9fEF`; Router: `0x12121212006e4CD160D18e3f00711DA5c3372600`; pUSD: `0xC011a7E12a19f7B1f670d46F03B03f3342E82DFB`. Relayer v2: `POST https://relayer-v2.polymarket.com/submit` (`RELAYER_API_KEY` + `RELAYER_API_KEY_ADDRESS`). Fallback batch threshold $N_{\min} = \lceil 10 \cdot \text{TxCost}_{\text{gas}} / s^* \rceil$. | ERC-1155 / pUSD 6-decimal `uint256`. | `Library/03-exchanges-and-protocols/polymarket/onchain-ctf-contracts.md:31-38`; `polymarket-docs:/resources/contracts`; Legacy `04-ctf-settlement-lifecycle.md:160` | **CORRUPTED ADDRESS IN LEGACY DOC REJECTED** (`06-developer-bibliography-and-resources.md:83` had wrong CTF hex!). Dispatches V2 `Router.merge` vs CTF `mergePositions` by market version. |
| **Rate Limits & Taker Delay** | CLOB burst/sustained: `POST /order` 500/s burst (80/s sustained), `POST /orders` max 15 orders/req (150/s burst), `DELETE /order` 500/s burst. Per-signer token-bucket headers `Poly-RateLimit-Remaining`, `Retry-After`. Crypto taker delay: `50ms` (read `takerDelayMs`/`itode` per market). | Orders/sec; ms. | `polymarket-docs:/changelog/predictions#april-8-2026`, `#aug-17-2026`; `Library/03-exchanges-and-protocols/polymarket/clob-rest-api.md:133-159` | Adaptive rate-limit tracker reads response headers and enforces backoff on `429`. |

---

### 1.2 Library & Legacy Contradictions, Bugs & Gaps Discovered (14 Findings)

| # | Contradiction / Trap Found | Conflicting Citations | Technical Impact If Uncaught | Authoritative Resolution Locked in Plan |
| :--- | :--- | :--- | :--- | :--- |
| **C1** | **Non-causal SG weights & corrupted comment** | `Library/microstructure-calibration-engine.md:29-33` (centered `[-3,12,17,12,-3]/35`) vs Legacy `03-rust-engine-implementation.md:76-83` (wrong comment `[3,-8,-7,12,31]/31`, right array `[3,-5,-3,9,31]/35`). | 200ms phase lag in live quoting; negative weights can overshoot `[0, 1]` (e.g. `[0.99,0.50,0.50,0.90,0.99]` -> `1.0789`). | Default causal `[3,-5,-3,9,31]/35` clamped to `[best_bid, best_ask]` + `[tick, 1-tick]`; symmetric available via `--sg-mode` for A/B replay; bypass SG on fast-path cancel. |
| **C2** | **Station 2+5 dimensional blow-up & $\tau^3$ skew collapse** | `Library/microstructure-calibration-engine.md:38-39,62` (`sigma_spot` annualized $\times ((T-t)/T)^2 \times (T-t)$) vs `polymarket-5m-btc-microstructure-model.md:75` (`p(1-p)*(tau/300)`) vs Legacy `03-rust-engine-implementation.md:153-184` (300s TWAP instead of 60s). | Passing annualized BTC vol with seconds blows up $r_i$ by $31.5\text{M}\times$; combining $((T-t)/T)^2$ with $(T-t)$ decays skew by $\tau^3$ (125×–630× too small in final minute!). | Implement true 60s rolling TWAP; unify Station 5 on `V_rem` in `USD^2` (`BinaryCara` default with `tau_floor_frac = 0.20`, plus `LibraryStrict` A/B toggle); guard `tau >= 0.0` via `saturating_sub`. |
| **C3** | **Station 3 $\kappa$ inverted sign + scale invariance + `NaN` bypass** | `Library/microstructure-calibration-engine.md:46-50` (`(ln(V_1)-ln(V_k))/(delta_k-delta_1)` on cumulative $V$). | Cumulative $V_k \ge V_1 \implies \kappa \le 0$ ALWAYS -> `NaN` in Station 4! Naive positive log-slope is scale-invariant and inverted by touch walls; `kappa=2..12` yields 17¢–100¢ spreads. | Default `TradeNormalizedDepth` (`V_cum / V_bar` weighted by `delta` with touch cap, UP book only), explicit `is_finite() && > 0.0` guard before clamping `[15.0, 200.0]`, fallback `50.0` (`s* ≈ 0.04`). Keep `LibraryLogSlope` behind `--kappa-mode` for A/B. |
| **C4** | **Inverted inventory skew signs in A-S Foundations** | `Library/avellaneda-stoikov-foundations.md:79-80` (`delta_a* = 0.5*S + q*...`, `delta_b* = 0.5*S - q*...`) vs lines `59,63,75-76`. | Moves ask UP and bid UP when long (`q > 0`), accelerating inventory blow-up instead of mean-reverting. | Reject lines 79–80. Strictly enforce `r_i = s_i - q * gamma * V_rem`, `bid_up = r_i - s*/2`, `bid_dn = 1.0 - (r_i + s*/2)`. |
| **C5** | **Missing variance term in Station 4 spread** | `Library/avellaneda-stoikov-foundations.md:72` includes `gamma * sigma^2 * (T-t)`; `microstructure-calibration-engine.md:54` omits it. | Under-prices spread during high volatility regimes. | Config toggle `include_variance_spread` (default `true`), plus fee-barrier floor `max(2*tick, fee_rate * p * (1-p) + tick)`. |
| **C6** | **Fee schedule & rebate tier transition (Nov 20, 2026)** | `Library/fees-and-maker-rebates.md:63,116-122` (`0.05`, 0–15% tiered) vs `binary-spread-capture.md:59,76` (`0.07`, 20% flat). | Wrong fee drag and profitability thresholds before/after Nov 20, 2026 23:59 UTC. | Read `feeSchedule` dynamically from Gamma/CLOB market metadata; configurable default `0.05`. |
| **C7** | **Station 7 gross inventory, first-order breach & mark-price martingale** | `Library/microstructure-calibration-engine.md:79-83` (`q_leg * p`) vs `inventory-skew-risk-management.md:69-74` (4 tiers on `|Delta_q|/q_max`) vs Legacy `03-rust-engine-implementation.md:288` (`base=100` breaches `V_max=50` at `p=0.60`) vs `05-runbook.md:33` (`BASE_ORDER_SIZE_UNITS` USD vs shares). | Losing leg dropping `$0.50 -> $0.10` drops mark exposure to 20% and triggers martingale buying; first order can exceed `V_max`; paired unmerged shares choke quoting; dust `< 5` shares loops FAK forever. | Measure `u` on **unhedged acquisition cost basis + open bid notional**; enforce `min(size, Q_max - unhedged_leg, affordable_shares)`; cap reducing leg at `<= |q|`; skip FAK on dust `< min_order_size`; clarify `BASE_ORDER_SIZE_SHARES` explicitly in shares. |
| **C8** | **Station 8 IEEE 754 failure & zero-hysteresis churn** | Legacy `03-rust-engine-implementation.md:291,349` (`.round()` breaks `< $1.00`; `0.52_f64 - 0.51_f64 = 0.009999999999999953 < 0.01_f64` returns `HOLD`!). | Misses real 1¢ shifts (adverse selection); conversely comparing already-floored ticks causes churn on `$0.00002` jitter across tick boundaries. | Evaluate continuous `|p_target_raw - active_price| >= 1.25 * tick` deadband; snap prices with conservative integer `floor_to_tick`; enforce strict per-leg order FSM (`PendingCancel` never overlaps `PendingPlace`). |
| **C9** | **Dead Man's Switch wrong product API (`Perps` vs `CLOB`)** | `Library/llms.txt:13`, `market-making-and-auto-cancel.md:58-88` (`PATCH /v1/trade/auto-cancel`) vs `polymarket-docs:/trading/manage-orders#order-heartbeats` (`POST /v1/heartbeats`). | `PATCH /v1/trade/auto-cancel` does NOT exist on `clob.polymarket.com` (it is Perps-only on `api.perpetuals.polymarket.com`) -> zero crash protection! | Implement `POST https://clob.polymarket.com/v1/heartbeats` serialized every 3.5s with `400 Invalid Heartbeat ID` retry + explicit `DELETE /cancel-all` on shutdown (no disarm endpoint exists). |
| **C10** | **`signatureType: 3` mislabeled & EIP-712 Order struct obsolete V1** | `Library/account-credentials-and-auth.md:36,111`, `clob-rest-api.md:112-127`, Legacy `05-runbook.md:53` vs `polymarket-docs:/trading/place-orders`. | Sending V1 struct (`nonce/feeRateBps/signed expiration`) or unwrapped `sig_type=3` rejects 100% of orders. | Implement 11-field V2/V3 `Order`, dynamic domain (`v3 ExchangeV3` vs `v2 CTF`), and typed `WalletMode` (`Proxy=1`, `Safe=2`, `DepositWallet1271=3` with ERC-7739 `TypedDataSign`). Confirm operator wallet type on-chain before Phase 5. |
| **C11** | **WebSocket wire schemas & PolyBolt role wrong in Library** | `Library/clob-websocket-streams.md:36,58-95,123-131,169` (`topic/type/payload`, single `price_change`, `timestamp/signature` auth, server pings) vs `polymarket-docs:/market-data/realtime-data`, `/trading/realtime-order-updates`, `/migrate/rtds-to-polybolt`. | Zero live CLOB WS messages deserialize; `/ws/user` auth fails; PolyBolt is reference-price/TWAP only (not L2 CLOB). | Parse raw flat `event_type` frames (`book`, `price_changes[]`, `last_trade_price`, `tick_size_change`, `order`, `trade`), authenticate `/ws/user` with `{apiKey, secret, passphrase}`, send client text `"PING"` every 10s, parse maker fills strictly from `maker_orders[]`. |
| **C12** | **Corrupted CTF address & Protocol V2 vs CTF merge split** | Legacy `06-developer-bibliography-and-resources.md:80,83` (`0x4bFb...`, corrupted `0x4D97DCdE725...`) vs `Library/onchain-ctf-contracts.md:33-38` & `polymarket-docs:/resources/contracts`. | Sending merge tx to corrupted contract or calling CTF `mergePositions` on a V2 market reverts on-chain. | Lock official `polymarket-docs` contract registry; dispatch V2 `Router.merge` vs CTF `mergePositions(pUSD)` per `market.version`; merge ONLY `CONFIRMED` on-chain balances. |
| **C13** | **GTD 180s minimum incompatible with 300s market** | `Library/market-making-and-auto-cancel.md:40-42` (GTD requires `>= 180s` future, expires 60s early). | Cannot set GTD to expire at `t=270s` once `t > 90s`. | Always use `GTC` + `postOnly: true` for passive quoting, paired with active `DELETE /order`, `T-30s` cancel-all, and `POST /v1/heartbeats`. |
| **C14** | **Hardcoded live credentials & inverted `DRY_RUN` comment** | `Library/account-credentials-and-auth.md:35-41,106-117` (plaintext live private key in `Default`); Legacy `06-bibliography.md:117-118` (inverted `DRY_RUN` comment); Legacy `05-runbook.md:149,172` (Gamma `limit=50` + wrong `tokens` field). | Secret leak risk; accidental live execution; Gamma discovery always fails with `"Tokens array missing"`. | Strictly env-only `SecretString` config; default `ExecutionMode::Paper`; require explicit `--mode canary-live --confirm-live-capital` double latch; parse Gamma `positionIds` (v2) AND `clobTokenIds` JSON string (v1). |

---

## 2. Pre-Mortem Risk Register

| ID | Silent Failure Mode | Failure Trigger / Mechanism | Concrete Rust Type / Runtime Guard | Verification Test |
| :--- | :--- | :--- | :--- | :--- |
| **R1** | **Silent `NaN` propagation through `.clamp()`** | Thin/empty L2 book produces `ln(0)` or `0/0` in $\kappa$; in Rust `f64::NAN.clamp(a, b)` returns `NaN`. | Newtype `FinitePosF64::try_new(v)` rejects `!v.is_finite() \|\| v <= 0.0`. Every station boundary validates finiteness and emits `MathFallbackReason` telemetry. | Phase 2 unit test: empty/zero/inverted book -> explicit fallback flag, zero `NaN` output. |
| **R2** | **IEEE 754 tick comparison & rounding breach** | `0.52_f64 - 0.51_f64 < 0.01_f64` returns `HOLD`; `.round()` turns `0.496 + 0.496` into `$1.00`. | `MicroUsdc(u64)` (`$1.00 = 1_000_000`) + `floor_to_tick(raw, tick_micro)`. Pair cost checked gracefully (`<= max_combined_bid_micro`), stepping down heavier leg or suppressing (never `assert!`). | Phase 0 & 2 tests: `0.51 -> 0.52`, `0.496 + 0.496`, `0.29 * 10.0`. |
| **R3** | **Pricing OS thread panic leaving orphan live quotes** | Any `unwrap()`, `assert!`, or `u64` subtraction underflow kills Pricing/Risk thread silently. | Workspace `#![deny(clippy::unwrap_used, clippy::expect_used, clippy::panic)]`; `saturating_sub` / `checked_sub`; supervisor catches thread exit / channel close and fires emergency `DELETE /cancel-all` + process exit non-zero. | Phase 4 `VERIFY-AGAIN`: fault-inject invalid tick & verify graceful degrade + clippy deny check. |
| **R4** | **Stale quote FIFO backlog & cyclic actor deadlock** | 1024-deep `crossbeam` queue stalls during REST `429` or blocks Tokio worker on `recv()`, deadlocking bidirectional `Risk <-> Execution`. | Pricing -> Risk uses lock-free **latest-wins** `ArcSwap<QuoteProposal>` (zero queue buildup). Risk -> Execution uses non-blocking `tokio::sync::mpsc::try_send` (bounded control channel). Execution -> Risk/Pricing uses lock-free `ArcSwap<OrderRegistrySnapshot>`. Zero blocking calls in Tokio tasks! | Phase 4 `VERIFY-AGAIN`: inject 500ms artificial Execution stall; verify latest quote wins immediately & heartbeat never stalls. |
| **R5** | **Maker fill inventory corruption & double-counting** | Reading top-level taker fields on `/ws/user` `trade` event when `trader_side == "MAKER"`, or counting both `order.size_matched` and `trade` (`MATCHED`+`MINED`+`CONFIRMED`). | `FillReconciler` parses `maker_orders[]` matching `(order_id, maker_address)`, deduplicates strictly by `HashMap<TradeId, TradeStatus>`, reverses `FAILED` trades, and blocks quoting on WS reconnect until REST `/data/orders` + `/data/trades` parity check succeeds. | Phase 5 unit + replay tests: duplicate `MATCHED`/`MINED`/`CONFIRMED`, `FAILED` reversal, and opposing-token maker match. |
| **R6** | **Non-atomic `CancelAndReplace` double fill / `order_not_found` race** | Placing replacement before cancel confirms, or immediately replacing on `order_not_found` when old order actually filled. | Strict per-leg `OrderSlotState` FSM (`Idle`, `PendingPlace`, `Live`, `PendingCancel`, `PendingReconcile`). Replacement queues behind confirmed cancel; `order_not_found` enters `PendingReconcile` until fill/cancel status is proven. | Phase 4 & 5 FSM transition unit tests + simulated race injection. |
| **R7** | **Mark-price martingale & reducing-leg flip** | Computing utilization `u` on mark price buys into crashes; unconstrained reducing leg flips `+65` to `-60`. | `u = (unhedged_cost_basis + open_bid_notional) / V_max`. Reducing leg size strictly `<= |Delta_q| - open_reducing_size`. Dust `0 < |Delta_q| < min_order_size` logged and skipped from FAK. | Phase 4 `VERIFY-AGAIN`: crash simulation `$0.50 -> $0.10` & dust `3 shares` test. |
| **R8** | **Stale WebSocket / half-open Windows TCP socket** | TCP connection silently stalls without FIN; bot quotes on 30-second-old prices. | Per-feed monotonic `last_msg_ns` watchdog (`poly_stale_ms = 3000`, `binance_stale_ms = 2000`), CLOB 10s text `"PING"` with 15s `"PONG"` timeout, automatic `StaleFeed` risk lockout + `CancelAll` + reconnect + REST `/book` snapshot resync before re-arming. | Phase 1 & 4 `VERIFY-AGAIN`: drop feed / blackhole mock WS -> lockout triggers within `< 3.5s` and recovers cleanly after resync. |
| **R9** | **Windows 15.625ms timer & `w32time` clock drift** | Default Windows scheduler adds 15ms jitter; drifted system clock causes `401 Invalid L1/L2` or mistimed `T-30s` flatten. | RAII `HighResTimerGuard` (`timeBeginPeriod(1)` / `CreateWaitableTimerExW`), `std::time::Instant` (QPC) for all durations/latencies, periodic exchange-clock offset sync (`GET /time` + Binance `/api/v3/time`) with `> 1,000ms` drift circuit breaker. | Phase 0 `doctor` & Phase 1 clock-offset trace. |
| **R10** | **Cross-book & self-crossing Post-Only rejections** | Async UP/DOWN WS delta skew or cancel-replace timing causes `invalid post-only order: order crosses book`, burning rate limit. | 3-way local guard: `bid_up <= min(best_ask_up, 1 - best_bid_dn, 1 - active_bid_dn) - tick`. On CLOB post-only reject, treat as non-fatal, refresh book, back off 1 tick; trip circuit breaker only on `>= 5` consecutive `400`/`401` errors in 10s. | Phase 2 & 5 unit + mock rejection tests. |
| **R11** | **Unintended live capital deployment or secret leak** | Accidentally launching live orders during testing or printing `.env` credentials in logs/panics. | `SecretString` custom `Debug` redaction; `ExecutionMode::Paper` default; live mode requires BOTH `DRY_RUN=false` AND CLI `--mode canary-live --confirm-live-capital` plus capped canary `V_max <= $25` check. `.gitignore` blocks `.env` and `logs/`. | Phase 0 & 5 tests asserting `Debug` + JSONL output contain zero secret substrings. |
| **R12** | **Heartbeat ID desync or missing shutdown cancel** | Crash/restart within 10s causes `400 Invalid Heartbeat ID` on `POST /v1/heartbeats`, or clean exit leaves quotes open for 10–15s. | Serialized 3.5s heartbeat task parses `400` response `heartbeat_id` and retries immediately. `ctrl_c` shutdown handler executes `DELETE /cancel-all` and verifies `GET /data/orders` empty before process exit. | Phase 5 mock HTTP server test for `400` recovery + shutdown cancel sequence. |

---

## 3. Phased Build Plan

### Workspace Directory Map (Locked Upfront)

```text
polymarket-as-rust/
├── .gitignore
├── .env.example
├── Cargo.toml                         # Workspace root + strict clippy lints
├── IMPLEMENTATION_PLAN.md             # This master specification
├── logs/                              # Gitignored JSONL telemetry output
└── crates/
    ├── core-types/                    # Phase 0: Fixed-point types, config, errors, clock, HDR, JSONL
    ├── feeds/                         # Phase 1: Gamma v1/v2 discovery, Binance/CLOB/PolyBolt WSS, rtrb
    ├── math-engine/                   # Phase 2: Zero-alloc Stations 1-6 + lead-lag + PipelineTrace
    ├── paper-sim/                     # Phase 3: FIFO queue-aware simulator, VWAP merge, BaselineTelemetry
    ├── risk-lifecycle/                # Phase 4: Stations 7-8, Tiers 1-4, 300s FSM, actor wiring
    ├── execution/                     # Phase 5: EIP-712 V2/V3, L2 HMAC, REST/Heartbeats, /ws/user, Merge
    └── cli/                           # Unified binary `poly-as` with phase subcommands
```

---

### Phase 0: Workspace Scaffold, Fixed-Point Types, Env Security, Windows Clock & JSONL Telemetry

#### [0.1 DO]
- **Initialize Git & Workspace:**
  - `git init`, create `.gitignore` (ignore `/target`, `.env`, `logs/*.jsonl`, `*.log`), create `.env.example` (placeholder values ONLY — never real keys!).
  - Create root `Cargo.toml` with `resolver = "2"`, `edition = "2024"`, `rust-version = "1.88"` (matching `hdrhistogram 7.6.0`), workspace members `["crates/core-types", "crates/cli"]` (expanding workspace members sequentially in each phase), and `[workspace.lints.clippy]` denying `unwrap_used`, `expect_used`, `panic`, `todo`, `unimplemented`.
- **Crate `crates/core-types/`:**
  - `src/money.rs`:
    - `MicroUsdc(u64)` (`$1.00 = 1_000_000`, `ONE_DOLLAR = 1_000_000`, `ONE_CENT = 10_000`), `SharesMicro(u64)` (`1 share = 1_000_000`), `SignedSharesMicro(i64)`, `FinitePosF64(f64)`.
    - Exact zero-allocation decimal parser `parse_decimal_micro(bytes: &[u8]) -> Result<u64, ParseDecimalError>` (no `f64` round-trip!).
    - Tick rounding helpers: `floor_to_tick(raw_usd: f64, tick_micro: MicroUsdc) -> Result<MicroUsdc, MathError>` (checks `is_finite()`, rejects `< 0.0`), `encode_buy_amounts(price: MicroUsdc, shares: SharesMicro, tick: TickPrecision) -> Result<WireAmounts, WireError>`, `encode_sell_amounts(...)` using exact `u128` arithmetic matching `polymarket-docs:/trading/place-orders` tick precision matrix (`0.1, 0.01, 0.005, 0.0025, 0.001, 0.0001`).
    - `FeeSchedule { fee_rate_bps_e6: u64, maker_fee_zero: bool }` with `compute_taker_fee_micro(shares: SharesMicro, price: MicroUsdc) -> MicroUsdc` (exact `shares * feeRate * p * (1-p)`, rounded to 5 decimals / `10` micro-USDC, `< 10` micro-USDC rounds to `0` per `fees-and-maker-rebates.md:54-55`).
  - `src/market.rs`:
    - `#[repr(u8)] #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)] pub enum Outcome { Up = 0, Down = 1 }`
    - `#[derive(Debug, Clone, Copy, PartialEq, Eq)] pub enum ProtocolVersion { CtfV1, PolyV2 }`
    - `#[derive(Debug, Clone, Copy, PartialEq, Eq)] pub enum WalletMode { Eoa, PolyProxy, GnosisSafe, DepositWallet1271 }`
    - `MarketDescriptor` (`condition_id`, `slug`, `question`, `version`, `neg_risk`, `asset_id_up`, `asset_id_dn`, `tick_size_micro`, `min_order_size_micro`, `taker_delay_ms`, `fee_rate`, `start_ts_ms`, `expiry_ts_ms`, `strike_price_usd: Option<f64>`).
    - `FixedBook<const N: usize>` (`bids: [(MicroUsdc, SharesMicro); N]`, `asks: [(MicroUsdc, SharesMicro); N]`, `bid_len: u8`, `ask_len: u8`, `last_update_ns: u64`, `exchange_ts_ms: u64`) — `Copy`, stack-allocated, zero heap!
  - `src/config.rs`:
    - `EngineConfig` (loaded strictly from env via `dotenvy` + `secrecy::SecretString` for optional live credentials; custom `Debug` redacting all secrets; explicit validation of `gamma > 0`, `v_max > 0`, `base_order_size_shares > 0`, `max_combined_bid_micro <= 999_900`).
  - `src/clock.rs`:
    - `MonotonicClock` wrapping `std::time::Instant` (QPC on Windows), `ClockOffsetEstimator` (`AtomicI64` ms offset between local `SystemTime` and exchange server time, drift breaker threshold `1_000ms`), and Windows RAII `HighResTimerGuard` calling `timeBeginPeriod(1)` on init and `timeEndPeriod(1)` on `Drop`.
  - `src/telemetry.rs`:
    - `LatencyTracker` wrapping `hdrhistogram::Histogram<u64>::new_with_bounds(1, 60_000_000_000, 3)` (1ns to 60s, 3 sigfigs) reporting `P50 / P95 / P99 / P99.9 / max` in `us`.
    - `JsonlWriter` writing newline-delimited structured JSON records (`ts_unix_ms`, `mono_ns`, `event`, `payload`) via buffered `BufWriter` to `logs/<subcommand>-<utc_ts>.jsonl`.
- **Crate `crates/cli/`:**
  - `src/main.rs` with `clap` CLI binary `poly-as` and Phase 0 subcommand `poly-as doctor [--jsonl logs/doctor.jsonl]`.
  - `doctor` initializes `HighResTimerGuard`, validates `.env`/config (never echoing secrets), benchmarks QPC timer resolution & 1,000 fixed-point + fee calculations, prints full human-readable trace + `LatencyTracker` percentiles to stdout, and writes JSONL verification events.

#### [0.2 VERIFY]
- **Live Terminal Calculation Trace:**
  ```powershell
  rtk cargo run -p cli --bin poly-as -- doctor --jsonl logs/phase0-doctor.jsonl
  ```
  Must print:
  - `(a)` Source: config env validation + Windows `timeBeginPeriod(1)` active + QPC resolution in ns.
  - `(b)` Exact inputs: `price=$0.52`, `size=10.00`, `tick=0.01`, `feeRate=0.05`, `p=0.50`, `shares=100`.
  - `(c)` Step-by-step integer encoding (`makerAmount`, `takerAmount`) & fee curve (`p = 0.01, 0.10, 0.50, 0.90, 0.99`).
  - `(d)` Exact outputs: BUY `5200000/10000000`, SELL `10000000/5200000`, fee at `p=0.50` = `$1.25000 USDC` (`1_250_000` micro-USDC).
  - `(e)` Compute latency: per-op and 1,000-op HDR `P50 / P99` in `ns/us`.
- **Numeric Unit-Test Vectors:**
  ```powershell
  rtk cargo test -p core-types -- --nocapture
  ```
  Required exact unit test assertions:
  - `parse_decimal_micro(b"0.52") == Ok(520_000)` and `parse_decimal_micro(b"0.29") == Ok(290_000)`
  - BUY `10.0` @ `$0.52` (`tick 0.01`) -> `makerAmount = "5200000"`, `takerAmount = "10000000"`
  - SELL `10.0` @ `$0.52` (`tick 0.01`) -> `makerAmount = "10000000"`, `takerAmount = "5200000"`
  - BUY `10.0` @ `$0.29` (`tick 0.01`) -> `makerAmount = "2900000"` (exact integer, zero float error)
  - Fee `100` shares @ `$0.50`, `feeRate=0.05` -> `1_250_000` micro-USDC (`$1.25000`); @ `$0.10` -> `450_000` (`$0.45000`); @ `$0.01` -> `49_500` (`$0.04950`)
  - Fee `100` shares @ `$0.50`, `feeRate=0.07` -> `1_750_000` micro-USDC (`$1.75000`)
  - Fee below min (`0.000004 USDC`) -> `0` micro-USDC; Maker fee -> always `0`
  - `floor_to_tick(0.4999, 10_000) == Ok(490_000)` and `520_000_u64.abs_diff(510_000) == 10_000 >= 10_000`
- **Structured JSONL Verification:**
  ```powershell
  Get-Content logs/phase0-doctor.jsonl | ForEach-Object { $_ | ConvertFrom-Json } | Select-Object ts_unix_ms, event
  ```
  Must parse cleanly with zero errors and contain zero secret substrings.

#### [0.3 VERIFY-AGAIN]
- Adversarial unit tests (`rtk cargo test -p core-types adversarial`):
  1. `parse_decimal_micro` rejects negative, `NaN`, `"1.1234567"` (>6 decimals), empty bytes, overflow `u64::MAX`.
  2. `floor_to_tick(f64::NAN, 10_000)`, `floor_to_tick(f64::INFINITY, 10_000)`, `floor_to_tick(-0.01, 10_000)` all return `Err`, never panic.
  3. `EngineConfig` debug-formatting with dummy secret `"SECRET_SENTINEL_123"` MUST NOT contain `"SECRET_SENTINEL_123"`.
  4. `ClockOffsetEstimator` with simulated `+1_250ms` exchange skew trips `is_drift_exceeded() == true`.
- **PASS/FAIL Criteria:** PASS iff 0 warnings under `clippy -D warnings`, all unit + adversarial tests green, `doctor` prints exact vectors & sub-microsecond math P99, and JSONL parses cleanly.

#### Phase 0 GATE CHECKLIST
- [x] `rtk cargo fmt --all -- --check` exits 0
- [x] `rtk cargo clippy --workspace --all-targets -- -D warnings` exits 0
- [x] `rtk cargo test --workspace` exits 0 (all numeric + adversarial vectors pass)
- [x] `rtk cargo run -p cli --bin poly-as -- doctor --jsonl logs/phase0-doctor.jsonl` verified
- [x] Mandatory reviewers completed with 0 CRITICAL/HIGH findings: `rust-reviewer`, `silent-failure-hunter`

---

### Phase 1: Market Discovery & Multi-Feed Ingestion Engine (`feeds`, `cli feeds`)

#### [1.1 DO]
- Add `crates/feeds/` to workspace:
  - `src/discovery.rs` (`GammaDiscoveryClient`):
    - Queries Gamma API (`https://gamma-api.polymarket.com/events` / `/markets` with pagination & slug/tag filtering for active 5-minute Bitcoin Up/Down markets expiring within `(now, now + 360s]`, NEVER `limit=50` unfiltered!).
    - Parses **BOTH** Protocol V2 (`version == "v2"`, `positionIds: [up, dn]`) AND CTF V1 (`clobTokenIds` JSON-encoded string `"[\"...\", \"...\"]"`), mapping outcomes strictly by `"Up"/"Yes"` vs `"Down"/"No"` index alignment.
    - Hydrates live constraints from CLOB `GET https://clob.polymarket.com/book?token_id={up_id}` (`version`, `tick_size`, `min_order_size`, `neg_risk`) and `GET https://clob.polymarket.com/clob-markets/{condition_id}` (`itode` / `takerDelayMs`, `feeSchedule`), plus `GET https://clob.polymarket.com/time` for initial `ClockOffsetEstimator` sync.
  - `src/events.rs`:
    - Zero-allocation `Copy` hot-path event structs for `rtrb`:
      - `PolyMarketEvent`: `BookSnapshot { outcome, book: FixedBook<10>, rx_mono_ns, exch_ts_ms }`, `PriceChange { outcome, side, price, size, best_bid, best_ask, rx_mono_ns, exch_ts_ms }`, `LastTrade { outcome, side, price, size, rx_mono_ns, exch_ts_ms }`, `TickSizeChange { outcome, new_tick_micro, rx_mono_ns }`.
      - `BinanceSpotEvent`: `BookTicker { bid_e8: u64, ask_e8: u64, bid_qty_e8: u64, ask_qty_e8: u64, update_id: u64, rx_mono_ns }`, `AggTrade { price_e8: u64, qty_e8: u64, is_buyer_maker: bool, exch_ts_ms: u64, rx_mono_ns }`.
      - `OracleTwapEvent`: `ChainlinkTwap { twap_e8: u64, spot_e8: u64, seq: u64, dropped: u32, exch_ts_ms: u64, rx_mono_ns }`.
    - `FeedHealthEvent` (sent over lossless `tokio::sync::mpsc` control channel): `Connected`, `Disconnected { reason }`, `ResyncingRest`, `Ready`, `StaleTimeout { feed, silent_ms }`, `RingOverflow { feed, dropped_count }`.
  - `src/clob_ws.rs` (`ClobMarketWsActor`):
    - Connects via `tokio_tungstenite::connect_async_with_config(req, None, true)` (`TCP_NODELAY = true`!) to `wss://ws-subscriptions-clob.polymarket.com/ws/market`.
    - Sends subscribe frame `{"assets_ids": [up, dn], "type": "market", "custom_feature_enabled": true}`.
    - Runs dedicated 10s text `"PING"` sender + 15s `"PONG"` liveness watchdog.
    - Parses raw flat `event_type` JSON (`book`, `price_change` with `price_changes[]`, `last_trade_price`, `tick_size_change`, `best_bid_ask`), interns `asset_id` -> `Outcome`, maintains sorted L2 book state, and pushes `Copy` events to `rtrb::Producer<PolyMarketEvent>` (capacity `4096`).
    - On drop/reconnect: emits `Disconnected` -> `ResyncingRest`, fetches REST `GET /book` for BOTH `Up` and `Down` tokens before emitting `Ready`! Supports zero-reconnect epoch rollover via `{"assets_ids":[...],"operation":"subscribe"|"unsubscribe"}`.
  - `src/binance_ws.rs` (`BinanceSpotWsActor`):
    - Connects with `TCP_NODELAY = true` to `wss://stream.binance.com:9443/stream?streams=btcusdt@bookTicker/btcusdt@aggTrade`.
    - Handles RFC 6455 Ping/Pong, proactive 23-hour rolling reconnect (before Binance's 24h forced drop), pushes `BinanceSpotEvent` into its OWN dedicated `rtrb::Producer<BinanceSpotEvent>` (capacity `4096`) — never contending with Polymarket WS!
  - `src/polybolt_ws.rs` (Optional `PolyBoltTwapWsActor`):
    - Enabled when `POLYBOLT_ENABLED=true` and credentials/Chainlink key are present; connects to `wss://ws-live-v2.polymarket.com/ws`, handles `op:"auth"`, `price.crypto` (`btcusd`, `chainlink`), `price.crypto.twap` (`window_seconds: 60`), dense `seq`/`dropped` tracking, and `4003` drain reconnect. Degrades cleanly with explicit startup log when disabled.
  - `src/book_maintainer.rs`: Deterministic L2 book state updater (`apply_snapshot`, `apply_price_change`, `top_n::<5>`) producing `FixedBook<10>`, rejecting crossed books (`best_bid >= best_ask`).
- **CLI Subcommand (`crates/cli/`):** `poly-as feeds --discover --duration 20s [--replay <file.jsonl>] [--jsonl logs/phase1-feeds.jsonl]`

#### [1.2 VERIFY]
- **Live Terminal Calculation Trace:**
  ```powershell
  rtk cargo run -p cli --bin poly-as -- feeds --discover --duration 20s --jsonl logs/phase1-feeds.jsonl
  ```
  Must print:
  - `(a)` Discovered live 5m BTC market (`slug`, `condition_id`, `version: v1|v2`, `up/dn` asset IDs, `tick_size`, `min_order_size`, `feeRate`, `takerDelayMs`, `expiry_ts`, `clock_offset_ms`).
  - `(b)` Live raw->parsed frames from BOTH Binance (`btcusdt@bookTicker`, `@aggTrade`) AND Polymarket CLOB WS (`book`, `price_change`, `last_trade_price`) showing `Outcome::Up`/`Down` interning.
  - `(c)` Maintained L2 top-5 depth, best bid/ask, spread, and UP+DOWN parity check (`best_ask_up` vs `1.00 - best_bid_dn`).
  - `(d)` Feed health state (`Ready`), ringbuffer utilization, zero `RingOverflow` / `StaleTimeout`.
  - `(e)` Ingress parse latency HDR `P50 / P99` in `us` (target `< 50 us` on Windows).
- **Unit-Test Vectors:**
  ```powershell
  rtk cargo test -p feeds -- --nocapture
  ```
  - Parse recorded real-shape Polymarket `book`, `price_change` (multi-item `price_changes[]`), `last_trade_price`, `tick_size_change` frames + Gamma V1 (`clobTokenIds` string) AND V2 (`positionIds` array) payloads + Binance `@bookTicker`/`@aggTrade` payloads.
  - Verify `BookMaintainer` sorts bids descending, asks ascending, deletes `size == "0"` levels, and detects crossed book (`best_bid >= best_ask`).
- **Structured JSONL Verification:**
  ```powershell
  (Get-Content logs/phase1-feeds.jsonl | Measure-Object -Line).Lines; Get-Content logs/phase1-feeds.jsonl -Head 5 | ForEach-Object { $_ | ConvertFrom-Json }
  ```
  Must contain both `binance_tick` and `poly_book` / `poly_price_change` records with monotonic `mono_ns`.

#### [1.3 VERIFY-AGAIN]
- Adversarial tests (`rtk cargo test -p feeds adversarial`):
  1. **Stale feed watchdog:** Mock WS stops sending for `3,100ms` -> `StaleTimeout` event fires within `3,500ms`.
  2. **Reconnect + REST resync gate:** Mock WS drops connection -> actor emits `Disconnected`, then `ResyncingRest`, blocks `Ready` until mock HTTP `/book` returns snapshot.
  3. **Malformed / unknown JSON & `price_change` with unknown `asset_id`:** Logged as explicit parse warning counter; never panics or corrupts book state.
  4. **Ringbuffer full pressure:** Fill `rtrb` to capacity -> emits `RingOverflow` telemetry counter without blocking WS ping/pong loop!
  5. **Regression:** `rtk cargo test --workspace` passes (Phase 0 + Phase 1).
- **PASS/FAIL Criteria:** PASS iff live discovery succeeds on real Polymarket Gamma/CLOB APIs, live Binance + Polymarket ticks stream for 20s with `0` panics and `0` dropped pings, parse latency `P99 < 100 us`, and all adversarial + regression tests pass.

#### Phase 1 GATE CHECKLIST
- [x] `rtk cargo fmt --all -- --check` exits 0
- [x] `rtk cargo clippy --workspace --all-targets -- -D warnings` exits 0
- [x] `rtk cargo test --workspace` exits 0
- [x] Live `poly-as feeds --discover --duration 20s` captures real Binance + Polymarket ticks & writes valid JSONL
- [x] Mandatory reviewers completed with 0 CRITICAL/HIGH findings: `rust-reviewer`, `silent-failure-hunter`

---

### Phase 2: Zero-Allocation 8-Station Quantitative Math Engine — Stations 1–6 (`math-engine`, `cli trace`)

#### [2.1 DO]
- Add `crates/math-engine/` to workspace (ZERO heap allocations in `evaluate()`, `#![forbid(unsafe_code)]`):
  - `src/station1_sg.rs` (`SavitzkyGolayFilter`):
    - Fixed `[f64; 5]` ringbuffer, `SgMode::{Causal, Symmetric}`, exact weights `CAUSAL = [3.0,-5.0,-3.0,9.0,31.0]/35.0`, `SYMMETRIC = [-3.0,12.0,17.0,12.0,-3.0]/35.0`.
    - `reset()` mandatory on epoch rollover / reconnect.
    - Returns `Option<SgOutput { raw_mid, sg_unclamped, s_i, clamped_to_bbo: bool }>`. Clamps `s_i` to `[best_bid, best_ask]` (intersected with `[tick, 1 - tick]`) so causal step-jump overshoot NEVER pushes fair mid outside the live spread!
  - `src/station2_twap.rs` (`RollingTwapAndVariance`):
    - Fixed-capacity circular buffer (`[SpotSample; 1024]`) computing exact time-weighted integral over the **trailing 60 seconds** (`window_secs = 60.0`), plus terminal-window `[T-60s, T]` lock-in tracker.
    - Computes `tau_secs = (expiry_ms.saturating_sub(now_ms)) as f64 / 1000.0` (strictly `>= 0.0`).
    - Computes `VarianceMode::{BinaryCara, LibraryStrict}`:
      - `BinaryCara` (default): `V_rem = p*(1-p) * (tau_secs / 300.0).max(tau_floor_frac)` (`tau_floor_frac = 0.20`), floored by EWMA empirical mid-price variance `sigma_ewma_sq * tau_secs`.
      - `LibraryStrict` (A/B): `sigma_eff_sq = sigma_0_sq * (tau_secs / 300.0).powi(2)`, `V_rem = sigma_eff_sq * (tau_secs / 300.0)`.
    - Computes Binance lead-lag fair probability `p_spot_star = norm_cdf(d_2)` and tick-to-tick `delta_p_star` (`cross-venue-basis-arbitrage.md:37-50`, pure fast `erfc` approximation, zero external heavy deps) with explicit `LeadLagSignal { p_spot_star, delta_p_star, trigger_defensive_cancel: bool }`.
  - `src/station3_kappa.rs` (`KappaEstimator`):
    - Operates strictly on UP book (never pooling mirrored UP+DOWN books).
    - Tracks EWMA public trade size `v_bar` (floor `>= 10.0` shares).
    - `KappaMode::{TradeNormalizedDepth, LibraryLogSlope}`:
      - `TradeNormalizedDepth` (default): Computes `kappa_emp = (sum delta_j * (V_cum_j / v_bar)) / (sum delta_j^2)` across up to 5 valid levels (`delta_j > 0, V_cum_j > 0`), capped by touch-protection `kappa_touch = (V_1 / v_bar) / delta_1` so a fragile 5-share touch ahead of a spoof wall cannot fake high liquidity!
      - `LibraryLogSlope` (A/B): `(ln(V_k) - ln(V_1)) / (delta_k - delta_1)`.
    - Explicit finite check (`!raw.is_finite() || raw <= 0.0`) BEFORE clamping `[kappa_min (15.0), kappa_max (200.0)]`; returns `KappaOutput { kappa, raw_kappa, fallback_used: bool, levels_used: u8 }`.
  - `src/station4_6_pricer.rs` (`DualBidPricer`):
    - **Station 4:** `s_star = (if include_var { gamma * v_rem } else { 0.0 }) + (2.0 / gamma) * (1.0 + gamma / kappa).ln()`. Applies fee-aware minimum floor `s_min = (2 * tick).max(fee_rate * p * (1-p) + tick)` (configurable).
    - **Station 5:** `r_i = (s_i - q * gamma * v_rem).clamp(tick, 1.0 - tick)`.
    - **Station 6:** `bid_up_raw = r_i - s_star / 2.0`, `bid_dn_raw = 1.0 - (r_i + s_star / 2.0)`.
      - Quantize via conservative `floor_to_tick`. If either raw bid `< tick`, suppress that leg (`Option<MicroUsdc>::None`) — NEVER saturate to `$0.00`!
      - Check `bid_up + bid_dn <= max_combined_bid_micro`: if violated (e.g. tail clamp), step down the heavier-inventory leg by 1 tick; if still violated, return `QuoteOutput::Suppressed(PairMarginViolation)` (NEVER `assert!`!).
      - Apply **3-way local Post-Only Guard**:
        - `bid_up <= min(best_ask_up, ONE_DOLLAR - best_bid_dn, ONE_DOLLAR - active_bid_dn) - tick`
        - `bid_dn <= min(best_ask_dn, ONE_DOLLAR - best_bid_up, ONE_DOLLAR - active_bid_up) - tick`
        - Suppress leg cleanly if `ceiling < tick` (e.g. `best_ask == tick` or ask book empty!).
  - `src/pipeline.rs` (`MathPipeline`): Combines Stations 1–6, times each station with `MonotonicClock` in nanoseconds, and emits `PipelineTrace` (all inputs, intermediates, outputs, fallback flags, per-station `ns` timings).
- **CLI Subcommand (`crates/cli/`):** `poly-as trace --source live|replay [--input logs/phase1-feeds.jsonl] --sg-mode causal|symmetric --kappa-mode normalized|log-slope --ticks 25 --inventory 0 --jsonl logs/phase2-trace.jsonl`

#### [2.2 VERIFY]
- **Live Terminal Calculation Trace:**
  ```powershell
  rtk cargo run -p cli --bin poly-as -- trace --source live --sg-mode causal --ticks 20 --jsonl logs/phase2-trace.jsonl
  ```
  Must print for every evaluated tick:
  - `(a)` Data source (`live` or `replay`) + raw UP/DOWN BBO & Binance BTC spot tick.
  - `(b)` Exact input variables (`raw_mid`, `q`, `gamma`, `tau_secs`, `tick_size`, `fee_rate`, `v_bar`, top-5 L2 depth).
  - `(c)` Step-by-step calculation through Stations 1 -> 2 -> 3 -> 4 -> 5 -> 6 (`s_i`, `twap_60s`, `v_rem`, `p_spot*`, `kappa` + `fallback`, `s*`, `r_i`, `bid_up_raw/floor`, `bid_dn_raw/floor`, 3-way post-only clamp).
  - `(d)` Exact outputs (`bid_up`, `bid_dn`, `pair_cost`, `gross_margin_bps`, `defensive_cancel`).
  - `(e)` Compute latency: per-station (`s1_ns..s6_ns`) and total `us` + HDR `P50 / P99` (target total compute `P99 < 25 us` per `latency-profiling-and-clock-sync.md:57`!).
- **Numeric Unit-Test Vectors:**
  ```powershell
  rtk cargo test -p math-engine -- --nocapture
  ```
  Exact verified assertions:
  1. **Station 1:** Window `[0.49, 0.50, 0.52, 0.51, 0.50]` -> Symmetric `0.514000`, Causal `0.500000` (tol `1e-9`). Overshoot `[0.99, 0.50, 0.50, 0.90, 0.99]` -> unclamped `1.078857`, clamped `<= 0.99`.
  2. **Station 2:** `t=0..20s: $90,000`, `t=20..60s: $90,300` -> 60s TWAP = `$90,200.00` exact. `LibraryStrict`: `sigma_0^2 = 0.0008, T = 300, tau = 150` -> `sigma_eff^2 = 0.0002`. `BinaryCara`: `p = 0.50, tau = 150` -> `V_rem = 0.125`; `tau = 30` -> floored at `0.25 * 0.20 = 0.05` (non-vanishing!).
  3. **Station 3:** `LibraryLogSlope` with `(delta_1=0.01, V_1=500), (delta_2=0.05, V_2=5000)` -> `kappa = 57.564627`. `TradeNormalizedDepth` scale test: multiplying book sizes by `10x` increases `kappa` by `10x` (before clamp); spoof-wall `[5, 5005, 5010, 5015, 5020]` with `v_bar=100` -> capped by touch `kappa_touch = (5/100)/0.01 = 5.0` -> clamped to `kappa_min = 15.0` (WIDE spread, never tricked!).
  4. **Stations 4–6 (`q = 0` & `q = +20`, `s_i = 0.50, kappa = 50.0, gamma = 0.15, V_rem = 0.03, include_var = false`):**
     - `s* = (2/0.15)*ln(1 + 0.15/50) = 0.03994012`, `half = 0.01997006`.
     - At `q = 0`: `r_i = 0.50`, `bid_up = 0.48`, `bid_dn = 0.48`, `pair_cost = 0.96 <= 0.99`.
     - At `q = +20`: `penalty = 20 * 0.15 * 0.03 = 0.09`, `r_i = 0.41`, `bid_up = 0.39`, `bid_dn = 0.57`, `pair_cost = 0.96 <= 0.99`!
     - With `include_var = true`: `s*_full = 0.15*0.03 + 0.03994012 = 0.04444012`, `bid_up = 0.47`, `bid_dn = 0.47`, `pair_cost = 0.94`.
- **Structured JSONL Verification:**
  ```powershell
  Get-Content logs/phase2-trace.jsonl | ForEach-Object { $r = $_ | ConvertFrom-Json; if ($null -eq $r.payload.total_compute_ns) { throw "Missing latency" } }; Write-Output "JSONL OK"
  ```

#### [2.3 VERIFY-AGAIN]
- Adversarial re-runs (`rtk cargo test -p math-engine adversarial` + CLI replay stress):
  1. **Nonzero heavy inventory (`q = +100` and `q = -100`):** Verify `r_i` shifts strictly downward for `q > 0` and upward for `q < 0`, quotes stay in `[tick, 1 - tick]` or cleanly `None`, never negative or `NaN`.
  2. **Empty / 1-level / zero-volume / spoofed L2 book:** Verify `kappa_fallback = true`, zero `NaN`, wide protective spread.
  3. **Near-expiry & clock overshoot (`tau = 1s`, `tau = 0s`, `now_ms > expiry_ms`):** Verify `tau_secs == 0.0`, zero negative-tau sign flip, zero division by zero in `d_2`.
  4. **Causal SG step-jump (`0.50 -> 0.60`):** Verify tick-2 unclamped `0.6143` is clamped to `<= best_ask` so bid NEVER crosses the spread.
  5. **Dynamic `tick_size_change` (`0.01 -> 0.001`) at tail (`p = 0.02`):** Verify zero panic, valid 3-decimal ticks, `bid_up + bid_dn <= max_combined_bid`.
  6. **Zero-allocation check & regression:** Benchmark 100,000 `evaluate()` iterations (`P99 < 25 us`) and run `rtk cargo test --workspace`.
- **PASS/FAIL Criteria:** PASS iff all exact vectors match to `1e-6`, `P99` compute `< 25 us`, all 5 adversarial cases pass without panic/`NaN`/crossed quotes, and Phase 0–1 tests stay green.

#### Phase 2 GATE CHECKLIST
- [x] `rtk cargo fmt --all -- --check` exits 0
- [x] `rtk cargo clippy --workspace --all-targets -- -D warnings` exits 0
- [x] `rtk cargo test --workspace` exits 0
- [x] Live/replay `poly-as trace` outputs complete 6-station trace + `P99 < 25 us` + valid JSONL
- [x] Mandatory reviewers completed with 0 CRITICAL/HIGH findings: `rust-reviewer`, `silent-failure-hunter`
- [x] Human-Readable Verification Scorecard (plain-English metric translation + 0–100 rubric score + PASS/FAIL verdict) presented alongside raw terminal output
- [x] Git delivery gate passed (`AGENTS.md`): phase branch, atomic conventional commits, PR + green CI merged to `main`, local `main` synced, and `phase-2-complete` tag pushed

---

### Phase 3: Event-Driven FIFO Queue-Aware Paper Simulator & Replay Harness (`paper-sim`, `cli paper`)

#### [3.1 DO]
- Add `crates/paper-sim/` to workspace:
  - `src/fifo_book.rs` (`FifoPaperBroker`):
    - Tracks simulated resting orders per `Outcome` with monotonic `u64` order IDs (`sim_seq`, never timestamp-collision `HashMap` overwrite!).
    - **Strict FIFO Queue Priority (`binary-spread-capture.md:55,74`):**
      - On `SubmitNew { outcome, price, size }`: rejects locally if `price >= current_best_ask` (simulating `postOnly: true` crossing rejection!). Otherwise initializes `volume_ahead = book_depth_at_price(outcome, price)` and adds order with configurable simulated wire latency (`order_latency_ms`, default `40ms` — order is `PendingPlace` until `now >= created_at + order_latency_ms`!).
      - On `Cancel` / `CancelAndReplace`: models cancel flight latency (`cancel_latency_ms`, default `40ms`) — order can STILL be adversely filled during cancel in-flight window!
      - On public `LastTrade { outcome, price, size }`:
        - If `trade.price < order.price` (traded strictly through): fills `min(order.remaining_size, trade.size)` (proportional to actual traded volume, NOT instant infinite fill on a 1-share print!).
        - If `trade.price == order.price`: decrements `order.volume_ahead`; fills ONLY `reachable = trade.size.saturating_sub(prev_volume_ahead)` capped at `order.remaining_size` (**exact partial fills!**).
      - On book depth drop at `order.price` (cancellations ahead in queue): optionally reduces `volume_ahead` conservatively (`min(volume_ahead, new_book_depth)` — configurable conservative vs optimistic queue decay, default conservative: only trade prints clear queue!).
  - `src/accounting.rs` (`PaperLedger`):
    - Tracks `filled_up`, `filled_dn`, `cost_basis_up_micro`, `cost_basis_dn_micro`, `realized_pnl_micro`, `fees_paid_micro`, `maker_weighted_volume_micro` (`sum shares * p * (1-p)`), `estimated_rebate_micro`, `total_pairs_merged`.
    - **Zero Maker Fee + Exact Taker Fee:** Passive fills pay `0` USDC; aggressive simulated `FAK` flattens pay exact `FeeSchedule::compute_taker_fee_micro`!
    - **VWAP Atomic Pair Merge (`min(filled_up, filled_dn)`):** Computes exact integer/rational VWAP per leg, credits `pairable * ($1.00 - avg_up - avg_dn)` to `realized_pnl_micro`, deducts merged shares & cost basis proportionally, and computes mark-to-market unrealized PnL on residual `Delta_q` plus terminal binary settlement PnL (`$1.00` winner / `$0.00` loser at `t = 300s`).
  - `src/telemetry.rs` (`BaselineTelemetry`):
    - Records exact metrics from Legacy `05-runbook.md:385-440` + `binary-spread-capture.md:78`:
      1. `CancelAndReplace` **RPM** (rolling 60s window).
      2. `(T - t)` skew & spread trajectory snapshots.
      3. **Paired-Fill Latency** ($\Delta t_{\text{pair}}$ between Leg A fill and Leg B completion).
      4. **Fill Ratio** (filled shares / posted shares) and **Adverse Selection Post-Fill Markout** (mid-price move `1s` and `5s` after fill!).
      5. **Null Baseline Comparison:** Tracks naive static-spread (`mid ± 1 tick`) benchmark PnL alongside adaptive A-S PnL (`binary-spread-capture.md:78`).
- **CLI Subcommand (`crates/cli/`):** `poly-as paper --source live|replay [--input <file.jsonl>] --duration 300s --v-max 50 --base-size 20 --latency-ms 40 --jsonl logs/phase3-paper.jsonl`

#### [3.2 VERIFY]
- **Live Terminal Calculation Trace:**
  ```powershell
  rtk cargo run -p cli --bin poly-as -- paper --source live --duration 60s --v-max 50 --base-size 20 --latency-ms 40 --jsonl logs/phase3-paper.jsonl
  ```
  Must print:
  - `(a)` Live market & feed status + simulated order latency (`40ms`).
  - `(b)` Every order placement (`sim_id`, `outcome`, `price`, `size`, `volume_ahead`) and every public trade tick evaluated against resting queue.
  - `(c)` Exact queue depletion (`vol_ahead: 150 -> 50 -> 0`, `reachable: 10`), partial fill math, VWAP pair cost, and simulated merge PnL (`1.00 - avg_up - avg_dn`).
  - `(d)` Rolling summary table: `q_up`, `q_dn`, `Delta_q`, `pairs_merged`, `realized_pnl`, `unrealized_pnl`, `fees_paid`, `RPM`, `fill_ratio`, `post_fill_markout_1s`, `baseline_pnl`.
  - `(e)` Per-tick math + sim compute latency HDR `P50 / P99` in `us`.
- **Numeric Unit-Test Vectors:**
  ```powershell
  rtk cargo test -p paper-sim -- --nocapture
  ```
  Exact verified assertions:
  1. **FIFO Partial Fill:** Order `size=20` @ `$0.48` with `volume_ahead=100`. Trade #1 `size=80` @ `$0.48` -> `0` filled (`volume_ahead=20`). Trade #2 `size=25` @ `$0.48` -> **`5` shares filled** (`remaining=15`, `volume_ahead=0`), NOT full 20!
  2. **Trade-Through Volume Cap:** Order `size=50` @ `$0.48`. Trade `size=12` @ `$0.47` -> **`12` shares filled** (`remaining=38`), NOT 50!
  3. **Post-Only Rejection in Sim:** Submitting bid @ `$0.52` when `best_ask == $0.52` -> rejected as `PostOnlyCrossed`, never added to resting book.
  4. **VWAP Pair Merge Math:** Buy 10 UP @ `$0.47` + 10 UP @ `$0.49` (`avg_up = $0.48`), Buy 10 DN @ `$0.48` -> merge 10 pairs -> `pair_cost = $0.96`, `realized_pnl = +$0.400000`, residual `filled_up = 10` (`cost_basis = $4.80`), `filled_dn = 0`.
  5. **Taker Flatten Fee Deduction:** FAK 100 shares @ `$0.50` (`feeRate=0.05`) -> `fees_paid = $1.250000`, net PnL reduced by exact `$1.25`.
- **Structured JSONL Verification:**
  ```powershell
  Get-Content logs/phase3-paper.jsonl | Select-String '"event":"(fill|merge|summary)"' | Select-Object -First 10
  ```

#### [3.3 VERIFY-AGAIN]
- Adversarial re-runs (`rtk cargo test -p paper-sim adversarial`):
  1. **Cancel in-flight adverse fill race:** Emit `Cancel` at `t=100ms` (`latency=40ms`), trade hits at `t=120ms` -> order MUST still fill (realistic adverse selection during cancel RTT!).
  2. **Same-millisecond multi-order placement:** Place 10 orders at identical `ts_ms` -> 10 distinct monotonic `sim_id`s, zero HashMap collisions.
  3. **Replay determinism:** Running `poly-as paper --source replay --input <fixture.jsonl>` twice produces byte-for-byte identical final PnL, fill count, and merged pairs.
  4. **Regression:** `rtk cargo test --workspace` passes (Phases 0–3).
- **PASS/FAIL Criteria:** PASS iff FIFO partial fills, cancel-race adverse fills, VWAP merges, and fee deductions match exact test vectors, live 60s paper session runs cleanly with valid JSONL, and all prior tests remain green.

#### Phase 3 GATE CHECKLIST
- [x] `rtk cargo fmt --all -- --check` exits 0
- [x] `rtk cargo clippy --workspace --all-targets -- -D warnings` exits 0
- [x] `rtk cargo test --workspace` exits 0
- [x] Live 60s `poly-as paper` + deterministic replay run verified with JSONL audit
- [x] Mandatory reviewers completed with 0 CRITICAL/HIGH findings: `rust-reviewer`, `silent-failure-hunter`
- [x] Human-Readable Verification Scorecard (plain-English metric translation + 0–100 rubric score + PASS/FAIL verdict) presented alongside raw terminal output
- [x] Git delivery gate passed (`AGENTS.md`): phase branch, atomic conventional commits, PR + green CI merged to `main`, local `main` synced, and `phase-3-complete` tag pushed

---

### Phase 4: Risk, Multi-Tier Inventory, Hysteresis & 300s Lifecycle Actor Pipeline (`risk-lifecycle`, `cli lifecycle`)

#### [4.1 DO]
- Add `crates/risk-lifecycle/` to workspace:
  - `src/station7_risk.rs` (`DollarNotionalRiskEngine`):
    - Consumes `OrderRegistrySnapshot` (`unhedged_up`, `unhedged_dn`, `unhedged_cost_up_micro`, `unhedged_cost_dn_micro`, `open_bid_notional_up`, `open_bid_notional_dn`, `paired_cost_basis`, `available_usdc_micro`).
    - Computes **cost-basis utilization** `u = (unhedged_cost_heavy + open_bid_notional_heavy) / V_max` (eliminating mark-price martingale!).
    - Computes `Q_max(p) = floor(V_max / p)` (`p > 0` guarded), `remaining_shares = Q_max.saturating_sub(unhedged_leg + open_bid_shares_leg)`, `affordable_shares = floor(available_usdc / p)`.
    - Applies unified **4-Tier Risk Policy** (`inventory-skew-risk-management.md:69-74`):
      - `Tier1Normal (u <= 0.20)`: linear Station 7 scaling both legs, capped by `remaining_shares` & `affordable_shares`.
      - `Tier2ModerateSkew (0.20 < u <= 0.60)`: accumulating leg `min(linear, floor(0.5 * base))`; reducing leg `min(floor(2.0 * base), |Delta_q| - open_red, affordable_shares)`.
      - `Tier3OneSided (0.60 < u <= 0.90)`: accumulating leg **`0`** (immediate cancel!); reducing leg `min(floor(2.5 * base), |Delta_q| - open_red, affordable_shares)`.
      - `Tier4HardStop (u > 0.90)`: cancel all resting quotes + emit `FlattenIntent` (comparing BUY opposite vs SELL heavy net of taker fee; skipping dust `0 < |Delta_q| < min_order_size` with explicit `ResidualDust` telemetry; guarded by `flatten_in_flight`).
    - Suppresses any leg where `final_size < market.min_order_size_micro`.
  - `src/station8_hysteresis.rs` (`HysteresisGate` + `OrderSlotFsm`):
    - Per-leg FSM: `Idle`, `PendingPlace { seq, sent_ns }`, `Live { order_id, price, original_size, remaining_size }`, `PendingCancel { order_id, next_quote: Option<SizedQuote>, sent_ns }`, `PendingReconcile { order_id }`.
    - **True Continuous Deadband:** If `Live` and `target_size >= min_order_size`:
      - Immediate `Cancel` (zero deadband) if `LeadLagDefensiveCancel`, `active_price >= best_ask`, `target_size == 0`, or `active.remaining_size > allowed_size`.
      - Refresh stub if `0 < active.remaining_size < min_order_size`.
      - Otherwise `CancelAndReplace` ONLY if `|p_target_raw - active_price_usd| >= (1.0 + hysteresis_buf) * tick_usd` (default `1.25 * tick`) AND `target_price != active_price`. Otherwise **`HOLD`** (FIFO preserved!).
    - Never emits `SubmitNew` while `PendingCancel` or `PendingPlace` or `PendingReconcile` is active!
  - `src/lifecycle.rs` (`EpochLifecycleController`):
    - Deterministic 4-phase 300s state machine (`polymarket-5m-btc-microstructure-model.md:91-111` + `inventory-skew-risk-management.md:82-85`):
      - `ActiveQuoting (tau > 60s)`
      - `PreExpiryLockout (30s < tau <= 60s)`: accumulating leg forced `0`; if `Delta_q == 0`, cancel BOTH sides and stay flat!
      - `MandatoryFlatten (15s < tau <= 30s)`: `CancelAll` resting quotes + single-flight fee-aware `FAK` rebalance of `|Delta_q|` (skipping dust `< min_order_size`).
      - `TerminalLockout (tau <= 15s)`: `CancelAll` verified, ZERO new orders permitted, enqueue `q_paired` for settlement, pre-discover next 5m epoch market, flush Station 1–2 buffers on rollover!
  - `src/breakers.rs` (`CircuitBreakers`):
    - Consecutive error breaker (`>= 5` consecutive `400`/`401` in 10s -> global kill-switch + `CancelAll`).
    - Feed staleness breaker (`poly > 3000ms` or `binance > 2000ms` -> `CancelAll` + pause quoting until `Ready`).
    - Clock drift breaker (`|offset_ms| > 1000ms` -> halt quoting + alert).
    - `TickSizeChange` freeze (pause quotes until pricing recomputes with updated `tick_size_micro`).
  - `src/runtime.rs` (`ActorPipelineOrchestrator`):
    - Wires the full **lock-free, deadlock-free** multi-thread topology:
      - Tokio Ingestion tasks -> separate `rtrb` SPSC rings (`rb_poly`, `rb_binance`, `rb_polybolt`) + lossless `tokio::sync::mpsc` control channel.
      - Pinned OS Thread #1 (`PricingActor`, optional `--pin-pricing-core N`): polls SPSC rings + 100ms high-res timer + fast-path BBO/Binance jump bypass -> writes latest `QuoteProposal` into `ArcSwap<QuoteProposal>` (zero backlog!) + wakes Risk via `Condvar`/waitable event.
      - Pinned OS Thread #2 (`RiskActor`, optional `--pin-risk-core M`): reads `ArcSwap<QuoteProposal>` + `ArcSwap<OrderRegistrySnapshot>` -> runs Stations 7–8 + Lifecycle + Breakers -> dispatches commands via non-blocking `tokio::sync::mpsc::Sender::try_send` to Execution/Paper Actor (with priority lane for `CancelAll` / `Flatten` so cancels NEVER queue behind routine quotes!).
      - Execution/Paper Tokio Actor updates `ArcSwap<OrderRegistrySnapshot>` on every order/fill transition (zero cyclic blocking!).
      - Thread panic watchdog: if Pricing or Risk thread exits unexpectedly, Tokio supervisor triggers emergency `CancelAll` and exits non-zero.
- **CLI Subcommand (`crates/cli/`):** `poly-as lifecycle --source live|sim-epoch [--fast-forward] [--inject-fault stale|tier4|tick-change|disconnect] --jsonl logs/phase4-lifecycle.jsonl`

#### [4.2 VERIFY]
- **Live Terminal Calculation Trace:**
  ```powershell
  rtk cargo run -p cli --bin poly-as -- lifecycle --source sim-epoch --fast-forward --jsonl logs/phase4-lifecycle.jsonl
  ```
  And live 5-minute epoch trace against real feeds (paper execution backend):
  ```powershell
  rtk cargo run -p cli --bin poly-as -- lifecycle --source live --duration 90s --jsonl logs/phase4-live.jsonl
  ```
  Must print:
  - `(a)` Actor thread startup (core pinning status, `rtrb` capacities, `ArcSwap` slots, high-res timer active).
  - `(b)` Live inputs: `tau_secs`, `EpochPhase`, `RiskTier`, `u` (cost-basis utilization), `q_up/q_dn/Delta_q`, `available_usdc`.
  - `(c)` Step-by-step Station 7 sizing & tier throttling + Station 8 continuous deadband comparison + FSM transitions (`Idle -> PendingPlace -> Live -> PendingCancel -> Idle`).
  - `(d)` Exact emitted actions (`Hold`, `SubmitNew`, `Cancel`, `CancelAndReplace`, `FlattenFak`, `CancelAll`) + phase transitions (`Active -> Lockout(60s) -> Flatten(30s) -> Terminal(15s) -> Rollover`).
  - `(e)` Wire-to-intent latency HDR `P50 / P99` (`Pricing < 25 us`, `Risk < 10 us`, end-to-end internal `< 100 us`).
- **Numeric Unit-Test Vectors:**
  ```powershell
  rtk cargo test -p risk-lifecycle -- --nocapture
  ```
  1. **First-Order Cap Vector:** `V_max=$50, base=100, p=$0.60, unhedged=0` -> `Q_max=83`, output `size=83` (`$49.80 <= $50.00`, NOT 100!).
  2. **Martingale Crash Vector:** Bought 100 UP @ `$0.50` (`cost=$50`), price crashes to `$0.10` -> `u = 50/50 = 1.00` (`Tier4HardStop`, NOT Tier 1!).
  3. **Reducing-Leg Overshoot Vector:** `Delta_q = +15`, Tier 3 (`2.5x * 20 = 50`), `p_dn = $0.10` (`Q_max = 500`) -> reducing DN bid capped strictly at `15` shares!
  4. **Continuous Hysteresis Vector:** Active `$0.49`, `p_target_raw` jitters `0.49999 <-> 0.50001` (`diff = 0.01001 < 0.0125`) -> **`HOLD`** (zero churn!). When `p_target_raw = 0.5030` (`diff = 0.0130 >= 0.0125`) -> `CancelAndReplace` to `$0.50`!
  5. **Dust Trap Vector:** `Delta_q = +3 < min_order_size (5)` at `tau = 25s` -> emits `ResidualDustUnflattenable`, fires **0** FAK orders.
- **Structured JSONL Verification:**
  ```powershell
  Get-Content logs/phase4-lifecycle.jsonl | Select-String '"event":"(phase_transition|risk_tier|order_action|breaker)"' | Select-Object -First 20
  ```

#### [4.3 VERIFY-AGAIN]
- Adversarial fault-injection runs (`rtk cargo test -p risk-lifecycle adversarial` + CLI fault flags):
  1. `--inject-fault stale`: Simulates 3.5s Polymarket WS silence -> `StaleTimeout` fires `CancelAll` within `< 3.5s`, halts quoting, resumes only after `Ready`.
  2. `--inject-fault tier4`: Forces `u = 0.95` -> instant `CancelAll` + single-flight `FlattenFak` (verifies zero duplicate FAKs during 50ms taker delay!).
  3. `--inject-fault tick-change`: Emits `tick_size_change 0.01 -> 0.001` -> freezes stale-tick intents until next Pricing snapshot with `tick = 0.001`.
  4. **Slow Execution Backpressure Test:** Block Execution consumer for 500ms while Pricing emits 500 quotes -> Risk & Pricing NEVER block/deadlock; when Execution resumes, it immediately sees the **latest** quote (zero 500-item stale backlog!).
  5. **Lead-Lag `<15ms` Defensive Cancel Test:** Inject sudden Binance `+$150` BTC jump -> `LeadLagDefensiveCancel` bypasses 100ms SG grid and dispatches `Cancel` in `< 1ms` internal latency.
  6. **Regression:** `rtk cargo test --workspace` passes (Phases 0–4).
- **PASS/FAIL Criteria:** PASS iff all 4 lifecycle phases, 4 risk tiers, continuous hysteresis, fault-injection breakers, backpressure conflation, and all Phase 0–3 regressions pass with zero deadlocks/panics.

#### Phase 4 GATE CHECKLIST
- [x] `rtk cargo fmt --all -- --check` exits 0
- [x] `rtk cargo clippy --workspace --all-targets -- -D warnings` exits 0
- [x] `rtk cargo test --workspace` exits 0
- [x] `poly-as lifecycle --source sim-epoch --fast-forward` and `--inject-fault` tests verified
- [x] Mandatory reviewers completed with 0 CRITICAL/HIGH findings: `rust-reviewer`, `silent-failure-hunter`
- [x] Human-Readable Verification Scorecard (plain-English metric translation + 0–100 rubric score + PASS/FAIL verdict) presented alongside raw terminal output
- [x] Git delivery gate passed (`AGENTS.md`): phase branch, atomic conventional commits, PR + green CI merged to `main`, local `main` synced, and `phase-4-complete` tag pushed

---

### Phase 5: Live Polymarket CLOB Execution, Signing, Heartbeats & Settlement (`execution`, `cli exec`)

#### [5.1 DO]
- Add `crates/execution/` to workspace:
  - `src/auth.rs` (`L2HmacSigner`):
    - Decodes `CLOB_API_SECRET` using `base64::engine::general_purpose::URL_SAFE` (handling `_`/`-` characters cleanly).
    - Signs `timestamp_secs + UPPER_METHOD + path_without_query + exact_serialized_body` with `HMAC-SHA256`, encodes as `urlsafeBase64WithPadding`.
    - Produces `POLY_ADDRESS`, `POLY_API_KEY`, `POLY_PASSPHRASE`, `POLY_TIMESTAMP`, `POLY_SIGNATURE`.
  - `src/eip712.rs` (`OrderSigner`):
    - Uses `alloy` (`sol!`, `eip712_domain!`, `PrivateKeySigner`, `SignerSync::sign_hash_sync`) with pre-computed domain separators per market (`< 80 us` signing latency!).
    - Defines exact 11-field V2/V3 `Order(uint256 salt, address maker, address signer, uint256 tokenId, uint256 makerAmount, uint256 takerAmount, uint8 side, uint8 signatureType, uint256 timestamp, bytes32 metadata, bytes32 builder)`.
    - Dynamic domain dispatch from `MarketDescriptor`:
      - `ProtocolVersion::PolyV2` -> domain `version: "3"`, `verifyingContract: 0xe3333700cA9d93003F00f0F71f8515005F6c00Aa`
      - `ProtocolVersion::CtfV1` (`neg_risk: false`) -> domain `version: "2"`, `verifyingContract: 0xE111180000d2663C0091e4f400237545B87B996B`
      - `ProtocolVersion::CtfV1` (`neg_risk: true`) -> domain `version: "2"`, `verifyingContract: 0xe2222d279d744050d28e00520010520000310F59`
    - Strict `WalletMode` signing branches:
      - `Eoa (0)`, `PolyProxy (1)`, `GnosisSafe (2)`: signs `Order` directly (`maker = wallet`, `signer = eoa` for 1/2).
      - `DepositWallet1271 (3)`: enforces `maker == signer == deposit_wallet`, signs outer `TypedDataSign(Order contents, string name, string version, uint256 chainId, address verifyingContract, bytes32 salt)` and applies ERC-7739 wrapper (`innerSig || appDomainSeparator || contentsHash || ORDER_TYPE || uint16(len)`).
    - Generates CSPRNG `salt` strictly `<= (1u64 << 53) - 1` (`Number.MAX_SAFE_INTEGER`).
  - `src/clob_rest.rs` (`ClobRestClient`):
    - Persistent HTTP/2 / HTTP/1.1 keep-alive `reqwest::Client` (`tcp_nodelay(true)`, explicit connect/request timeouts).
    - Implements `post_order` (`POST /order`), `post_orders_batch` (`POST /orders`, max 15 — sends UP+DOWN passive bids atomically to eliminate inter-leg RTT!), `cancel_order` (`DELETE /order`), `cancel_orders` (`DELETE /orders`), `cancel_market_orders` (`DELETE /cancel-market-orders`), `cancel_all` (`DELETE /cancel-all`), `get_open_orders` (`GET /data/orders`), `get_trades` (`GET /data/trades`).
    - Hard protocol guards: passive maker intents ALWAYS set `orderType: "GTC"`, `postOnly: true`, `expiration: "0"`; flatten intents ALWAYS set `orderType: "FAK"`, `postOnly: false`, `expiration: "0"`.
    - Error recovery matrix (`clob-error-codes-and-edge-cases.md:38-88`):
      - `invalid post-only order: order crosses book` -> non-fatal, refresh book, back off 1 tick.
      - `order_not_found` on cancel -> transition FSM to `PendingReconcile` (NEVER blindly re-place before fill check!).
      - `429 Too Many Requests` -> parse `Retry-After` + jittered backoff; monitor `Poly-RateLimit-Remaining`.
      - `503` (`cancel-only` / `post-only mode` / `disabled`) -> enter corresponding venue mode; `>= 5` consecutive `400`/`401` in 10s -> trip breaker + `CancelAll`.
  - `src/heartbeat.rs` (`OrderHeartbeatActor`):
    - Strictly serialized loop calling `POST https://clob.polymarket.com/v1/heartbeats` every **3,500 ms** with `{"heartbeat_id": current_id}` (initially `""`).
    - On `400 Bad Request` containing `heartbeat_id`, immediately re-signs and retries with the server's expected `heartbeat_id`.
    - **Safety Latch:** Only activates in `canary-live` mode (NEVER fires on dry-run so we never arm API-key-wide timeout cancellation accidentally!). On graceful shutdown (`SIGINT`/`CTRL+C`), explicitly calls `DELETE /cancel-all` and confirms `GET /data/orders` is empty!
  - `src/user_ws.rs` (`ClobUserWsActor` & `FillReconciler`):
    - Connects to `wss://ws-subscriptions-clob.polymarket.com/ws/user`, authenticates with `{"auth":{"apiKey","secret","passphrase"},"markets":[condition_id],"type":"user"}`, sends 10s text `"PING"` + 15s `"PONG"` watchdog.
    - Parses flat `event_type: "order"` and `event_type: "trade"`.
    - **Maker-Safe Fill Accounting:** When `trader_side == "MAKER"`, extracts fills strictly from `maker_orders[]` matching our `order_id` / `maker_address`! Deduplicates by `trade_id` (`Matched -> Mined -> Confirmed`), reverses `FAILED` trades, tracks `confirmed_up` / `confirmed_dn` separately from `matched_unconfirmed`, and updates `ArcSwap<OrderRegistrySnapshot>`.
    - On WS reconnect: holds `Reconciling` lock (pausing new quotes) until REST `GET /data/orders` + `GET /data/trades` finishes reconciling missed events.
  - `src/settlement.rs` (`SettlementWorker`):
    - Background async Tokio worker consuming confirmed pairable balances (`min(confirmed_up, confirmed_dn) >= min_batch_threshold`).
    - Dispatches per `MarketDescriptor.version`:
      - `ProtocolVersion::PolyV2` -> `Router (0x1212...) :: merge(bytes31 conditionId, uint256 amount)`
      - `ProtocolVersion::CtfV1` -> `CTF (0x4D97DCd97eC945f40cF65F87097ACe5EA0476045) :: mergePositions(pUSD 0xC011..., bytes32(0), conditionId, [1, 2], amount)`
    - Primary path: Polymarket Gasless Relayer v2 (`POST https://relayer-v2.polymarket.com/submit` with `RELAYER_API_KEY` + `RELAYER_API_KEY_ADDRESS`, wallet nonce fetch `/v1/account/transactions/params`, wallet-specific EIP-712 batch signing, polling until `STATE_CONFIRMED`).
    - Fallback direct Polygon RPC path (opt-in): enforces dynamic gas-batching floor $N_{\min} = \lceil 10 \cdot \text{TxCost}_{\text{gas}} / s^* \rceil$ and calls wallet execute (never raw EOA CTF call!).
    - Pre-flight startup approval verifier checking ERC-20 `pUSD` allowance + ERC-1155 `isApprovedForAll` before enabling live quoting!
    - Automatic read-only on-chain/profile wallet-type verification (`eth_getCode` / Polymarket profile check confirming `0xeE3B...` matches configured `POLY_SIG_TYPE` `3` vs `2` before Phase 5 live canary starts, refusing live start on mismatch).
- **CLI Subcommand (`crates/cli/`):** `poly-as exec --mode dry-run|canary-live [--confirm-live-capital] [--canary-max-usd 10] --jsonl logs/phase5-exec.jsonl`

#### [5.2 VERIFY]
- **Live Terminal Calculation Trace (Dry-Run Mode — Zero Capital At Risk):**
  ```powershell
  rtk cargo run -p cli --bin poly-as -- exec --mode dry-run --duration 60s --jsonl logs/phase5-exec-dry.jsonl
  ```
  Must print:
  - `(a)` Discovered live 5m BTC market (`version`, `domain_version`, `verifying_contract`, `wallet_mode`, `feeRate`, `takerDelayMs`).
  - `(b)` Live inputs + signed EIP-712 order payloads (redacting private key & API secret) + L2 HMAC headers + exact 6-decimal `makerAmount`/`takerAmount`.
  - `(c)` Step-by-step Stations 1–8 -> Wire Encoding -> EIP-712 Signing (`recover_address` self-check!) -> Dry-Run Dispatch Trace -> Simulated Settlement Calldata (`Router.merge` vs `CTF.mergePositions`).
  - `(d)` Wire-to-wire checkpoint latencies (`tau_1` ingress, `tau_2` compute, `tau_3` signing `< 80 us`, `tau_4` mock/HTTP egress) + HDR `P50 / P99`.
- **Unit & Mock-Exchange Integration Tests:**
  ```powershell
  rtk cargo test -p execution -- --nocapture
  ```
  1. **L2 HMAC Test Vector:** Verify `URL_SAFE` base64 secret containing `_` decodes and signs `ts + "POST" + "/order" + body` and strips query string on `GET /data/orders?market=0x123` (signing `/data/orders`).
  2. **EIP-712 Signature Recovery Test:** Sign V2 CTF order (`sig_type=2` Safe) AND V3 PolyV2 DepositWallet order (`sig_type=3` ERC-7739 wrapped); recover exact signer address via `alloy` and verify ERC-7739 trailer byte layout (`len == 65 + 32 + 32 + ORDER_TYPE.len() + 2`).
  3. **Mock HTTP CLOB Server (`wiremock` / `tokio::net::TcpListener`):**
     - Verifies `POST /orders` sends exact JSON with `postOnly: true`, `orderType: "GTC"`, `side: "BUY"`.
     - Verifies `POST /v1/heartbeats` cycles `"" -> "hb-1" -> "hb-2"` and recovers `400 {"error_msg":"Invalid Heartbeat ID","heartbeat_id":"hb-99"}` immediately.
     - Verifies `429` honors `Retry-After`, `503 cancel-only` cancels resting orders and pauses placement, and `5x` consecutive `401` trips circuit breaker + sends `DELETE /cancel-all`.
  4. **Maker Fill Accounting Test:** Feed `/ws/user` `trade` event where taker bought `DOWN` (`asset_id = dn`) matching our resting `UP` maker bid inside `maker_orders[]` -> verify `q_up` increments (NOT `q_dn`!), duplicate `MINED` + `CONFIRMED` events do NOT double-count, and `TRADE_STATUS_FAILED` reverses inventory cleanly!
- **Structured JSONL Verification:**
  ```powershell
  Get-Content logs/phase5-exec-dry.jsonl | Select-String '"event":"(signed_order|latency_checkpoints|settlement_preview)"' | Select-Object -First 10
  ```

#### [5.3 VERIFY-AGAIN]
- Adversarial re-runs (`rtk cargo test -p execution adversarial`):
  1. **Secret Redaction Audit:** Scan `logs/phase5-exec-dry.jsonl` and stdout capture for private key, L2 secret, passphrase, relayer key -> MUST return 0 matches.
  2. **`order_not_found` Race Test:** Mock `DELETE /order` returns `order_not_found` -> verify FSM enters `PendingReconcile` and does NOT submit replacement until mock `GET /data/orders` confirms filled vs canceled!
  3. **User WS Drop & Reconcile Lock:** Drop mock `/ws/user` socket while an unseen fill occurs -> verify quoting halts until REST reconcile detects fill and updates `Delta_q`.
  4. **Merge Before Confirmation Guard:** Attempt settlement when fill is `MATCHED` (not `CONFIRMED`) -> verify `SettlementWorker` defers merge until `CONFIRMED`!
  5. **Canary Safety Latch:** Running `poly-as exec --mode canary-live` WITHOUT `--confirm-live-capital` OR with `DRY_RUN=true` MUST refuse to start (non-zero exit).
  6. **Full Regression:** `rtk cargo test --workspace` + `rtk cargo build --release` passes cleanly.
- **Optional Live Canary (ONLY after explicit operator sign-off on Phase 5 dry-run + Open Question #1 wallet verification):** Run `poly-as exec --mode canary-live --confirm-live-capital --canary-max-usd 10` for 1 single 5-minute epoch with live operator supervision, verifying `GET /data/orders` empty on shutdown.
- **PASS/FAIL Criteria:** PASS iff all signing, HMAC, heartbeat recovery, maker-fill accounting, mock CLOB error handling, secret-redaction, release build, and workspace regression checks pass with `tau_3 < 80 us` and zero CRITICAL/HIGH reviewer findings.

#### Phase 5 GATE CHECKLIST
- [ ] `rtk cargo fmt --all -- --check` exits 0
- [ ] `rtk cargo clippy --workspace --all-targets -- -D warnings` exits 0
- [ ] `rtk cargo test --workspace` exits 0
- [ ] `rtk cargo build --release` exits 0
- [ ] `poly-as exec --mode dry-run --duration 60s` verified (`tau_3 < 80 us`, 0 leaked secrets, valid JSONL)
- [ ] Mandatory reviewers completed with 0 CRITICAL/HIGH findings: `rust-reviewer`, `silent-failure-hunter`
- [ ] Human-Readable Verification Scorecard (plain-English metric translation + 0–100 rubric score + PASS/FAIL verdict) presented alongside raw terminal output
- [ ] Git delivery gate passed (`AGENTS.md`): phase branch, atomic conventional commits, PR + green CI merged to `main`, local `main` synced, and `phase-5-complete` tag pushed

---

## 4. Strict Gating Rule (Non-Negotiable Execution Law)

> **STRICT SEQUENTIAL PHASE GATE:**
> Work may advance from **Phase $N$** to **Phase $N+1$** **ONLY** when ALL SIX conditions are satisfied with fresh terminal evidence:
> 1. **[N.1 DO]** All crates, modules, structs, and CLI subcommands for Phase $N$ are implemented cleanly with zero placeholders.
> 2. **[N.2 VERIFY]** The live terminal calculation trace (`cargo run ...`), all exact numeric unit-test vectors (`cargo test ...`), and structured JSONL log validation have been executed fresh and confirmed passing.
> 3. **[N.3 VERIFY-AGAIN]** All adversarial edge-case re-runs and full workspace regression tests (`cargo test --workspace`) have been executed fresh and confirmed passing.
> 4. **MANDATORY REVIEW SIGN-OFF:** Both `rust-reviewer` (`cargo fmt --check`, `cargo clippy -- -D warnings`, ownership/concurrency/safety audit) and `silent-failure-hunter` (swallowed errors, bad fallbacks, missing timeouts/rollbacks) report **0 CRITICAL and 0 HIGH findings**. Invoke `rust-build-resolver` immediately on any compilation/borrow-checker/Cargo error.
> 5. **HUMAN-READABLE VERIFICATION SCORECARD:** Present a structured plain-English scorecard alongside raw terminal telemetry containing:
>    - **Metric Translation Table:** Columns `Terminal Field | Measured Value | What It Measures (Plain English) | Target | Status` translating every key output metric from `[N.2 VERIFY]` and `[N.3 VERIFY-AGAIN]`.
>    - **100-Point Rubric (5 weighted pillars):**
>      - Data & Protocol Integrity (`25 pts`)
>      - Math / Logic Correctness (`25 pts`)
>      - Latency & Performance Budget (`20 pts`)
>      - Adversarial & Edge-Case Resilience (`20 pts`)
>      - Code Safety & Hygiene (`10 pts`)
>    - **Pass/Fail Override Rule:** `95–100` = `PASS` (Production-Ready); `85–94` = `PASS WITH NOTES` (functional, minor non-blocking headroom/tuning notes); `< 85` or ANY failed hard gate (failing test, panic, leaked secret, or `CRITICAL`/`HIGH` reviewer finding) = **`FAIL` (Blocked)** regardless of numeric score.
>    - **Plain-English Verdict:** 2–3 short sentences directly answering whether the output matches what a working, completed Phase $N$ should produce.
> 6. **GIT DELIVERY GATE:** All work lives on a dedicated branch with atomic conventional commits (`AGENTS.md`), zero leaked secrets or ignored artifacts, pushed to `origin`, opened as a PR with the Verification Scorecard and green GitHub Actions CI, merged to `main` (`--merge --delete-branch`), local `main` fast-forwarded cleanly, and annotated tag `phase-<N>-complete` pushed.
>
> **STOP RULE:** If ANY check fails, **STOP immediately**, fix the root cause in Phase $N$, and re-run **BOTH** `[N.2 VERIFY]` and `[N.3 VERIFY-AGAIN]` from scratch before advancing. **NEVER skip, batch, or overlap phases.**

---

## 5. Definition of DONE & Locked Operator Decisions

### 5.1 Project Definition of DONE
The greenfield Rust project is **DONE** when:
1. Phases 0–5 have each individually passed `[X.1 DO]`, `[X.2 VERIFY]`, `[X.3 VERIFY-AGAIN]`, and mandatory `rust-reviewer` + `silent-failure-hunter` gates.
2. `rtk cargo build --release`, `rtk cargo test --workspace`, `rtk cargo clippy --workspace --all-targets -- -D warnings`, and `rtk cargo fmt --all -- --check` all exit `0`.
3. `poly-as` executes cleanly on Windows 11 with zero UI dependencies, sub-25 $\mu\text{s}$ P99 math compute latency, sub-80 $\mu\text{s}$ P99 EIP-712 signing latency, structured JSONL telemetry on disk, and zero leaked secrets.
4. At least 30 minutes of live paper trading (`poly-as paper --source live`) and 1 full 5-minute epoch of `poly-as exec --mode dry-run` complete with zero panics, zero inventory drift, clean epoch rollover, and verified PnL/fee/markout telemetry.

### 5.2 Locked Operator Decisions (Confirmed Before Phase 0)

1. **Wallet Type (`0xeE3B778A783510BC833384919F709e3d2fee1624`):** Default to `POLY_SIG_TYPE=3` (`WalletMode::DepositWallet1271`), keep `signatureType=2` (`WalletMode::GnosisSafe`) supported in code, and enforce an automatic read-only on-chain/profile wallet-type verification check before Phase 5 live canary starts.
2. **Math Defaults:** Use the audit-fixed defaults (`BinaryCara` variance + full A-S spread + `TradeNormalizedDepth` $\kappa$), keeping the literal Library formulas behind CLI flags (`--variance-mode library-strict`, `--kappa-mode log-slope`, `--sg-mode symmetric`) for paper A/B comparison.
3. **Risk Defaults:** `V_max = $50.00`, `BASE_ORDER_SIZE_SHARES = 20`, and `>= 2 cents` minimum combined spread floor (`max_combined_bid = $0.98`).
4. **Chainlink PolyBolt:** No Chainlink API key — default to Binance Spot WSS + local 60s rolling TWAP, with PolyBolt opt-in (`POLYBOLT_ENABLED=false`).
5. **Execution Client:** Direct Native Rust (`reqwest` + `alloy` + `hmac`).
6. **Cross-Model Review (`doubt-driven-development`):** Skipped by operator; proceeding with single-model adversarial findings only.
7. **Operator Verification Scorecard:** Starting from Phase 2 onward, every phase verification report MUST include a Human-Readable Verification Scorecard (plain-English metric translation, target vs. measured values, transparent 0–100 rubric score, and PASS/FAIL verdict) alongside the raw terminal output.
8. **Git Workflow & Repository Conventions:** Repository lives at `AI-Degen-69/polymarket-as-rust` (private). Starting from Phase 2 onward, all changes follow `AGENTS.md` (short-lived feature branches, atomic conventional commits, PR + green CI before merging to `main`, and `phase-<N>-complete` tags).