# `poly-as` Command Cheatsheet

Polymarket 5-Minute BTC Avellaneda–Stoikov HFT Bot (terminal CLI).

## Global

```powershell
# Top-level help, version, and per-command help
poly-as --help
poly-as --version
poly-as <command> --help

# Global flag (any position)
# --color auto|always|never  (ANSI terminal color)
```

Non-secret engine and simulation defaults come from root [`DEFAULTS.json`](../DEFAULTS.json) (embedded at compile time). All subcommands read `.env` (if present) and validated process env; see [`.env.example`](../.env.example) for env keys. CLI flags override env values, which override `DEFAULTS.json`.

**Host-clock preflight:** Live commands (`feeds --discover`, `trace --source live`, `lifecycle --source live`, and `exec --mode canary-live`) automatically verify local clock drift against exchange time (`CLOCK_DRIFT_MAX_MS`, default `1000ms`) and check the Windows `w32time` service before starting; offline replay, `doctor`, `sandbox`, and `pmdata` skip this check.

---

## Quick start by intent

| What you want | Command |
|---|---|
| Sanity-check the environment | `poly-as doctor` |
| Capture live feed data (record) | `poly-as feeds --discover --windows 2` |
| Replay recorded data | `poly-as feeds --replay logs/captures/<file>.jsonl` |
| Watch math-engine ticks | `poly-as trace --source replay --input <file>.jsonl` |
| Compare configs / performance sweep | `pwsh -File scripts/sweep-params.ps1` (see below) |
| Full risk/lifecycle epoch | `poly-as lifecycle --source sim-epoch` |
| Download/normalize/reconcile historical PMData | `poly-as pmdata catalog\|download\|status\|normalize\|verify-historical\|reconcile` |
| Verify signing + settlement preview | `poly-as exec` |
| NEX sandbox checks | `poly-as sandbox health` / `markets` / `book` / `validate` / `feeds` |

### Checking config performance
The canonical way to measure a config's performance is the offline sweep wrapper, which drives `lifecycle --source replay` over recorded captures and ranks by **NetPnL**:

```powershell
pwsh -File scripts/sweep-params.ps1                                  # latest capture
pwsh -File scripts/sweep-params.ps1 -InputPath logs/captures/<file>.jsonl
pwsh -File scripts/sweep-params.ps1 -AllCaptures -Gammas 0.25
```

For a single replay of the current default config:

```powershell
poly-as lifecycle --source replay --input logs/captures/<file>.jsonl
```

---

## Phase 0 — Diagnostics

### `doctor`
Verify Windows clock resolution, fixed-point wire math, config redaction, JSONL telemetry.

```powershell
poly-as doctor [--jsonl PATH]
```

Log default: `logs/doctor/<DD-MM_HH-MM-SSZ>.jsonl`.

---

## Phase 1 — Feeds

### `feeds`
Live Gamma/CLOB discovery, Binance Spot & Polymarket WS streams, or offline replay.

```powershell
# Live capture (one 5-min window, or N windows)
poly-as feeds --discover
poly-as feeds --discover --windows 2
poly-as feeds --discover --duration 60s
poly-as feeds --discover --allow-mid-window

# Offline replay
poly-as feeds --replay logs/captures/<file>.jsonl
poly-as feeds --replay logs/captures/<file>.jsonl --replay-out report.jsonl
```

Flags:

| Flag | Meaning |
|---|---|
| `--discover` | Discover active 5-min BTC market and stream live feeds |
| `--duration <20s\|500ms\|1m>` | Live capture duration (default `20s`; conflicts with `--windows`) |
| `--windows N` | Capture `N` full 5-min windows, stop cleanly at window end (conflicts with `--duration`) |
| `--allow-mid-window` | Start mid-window instead of waiting for a fresh boundary |
| `--replay PATH` | Replay a recorded JSONL feed log offline |
| `--replay-out PATH` | Separate report path for `--replay` (never overwrites input) |
| `--jsonl PATH` | Output telemetry path |

Capture default: `logs/captures/<stamp>_<dur>.inprogress.jsonl` (renamed on clean close).

---

## Phase 2 — Math engine tracer

### `trace`
Stream live or replayed ticks through Stations 1–6.

```powershell
poly-as trace --source replay --input logs/captures/<file>.jsonl
poly-as trace --source live --ticks 25 --inventory 0
```

Flags:

| Flag | Meaning |
|---|---|
| `--source live\|replay` | Feed source (default `live`) |
| `--input PATH` | Input JSONL for `--source replay` (default: newest capture) |
| `--sg-mode causal\|symmetric` | Station 1 Savitzky–Golay mode (default `causal`) |
| `--kappa-mode normalized\|log-slope` | Station 3 liquidity-decay estimator (default `normalized`) |
| `--variance-mode binary-cara\|library-strict` | Station 2/5 variance mode (default `binary-cara`) |
| `--ticks N` | Post-warmup 100ms evaluation ticks to print/record (default `25`) |
| `--inventory Q` | Simulated net UP–DOWN share inventory (default `0`) |
| Math overrides | See the shared table below |
| `--jsonl PATH` | Output telemetry path |

Log default: `logs/trace/<DD-MM_HH-MM-SSZ>.jsonl`.

---

## Phase 4 — Risk & lifecycle actor pipeline

### `lifecycle`
Run a deterministic 300s epoch, live feeds, or offline replay (legacy live JSONL or `CanonicalEvent` `5m`/`15m`/`1h` JSONL) with 4-tier risk limits.

```powershell
poly-as lifecycle --source sim-epoch
poly-as lifecycle --source replay --input logs/captures/<file>.jsonl
poly-as lifecycle --source live --duration 90s --inject-fault tier4
```

Flags:

| Flag | Meaning |
|---|---|
| `--source live\|sim-epoch\|replay` | Source mode (default `sim-epoch`) |
| `--display stream\|raw` | `stream` = events + heartbeats; `raw` = 10 Hz tick lines (default `stream`) |
| `--verbose-latency` | Print every latency row, disabling the 10s nominal health-check throttle (default `false`) |
| `--input PATH` | Input JSONL for `--source replay` (default: newest capture in `logs/captures/`) |
| `--duration <60s\|90s\|300s>` | Live run duration (default `90s`) |
| `--allow-mid-window` | Start `--source live` mid-window |
| `--fast-forward` | Fast-forward the virtual clock in `sim-epoch` mode |
| `--inject-fault <none\|stale\|tier4\|tick-change\|disconnect>` | Adversarial fault injection (default `none`; alias `--fault`) |
| `--v-max USD` | Max position capital (default from `DEFAULTS.json`: `$50`) |
| `--base-size shares` | Base order size in shares (default from `DEFAULTS.json`: `20`) |
| `--latency-ms` | Simulated wire place/cancel latency, applied to both placement and cancel (default `40ms`) |
| `--queue-decay conservative\|optimistic` | FIFO queue decay policy (default from `DEFAULTS.json`: `conservative`) |
| `--pin-pricing-core N` | Pin Pricing actor OS thread to CPU core N |
| `--pin-risk-core N` | Pin Risk actor OS thread to CPU core N |
| `--source-filter all\|live\|pmdata` | Source filter for canonical replay (default `all`) |
| `--min-book-quality any\|direct-yes\|direct-both` | Minimum book derivation quality for canonical replay (default `any`) |
| Math overrides | See the shared table below |
| `--jsonl PATH` | Output telemetry path |

Log default: `logs/lifecycle/<DD-MM_HH-MM-SSZ>.jsonl`.

---

## Historical Data (PMData)

### `pmdata <action>`
Query the free PMData catalog, download historical L2/trade files under the `650`-unit quota ledger, normalize to `CanonicalEvent` JSONL, audit cached samples, or reconcile against live captures. Full per-command reference: [`docs/pmdata/README.md`](pmdata/README.md).

```powershell
poly-as pmdata catalog --asset btc --interval 15m --from-date 2026-10-28 --to-date 2026-11-03
poly-as pmdata status
poly-as pmdata download --date 2026-10-28 --asset btc --interval 15m --mode full --dry-run
poly-as pmdata download --market-slug btc-updown-15m-1793146500 --mode full
poly-as pmdata normalize --source live-jsonl --input logs/captures/<file>.jsonl --output data/canonical/live.canonical.jsonl
poly-as pmdata verify-historical --samples btc-updown-15m-1793146500
poly-as pmdata reconcile --live logs/captures/<file>.jsonl --yes-l2 <yes.parquet> --no-l2 <no.parquet> --trades <trades.parquet>
```

| Action | Purpose | Key flags |
|---|---|---|
| `catalog` | Fetch/query `markets.parquet` (`0` quota) | `--force-refresh`, `--filter`, `--interval`, `--asset`, `--from-date`, `--to-date`, `--limit` |
| `download` | Download single market or Day Bundle (`<= 650` cap) | `--date`, `--from-date`, `--to-date`, `--asset`, `--interval`, `--market-slug`, `--mode full\|quota-saving`, `--include-onchain`, `--dry-run`, `--yes` |
| `status` | Print persistent quota ledger (`data/pmdata/manifest.json`) | *(none)* |
| `normalize` | Convert PMData Parquets or live JSONL to canonical JSONL | `--source pmdata\|live-jsonl`, `--input`, `--yes-l2`, `--no-l2`, `--trades`, `--onchain-fills`, `--market-slug`, `--catalog`, `--mode`, `--resolution-override up\|down`, `--output` |
| `verify-historical` | Phase 0 schema/reconstruction audit of cached samples (`0` quota) | `--samples`, `--catalog`, `--data-dir`, `--report-out` |
| `reconcile` | 10-axis comparison of one live capture vs PMData (`0` quota) | `--live`, `--yes-l2`, `--no-l2`, `--trades`, `--catalog`, `--market-slug`, `--mode`, `--report-out` |

---

## Phase 5 — Execution tracer

### `exec`
EIP-712 signing, synthetic dry-run dispatch, and settlement preview. Read-only using synthetic books and ephemeral keys; **never places a live order or connects to live CLOB/relayer order endpoints in this build**.

```powershell
poly-as exec                                                        # dry-run default
poly-as exec --mode dry-run --duration 60s
# Canary-live is hard-gated off: refused unless all latches pass, and still refused in this build.
poly-as exec --mode canary-live --confirm-live-capital --canary-max-usd 10
```

Flags:

| Flag | Meaning |
|---|---|
| `--mode dry-run\|canary-live` | Execution safety mode (default `dry-run`; `canary-live` is hard-gated off) |
| `--confirm-live-capital` | Explicit live-capital acknowledgement (required for `canary-live`) |
| `--canary-max-usd N` | Canary dollar cap (enforced `<= 25`; default `10`) |
| `--duration <60s>` | Dry-run trace duration (default `60s`) |
| `--jsonl PATH` | Output telemetry path |

Log default: `logs/exec/<DD-MM_HH-MM-SSZ>.jsonl`.

---

## NEX Sandbox (Amoy 80002)

### `sandbox <action>`
Health gate, market discovery, validation order, and feeds against the NEX preprod sandbox.

```powershell
poly-as sandbox health [--require-allowlisted-egress] [--jsonl PATH]
poly-as sandbox markets [--limit 20] [--jsonl PATH]
poly-as sandbox book --asset-id <ID> [--jsonl PATH]
poly-as sandbox validate --asset-id <ID> --confirm-sandbox-trade [--jsonl PATH]
poly-as sandbox feeds --up-asset-id <ID> --dn-asset-id <ID> [--duration 20s] [--ws-version original|subs-v2] [--reconnect-after-secs N] [--jsonl PATH]
```

| Action | Purpose |
|---|---|
| `health` | Prove allowlisted public route: egress IP, pinned ingress, TCP 443, TLS SNI/Host, `GET /health` |
| `markets` | List sandbox markets via Gamma preprod edge (NEX CLOB fallback) |
| `book` | Read `GET /book?token_id=...` for one outcome |
| `validate` | Place → read back → cancel a minimum-size post-only limit buy at one tick (requires `--confirm-sandbox-trade`) |
| `feeds` | Stream NEX market + authenticated user feeds with heartbeats/snapshot rebuilds |

Log default: `logs/sandbox/<DD-MM_HH-MM-SSZ>_<action>.jsonl`.

---

## Shared math overrides (trace / lifecycle)

Defaults shown match [`DEFAULTS.json`](../DEFAULTS.json), [`.env.example`](../.env.example), and `EngineConfig::default()`.

| Flag | Default | Notes |
|---|---|---|
| `--gamma <f64>` | `0.12` | CARA risk aversion `USD^-1` |
| `--max-combined-bid-micro <u64>` | `980000` | Max UP+DOWN bid sum (micro-USDC, `<= 990000`) |
| `--include-variance-spread <bool>` | `true` | Include `gamma * V_rem` variance term in Station 4 |
| `--min-spread-ticks <1\|2>` | `1` | Minimum spread floor in ticks |
| `--fee-spread-floor <bool>` | `false` | Enforce taker-fee spread floor (auto-disabled at `1` tick unless set) |
| `--max-cross-tick-loss-micro <n\|off\|none>` | `80000` | Cross-tick pair-loss cap; `0` enforces `cost_old + bid_new <= max_combined_bid_micro` |
| `--max-entry-prob <p\|off\|none>` | `0.80` | Extreme-probability entry guard in `(0.5, 1.0)` |
| `--max-entry-price <p\|off\|none>` | `0.65` | Upper-only favorite entry price cap in `(0.0, 1.0)` |
| `--lead-lag-threshold <f64>` | `0.015` | Binance lead-lag `\|delta_p_star\|` cancel threshold |
| `--lead-lag-window-ms <u64>` | `1000` | Rolling lookback window (`0` or `50..=6000` ms) |
| `--lead-lag-cooldown-ms <u64>` | `1000` | Per-side cancel cooldown (`0..=10000` ms) |
| `--lead-lag-directional <bool>` | `true` | Cancel only the endangered leg |
| `--hedge-skew-mult <f64>` | `1.0` | Asymmetric reducing-leg skew multiplier in `[0.0, 1.0]` |
| `--cap-tier1-hedge <bool>` | `true` | Cap Tier-1 reducing-leg size to `\|Delta_q\|` when `\|Delta_q\| >= 1` |
| `--tier1-hedge-trigger-price <p\|off\|none>` | `off` | Opposing-ask trigger in `(0.0, 1.0)` unlocking Tier-1/Tier-2 passive 1:1 deficit override (requires `--cap-tier1-hedge true`) |
| `--entry-open-delay-secs <u64>` | `15` | Seconds after epoch open during which fresh Leg-1 entries are skipped (`0` disables) |
| `--entry-cutoff-secs <u64\|off\|none>` | `120` | Elapsed seconds after epoch open at which fresh Leg-1 entries stop (`1..=300` or `off`) |

---

## Default log paths

| Feature | Default path |
|---|---|
| doctor | `logs/doctor/<stamp>.jsonl` |
| feeds (capture) | `logs/captures/<stamp>_<dur>.inprogress.jsonl` |
| trace | `logs/trace/<stamp>.jsonl` |
| lifecycle | `logs/lifecycle/<stamp>.jsonl` |
| sandbox | `logs/sandbox/<stamp>_<action>.jsonl` |
| exec | `logs/exec/<stamp>.jsonl` |
| sweeps | `logs/sweeps/<stamp>_<N>runs_<capture>` (per-run `summary.txt`) |

`<stamp>` = UTC `DD-MM_HH-MM-SSZ`.

---

## Safety notes

- **`exec`** is dry-run only using synthetic books and ephemeral keys. `canary-live` is hard-gated off in this build (refused even with all latches passed).
- **`lifecycle --source sim-epoch`** and **`--source replay`** never touch real capital.
- **`sandbox validate`** places a real test-pUSD order on Amoy — it requires `--confirm-sandbox-trade`.
- Secrets (private keys, API keys) live only in `.env`; never commit them. `.env.example` documents every key with no secrets.
