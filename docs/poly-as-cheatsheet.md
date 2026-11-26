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

All subcommands read `.env` (if present) and validated process env; see [`.env.example`](../.env.example) for every key and its default. CLI flags override env values.

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
| Verify signing + settlement mechanics | `poly-as exec` |
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
Run a deterministic 300s epoch, live feeds, or offline replay with 4-tier risk limits.

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
| `--input PATH` | Input JSONL for `--source replay` |
| `--duration <60s\|90s\|300s>` | Live run duration (default `90s`) |
| `--allow-mid-window` | Start `--source live` mid-window |
| `--fast-forward` | Fast-forward the virtual clock in `sim-epoch` mode |
| `--inject-fault <none\|stale\|tier4\|tick-change\|disconnect>` | Adversarial fault injection (default `none`; alias `--fault`) |
| `--v-max USD` | Max position capital (default `$50`) |
| `--base-size shares` | Base order size (default `20`) |
| `--latency-ms` | Simulated wire place/cancel latency, applied to both placement and cancel (default `40ms`) |
| `--queue-decay conservative\|optimistic` | FIFO queue decay policy (default `conservative`) |
| `--pin-pricing-core N` | Pin Pricing actor OS thread to CPU core N |
| `--pin-risk-core N` | Pin Risk actor OS thread to CPU core N |
| Math overrides | See shared table below |
| `--jsonl PATH` | Output telemetry path |

Log default: `logs/lifecycle/<DD-MM_HH-MM-SSZ>.jsonl`.

---

## Phase 5 — Execution tracer

### `exec`
EIP-712 signing, dry-run dispatch, and settlement preview. Read-only; **never places a live order in this build**.

```powershell
poly-as exec                                                        # dry-run default
poly-as exec --mode dry-run --duration 60s
# Canary-live is gated off: refused unless all latches pass, and still refused in this build.
poly-as exec --mode canary-live --confirm-live-capital --canary-max-usd 10
```

Flags:

| Flag | Meaning |
|---|---|
| `--mode dry-run\|canary-live` | Execution safety mode (default `dry-run`; canary-live is gated) |
| `--confirm-live-capital` | Explicit live-capital acknowledgement (required for canary-live) |
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
| `markets` | List sandbox markets via Gamma preprod edge |
| `book` | Read `GET /book?token_id=...` for one outcome |
| `validate` | Place → read back → cancel a minimum-size post-only limit buy at one tick (requires `--confirm-sandbox-trade`) |
| `feeds` | Stream NEX market + authenticated user feeds with heartbeats/snapshot rebuilds |

---

## Shared math overrides (trace / lifecycle)

Defaults shown are the calibrated repo defaults (mirrored in `.env.example` and `EngineConfig::default()`).

| Flag | Default | Notes |
|---|---|---|
| `--gamma <f64>` | `0.12` | CARA risk aversion `USD^-1` |
| `--max-combined-bid-micro <u64>` | `980000` | Max UP+DOWN bid sum (micro-USDC, `<= 990000`) |
| `--include-variance-spread <bool>` | `true` | Include `gamma * V_rem` variance term in Station 4 |
| `--min-spread-ticks <1\|2>` | `1` | Minimum spread floor in ticks |
| `--fee-spread-floor <bool>` | `false` | Enforce taker-fee spread floor (auto-disabled at `1` tick unless set) |
| `--max-cross-tick-loss-micro <n\|off\|none>` | `80000` | Cross-tick pair-loss cap; `0` enforces `cost_old + bid_new <= max_combined_bid_micro` |
| `--max-entry-prob <p\|off\|none>` | `0.80` | Extreme-probability entry guard in `(0.5, 1.0)` |
| `--lead-lag-threshold <f64>` | `0.015` | Binance lead-lag `\|delta_p_star\|` cancel threshold |
| `--lead-lag-window-ms <u64>` | `1000` | Rolling lookback window (`0..=6000`) |
| `--lead-lag-cooldown-ms <u64>` | `1000` | Per-side cancel cooldown (`0..=10000`) |
| `--lead-lag-directional <bool>` | `true` | Cancel only the endangered leg |
| `--hedge-skew-mult <f64>` | `1.0` | Asymmetric reducing-leg skew multiplier in `[0.0, 1.0]` |
| `--cap-tier1-hedge <bool>` | `false` | Cap Tier-1 reducing-leg size to `\|Delta_q\|` when `\|Delta_q\| >= 1` |

---

## Default log paths

| Feature | Default path |
|---|---|
| doctor | `logs/doctor/<stamp>.jsonl` |
| feeds (capture) | `logs/captures/<stamp>_<dur>.inprogress.jsonl` |
| trace | `logs/trace/<stamp>.jsonl` |
| lifecycle | `logs/lifecycle/<stamp>.jsonl` |
| exec | `logs/exec/<stamp>.jsonl` |
| sweeps | `logs/sweeps/<stamp>_<N>runs_<capture>` (per-run `summary.txt`) |

`<stamp>` = UTC `DD-MM_HH-MM-SSZ`.

---

## Safety notes

- **`exec`** is dry-run only. `canary-live` is hard-gated in this build (refused even with all latches passed).
- **`lifecycle --source sim-epoch`** never touches real capital.
- **`sandbox validate`** places a real test-pUSD order on Amoy — it requires `--confirm-sandbox-trade`.
- Secrets (private keys) live only in `.env`; never commit them. `.env.example` documents every key with no secrets.
