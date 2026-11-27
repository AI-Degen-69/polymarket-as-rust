# polymarket-as-rust

Pure-Rust, terminal-first Avellaneda-Stoikov dual-bid market maker for Polymarket 5-minute BTC UP/DOWN binary markets (with 5m, 15m, and 1h canonical historical replay support).

## Status

| Scope | Status | Notes |
| :--- | :--- | :--- |
| Phases 0–5 | Complete — tagged `phase-0-complete` through `phase-5-complete` | Core types, live feeds, math engine, FIFO paper simulator, risk/lifecycle actors, and execution modules. |
| Post-Phase-5 additions | Implemented on `main` | `crates/historical-data` (`poly-as pmdata`), NEX Amoy sandbox (`poly-as sandbox`), and `DEFAULTS.json` calibration source. |
| Mainnet live execution | **Gated off / preview-only** | `poly-as exec --mode canary-live` is hard-gated off in `crates/cli/src/exec_cmd.rs`. `exec` dry-run uses synthetic books and ephemeral keys, and `RelayerClient::submit_merge` currently posts a `"calldataPreview"` payload rather than ABI-encoded signed relayer transactions. |

## Workspace Crates (8)

- `crates/core-types` — Fixed-point monetary types (`MicroUsdc`, `SharesMicro`), `DEFAULTS.json` / env / secret config, Windows high-resolution clock, NEX sandbox profile, and HDR/JSONL telemetry.
- `crates/feeds` — Gamma market discovery, Binance Spot, Polymarket CLOB, and PolyBolt WebSocket actors with lock-free `rtrb` rings.
- `crates/historical-data` — PMData catalog/quota-guarded downloader (`<= 650` units), `CanonicalEvent` normalizer, Phase 0 historical audit, and live-vs-PMData reconciliation.
- `crates/math-engine` — Zero-allocation quantitative math engine (Stations 1–6) and Binance lead-lag signal.
- `crates/paper-sim` — Event-driven FIFO queue-aware paper simulator, VWAP pair-merge ledger, and markout telemetry.
- `crates/risk-lifecycle` — 4-tier cost-basis risk limits, continuous hysteresis, circuit breakers, and 300s epoch lifecycle.
- `crates/execution` — EIP-712 order signing, L2 HMAC auth, CLOB REST/WS clients, heartbeats, settlement preview, and NEX Amoy sandbox integration.
- `crates/cli` — Unified `poly-as` CLI binary (`doctor`, `feeds`, `trace`, `lifecycle`, `pmdata`, `sandbox`, `exec`).

## Quick Start (PowerShell)

Requires Rust `1.89+` (`edition = "2024"`).

```powershell
# Enable tracked pre-commit hook once per clone
git config core.hooksPath .githooks

# Copy env template (never commit .env; non-secret defaults live in DEFAULTS.json)
Copy-Item .env.example .env

# Run workspace tests and PowerShell style tests
cargo test --workspace
pwsh -NoProfile -File scripts/sweep-style.tests.ps1

# Diagnostics and live feed capture
cargo run -p cli -- doctor
cargo run -p cli -- feeds --discover --duration 20s

# Replay recorded or canonical feeds and run parameter sweeps
cargo run -p cli -- lifecycle --source replay --input logs/captures/<file>.jsonl
pwsh -File scripts/sweep-params.ps1

# Inspect PMData quota ledger
cargo run -p cli -- pmdata status
```

See [docs/README.md](docs/README.md) for the full documentation index and [docs/poly-as-cheatsheet.md](docs/poly-as-cheatsheet.md) for every `poly-as` subcommand, flag, and default.

## Configuration & Defaults

- Repository root [`DEFAULTS.json`](DEFAULTS.json) is the single source of truth for non-secret engine and simulation defaults (embedded into `EngineConfig::default()` at compile time).
- `.env` and process environment variables override `DEFAULTS.json`; CLI flags override environment variables.

## NEX Sandbox (Paper Venue)

Sandbox runs use Polymarket's NEX preprod exchange on Polygon Amoy (`80002`) instead of any local simulator. Production hosts and keys are rejected by `SandboxProfile`.

```powershell
# Prove egress IP, pinned ingress, TCP 443, SNI/Host, and GET /health == 200
cargo run -p cli --bin poly-as -- sandbox health

# List sandbox markets (Gamma preprod edge headers; NEX CLOB fallback)
cargo run -p cli --bin poly-as -- sandbox markets --limit 20

# Read one outcome book
cargo run -p cli --bin poly-as -- sandbox book --asset-id <TOKEN_ID>

# Minimum-size post-only limit buy at one tick, read back, then cancel
cargo run -p cli --bin poly-as -- sandbox validate --asset-id <TOKEN_ID> --confirm-sandbox-trade

# Market + authenticated user feeds (10s PING, 60s staleness, snapshot rebuild)
cargo run -p cli --bin poly-as -- sandbox feeds `
  --up-asset-id <UP_ID> --dn-asset-id <DN_ID> --duration 30s --reconnect-after-secs 15
```

Both NEX hostnames must resolve to `51.24.102.79` (hosts file or resolver) and the trading egress must match `NEX_ALLOWLISTED_EGRESS_IP`; the gate fails closed otherwise. Every sandbox JSONL record carries `"origin": "sandbox"` and `"network": "nex-amoy-80002"` so Amoy test fills never enter production volatility, arrival-rate, or PnL estimates. See `.env.example` and [docs/nex-sandbox-runbook.md](docs/nex-sandbox-runbook.md) for sandbox configuration and activation steps.

## Safety Notes

- `.env`, `.env.*` (except `.env.example`), `logs/`, `data/`, `*.jsonl`, `*.log`, and `/target/` are gitignored.
- Default configuration runs with `EXECUTION_MODE=paper` and `DRY_RUN=true`.
- Mainnet `poly-as exec --mode canary-live` is hard-gated off in this build (`crates/cli/src/exec_cmd.rs`), and `exec` dry-run never connects to live order or relayer endpoints.
- Live commands (`feeds --discover`, `trace --source live`, `lifecycle --source live`, `exec --mode canary-live`) run a host-clock drift preflight against exchange time before starting.
