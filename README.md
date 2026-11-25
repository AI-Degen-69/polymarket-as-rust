# polymarket-as-rust

Pure-Rust, terminal-first Avellaneda-Stoikov dual-bid market maker for Polymarket 5-minute BTC UP/DOWN binary markets.

## Status

| Phase | Scope | Status |
| :--- | :--- | :--- |
| Phase 0 | `core-types` and `poly-as doctor` | Complete — tagged `phase-0-complete` |
| Phase 1 | `feeds` and `poly-as feeds` | Complete — tagged `phase-1-complete` |
| Phases 2–5 | `math-engine`, `paper-sim`, `risk-lifecycle`, and `execution` | Complete — tagged `phase-2-complete` through `phase-5-complete` |

## Workspace Crates

- `crates/core-types` — Fixed-point monetary types (`MicroUsdc`, `SharesMicro`), env/secret config, Windows high-resolution clock, and HDR/JSONL telemetry.
- `crates/feeds` — Gamma market discovery, Binance Spot, Polymarket CLOB, and PolyBolt WebSocket actors with lock-free `rtrb` rings.
- `crates/math-engine` — Zero-allocation quantitative math engine (Stations 1–6).
- `crates/paper-sim` — Event-driven FIFO queue-aware paper simulator and replay harness.
- `crates/risk-lifecycle` — Risk, multi-tier inventory limits, continuous hysteresis, and 300s epoch lifecycle.
- `crates/execution` — Polymarket CLOB execution, EIP-712 signing, heartbeats, and settlement.
- `crates/cli` — Unified `poly-as` CLI binary (`doctor`, `feeds`, and the later phase subcommands).

## Quick Start (PowerShell)

Requires Rust `1.89+` (`edition = "2024"`).

```powershell
# Copy config template (never commit .env)
Copy-Item .env.example .env

# Run workspace tests
cargo test --workspace

# Run Phase 0 diagnostics
cargo run -p cli -- doctor

# Run Phase 1 live feed discovery
cargo run -p cli -- feeds --discover --duration 20s

# Replay recorded feeds offline
cargo run -p cli -- feeds --replay logs/phase1-feeds.jsonl
```

## NEX Sandbox (Paper Venue)

Sandbox runs use Polymarket's NEX preprod exchange on Polygon Amoy (`80002`) instead of any
local simulator. Production hosts and keys are rejected by `SandboxProfile`.

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
cargo run -p cli --bin poly-as -- sandbox feeds \
  --up-asset-id <UP_ID> --dn-asset-id <DN_ID> --duration 30s --reconnect-after-secs 15
```

Both NEX hostnames must resolve to `51.24.102.79` (hosts file or resolver) and the trading
egress must match `NEX_ALLOWLISTED_EGRESS_IP`; the gate fails closed otherwise. Every sandbox
JSONL record carries `"origin": "sandbox"` and `"network": "nex-amoy-80002"` so Amoy test
fills never enter production volatility, arrival-rate, or PnL estimates. See `.env.example`
for the sandbox profile keys.

## Safety Notes

- `.env`, `.env.*` (except `.env.example`), `logs/`, `*.jsonl`, `*.log`, and `/target/` are gitignored.
- Default configuration runs with `EXECUTION_MODE=paper` and `DRY_RUN=true`.
