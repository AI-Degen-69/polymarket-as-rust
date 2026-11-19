# polymarket-as-rust

Pure-Rust, terminal-first Avellaneda-Stoikov dual-bid market maker for Polymarket 5-minute BTC UP/DOWN binary markets.

## Status

| Phase | Scope | Status |
| :--- | :--- | :--- |
| Phase 0 | `core-types` and `poly-as doctor` | Complete |
| Phase 1 | `feeds` and `poly-as feeds` | Complete |
| Phases 2–5 | `math-engine`, `paper-sim`, `risk-lifecycle`, and `execution` | Planned — see [IMPLEMENTATION_PLAN.md](IMPLEMENTATION_PLAN.md) |

## Workspace Crates

- `crates/core-types` — Fixed-point monetary types (`MicroUsdc`, `SharesMicro`), env/secret config, Windows high-resolution clock, and HDR/JSONL telemetry.
- `crates/feeds` — Gamma market discovery, Binance Spot, Polymarket CLOB, and PolyBolt WebSocket actors with lock-free `rtrb` rings.
- `crates/cli` — Unified `poly-as` CLI binary (`doctor` and `feeds` subcommands).

## Quick Start (PowerShell)

Requires Rust `1.88+` (`edition = "2024"`).

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

## Safety Notes

- `.env`, `.env.*` (except `.env.example`), `logs/`, `*.jsonl`, `*.log`, and `/target/` are gitignored.
- Default configuration runs with `EXECUTION_MODE=paper` and `DRY_RUN=true`.
