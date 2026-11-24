# Repository Git & Engineering Contract

## 1. Source of Truth

[IMPLEMENTATION_PLAN.md](IMPLEMENTATION_PLAN.md) governs the phased architecture, mathematical invariants, and sequential phase gates (Phases 0–5).
Re-read the active phase section in `IMPLEMENTATION_PLAN.md` before writing code.

## 2. Branching (Trunk-Based)

- Never commit feature or phase work directly to `main`. Keep `main` green and synced with `origin/main`.
- Sync `main` first, then create a short-lived branch:

```powershell
rtk git checkout main
rtk git pull --ff-only origin main
rtk git checkout -b feat/phase-<N>-<short-slug>
```

- Branch prefixes:
  - Phase features: `feat/phase-<N>-<short-slug>` (for example, `feat/phase-2-math-engine`, `feat/phase-3-paper-sim`, `feat/phase-4-risk-lifecycle`, `feat/phase-5-execution`)
  - Fixes and polish: `fix/<short-slug>` (keep prior-phase polish on a separate PR or in a dedicated leading atomic commit)
  - Docs, CI, and tooling: `docs/<short-slug>` or `chore/<short-slug>`

## 3. Atomic Conventional Commits (Save-Point Pattern)

- Never land an entire phase in one giant commit. Work in small slices: `implement slice -> fmt/clippy/test -> commit -> next slice`.
- Commit message format: `<type>(<scope>): <imperative summary>` with subject `<= 72` characters and an optional body explaining *why*.
- Allowed types: `feat`, `fix`, `refactor`, `test`, `docs`, `chore`, `perf`.
- Keep refactors, formatting fixes, and prior-phase polish in separate commits from new feature slices.
- Stage by explicit path (`rtk git add <paths>`) after checking `rtk git status --short` and `rtk git diff --cached --stat`. Never use `git add -A` on a mixed working tree.

## 4. Pre-Commit Gate (Mandatory Before Every Commit)

Run all four checks before every commit:

1. `rtk cargo fmt --all -- --check`
2. `rtk cargo clippy --workspace --all-targets -- -D warnings`
3. `rtk cargo test --workspace`
4. Inspect `git diff --cached` and confirm zero `.env` files, secrets, private keys, or `logs/` artifacts are staged.

The tracked hook automates checks 1 and 4. Enable it once per clone:

```powershell
git config core.hooksPath .githooks
```

## 5. Push, Pull Request, CI & Merge Gate

1. Push your branch early (`rtk git push -u origin <branch>`) and after verified milestones.
2. Open a pull request against `main` using `gh pr create` with [.github/PULL_REQUEST_TEMPLATE.md](.github/PULL_REQUEST_TEMPLATE.md). Include the Phase Verification Scorecard and both `rust-reviewer` and `silent-failure-hunter` sign-offs.
3. Wait for GitHub Actions CI ([.github/workflows/ci.yml](.github/workflows/ci.yml)) to finish green (`gh pr checks --watch`).
4. Merge with `--merge --delete-branch` so atomic slice commits stay preserved under one clean phase merge commit on `main`:

```powershell
gh pr merge --merge --delete-branch
```

5. Sync local `main` and push an annotated phase milestone tag:

```powershell
rtk git checkout main
rtk git pull --ff-only origin main
git tag -a phase-<N>-complete -m "Phase <N> verified and merged"
rtk git push origin phase-<N>-complete
```

## 6. Hard Prohibitions

- Never force-push `main` or rewrite pushed history.
- Never commit `.env`, API keys, private keys, or `logs/*.jsonl`.
- Never bypass checks with `--no-verify` or relax workspace deny lints outside `#[cfg(test)]`.
- Never silently bundle unrelated edits into a commit.

### Live Capture Safety

A live capture may run `target\release\poly-as.exe`. Check it first:

```powershell
Get-Process poly-as -ErrorAction SilentlyContinue
```

While that process runs, never:

- kill `poly-as.exe`;
- run `cargo build --release`;
- run `cargo clean`.

Safe during capture: `cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets`, and `cargo test --workspace` (all debug).

## 7. Key Files & Calibration Sync Surface

Every engine calibration touches five files. Edit all five in one change, or the defaults drift.

| File | Role |
| --- | --- |
| `crates/core-types/src/config.rs` | Env parsing, defaults, validation |
| `crates/core-types/src/lib.rs` | Default-value and env-roundtrip tests |
| `crates/cli/src/main.rs` | CLI paths and test configs |
| `scripts/sweep-params.ps1` | Sweep env overrides |
| `.env.example` | Documented operator values |

`.env.example` must mirror the `EngineConfig::default()` values in `config.rs`. A mismatch is drift: fix it in the same change.

## 8. Windows Grep Alternation

On Windows, `rtk grep "a\|b"` returns exit 1 and needs a retry. Use PowerShell `Select-String -Pattern "a\|b"` (or `rtk rg`) for alternation patterns in files.

## 9. Terminal Output Alignment & Fixed-Width Contract

All live progress tickers, status lines, and multi-line console output must follow strict static column alignment:
- **Left-Aligned Timestamps**: Pin timestamps to the far left of the row (`[{local_time}]`).
- **Equal Badge Widths**: Pad badges to equal visual width so following columns align (e.g., `[LIVE]  ` and `[WINDOW]` both occupy 8 columns).
- **Fixed-Width Numeric Columns**: Use explicit format widths for all counters, window identifiers, and financial figures (e.g., `Win {:>2}/{:<2}`, `CLOB: {:>8}`, `Bin: {:>7}`, `BTC ${:>8.2}`) to prevent horizontal jitter as numbers grow.
- **Fixed-Width Sub-Elements**: Pad composite fields such as bid/ask quotes (`Up {:>5}/{:<5} · Dn {:>5}/{:<5}`) to fixed width so delimiters (`│`) remain vertically aligned across ticks.
- **Visual Breathing Room**: Insert clean blank lines before and after window transitions to visually segment distinct phases and windows.
