# `pmdata download`

Downloads PMData historical files into `data/pmdata`. Two shapes exist: a single market
(`--market-slug`) and a Day Bundle (`--date` or `--from-date/--to-date`). Both are protected by the
`650`-unit quota guard.

## Syntax

```powershell
# Single market
poly-as pmdata download --market-slug <slug> [--date YYYY-MM-DD] [OPTIONS]

# Day Bundle(s)
poly-as pmdata download (--date YYYY-MM-DD | --from-date YYYY-MM-DD --to-date YYYY-MM-DD) [OPTIONS]
```

Passing `--market-slug` switches to single-market mode; the date arguments are then used only as the
UTC date for that market.

## Options

| Flag | Default | Meaning |
| :--- | :--- | :--- |
| `--market-slug <SLUG>` | none | Download `yes_l2`, optional `no_l2`, and `trades` for one market. |
| `--date <YYYY-MM-DD>` | none | One UTC date: the target date for a slug, or one Day Bundle date. |
| `--from-date <YYYY-MM-DD>` | none | Inclusive start of a Day Bundle date range. |
| `--to-date <YYYY-MM-DD>` | none | Inclusive end of a Day Bundle date range. |
| `--asset <SYMBOL>` | `btc` | Underlying asset. |
| `--interval <5m\|15m\|1h>` | `5m` | UpDown interval. |
| `--mode <full\|quota-saving>` | `full` | L2 ingestion mode. |
| `--include-onchain` | off | Also fetch `onchain_fills`: `+1` unit per single-market request, or one extra bundle archive (`6`–`72` units, same cost as the interval's bundle). |
| `--dry-run` | off | Print URLs, cache paths, and quota units. Sends no network request and spends nothing. |
| `--yes` | off | Confirm Day Bundle spending. Required when the plan needs quota. |

## Quota Cost

| Shape | `quota-saving` | `full` | `full --include-onchain` |
| :--- | :--- | :--- | :--- |
| Single market | `2` | `3` | `4` |
| `5m` Day Bundle | `144` | `216` | `288` |
| `15m` Day Bundle | `48` | `72` | `96` |
| `1h` Day Bundle | `12` | `18` | `24` |

Single-market requests are charged `1` unit per file. Bundles are charged per archive
(`5m=72`, `15m=24`, `1h=6`); `full` mode downloads a Yes archive and a No archive.

Already-cached entries cost `0`. A bundle interrupted after a charged response resumes from its
`.part` file with a credit for units already billed.

## Behavior

### Single market

1. Resolves the UTC date from `--date`, else from the epoch seconds at the end of the slug.
2. Fetches each file separately and prints one line per file.
3. `--dry-run` prints `[DRY-RUN]` lines plus the planned total.

### Day Bundle

1. Builds a plan for every date in the range and prints it before any spend.
2. Refuses the download when the plan exceeds the remaining quota (`refusing to exceed quota cap`).
3. Refuses without `--yes` when the plan needs more than `0` units.
4. Downloads each archive. A cached-and-verified archive is skipped.

Both shapes end by printing the manifest summary (`charged`, `reserved`, `remaining / cap`).

## Output Lines

```text
[DRY-RUN] <url> -> <cache path> (cached=false, est_cost=72)
[CACHE HIT] <entry key> -> <path> (0 quota charged)
[DOWNLOADED] <entry key> (status=200, bytes=..., cost=72, remaining=..., extracted=96)
[404 NOT FOUND] <entry key> (0 quota charged, remaining=...)
```

Day Bundle archives extract into `data/pmdata/extracted/<date>/<asset>-<interval>_<dataset>[_yes|_no>/`.

## Examples

```powershell
# Preview a single 15m day in full mode (0 quota, no network)
poly-as pmdata download --date 2026-10-28 --asset btc --interval 15m --mode full --dry-run

# Download that day in full mode
poly-as pmdata download --date 2026-10-28 --asset btc --interval 15m --mode full --yes

# Download one market without confirmation
poly-as pmdata download --market-slug btc-updown-5m-1793145900 --mode full

# Quota-saving range with on-chain fills
poly-as pmdata download --from-date 2026-10-28 --to-date 2026-10-30 --interval 5m `
  --mode quota-saving --include-onchain --yes
```

## Errors

| Message | Meaning |
| :--- | :--- |
| `specify either --market-slug, --date ..., or --from-date ... --to-date ...` | No download shape was selected. |
| `--to-date must not precede --from-date` | Inverted range. |
| `planned bundles need N quota units but only M/650 remain` | Plan would break the cap. Nothing was downloaded. |
| `refusing to spend N quota units on Day Bundles without '--yes'` | Add `--yes`, or preview first. |
| `bundle '<url>' became uncached or corrupted after planning` | Cache changed mid-run; re-run with `--yes` to re-download. |
| `cannot infer UTC date from --market-slug` | Slug lacks trailing epoch seconds; pass `--date`. |
