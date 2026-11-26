#!/usr/bin/env python3
"""Read-only feasibility analysis of pair-merge edge vs window-level Net PnL.

Reads the 21-capture replay sweep logs and raw capture inventory without
modifying any files or running new simulations.
"""

from __future__ import annotations

import argparse
import glob
import json
import math
import os
import random
import statistics
import subprocess
import sys
from collections import defaultdict
from typing import Any, Callable

SWEEP_SUBDIR = os.path.join("logs", "sweeps", "26-11_07-55-07Z_1runs_21caps-241w")
CAPTURES_SUBDIR = os.path.join("logs", "captures")
HOLDOUT_MARKER = "07-40-00Z"  # Capture #22; never replayed or opened


def _logs_path(subdir: str) -> str:
    """Resolve <repo-root>/<subdir>, falling back to the main checkout of a worktree."""
    here = os.path.dirname(os.path.abspath(__file__))
    local = os.path.join(os.path.dirname(here), subdir)
    if os.path.isdir(local):
        return local
    # shortcut: --path-format=absolute needs git >= 2.31; older git falls back to `local`.
    try:
        common = subprocess.run(
            ["git", "rev-parse", "--path-format=absolute", "--git-common-dir"],
            cwd=here,
            capture_output=True,
            text=True,
            check=True,
        ).stdout.strip()
    except (OSError, subprocess.CalledProcessError):
        return local
    if not common:
        return local
    return os.path.join(os.path.dirname(os.path.abspath(common)), subdir)


DEFAULT_SWEEP_DIR = _logs_path(SWEEP_SUBDIR)
DEFAULT_CAPTURES_DIR = _logs_path(CAPTURES_SUBDIR)

Z_ALPHA_05_1SIDED = 1.6448536269514722
Z_BETA_80_POWER = 0.8416212335729143
Z_SUM_SQ = (Z_ALPHA_05_1SIDED + Z_BETA_80_POWER) ** 2
WINDOWS_PER_DAY = 288  # 24h * 12 five-minute windows/h


def quantiles(xs: list[float], probs: list[float]) -> dict[float, float]:
    """Compute linear-interpolated quantiles for a non-empty sorted sample."""
    s = sorted(xs)
    n = len(s)
    out: dict[float, float] = {}
    for p in probs:
        k = (n - 1) * p
        f = math.floor(k)
        c = math.ceil(k)
        out[p] = s[f] if f == c else s[f] + (k - f) * (s[c] - s[f])
    return out


def weighted_quantiles(
    values: list[float], weights: list[float], probs: list[float]
) -> dict[float, float]:
    """Compute weighted empirical quantiles where each pair unit carries weight."""
    pairs = sorted(zip(values, weights), key=lambda x: x[0])
    total_w = sum(w for _, w in pairs)
    out: dict[float, float] = {}
    for p in probs:
        target = p * total_w
        cum = 0.0
        chosen = pairs[-1][0]
        for v, w in pairs:
            cum += w
            if cum >= target - 1e-12:
                chosen = v
                break
        out[p] = chosen
    return out


def inspect_inventory(captures_dir: str, sweep_stems: list[str]) -> dict[str, Any]:
    """Inspect capture directory inventory and compare against sweep manifest."""
    entries = sorted(os.listdir(captures_dir))
    complete_files = [
        f
        for f in entries
        if f.endswith(".jsonl") and not f.endswith(".inprogress.jsonl")
    ]
    inprogress_files = [f for f in entries if f.endswith(".inprogress.jsonl")]
    complete_stems = [os.path.splitext(f)[0] for f in complete_files]
    untouched = [s for s in complete_stems if s not in set(sweep_stems)]
    missing_from_captures = [s for s in sweep_stems if s not in set(complete_stems)]
    return {
        "complete_files": complete_files,
        "inprogress_files": inprogress_files,
        "sweep_stems": sweep_stems,
        "untouched_complete": untouched,
        "missing_from_captures": missing_from_captures,
    }


def load_sweep_data(
    sweep_dir: str,
) -> tuple[
    list[dict[str, Any]],
    list[dict[str, Any]],
    list[dict[str, Any]],
    list[dict[str, Any]],
    list[str],
    dict[str, int],
    int,
]:
    """Load and reconcile all replay JSONL logs in manifest order."""
    manifest_path = os.path.join(sweep_dir, "sweep-manifest.json")
    with open(manifest_path, "r", encoding="utf-8") as f:
        manifest = json.load(f)
    stems: list[str] = manifest["capture_stems"]
    holdouts = [s for s in stems if HOLDOUT_MARKER in s]
    if holdouts:
        raise RuntimeError(f"Holdout captures in development manifest: {holdouts}")

    all_windows: list[dict[str, Any]] = []
    all_merges: list[dict[str, Any]] = []
    all_flattens: list[dict[str, Any]] = []
    cap_rows: list[dict[str, Any]] = []
    reconcile_errors: list[str] = []
    event_counts: dict[str, int] = defaultdict(int)
    max_approx_diff_micro = 0

    for ci, stem in enumerate(stems):
        cap_dir = os.path.join(sweep_dir, stem)
        jsonls = sorted(
            p
            for p in glob.glob(os.path.join(cap_dir, "*.jsonl"))
            if not p.endswith(".inprogress.jsonl")
        )
        if len(jsonls) != 1:
            raise RuntimeError(f"Expected 1 JSONL in {cap_dir}, found {jsonls}")
        path = jsonls[0]

        with open(path, "r", encoding="utf-8") as f:
            raw_events = [json.loads(line) for line in f if line.strip()]

        fill_keys: set[tuple[Any, Any]] = set()
        flatten_by_key: dict[tuple[Any, Any], dict[str, Any]] = {}
        for obj in raw_events:
            ev = obj["event"]
            event_counts[ev] += 1
            key = (obj.get("ts_ms"), obj.get("mono_ns"))
            if ev == "fill":
                fill_keys.add(key)
            elif ev == "flatten":
                flatten_by_key[key] = obj["payload"]

        cap_summary: dict[str, Any] | None = None
        w_merges: list[dict[str, Any]] = []
        w_flattens: list[dict[str, Any]] = []
        cap_w_start = len(all_windows)

        cap_merge_gain_micro = 0
        cap_merge_pos_micro = 0
        cap_merge_neg_micro = 0
        cap_pairs_micro = 0
        cap_fees_micro = 0
        cap_settle_micro = 0
        cap_net_micro = 0

        for obj in raw_events:
            ev = obj["event"]
            p = obj.get("payload", {})
            key = (obj.get("ts_ms"), obj.get("mono_ns"))

            if ev == "merge":
                is_from_flatten = key in flatten_by_key
                is_from_fill = key in fill_keys
                pairs_m = int(p["merged_pairs_micro"])
                pair_cost_m = int(p["pair_cost_micro"])
                gain_m = int(p["realized_gain_micro"])
                approx_gain = (pairs_m * (1_000_000 - pair_cost_m)) // 1_000_000
                max_approx_diff_micro = max(
                    max_approx_diff_micro, abs(gain_m - approx_gain)
                )

                m_rec = {
                    "cap_idx": ci,
                    "stem": stem,
                    "day": stem[:5],
                    "slug": p["market_slug"],
                    "ts_ms": obj.get("ts_ms"),
                    "win_idx": len(all_windows),
                    "merged_pairs_micro": pairs_m,
                    "pairs": pairs_m / 1e6,
                    "avg_cost_up_micro": int(p["avg_cost_up_micro"]),
                    "avg_cost_dn_micro": int(p["avg_cost_dn_micro"]),
                    "pair_cost_micro": pair_cost_m,
                    "pair_cost": pair_cost_m / 1e6,
                    "realized_gain_micro": gain_m,
                    "gain": gain_m / 1e6,
                    "gain_per_pair": gain_m / pairs_m if pairs_m > 0 else 0.0,
                    "from_flatten": is_from_flatten,
                    "from_fill": is_from_fill,
                    "flatten_fee_micro": (
                        int(flatten_by_key[key]["fee_micro"])
                        if is_from_flatten
                        else 0
                    ),
                }
                w_merges.append(m_rec)
                all_merges.append(m_rec)
                cap_merge_gain_micro += gain_m
                if gain_m >= 0:
                    cap_merge_pos_micro += gain_m
                else:
                    cap_merge_neg_micro += abs(gain_m)
                cap_pairs_micro += pairs_m

            elif ev == "flatten":
                f_rec = {
                    "cap_idx": ci,
                    "stem": stem,
                    "slug": p["market_slug"],
                    "side": p["side"],
                    "outcome": p["outcome"],
                    "size_micro": int(p["size_micro"]),
                    "price_micro": int(p["price_micro"]),
                    "fee_micro": int(p["fee_micro"]),
                }
                w_flattens.append(f_rec)
                all_flattens.append(f_rec)
                cap_fees_micro += int(p["fee_micro"])

            elif ev == "window_end":
                mg_micro = sum(m["realized_gain_micro"] for m in w_merges)
                mg_maker_micro = sum(
                    m["realized_gain_micro"] for m in w_merges if not m["from_flatten"]
                )
                mg_flat_buy_micro = sum(
                    m["realized_gain_micro"] for m in w_merges if m["from_flatten"]
                )
                mg_pos_micro = sum(
                    m["realized_gain_micro"]
                    for m in w_merges
                    if m["realized_gain_micro"] >= 0
                )
                mg_neg_micro = sum(
                    abs(m["realized_gain_micro"])
                    for m in w_merges
                    if m["realized_gain_micro"] < 0
                )
                pairs_micro = sum(m["merged_pairs_micro"] for m in w_merges)

                net_micro = int(p["net_pnl_micro"])
                real_micro = int(p["realized_pnl_micro"])
                unreal_micro = int(p["unrealized_pnl_micro"])
                sett_micro = int(p["settlement_pnl_delta_micro"])
                fees_micro = int(p["fees_paid_micro"])
                reb_micro = int(p["estimated_rebate_micro"])

                # Residual other = realized_pnl - (merge_gain + settlement - fees)
                # Separate incomplete-window unrealized MTM cleanly.
                other_realized_micro = (
                    real_micro - mg_micro - sett_micro + fees_micro
                )
                if pairs_micro != int(p["total_pairs_merged_micro"]):
                    reconcile_errors.append(
                        f"Window {p['market_slug']} pairs mismatch"
                    )
                if len(w_merges) != int(p["merges_count"]):
                    reconcile_errors.append(
                        f"Window {p['market_slug']} merges_count mismatch"
                    )
                if net_micro != real_micro + unreal_micro:
                    reconcile_errors.append(
                        f"Window {p['market_slug']} net != realized + unrealized"
                    )
                if other_realized_micro != 0 and not any(
                    fl["side"] == "SELL" for fl in w_flattens
                ):
                    reconcile_errors.append(
                        f"Window {p['market_slug']} unexplained realized delta {other_realized_micro}"
                    )

                w_rec = {
                    "idx": len(all_windows),
                    "cap_idx": ci,
                    "stem": stem,
                    "day": stem[:5],
                    "slug": p["market_slug"],
                    "complete": bool(p["complete"]),
                    "winning_outcome": p.get("winning_outcome"),
                    "fills": int(p["fills_count"]),
                    "merges": int(p["merges_count"]),
                    "pairs_micro": pairs_micro,
                    "pairs": pairs_micro / 1e6,
                    "net_micro": net_micro,
                    "net": net_micro / 1e6,
                    "real_micro": real_micro,
                    "real": real_micro / 1e6,
                    "unreal_micro": unreal_micro,
                    "unreal": unreal_micro / 1e6,
                    "sett_micro": sett_micro,
                    "sett": sett_micro / 1e6,
                    "fees_micro": fees_micro,
                    "fees": fees_micro / 1e6,
                    "reb_micro": reb_micro,
                    "reb": reb_micro / 1e6,
                    "mg_micro": mg_micro,
                    "mg": mg_micro / 1e6,
                    "mg_maker_micro": mg_maker_micro,
                    "mg_maker": mg_maker_micro / 1e6,
                    "mg_flat_buy_micro": mg_flat_buy_micro,
                    "mg_flat_buy": mg_flat_buy_micro / 1e6,
                    "mg_pos_micro": mg_pos_micro,
                    "mg_pos": mg_pos_micro / 1e6,
                    "mg_neg_micro": mg_neg_micro,
                    "mg_neg": mg_neg_micro / 1e6,
                    "sell_flat_micro": other_realized_micro,
                    "sell_flat": other_realized_micro / 1e6,
                    "flatten_attempts": int(p.get("flatten_attempts", 0)),
                    "flattens_executed": int(p.get("flattens_executed", 0)),
                }
                all_windows.append(w_rec)
                cap_settle_micro += sett_micro
                cap_net_micro += net_micro
                w_merges = []
                w_flattens = []

            elif ev == "summary":
                cap_summary = p

        if cap_summary is None:
            reconcile_errors.append(f"Cap {stem} missing summary event")
            continue

        if cap_merge_gain_micro != int(cap_summary["total_merge_gain_micro"]):
            reconcile_errors.append(f"Cap {stem} total_merge_gain_micro mismatch")
        if cap_merge_pos_micro != int(cap_summary["positive_merge_gain_micro"]):
            reconcile_errors.append(f"Cap {stem} positive_merge_gain_micro mismatch")
        if cap_merge_neg_micro != int(cap_summary["negative_merge_loss_micro"]):
            reconcile_errors.append(f"Cap {stem} negative_merge_loss_micro mismatch")
        if cap_pairs_micro != int(cap_summary["total_pairs_merged_micro"]):
            reconcile_errors.append(f"Cap {stem} total_pairs_merged_micro mismatch")
        if cap_fees_micro != int(cap_summary["fees_paid_micro"]):
            reconcile_errors.append(f"Cap {stem} fees_paid_micro mismatch")
        if cap_settle_micro != int(cap_summary["settlement_pnl_micro"]):
            reconcile_errors.append(f"Cap {stem} settlement_pnl_micro mismatch")
        if cap_net_micro != int(cap_summary["net_pnl_micro"]):
            reconcile_errors.append(f"Cap {stem} net_pnl_micro mismatch")

        cap_ws = all_windows[cap_w_start:]
        cap_rows.append(
            {
                "ci": ci + 1,
                "stem": stem,
                "day": stem[:5],
                "windows": len(cap_ws),
                "fills": sum(w["fills"] for w in cap_ws),
                "merges": sum(w["merges"] for w in cap_ws),
                "pairs": sum(w["pairs"] for w in cap_ws),
                "mg": sum(w["mg"] for w in cap_ws),
                "sett": sum(w["sett"] for w in cap_ws),
                "unreal": sum(w["unreal"] for w in cap_ws),
                "real": sum(w["real"] for w in cap_ws),
                "net": sum(w["net"] for w in cap_ws),
                "reb": sum(w["reb"] for w in cap_ws),
                "volume_in": int(cap_summary["volume_in_micro"]) / 1e6,
            }
        )

    return (
        all_windows,
        all_merges,
        all_flattens,
        cap_rows,
        reconcile_errors,
        dict(event_counts),
        max_approx_diff_micro,
    )


def metric_power_summary(
    name: str,
    is_vals: list[float],
    oos_vals: list[float],
    all_vals: list[float],
    deff_cap: float | None = None,
) -> dict[str, Any]:
    """Compute descriptive stats, retention ratio, and IID/cluster sample sizes."""
    mu_is = statistics.mean(is_vals)
    sd_is = statistics.stdev(is_vals)
    se_is = sd_is / math.sqrt(len(is_vals))

    mu_oos = statistics.mean(oos_vals)
    sd_oos = statistics.stdev(oos_vals)
    se_oos = sd_oos / math.sqrt(len(oos_vals))
    t_oos = mu_oos / se_oos if se_oos > 0 else float("nan")

    mu_all = statistics.mean(all_vals)
    sd_all = statistics.stdev(all_vals)
    se_all = sd_all / math.sqrt(len(all_vals))
    t_all = mu_all / se_all if se_all > 0 else float("nan")

    ret_pct = (mu_oos / mu_is * 100.0) if mu_is > 1e-9 else float("nan")
    delta_50 = 0.5 * mu_is
    n_iid_50_oos_sd = (
        Z_SUM_SQ * (sd_oos**2) / (delta_50**2) if delta_50 > 0 else float("nan")
    )
    n_iid_50_all_sd = (
        Z_SUM_SQ * (sd_all**2) / (delta_50**2) if delta_50 > 0 else float("nan")
    )
    n_iid_100_all_sd = (
        Z_SUM_SQ * (sd_all**2) / (mu_is**2) if mu_is > 0 else float("nan")
    )
    n_clust_50_all_sd = (
        n_iid_50_all_sd * deff_cap if deff_cap is not None else float("nan")
    )
    return {
        "name": name,
        "n_is": len(is_vals),
        "n_oos": len(oos_vals),
        "n_all": len(all_vals),
        "mu_is": mu_is,
        "sd_is": sd_is,
        "se_is": se_is,
        "cv_is": (se_is / mu_is) if abs(mu_is) > 1e-12 else float("nan"),
        "mu_oos": mu_oos,
        "sd_oos": sd_oos,
        "se_oos": se_oos,
        "t_oos": t_oos,
        "ci95_oos": (mu_oos - 1.96 * se_oos, mu_oos + 1.96 * se_oos),
        "mu_all": mu_all,
        "sd_all": sd_all,
        "se_all": se_all,
        "t_all": t_all,
        "ret_pct": ret_pct,
        "delta_50": delta_50,
        "n_iid_50_oos_sd": n_iid_50_oos_sd,
        "n_iid_50_all_sd": n_iid_50_all_sd,
        "n_iid_100_all_sd": n_iid_100_all_sd,
        "n_clust_50_all_sd": n_clust_50_all_sd,
    }


def cluster_analysis(
    windows: list[dict[str, Any]],
    getter: Callable[[dict[str, Any]], float],
    seed: int = 42,
    n_boot: int = 10_000,
) -> dict[str, Any]:
    """Compute Liang-Zeger cluster-robust SE, DEFF, ACF(1), and block bootstrap."""
    cap_groups: dict[int, list[float]] = defaultdict(list)
    day_groups: dict[str, list[float]] = defaultdict(list)
    vals: list[float] = []
    for w in windows:
        v = getter(w)
        vals.append(v)
        cap_groups[w["cap_idx"]].append(v)
        day_groups[w["day"]].append(v)

    n = len(vals)
    mu = statistics.mean(vals)
    se_iid = statistics.stdev(vals) / math.sqrt(n)

    g_cap = len(cap_groups)
    cap_ss = sum((sum(v - mu for v in grp)) ** 2 for grp in cap_groups.values())
    se_cap = math.sqrt((g_cap / (g_cap - 1)) * cap_ss) / n
    deff_cap = (se_cap / se_iid) ** 2 if se_iid > 0 else float("nan")

    g_day = len(day_groups)
    day_ss = sum((sum(v - mu for v in grp)) ** 2 for grp in day_groups.values())
    se_day = math.sqrt((g_day / (g_day - 1)) * day_ss) / n
    deff_day = (se_day / se_iid) ** 2 if se_iid > 0 else float("nan")

    num = sum((vals[i] - mu) * (vals[i - 1] - mu) for i in range(1, n))
    den = sum((v - mu) ** 2 for v in vals)
    acf1 = num / den if den > 0 else 0.0

    rng = random.Random(seed)
    cap_list = list(cap_groups.values())
    boot_means: list[float] = []
    for _ in range(n_boot):
        drawn = [rng.choice(cap_list) for _ in range(g_cap)]
        flat = [x for grp in drawn for x in grp]
        boot_means.append(sum(flat) / len(flat))
    boot_means.sort()
    ci_lo = boot_means[int(0.025 * n_boot)]
    ci_hi = boot_means[int(0.975 * n_boot)]
    p_le_zero = sum(1 for x in boot_means if x <= 0.0) / n_boot

    return {
        "mean": mu,
        "se_iid": se_iid,
        "se_cap": se_cap,
        "deff_cap": deff_cap,
        "se_day": se_day,
        "deff_day": deff_day,
        "acf1": acf1,
        "boot_ci95": (ci_lo, ci_hi),
        "boot_p_le_zero": p_le_zero,
    }


def retention_bootstrap(
    windows: list[dict[str, Any]],
    getter: Callable[[dict[str, Any]], float],
    seed: int = 42,
    n_boot: int = 10_000,
) -> dict[str, Any]:
    """Capture-block bootstrap the 70/30 OOS/IS retention ratio stability."""
    n_is = int(math.floor(len(windows) * 0.7))
    is_caps: dict[int, list[float]] = defaultdict(list)
    oos_caps: dict[int, list[float]] = defaultdict(list)
    for w in windows[:n_is]:
        is_caps[w["cap_idx"]].append(getter(w))
    for w in windows[n_is:]:
        oos_caps[w["cap_idx"]].append(getter(w))

    is_list = list(is_caps.values())
    oos_list = list(oos_caps.values())
    rng = random.Random(seed)
    ratios: list[float] = []
    non_pos_is = 0
    for _ in range(n_boot):
        s_is = [x for grp in (rng.choice(is_list) for _ in range(len(is_list))) for x in grp]
        s_oos = [x for grp in (rng.choice(oos_list) for _ in range(len(oos_list))) for x in grp]
        mu_is = sum(s_is) / len(s_is)
        mu_oos = sum(s_oos) / len(s_oos)
        if mu_is <= 1e-9:
            non_pos_is += 1
        else:
            ratios.append((mu_oos / mu_is) * 100.0)

    ratios.sort()
    q = quantiles(ratios, [0.05, 0.25, 0.50, 0.75, 0.95])
    pass_50 = sum(1 for r in ratios if r >= 50.0) / n_boot
    return {
        "non_pos_is_pct": non_pos_is / n_boot * 100.0,
        "quantiles": q,
        "pass_50_pct": pass_50 * 100.0,
    }


def pearson_r(xs: list[float], ys: list[float]) -> float:
    """Compute sample Pearson correlation."""
    mx = statistics.mean(xs)
    my = statistics.mean(ys)
    cov = sum((a - mx) * (b - my) for a, b in zip(xs, ys)) / (len(xs) - 1)
    return cov / (statistics.stdev(xs) * statistics.stdev(ys))


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Audit pair-merge edge vs Net PnL OOS retention feasibility."
    )
    parser.add_argument("--sweep-dir", default=DEFAULT_SWEEP_DIR)
    parser.add_argument("--captures-dir", default=DEFAULT_CAPTURES_DIR)
    args = parser.parse_args()

    (
        windows,
        merges,
        flattens,
        cap_rows,
        reconcile_errors,
        event_counts,
        max_approx_diff_micro,
    ) = load_sweep_data(args.sweep_dir)

    inv = inspect_inventory(args.captures_dir, [c["stem"] for c in cap_rows])

    print("=" * 88)
    print("1. CAPTURE INVENTORY & HOLDOUT CHECK")
    print("=" * 88)
    print(f"Complete captures in logs/captures:   {len(inv['complete_files'])}")
    print(f"In-progress captures:                 {len(inv['inprogress_files'])} -> {inv['inprogress_files']}")
    print(f"Captures in 21-cap sweep manifest:    {len(inv['sweep_stems'])} ({len(windows)} windows)")
    print(f"Untouched complete captures:          {inv['untouched_complete']}")
    print(f"Sweep stems missing from captures:    {inv['missing_from_captures']}")

    print("\n" + "=" * 88)
    print("2. ACCOUNTING RECONCILIATION & SCHEMA COVERAGE")
    print("=" * 88)
    print(f"Event counts: {event_counts}")
    print(f"Reconciliation errors: {len(reconcile_errors)}")
    for err in reconcile_errors:
        print(f"  ERROR: {err}")
    print(
        f"Max |merge.realized_gain_micro - floor(pairs*(1-pair_cost)/1e6)|: "
        f"{max_approx_diff_micro} micro-USDC"
    )
    m_maker = sum(1 for m in merges if m["from_fill"] and not m["from_flatten"])
    m_flat = sum(1 for m in merges if m["from_flatten"] and not m["from_fill"])
    print(f"Merge provenance: maker_fill={m_maker}, taker_buy_flatten={m_flat}, flattens={len(flattens)}")
    incomplete_ws = [w for w in windows if not w["complete"]]
    print(f"Incomplete windows (complete=false): {len(incomplete_ws)}")
    for w in incomplete_ws:
        print(
            f"  win#{w['idx']} {w['stem']} {w['slug']}: "
            f"real=${w['real']:+.4f}, unreal=${w['unreal']:+.4f}, net=${w['net']:+.4f}, mg=${w['mg']:+.4f}"
        )

    print("\n" + "=" * 88)
    print("3. PAIR-MERGING DISTRIBUTION (EVENTS, PAIRS, WINDOWS)")
    print("=" * 88)
    ev_gains = [m["gain"] for m in merges]
    ev_gpps = [m["gain_per_pair"] for m in merges]
    ev_pairs = [m["pairs"] for m in merges]
    tot_pairs = sum(ev_pairs)
    w_mean_gpp = sum(ev_gains) / tot_pairs
    w_sd_gpp = math.sqrt(
        sum(p * (gpp - w_mean_gpp) ** 2 for p, gpp in zip(ev_pairs, ev_gpps)) / tot_pairs
    )
    pos_ev = sum(1 for g in ev_gains if g > 0)
    zero_ev = sum(1 for g in ev_gains if g == 0)
    neg_ev = sum(1 for g in ev_gains if g < 0)
    pos_pairs = sum(m["pairs"] for m in merges if m["gain"] > 0)
    zero_pairs = sum(m["pairs"] for m in merges if m["gain"] == 0)
    neg_pairs = sum(m["pairs"] for m in merges if m["gain"] < 0)
    probs = [0.0, 0.05, 0.10, 0.25, 0.50, 0.75, 0.90, 0.95, 1.0]
    ev_q = quantiles(ev_gains, probs)
    gpp_q = quantiles(ev_gpps, probs)
    wgpp_q = weighted_quantiles(ev_gpps, ev_pairs, probs)

    print(
        f"Merge events: {len(merges)} (win={pos_ev} [{pos_ev/len(merges)*100:.1f}%], "
        f"zero={zero_ev} [{zero_ev/len(merges)*100:.1f}%], neg={neg_ev} [{neg_ev/len(merges)*100:.1f}%])"
    )
    print(
        f"Merged pairs: {tot_pairs:.2f} (win={pos_pairs:.2f} [{pos_pairs/tot_pairs*100:.1f}%], "
        f"zero={zero_pairs:.2f} [{zero_pairs/tot_pairs*100:.1f}%], neg={neg_pairs:.2f} [{neg_pairs/tot_pairs*100:.1f}%])"
    )
    print(
        f"Per-event gain ($): mean={statistics.mean(ev_gains):+.4f}, median={statistics.median(ev_gains):+.4f}, "
        f"sd={statistics.stdev(ev_gains):.4f}, var={statistics.variance(ev_gains):.4f}"
    )
    print("  Percentiles ($/event):", {int(k * 100): round(v, 4) for k, v in ev_q.items()})
    print(
        f"Per-pair gain unweighted ($/pair): mean={statistics.mean(ev_gpps):+.5f}, "
        f"median={statistics.median(ev_gpps):+.5f}, sd={statistics.stdev(ev_gpps):.5f}"
    )
    print(
        f"Per-pair gain pair-weighted ($/pair): mean={w_mean_gpp:+.5f}, "
        f"sd={w_sd_gpp:.5f}, var={w_sd_gpp**2:.6f}"
    )
    print("  Event-weighted percentiles ($/pair):", {int(k * 100): round(v, 5) for k, v in gpp_q.items()})
    print("  Pair-weighted percentiles ($/pair): ", {int(k * 100): round(v, 5) for k, v in wgpp_q.items()})

    active_m_ws = [w for w in windows if w["merges"] > 0]
    zero_m_ws = [w for w in windows if w["merges"] == 0]
    active_f_ws = [w for w in windows if w["fills"] > 0]
    print(
        f"Windows: total={len(windows)}, w/fills={len(active_f_ws)}, "
        f"w/merges={len(active_m_ws)}, no-merge={len(zero_m_ws)} ({len(zero_m_ws)/len(windows)*100:.1f}%)"
    )

    print("\n--- Per-Capture Breakdown ---")
    cum_w = 0
    for c in cap_rows:
        split = "IS" if cum_w + c["windows"] <= 168 else ("OOS" if cum_w >= 168 else "SPLIT")
        gpp = c["mg"] / c["pairs"] if c["pairs"] > 0 else float("nan")
        print(
            f"Cap#{c['ci']:2d} {c['stem']:30s} [{split:5s}] w={c['windows']:2d} "
            f"fills={c['fills']:2d} m={c['merges']:2d} pairs={c['pairs']:6.2f} "
            f"mg={c['mg']:+7.4f} ($/p={gpp:+.4f}) sett={c['sett']:+7.4f} "
            f"unr={c['unreal']:+6.4f} net={c['net']:+7.4f}"
        )
        cum_w += c["windows"]

    print("\n--- Per-Day Breakdown ---")
    by_day: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for w in windows:
        by_day[w["day"]].append(w)
    for day in sorted(by_day):
        ws = by_day[day]
        p_sum = sum(w["pairs"] for w in ws)
        mg_sum = sum(w["mg"] for w in ws)
        sett_sum = sum(w["sett"] for w in ws)
        unr_sum = sum(w["unreal"] for w in ws)
        net_sum = sum(w["net"] for w in ws)
        print(
            f"Day {day}: w={len(ws):3d} (m_act={sum(1 for w in ws if w['merges']>0):2d}) "
            f"fills={sum(w['fills'] for w in ws):3d} pairs={p_sum:6.2f} "
            f"mg={mg_sum:+8.4f} ($/p={mg_sum/p_sum if p_sum else 0:+.4f}, $/w={mg_sum/len(ws):+.4f}) "
            f"sett={sett_sum:+8.4f} unr={unr_sum:+6.4f} net={net_sum:+8.4f} ($/w={net_sum/len(ws):+.4f})"
        )

    print("\n" + "=" * 88)
    print("4. ECONOMIC DECOMPOSITION (ALL 241w / IS 168w / OOS 73w)")
    print("=" * 88)
    n_is = int(math.floor(len(windows) * 0.7))
    is_w = windows[:n_is]
    oos_w = windows[n_is:]

    for label, ws in [("ALL (241w)", windows), ("IS (168w)", is_w), ("OOS (73w)", oos_w)]:
        mg_mak = sum(w["mg_maker"] for w in ws)
        mg_fb = sum(w["mg_flat_buy"] for w in ws)
        mg_tot = sum(w["mg"] for w in ws)
        mg_pos = sum(w["mg_pos"] for w in ws)
        mg_neg = sum(w["mg_neg"] for w in ws)
        s_flat = sum(w["sell_flat"] for w in ws)
        fees = sum(w["fees"] for w in ws)
        sett = sum(w["sett"] for w in ws)
        real = sum(w["real"] for w in ws)
        unr = sum(w["unreal"] for w in ws)
        net = sum(w["net"] for w in ws)
        reb = sum(w["reb"] for w in ws)
        pairs = sum(w["pairs"] for w in ws)
        fills = sum(w["fills"] for w in ws)
        sett_loss = max(0.0, -(sett + min(unr, 0.0)))
        drag_pct = (sett_loss / mg_pos * 100.0) if mg_pos > 0 else 0.0
        tail_drag_pct = ((sett_loss + mg_neg) / mg_pos * 100.0) if mg_pos > 0 else 0.0
        print(f"\n[{label}] fills={fills}, pairs={pairs:.2f}")
        print(f"  1. Maker pair-merge gain:       ${mg_mak:+.6f} (pos=${mg_pos:+.4f}, neg=-${mg_neg:.4f})")
        print(f"  2. Taker-BUY flatten merge:     ${mg_fb:+.6f}")
        print(f"  3. Taker-SELL flatten PnL:      ${s_flat:+.6f}")
        print(f"  4. Taker fees paid:             ${-fees:+.6f}")
        print(f"  5. Terminal settlement PnL:     ${sett:+.6f}")
        print(f"  -> Realized PnL (1+2+3+4+5):    ${real:+.6f} (check={mg_tot+mg_fb*0+s_flat-fees+sett:+.6f})")
        print(f"  6. Incomplete-window MTM:       ${unr:+.6f}")
        print(f"  -> Reported Net PnL (@0% reb):  ${net:+.6f}")
        print(f"  7. Memo 3% estimated rebate:    ${reb:+.6f} -> Net+Reb=${net+reb:+.6f}")
        print(f"  G5a Expiry Drag={drag_pct:.2f}% | G5b Tail Drag={tail_drag_pct:.2f}%")

    nz_sett = [w for w in windows if w["sett_micro"] != 0]
    s_wins = [w for w in nz_sett if w["sett"] > 0]
    s_losses = [w for w in nz_sett if w["sett"] < 0]
    print(
        f"\nSettlement adverse-selection profile: {len(nz_sett)} non-zero windows -> "
        f"{len(s_wins)} wins ({len(s_wins)/len(nz_sett)*100:.1f}%, sum=${sum(w['sett'] for w in s_wins):+.4f}), "
        f"{len(s_losses)} losses ({len(s_losses)/len(nz_sett)*100:.1f}%, sum=${sum(w['sett'] for w in s_losses):+.4f})"
    )

    print("\n" + "=" * 88)
    print("5. CANDIDATE METRICS, CLUSTERING & STATISTICAL FEASIBILITY")
    print("=" * 88)
    c_net = cluster_analysis(windows, lambda w: w["net"])
    c_real = cluster_analysis(windows, lambda w: w["real"])
    c_mg = cluster_analysis(windows, lambda w: w["mg"])
    c_sett = cluster_analysis(windows, lambda w: w["sett"])

    summaries = [
        metric_power_summary(
            "A: Window Net PnL (241w, $)",
            [w["net"] for w in is_w],
            [w["net"] for w in oos_w],
            [w["net"] for w in windows],
            c_net["deff_cap"],
        ),
        metric_power_summary(
            "A-real: Realized Window PnL (241w, $)",
            [w["real"] for w in is_w],
            [w["real"] for w in oos_w],
            [w["real"] for w in windows],
            c_real["deff_cap"],
        ),
        metric_power_summary(
            "B1: Window Merge Gain (241w, $)",
            [w["mg"] for w in is_w],
            [w["mg"] for w in oos_w],
            [w["mg"] for w in windows],
            c_mg["deff_cap"],
        ),
        metric_power_summary(
            "B2: Window Merge Gain (75 merge-active w, $)",
            [w["mg"] for w in is_w if w["merges"] > 0],
            [w["mg"] for w in oos_w if w["merges"] > 0],
            [w["mg"] for w in windows if w["merges"] > 0],
        ),
        metric_power_summary(
            "B3: Window Merge Gain/Pair (75 merge-active w, $/pair)",
            [w["mg"] / w["pairs"] for w in is_w if w["pairs"] > 0],
            [w["mg"] / w["pairs"] for w in oos_w if w["pairs"] > 0],
            [w["mg"] / w["pairs"] for w in windows if w["pairs"] > 0],
        ),
        metric_power_summary(
            "C: Window Net Merge Gain (merge-fees, 241w, $)",
            [w["mg"] - w["fees"] for w in is_w],
            [w["mg"] - w["fees"] for w in oos_w],
            [w["mg"] - w["fees"] for w in windows],
            c_mg["deff_cap"],
        ),
        metric_power_summary(
            "D1: Fill-Active Window Net PnL (83w, $)",
            [w["net"] for w in is_w if w["fills"] > 0],
            [w["net"] for w in oos_w if w["fills"] > 0],
            [w["net"] for w in windows if w["fills"] > 0],
        ),
        metric_power_summary(
            "Component: Window Settlement PnL (241w, $)",
            [w["sett"] for w in is_w],
            [w["sett"] for w in oos_w],
            [w["sett"] for w in windows],
            c_sett["deff_cap"],
        ),
    ]

    for s in summaries:
        print(f"\n[{s['name']}]")
        print(
            f"  IS  (n={s['n_is']:3d}): mean={s['mu_is']:+.5f}, sd={s['sd_is']:.5f}, "
            f"SE={s['se_is']:.5f}, CV={s['cv_is']:.2%}"
        )
        print(
            f"  OOS (n={s['n_oos']:3d}): mean={s['mu_oos']:+.5f}, sd={s['sd_oos']:.5f}, "
            f"SE={s['se_oos']:.5f}, t={s['t_oos']:+.2f}, "
            f"95%CI=[{s['ci95_oos'][0]:+.4f}, {s['ci95_oos'][1]:+.4f}]"
        )
        print(
            f"  ALL (n={s['n_all']:3d}): mean={s['mu_all']:+.5f}, sd={s['sd_all']:.5f}, "
            f"SE={s['se_all']:.5f}, t={s['t_all']:+.2f}"
        )
        print(
            f"  Retain={s['ret_pct']:.2f}% | delta(50%IS)={s['delta_50']:+.5f} | "
            f"n_IID(OOS_sd)={s['n_iid_50_oos_sd']:.1f} | "
            f"n_IID(ALL_sd)={s['n_iid_50_all_sd']:.1f} | "
            f"n_Clust(ALL_sd)={s['n_clust_50_all_sd']:.1f} | "
            f"n_IID(100%IS)={s['n_iid_100_all_sd']:.1f}"
        )

    fills_per_w = sum(w["fills"] for w in windows) / len(windows)
    n_rep = summaries[0]["n_iid_50_oos_sd"]
    print(
        f"\nBrainstorming reproduction check: n_OOS={n_rep:.1f}w, "
        f"days={n_rep / WINDOWS_PER_DAY:.2f}d (@{WINDOWS_PER_DAY}w/d), "
        f"fills={n_rep * fills_per_w:.0f} (@{fills_per_w:.3f} fills/w)"
    )

    print("\n--- Clustering & Capture-Block Bootstrap (10,000 draws, seed=42) ---")
    for label, c in [
        ("Window Net PnL (241w)", c_net),
        ("Window Realized PnL (241w)", c_real),
        ("Window Merge Gain (241w)", c_mg),
        ("Window Settlement PnL (241w)", c_sett),
    ]:
        print(
            f"  {label:30s}: mean={c['mean']:+.5f}, SE_iid={c['se_iid']:.5f}, "
            f"SE_cap={c['se_cap']:.5f} (DEFF={c['deff_cap']:.2f}x), "
            f"SE_day={c['se_day']:.5f} (DEFF={c['deff_day']:.2f}x), "
            f"ACF1={c['acf1']:+.3f}, boot95=[{c['boot_ci95'][0]:+.4f}, {c['boot_ci95'][1]:+.4f}], "
            f"P(mean<=0)={c['boot_p_le_zero']:.4f}"
        )

    r_all = pearson_r([w["mg"] for w in windows], [w["sett"] for w in windows])
    r_act = pearson_r(
        [w["mg"] for w in active_f_ws], [w["sett"] for w in active_f_ws]
    )
    print(f"\nCorrelation(merge_gain, settlement_pnl): 241w r={r_all:+.4f} | 83 fill-active w r={r_act:+.4f}")

    rb_net = retention_bootstrap(windows, lambda w: w["net"])
    rb_mg = retention_bootstrap(windows, lambda w: w["mg"])
    print("\n--- Retention Ratio Stability (Capture-Block Bootstrap) ---")
    print(
        f"  Net PnL Retention: non-pos IS={rb_net['non_pos_is_pct']:.2f}%, "
        f"P(Retain>=50%)={rb_net['pass_50_pct']:.1f}%, "
        f"pcts={ {int(k*100): round(v,1) for k,v in rb_net['quantiles'].items()} }"
    )
    print(
        f"  Merge Gain Retention: non-pos IS={rb_mg['non_pos_is_pct']:.2f}%, "
        f"P(Retain>=50%)={rb_mg['pass_50_pct']:.1f}%, "
        f"pcts={ {int(k*100): round(v,1) for k,v in rb_mg['quantiles'].items()} }"
    )

    if reconcile_errors:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
