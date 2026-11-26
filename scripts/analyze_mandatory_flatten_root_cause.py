#!/usr/bin/env python3
"""Forensic & causal counterfactual analysis of settlement losses and MandatoryFlatten.

Reads ONLY the 21 Development/IS captures listed in sweep-manifest.json and their
corresponding replay JSONL logs. Never touches Capture #22 or .inprogress files.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import glob
import json
import os
import statistics
import subprocess
import sys
from collections import defaultdict
from typing import Any

SWEEP_SUBDIR = os.path.join("logs", "sweeps", "26-11_07-55-07Z_1runs_21caps-241w")
CAPTURES_SUBDIR = os.path.join("logs", "captures")


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

MICRO_SCALE = 1_000_000
MIN_ORDER_SHARES_MICRO = 5 * MICRO_SCALE
FEE_RATE_E6 = 50_000  # Post-Nov-20 BTC 5m feeRate = 0.05

POLICIES: list[dict[str, Any]] = [
    {"id": "A_baseline_30s", "kind": "time_best1", "tau_start": 30.0, "tau_end": 15.0},
    {"id": "B_early_60s", "kind": "time_sweep", "tau_start": 60.0, "tau_end": 15.0},
    {"id": "B_early_90s", "kind": "time_sweep", "tau_start": 90.0, "tau_end": 15.0},
    {"id": "B_early_120s", "kind": "time_sweep", "tau_start": 120.0, "tau_end": 15.0},
    {"id": "C_grad_30s", "kind": "grad_30s", "tau_start": 30.0, "tau_end": 15.0},
    {"id": "C_grad_60s", "kind": "grad_60s", "tau_start": 60.0, "tau_end": 15.0},
    {"id": "D_partial_30s", "kind": "time_best1", "tau_start": 30.0, "tau_end": 15.0},
    {"id": "E_stopcross_110", "kind": "stop_cross", "comb_thresh": 1_100_000, "tau_start": 300.0, "tau_end": 15.0},
    {"id": "E_stopcross_115", "kind": "stop_cross", "comb_thresh": 1_150_000, "tau_start": 300.0, "tau_end": 15.0},
    {"id": "E_stopcross_120", "kind": "stop_cross", "comb_thresh": 1_200_000, "tau_start": 300.0, "tau_end": 15.0},
    {"id": "E_stopcross_125", "kind": "stop_cross", "comb_thresh": 1_250_000, "tau_start": 300.0, "tau_end": 15.0},
    {"id": "E_stopcross_130", "kind": "stop_cross", "comb_thresh": 1_300_000, "tau_start": 300.0, "tau_end": 15.0},
    {"id": "E_stop_120_tau90", "kind": "stop_cross", "comb_thresh": 1_200_000, "tau_start": 90.0, "tau_end": 15.0},
    {"id": "E_stop_130_tau90", "kind": "stop_cross", "comb_thresh": 1_300_000, "tau_start": 90.0, "tau_end": 15.0},
    {"id": "E_stop_120_tau60", "kind": "stop_cross", "comb_thresh": 1_200_000, "tau_start": 60.0, "tau_end": 15.0},
]


def compute_taker_fee_micro(shares_micro: int, price_micro: int) -> int:
    """Exact mirror of FeeSchedule::compute_taker_fee_micro in money.rs."""
    if shares_micro <= 0 or price_micro <= 0 or price_micro >= MICRO_SCALE:
        return 0
    one_minus_p = MICRO_SCALE - price_micro
    num = shares_micro * FEE_RATE_E6 * price_micro * one_minus_p
    unit_5dp = 10_000_000_000_000_000_000  # 10^19
    rounded_5dp = (num + unit_5dp // 2) // unit_5dp
    return rounded_5dp * 10


def walk_l2_fak(
    levels: dict[int, int],
    side: str,
    requested_micro: int,
    limit_price_micro: int | None = None,
) -> tuple[list[tuple[int, int]], int, int, int]:
    """Walk visible L2 depth on a single book side.

    Returns (fills[(px, sz)], total_filled, total_notional, total_fee).
    """
    if requested_micro <= 0 or not levels:
        return [], 0, 0, 0
    if side == "BUY":
        sorted_lvls = sorted(levels.items(), key=lambda x: x[0])
    else:
        sorted_lvls = sorted(levels.items(), key=lambda x: -x[0])

    rem = requested_micro
    fills: list[tuple[int, int]] = []
    notional = 0
    fees = 0
    for px, sz in sorted_lvls:
        if sz <= 0 or px <= 0:
            continue
        if limit_price_micro is not None:
            if side == "BUY" and px > limit_price_micro:
                break
            if side == "SELL" and px < limit_price_micro:
                break
        take = min(rem, sz)
        if take > 0:
            fills.append((px, take))
            notional += (take * px) // MICRO_SCALE
            fees += compute_taker_fee_micro(take, px)
            rem -= take
        if rem == 0:
            break
    filled = requested_micro - rem
    return fills, filled, notional, fees


def load_replay_windows(
    sweep_dir: str,
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    """Reconstruct exact per-window timelines from the 21 replay logs."""
    manifest_path = os.path.join(sweep_dir, "sweep-manifest.json")
    with open(manifest_path, "r", encoding="utf-8") as f:
        manifest = json.load(f)
    stems: list[str] = manifest["capture_stems"]

    windows: list[dict[str, Any]] = []
    totals = {
        "merge_micro": 0,
        "settle_micro": 0,
        "unreal_micro": 0,
        "net_micro": 0,
        "fees_micro": 0,
        "flattens": 0,
        "fills": 0,
    }

    for ci, stem in enumerate(stems):
        rep_path = sorted(glob.glob(os.path.join(sweep_dir, stem, "*.jsonl")))[0]
        q_up = q_dn = cb_up = cb_dn = 0
        max_unpaired = 0
        init_unpaired = 0
        first_unp_ts: int | None = None
        first_fill_ts: int | None = None
        first_fill_out: str | None = None
        first_fill_px = 0
        cum_mg = 0
        w_fills: list[dict[str, Any]] = []
        inv_events: list[dict[str, Any]] = []
        oa_counts: dict[str, int] = defaultdict(int)
        last_hedge_px: dict[str, int] = {}
        last_nonhold_tau: int | None = None
        last_cancel_reason: str | None = None
        mand_reached = False
        pre_reached = False

        with open(rep_path, "r", encoding="utf-8") as f:
            for line in f:
                if not line.strip():
                    continue
                o = json.loads(line)
                ev = o["event"]
                p = o.get("payload", {})

                if ev == "fill":
                    totals["fills"] += 1
                    sh = int(p["filled_shares_micro"])
                    px = int(p["price_micro"])
                    fts = int(p["fill_ts_ms"])
                    out = p["outcome"]
                    cost = (sh * px) // MICRO_SCALE
                    if first_fill_ts is None:
                        first_fill_ts = fts
                        first_fill_out = out
                        first_fill_px = px
                    if out == "Up":
                        q_up += sh
                        cb_up += cost
                    else:
                        q_dn += sh
                        cb_dn += cost
                    w_fills.append({"ts": fts, "outcome": out, "px": px, "sh": sh})
                    unp = abs(q_up - q_dn)
                    if unp > max_unpaired:
                        max_unpaired = unp
                    if init_unpaired == 0 and unp > 0:
                        init_unpaired = unp
                    if unp > 0 and first_unp_ts is None:
                        first_unp_ts = fts
                    inv_events.append(
                        {
                            "ts": fts,
                            "q_up": q_up,
                            "q_dn": q_dn,
                            "cb_up": cb_up,
                            "cb_dn": cb_dn,
                            "cum_mg": cum_mg,
                        }
                    )

                elif ev == "merge":
                    pairs = int(p["merged_pairs_micro"])
                    gain = int(p["realized_gain_micro"])
                    if q_up > 0 and q_dn > 0:
                        cu = (pairs * cb_up) // q_up
                        cd = (pairs * cb_dn) // q_dn
                        q_up = max(0, q_up - pairs)
                        q_dn = max(0, q_dn - pairs)
                        cb_up = 0 if q_up == 0 else max(0, cb_up - cu)
                        cb_dn = 0 if q_dn == 0 else max(0, cb_dn - cd)
                    cum_mg += gain
                    if q_up == 0 and q_dn == 0:
                        first_unp_ts = None
                    mts = w_fills[-1]["ts"] if w_fills else 0
                    inv_events.append(
                        {
                            "ts": mts,
                            "q_up": q_up,
                            "q_dn": q_dn,
                            "cb_up": cb_up,
                            "cb_dn": cb_dn,
                            "cum_mg": cum_mg,
                        }
                    )

                elif ev == "flatten":
                    totals["flattens"] += 1

                elif ev == "phase_transition":
                    tp = p.get("to_phase")
                    if tp == "MandatoryFlatten":
                        mand_reached = True
                    elif tp == "PreExpiryLockout":
                        pre_reached = True

                elif ev == "order_action":
                    act = p.get("action")
                    tau = int(p.get("tau_secs", -1))
                    out = p.get("outcome")
                    if act != "Hold":
                        last_nonhold_tau = tau
                        if isinstance(act, dict):
                            if "SubmitNew" in act:
                                last_hedge_px[out] = int(act["SubmitNew"]["price"])
                            elif "CancelAndReplace" in act:
                                last_hedge_px[out] = int(
                                    act["CancelAndReplace"]["new_price"]
                                )
                            elif "Cancel" in act:
                                last_cancel_reason = act["Cancel"].get("reason")
                        if 30 < tau <= 60:
                            oa_counts["pre_nonhold"] += 1
                        elif 15 < tau <= 30:
                            oa_counts["mand_nonhold"] += 1
                    elif 15 < tau <= 30:
                        oa_counts["mand_hold"] += 1

                elif ev == "window_end":
                    slug = p["market_slug"]
                    start_ms = int(slug.split("-")[-1]) * 1000
                    expiry_ms = start_ms + 300_000
                    sett = int(p["settlement_pnl_delta_micro"])
                    unreal = int(p["unrealized_pnl_micro"])
                    net = int(p["net_pnl_micro"])
                    fees = int(p["fees_paid_micro"])

                    totals["merge_micro"] += cum_mg
                    totals["settle_micro"] += sett
                    totals["unreal_micro"] += unreal
                    totals["net_micro"] += net
                    totals["fees_micro"] += fees

                    res_dir = "Up" if q_up > q_dn else ("Down" if q_dn > q_up else None)
                    res_q = abs(q_up - q_dn)
                    res_cb = cb_up if res_dir == "Up" else (cb_dn if res_dir == "Down" else 0)
                    res_vwap = (res_cb * MICRO_SCALE // res_q) if res_q > 0 else 0

                    collapsed_inv: list[dict[str, Any]] = []
                    for ie in inv_events:
                        if collapsed_inv and collapsed_inv[-1]["ts"] == ie["ts"]:
                            collapsed_inv[-1] = ie
                        else:
                            collapsed_inv.append(ie)

                    windows.append(
                        {
                            "win_idx": len(windows),
                            "cap_idx": ci + 1,
                            "stem": stem,
                            "day": stem[:5],
                            "slug": slug,
                            "start_ms": start_ms,
                            "expiry_ms": expiry_ms,
                            "complete": bool(p["complete"]),
                            "winner": p.get("winning_outcome"),
                            "fills_count": int(p["fills_count"]),
                            "merges_count": int(p["merges_count"]),
                            "pairs_micro": int(p["total_pairs_merged_micro"]),
                            "mg_micro": cum_mg,
                            "sett_micro": sett,
                            "unreal_micro": unreal,
                            "net_micro": net,
                            "fees_micro": fees,
                            "res_dir": res_dir,
                            "res_q_micro": res_q,
                            "res_cb_micro": res_cb,
                            "res_vwap_micro": res_vwap,
                            "init_unpaired_micro": init_unpaired,
                            "max_unpaired_micro": max_unpaired,
                            "first_unp_ts": first_unp_ts,
                            "first_fill_ts": first_fill_ts,
                            "first_fill_out": first_fill_out,
                            "first_fill_px_micro": first_fill_px,
                            "last_hedge_px": dict(last_hedge_px),
                            "last_nonhold_tau": last_nonhold_tau,
                            "last_cancel_reason": last_cancel_reason,
                            "mand_reached": mand_reached,
                            "pre_reached": pre_reached,
                            "oa_counts": dict(oa_counts),
                            "inv_timeline": collapsed_inv,
                        }
                    )

                    q_up = q_dn = cb_up = cb_dn = 0
                    max_unpaired = init_unpaired = cum_mg = 0
                    first_unp_ts = first_fill_ts = first_fill_out = None
                    first_fill_px = 0
                    w_fills, inv_events = [], []
                    oa_counts = defaultdict(int)
                    last_hedge_px = {}
                    last_nonhold_tau = None
                    last_cancel_reason = None
                    mand_reached = pre_reached = False

    return windows, totals


def process_one_capture(
    task: tuple[str, str, list[dict[str, Any]]],
) -> tuple[dict[int, dict[str, Any]], dict[str, Any], dict[int, dict[str, dict[str, Any]]]]:
    """Worker scanning a single capture JSONL for its target windows."""
    stem, captures_dir, wlist = task
    cap_path = os.path.join(captures_dir, f"{stem}.jsonl")
    wlist_sorted = sorted(wlist, key=lambda x: x["start_ms"])
    min_s = wlist_sorted[0]["start_ms"]
    max_e = wlist_sorted[-1]["expiry_ms"]

    w_states: dict[int, dict[str, Any]] = {}
    for w in wlist_sorted:
        pol_state = {
            pol["id"]: {
                "halted": False,
                "triggered": False,
                "rem_q": 0,
                "rem_cb": 0,
                "heavy": None,
                "realized_mg": 0,
                "sell_pnl": 0,
                "fees": 0,
                "shares_flat": 0,
                "last_inv_idx": -1,
            }
            for pol in POLICIES
        }
        w_states[w["win_idx"]] = {
            "w": w,
            "books": {"Up": {"bids": {}, "asks": {}}, "Down": {"bids": {}, "asks": {}}},
            "last_opp_ask_tau": None,
            "last_opp_ask_px": None,
            "last_heavy_bid_tau": None,
            "last_heavy_bid_px": None,
            "mand_opp_ask_events": 0,
            "mand_heavy_bid_events": 0,
            "mand_total_events": 0,
            "at_entry_opp_ask": None,
            "at_entry_heavy_bid": None,
            "at_pre60_opp_ask": None,
            "at_pre60_heavy_bid": None,
            "at_mand30_opp_ask": None,
            "at_mand30_heavy_bid": None,
            "pol": pol_state,
            "w238_cp_done": set(),
            "last_eval_ts": 0,
        }

    win238_checkpoints: list[dict[str, Any]] = []
    w238_cps = [187.0, 185.0, 180.0, 175.0, 170.0, 165.0, 160.0, 150.0, 120.0, 90.0, 75.0, 65.0, 63.3, 60.0, 45.0, 30.0, 20.0, 15.0]
    w_idx_ptr = 0
    n_w = len(wlist_sorted)

    with open(cap_path, "r", encoding="utf-8") as f:
        for line in f:
            if '"binance_tick"' in line:
                continue
            is_pb = '"poly_book"' in line
            is_pc = not is_pb and '"poly_price_change"' in line
            if not is_pb and not is_pc:
                continue

            idx = line.find('"ts_unix_ms":')
            if idx == -1:
                continue
            ts = int(line[idx + 13 : idx + 26].split(",")[0].split("}")[0])
            if ts < min_s:
                continue
            if ts > max_e:
                break

            while w_idx_ptr < n_w and ts > wlist_sorted[w_idx_ptr]["expiry_ms"]:
                w_idx_ptr += 1
            if w_idx_ptr >= n_w:
                break
            cur_w = wlist_sorted[w_idx_ptr]
            if ts < cur_w["start_ms"]:
                continue

            # Skip poly_price_change lines more than 2s before first fill unless book_snapshot resets anyway
            first_fill = cur_w["first_fill_ts"]
            if is_pc and (first_fill is None or ts < first_fill - 2_000):
                continue

            o = json.loads(line)
            ev = o["event"]
            p = o["payload"]
            st = w_states[cur_w["win_idx"]]
            books = st["books"]

            if ev == "poly_book" and "BookSnapshot" in p:
                bs = p["BookSnapshot"]
                out = bs["outcome"]
                bk = bs["book"]
                books[out]["bids"] = {
                    int(x["price"]): int(x["size"])
                    for x in bk.get("bids", [])
                    if int(x["size"]) > 0
                }
                books[out]["asks"] = {
                    int(x["price"]): int(x["size"])
                    for x in bk.get("asks", [])
                    if int(x["size"]) > 0
                }
            elif ev == "poly_price_change" and "PriceChange" in p:
                pc = p["PriceChange"]
                out = pc["outcome"]
                side_map = "bids" if pc["side"] == "Buy" else "asks"
                px, sz = int(pc["price"]), int(pc["size"])
                if sz == 0:
                    books[out][side_map].pop(px, None)
                else:
                    books[out][side_map][px] = sz
            else:
                continue

            tau = (cur_w["expiry_ms"] - ts) / 1000.0
            # Settlement diagnostics
            if cur_w["sett_micro"] != 0 and cur_w["res_dir"] is not None:
                heavy = cur_w["res_dir"]
                opp = "Down" if heavy == "Up" else "Up"
                oa = books[opp]["asks"]
                hb = books[heavy]["bids"]
                best_oa = (min(oa), oa[min(oa)], sum(oa.values())) if oa else None
                best_hb = (max(hb), hb[max(hb)], sum(hb.values())) if hb else None
                if best_oa is not None:
                    st["last_opp_ask_tau"] = tau
                    st["last_opp_ask_px"] = best_oa[0]
                if best_hb is not None:
                    st["last_heavy_bid_tau"] = tau
                    st["last_heavy_bid_px"] = best_hb[0]
                if cur_w["first_unp_ts"] is not None and ts >= cur_w["first_unp_ts"] and st["at_entry_opp_ask"] is None:
                    st["at_entry_opp_ask"] = best_oa
                    st["at_entry_heavy_bid"] = best_hb
                if tau <= 60.0 and st["at_pre60_opp_ask"] is None:
                    st["at_pre60_opp_ask"] = best_oa
                    st["at_pre60_heavy_bid"] = best_hb
                if tau <= 30.0 and st["at_mand30_opp_ask"] is None:
                    st["at_mand30_opp_ask"] = best_oa
                    st["at_mand30_heavy_bid"] = best_hb
                if 15.0 < tau <= 30.0:
                    st["mand_total_events"] += 1
                    if best_oa is not None:
                        st["mand_opp_ask_events"] += 1
                    if best_hb is not None:
                        st["mand_heavy_bid_events"] += 1

            # Window #238 checkpoints
            if cur_w["win_idx"] == 238:
                for cp in w238_cps:
                    if cp not in st["w238_cp_done"] and tau <= cp:
                        st["w238_cp_done"].add(cp)
                        up_asks = books["Up"]["asks"]
                        dn_bids = books["Down"]["bids"]
                        _, f_u, not_u, fee_u = walk_l2_fak(up_asks, "BUY", 20 * MICRO_SCALE)
                        _, f_d, not_d, fee_d = walk_l2_fak(dn_bids, "SELL", 20 * MICRO_SCALE)
                        best_ua = (min(up_asks), up_asks[min(up_asks)]) if up_asks else None
                        best_db = (max(dn_bids), dn_bids[max(dn_bids)]) if dn_bids else None
                        win238_checkpoints.append(
                            {
                                "cp": cp,
                                "tau": tau,
                                "ts": ts,
                                "best_up_ask": best_ua,
                                "tot_up_ask_sh": sum(up_asks.values()) / 1e6,
                                "best_dn_bid": best_db,
                                "tot_dn_bid_sh": sum(dn_bids.values()) / 1e6,
                                "buy20_up_filled": f_u / 1e6,
                                "buy20_up_notional": not_u / 1e6,
                                "buy20_up_fee": fee_u / 1e6,
                                "buy20_up_net_pnl": (20.0 - 9.0 - not_u / 1e6 - fee_u / 1e6) if f_u == 20 * MICRO_SCALE else None,
                                "sell20_dn_filled": f_d / 1e6,
                                "sell20_dn_proceeds": not_d / 1e6,
                                "sell20_dn_fee": fee_d / 1e6,
                                "sell20_dn_net_pnl": (not_d / 1e6 - 9.0 - fee_d / 1e6) if f_d == 20 * MICRO_SCALE else None,
                            }
                        )

            # Evaluate policies at >= 50ms cadence (aligned with RiskActor ~10Hz cadence, or on BookSnapshot)
            if cur_w["max_unpaired_micro"] < MIN_ORDER_SHARES_MICRO:
                continue
            inv_tl = cur_w["inv_timeline"]
            if not inv_tl or ts < inv_tl[0]["ts"]:
                continue
            if not is_pb and (ts - st["last_eval_ts"]) < 50:
                continue
            st["last_eval_ts"] = ts

            best_cache: dict[str, int | None] = {}
            for pol in POLICIES:
                ps = st["pol"][pol["id"]]
                if not ps["halted"]:
                    idx_p = ps["last_inv_idx"]
                    while idx_p + 1 < len(inv_tl) and inv_tl[idx_p + 1]["ts"] <= ts:
                        idx_p += 1
                    ps["last_inv_idx"] = idx_p
                    if idx_p < 0:
                        continue
                    cur_inv = inv_tl[idx_p]
                    q_u, q_d = cur_inv["q_up"], cur_inv["q_dn"]
                    cb_u, cb_d = cur_inv["cb_up"], cur_inv["cb_dn"]
                    ps["realized_mg"] = cur_inv["cum_mg"]
                    if q_u > q_d:
                        ps["heavy"] = "Up"
                        ps["rem_q"] = q_u - q_d
                        ps["rem_cb"] = cb_u
                    elif q_d > q_u:
                        ps["heavy"] = "Down"
                        ps["rem_q"] = q_d - q_u
                        ps["rem_cb"] = cb_d
                    else:
                        ps["heavy"] = None
                        ps["rem_q"] = 0
                        ps["rem_cb"] = 0

                if ps["rem_q"] < MIN_ORDER_SHARES_MICRO or ps["heavy"] is None:
                    continue
                if tau <= pol["tau_end"]:
                    continue

                heavy = ps["heavy"]
                opp = "Down" if heavy == "Up" else "Up"
                oa = books[opp]["asks"]
                hb = books[heavy]["bids"]
                if opp not in best_cache:
                    best_cache[opp] = min(oa) if oa else None
                if heavy not in best_cache:
                    best_cache[heavy] = max(hb) if hb else None
                best_oa_px = best_cache[opp]
                best_hb_px = best_cache[heavy]

                should_fire = False
                buy_lim: int | None = None
                sell_lim: int | None = None
                kind = pol["kind"]

                if kind == "time_best1":
                    if tau <= pol["tau_start"]:
                        should_fire = True
                        buy_lim = best_oa_px
                        sell_lim = best_hb_px
                elif kind == "time_sweep":
                    if tau <= pol["tau_start"]:
                        should_fire = True
                elif kind == "grad_30s":
                    if tau <= 30.0:
                        should_fire = True
                        if tau > 25.0 and best_oa_px is not None:
                            buy_lim = min(990_000, best_oa_px + 20_000)
                            sell_lim = max(10_000, best_hb_px - 20_000) if best_hb_px else None
                        elif tau > 20.0 and best_oa_px is not None:
                            buy_lim = min(990_000, best_oa_px + 50_000)
                            sell_lim = max(10_000, best_hb_px - 50_000) if best_hb_px else None
                elif kind == "grad_60s":
                    if tau <= 60.0:
                        should_fire = True
                        if tau > 45.0 and best_oa_px is not None:
                            buy_lim = min(990_000, best_oa_px + 20_000)
                            sell_lim = max(10_000, best_hb_px - 20_000) if best_hb_px else None
                        elif tau > 30.0 and best_oa_px is not None:
                            buy_lim = min(990_000, best_oa_px + 50_000)
                            sell_lim = max(10_000, best_hb_px - 50_000) if best_hb_px else None
                elif kind == "stop_cross":
                    if tau <= pol["tau_start"]:
                        vwap_heavy = (ps["rem_cb"] * MICRO_SCALE) // ps["rem_q"]
                        if best_oa_px is not None and (vwap_heavy + best_oa_px) >= pol["comb_thresh"]:
                            should_fire = True
                        elif best_oa_px is None and best_hb_px is not None and (vwap_heavy + (MICRO_SCALE - best_hb_px)) >= pol["comb_thresh"]:
                            should_fire = True

                if not should_fire:
                    continue

                req = ps["rem_q"]
                _, f_b, not_b, fee_b = walk_l2_fak(oa, "BUY", req, buy_lim)
                _, f_s, not_s, fee_s = walk_l2_fak(hb, "SELL", req, sell_lim)

                if f_b == 0 and f_s == 0:
                    ps["triggered"] = True
                    continue

                ps["triggered"] = True
                ps["halted"] = True

                if kind == "time_best1":
                    use_buy = f_b > 0
                else:
                    if f_b > f_s:
                        use_buy = True
                    elif f_s > f_b:
                        use_buy = False
                    else:
                        use_buy = (f_b - not_b - fee_b) >= (not_s - fee_s)

                if use_buy and f_b > 0:
                    k = f_b
                    cb_ded = (k * ps["rem_cb"]) // ps["rem_q"]
                    ps["realized_mg"] += k - cb_ded - not_b
                    ps["fees"] += fee_b
                    ps["shares_flat"] += k
                    ps["rem_q"] -= k
                    ps["rem_cb"] = 0 if ps["rem_q"] == 0 else max(0, ps["rem_cb"] - cb_ded)
                elif f_s > 0:
                    k = f_s
                    cb_ded = (k * ps["rem_cb"]) // ps["rem_q"]
                    ps["sell_pnl"] += not_s - cb_ded
                    ps["fees"] += fee_s
                    ps["shares_flat"] += k
                    ps["rem_q"] -= k
                    ps["rem_cb"] = 0 if ps["rem_q"] == 0 else max(0, ps["rem_cb"] - cb_ded)

    diag_out: dict[int, dict[str, Any]] = {}
    pol_out: dict[int, dict[str, dict[str, Any]]] = {}
    for w in wlist_sorted:
        st = w_states[w["win_idx"]]
        if w["sett_micro"] != 0:
            diag_out[w["win_idx"]] = {
                "last_opp_ask_tau": st["last_opp_ask_tau"],
                "last_opp_ask_px": st["last_opp_ask_px"],
                "last_heavy_bid_tau": st["last_heavy_bid_tau"],
                "last_heavy_bid_px": st["last_heavy_bid_px"],
                "mand_opp_ask_events": st["mand_opp_ask_events"],
                "mand_heavy_bid_events": st["mand_heavy_bid_events"],
                "mand_total_events": st["mand_total_events"],
                "at_entry_opp_ask": st["at_entry_opp_ask"],
                "at_entry_heavy_bid": st["at_entry_heavy_bid"],
                "at_pre60_opp_ask": st["at_pre60_opp_ask"],
                "at_pre60_heavy_bid": st["at_pre60_heavy_bid"],
                "at_mand30_opp_ask": st["at_mand30_opp_ask"],
                "at_mand30_heavy_bid": st["at_mand30_heavy_bid"],
            }
        pol_out[w["win_idx"]] = st["pol"]

    return diag_out, {"checkpoints": win238_checkpoints}, pol_out


def run_selective_capture_audit(
    captures_dir: str,
    windows: list[dict[str, Any]],
) -> tuple[dict[int, dict[str, Any]], dict[str, Any], dict[str, dict[str, Any]]]:
    """Run capture scans across worker processes and aggregate counterfactuals."""
    target_windows = [
        w
        for w in windows
        if w["sett_micro"] != 0 or w["max_unpaired_micro"] >= MIN_ORDER_SHARES_MICRO
    ]
    by_stem: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for w in target_windows:
        by_stem[w["stem"]].append(w)

    tasks = [(stem, captures_dir, wlist) for stem, wlist in by_stem.items()]

    win_book_diag: dict[int, dict[str, Any]] = {}
    win238_detail: dict[str, Any] = {"checkpoints": []}
    all_pol_states: dict[int, dict[str, dict[str, Any]]] = {}

    with concurrent.futures.ProcessPoolExecutor(max_workers=8) as pool:
        for diag_part, w238_part, pol_part in pool.map(process_one_capture, tasks):
            win_book_diag.update(diag_part)
            if w238_part["checkpoints"]:
                win238_detail = w238_part
            all_pol_states.update(pol_part)

    policy_results: dict[str, dict[str, Any]] = {
        pol["id"]: {
            "triggered_windows": 0,
            "filled_windows": 0,
            "shares_flattened_micro": 0,
            "fees_micro": 0,
            "new_mg_micro": 0,
            "new_sett_micro": 0,
            "new_net_micro": 0,
            "worst_window_micro": 0,
            "details": [],
        }
        for pol in POLICIES
    }

    for w in windows:
        wid = w["win_idx"]
        for pol in POLICIES:
            pid = pol["id"]
            res = policy_results[pid]
            ps = all_pol_states.get(wid, {}).get(pid)
            if ps is None or not ps["triggered"] or ps["shares_flat"] == 0:
                if ps is not None and ps["triggered"]:
                    res["triggered_windows"] += 1
                mg = w["mg_micro"]
                sett = w["sett_micro"]
                net = w["net_micro"]
            else:
                res["triggered_windows"] += 1
                if ps["shares_flat"] > 0:
                    res["filled_windows"] += 1
                res["shares_flattened_micro"] += ps["shares_flat"]
                res["fees_micro"] += ps["fees"]
                rem_q = ps["rem_q"]
                rem_cb = ps["rem_cb"]
                if rem_q > 0 and w["winner"] is not None:
                    sett = (rem_q - rem_cb) if w["winner"] == ps["heavy"] else -rem_cb
                else:
                    sett = 0
                mg = ps["realized_mg"]
                unreal = 0 if ps["halted"] else w["unreal_micro"]
                net = mg + ps["sell_pnl"] - ps["fees"] + sett + unreal
                if ps["shares_flat"] > 0:
                    res["details"].append(
                        {
                            "win_idx": wid,
                            "slug": w["slug"],
                            "base_net": w["net_micro"] / 1e6,
                            "new_net": net / 1e6,
                            "shares_flat": ps["shares_flat"] / 1e6,
                            "fee": ps["fees"] / 1e6,
                        }
                    )

            res["new_mg_micro"] += mg
            res["new_sett_micro"] += sett
            res["new_net_micro"] += net
            if net < res["worst_window_micro"]:
                res["worst_window_micro"] = net

    return win_book_diag, win238_detail, policy_results


def print_40_windows_report(
    windows: list[dict[str, Any]],
    win_book_diag: dict[int, dict[str, Any]],
) -> None:
    """Print complete 40-window inventory, diagnostics, and Section 8 breakdowns."""
    sett_ws = [w for w in windows if w["sett_micro"] != 0]
    dust = [w for w in sett_ws if w["res_q_micro"] < MIN_ORDER_SHARES_MICRO]
    elig = [w for w in sett_ws if w["res_q_micro"] >= MIN_ORDER_SHARES_MICRO]

    print("\n" + "=" * 100)
    print("2. ALL 40 SETTLEMENT-EXPOSED WINDOWS (SORTED CHRONOLOGICALLY)")
    print("=" * 100)
    print(
        f"Count={len(sett_ws)} | Dust(<5sh)={len(dust)} (${sum(w['sett_micro'] for w in dust)/1e6:+.4f}) "
        f"| Eligible(>=5sh)={len(elig)} (${sum(w['sett_micro'] for w in elig)/1e6:+.4f})"
    )

    for w in sett_ws:
        bd = win_book_diag.get(w["win_idx"], {})
        is_dust = w["res_q_micro"] < MIN_ORDER_SHARES_MICRO
        last_oa = bd.get("last_opp_ask_tau")
        last_oa_str = f"{last_oa:.1f}s" if last_oa is not None else "None"
        mand_oa = bd.get("mand_opp_ask_events", 0)
        e_oa = bd.get("at_entry_opp_ask")
        e_oa_str = f"${e_oa[0]/1e6:.2f}({e_oa[2]/1e6:.0f})" if e_oa else "None"
        first_tau = (
            (w["expiry_ms"] - w["first_unp_ts"]) / 1000.0
            if w["first_unp_ts"] is not None
            else -1.0
        )
        cause = "H4:Dust(<5sh)" if is_dust else "H2:EmptyBook@30s+H5:CrossTickCap"
        last_h = w["last_hedge_px"]
        opp_side = "Down" if w["res_dir"] == "Up" else "Up"
        lh_str = f"${last_h[opp_side]/1e6:.2f}" if opp_side in last_h else "None"
        print(
            f"  win#{w['win_idx']:3d} Cap#{w['cap_idx']:2d} {w['slug']} "
            f"{w['res_dir']:4s}/{w['winner']:4s} init={w['init_unpaired_micro']/1e6:5.2f} "
            f"max={w['max_unpaired_micro']/1e6:5.2f} res={w['res_q_micro']/1e6:5.2f} "
            f"vwap=${w['res_vwap_micro']/1e6:.2f} cb=${w['res_cb_micro']/1e6:5.2f} "
            f"1st_tau={first_tau:5.1f}s lh={lh_str:5s} entry_oa={e_oa_str:13s} "
            f"last_oa={last_oa_str:6s} mand_oa={mand_oa:4d} "
            f"sett=${w['sett_micro']/1e6:+7.4f} [{cause}]"
        )

    print("\n--- WORST 10 WINDOWS BY SETTLEMENT LOSS ---")
    for rank, w in enumerate(sorted(sett_ws, key=lambda x: x["sett_micro"])[:10], 1):
        bd = win_book_diag.get(w["win_idx"], {})
        last_oa = bd.get("last_opp_ask_tau")
        print(
            f"  #{rank:2d} win#{w['win_idx']:3d} Cap#{w['cap_idx']:2d} {w['slug']} "
            f"{w['res_dir']:4s} (win={w['winner']:4s}) q={w['res_q_micro']/1e6:5.2f} "
            f"vwap=${w['res_vwap_micro']/1e6:.2f} sett=${w['sett_micro']/1e6:+7.4f} "
            f"last_oa_tau={last_oa if last_oa is not None else -1:.2f}s"
        )

    print("\n" + "=" * 100)
    print("3. SECTION 8 BREAKDOWN: REASSESSING THE 85% SETTLEMENT LOSS RATE")
    print("=" * 100)
    s_wins = [w for w in sett_ws if w["sett_micro"] > 0]
    s_loss = [w for w in sett_ws if w["sett_micro"] < 0]
    print(
        f"Win severity:  n={len(s_wins):2d}, mean=${statistics.mean(w['sett_micro']/1e6 for w in s_wins):+.4f}, "
        f"median=${statistics.median(w['sett_micro']/1e6 for w in s_wins):+.4f}, max=${max(w['sett_micro']/1e6 for w in s_wins):+.4f}"
    )
    print(
        f"Loss severity: n={len(s_loss):2d}, mean=${statistics.mean(w['sett_micro']/1e6 for w in s_loss):+.4f}, "
        f"median=${statistics.median(w['sett_micro']/1e6 for w in s_loss):+.4f}, min=${min(w['sett_micro']/1e6 for w in s_loss):+.4f}"
    )

    def bucket_stats(title: str, group_fn: Any, subset: list[dict[str, Any]]) -> None:
        print(f"\n  [{title}]")
        groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for w in subset:
            groups[group_fn(w)].append(w)
        for k in sorted(groups):
            gs = groups[k]
            nw = sum(1 for x in gs if x["sett_micro"] > 0)
            nl = sum(1 for x in gs if x["sett_micro"] < 0)
            ss = sum(x["sett_micro"] for x in gs) / 1e6
            sm = sum(x["mg_micro"] for x in gs) / 1e6
            sn = sum(x["net_micro"] for x in gs) / 1e6
            print(
                f"    {k:22s}: n={len(gs):2d} (W={nw:2d}/L={nl:2d}, L%={nl/len(gs)*100:5.1f}%) "
                f"sett=${ss:+8.4f} merge=${sm:+8.4f} net=${sn:+8.4f}"
            )

    bucket_stats("By Residual Direction", lambda w: w["res_dir"], sett_ws)
    bucket_stats("By Dust vs Eligible", lambda w: "Dust (<5 sh)" if w["res_q_micro"] < MIN_ORDER_SHARES_MICRO else "Eligible (>=5 sh)", sett_ws)
    bucket_stats(
        "By Residual Entry VWAP",
        lambda w: "<$0.35" if w["res_vwap_micro"] < 350_000 else ("$0.35-$0.50" if w["res_vwap_micro"] <= 500_000 else ">$0.50"),
        sett_ws,
    )
    bucket_stats(
        "By First Unpaired Tau",
        lambda w: "Early (>240s)" if (w["expiry_ms"] - w["first_unp_ts"]) / 1000 > 240 else ("Mid (180-240s)" if (w["expiry_ms"] - w["first_unp_ts"]) / 1000 >= 180 else "Late (<180s)"),
        sett_ws,
    )
    bucket_stats(
        "By Max Unpaired Qty",
        lambda w: "<5 sh" if w["max_unpaired_micro"] < 5_000_000 else ("5-19.99 sh" if w["max_unpaired_micro"] < 20_000_000 else ">=20 sh"),
        sett_ws,
    )

    fill_ws = [w for w in windows if w["fills_count"] > 0]
    print(f"\n  [ALL {len(fill_ws)} FILL-ACTIVE WINDOWS BY LEG-1 ENTRY PRICE]")
    leg1_groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for w in fill_ws:
        px = w["first_fill_px_micro"] / 1e6
        b = "<$0.30" if px < 0.30 else ("$0.30-$0.35" if px < 0.35 else ("$0.35-$0.50" if px <= 0.50 else ">$0.50"))
        leg1_groups[b].append(w)
    for k in ["<$0.30", "$0.30-$0.35", "$0.35-$0.50", ">$0.50"]:
        gs = leg1_groups.get(k, [])
        if not gs:
            continue
        sm = sum(x["mg_micro"] for x in gs) / 1e6
        ss = sum(x["sett_micro"] for x in gs) / 1e6
        sn = sum(x["net_micro"] for x in gs) / 1e6
        n_nz = sum(1 for x in gs if x["sett_micro"] != 0)
        n_sl = sum(1 for x in gs if x["sett_micro"] < 0)
        print(
            f"    {k:12s}: w={len(gs):2d} | sett_nz={n_nz:2d} (loss={n_sl:2d}) "
            f"| merge=${sm:+8.4f} | sett=${ss:+8.4f} | net=${sn:+8.4f}"
        )


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Forensic & counterfactual audit of MandatoryFlatten and settlement losses."
    )
    parser.add_argument("--sweep-dir", default=DEFAULT_SWEEP_DIR)
    parser.add_argument("--captures-dir", default=DEFAULT_CAPTURES_DIR)
    args = parser.parse_args()

    windows, totals = load_replay_windows(args.sweep_dir)
    print("=" * 100)
    print("1. BASELINE REPLAY VERIFICATION (21 CAPTURES, 241 WINDOWS)")
    print("=" * 100)
    print(
        f"Fills={totals['fills']}, Flattens={totals['flattens']}, "
        f"Merge=${totals['merge_micro']/1e6:+.4f}, Settle=${totals['settle_micro']/1e6:+.4f}, "
        f"Unreal=${totals['unreal_micro']/1e6:+.4f}, Net=${totals['net_micro']/1e6:+.4f}"
    )

    ever_elig = [w for w in windows if w["max_unpaired_micro"] >= MIN_ORDER_SHARES_MICRO]
    print(f"Windows that ever held >=5.0 unpaired shares: {len(ever_elig)} / {len(windows)}")

    win_book_diag, win238_detail, policy_results = run_selective_capture_audit(
        args.captures_dir, windows
    )

    print_40_windows_report(windows, win_book_diag)

    print("\n" + "=" * 100)
    print("4. WINDOW #238 (btc-updown-5m-1795657800) CHECKPOINT BOOK & FAK COST")
    print("=" * 100)
    for cp in win238_detail["checkpoints"]:
        ua_s = f"${cp['best_up_ask'][0]/1e6:.2f}x{cp['best_up_ask'][1]/1e6:.0f}" if cp["best_up_ask"] else "None"
        db_s = f"${cp['best_dn_bid'][0]/1e6:.2f}x{cp['best_dn_bid'][1]/1e6:.0f}" if cp["best_dn_bid"] else "None"
        bn_s = f"${cp['buy20_up_net_pnl']:+.4f} (fee=${cp['buy20_up_fee']:.4f})" if cp["buy20_up_net_pnl"] is not None else "UNFILLED"
        sn_s = f"${cp['sell20_dn_net_pnl']:+.4f} (fee=${cp['sell20_dn_fee']:.4f})" if cp["sell20_dn_net_pnl"] is not None else "UNFILLED"
        print(
            f"  tau={cp['tau']:6.2f}s | UP_ask={ua_s:11s} (tot={cp['tot_up_ask_sh']:6.0f}) "
            f"| DN_bid={db_s:11s} (tot={cp['tot_dn_bid_sh']:6.0f}) "
            f"| BUY20_UP net={bn_s:24s} | SELL20_DN net={sn_s}"
        )

    print("\n" + "=" * 100)
    print("5. CAUSAL OFFLINE COUNTERFACTUALS ACROSS ALL 241 WINDOWS")
    print("=" * 100)
    base_mg = totals["merge_micro"] / 1e6
    base_sett = totals["settle_micro"] / 1e6
    base_net = totals["net_micro"] / 1e6
    print(f"BASELINE: Merge=${base_mg:+.4f} | Settle=${base_sett:+.4f} | Fees=$0.0000 | Net=${base_net:+.4f}")
    for pid, res in policy_results.items():
        mg = res["new_mg_micro"] / 1e6
        sett = res["new_sett_micro"] / 1e6
        fees = res["fees_micro"] / 1e6
        net = res["new_net_micro"] / 1e6
        d_net = net - base_net
        d_mg = mg - base_mg
        d_sett = sett - base_sett
        worst = res["worst_window_micro"] / 1e6
        print(
            f"  {pid:16s}: trig={res['triggered_windows']:2d}, filled={res['filled_windows']:2d}, "
            f"flat_sh={res['shares_flattened_micro']/1e6:6.1f}, fees=${fees:5.3f} | "
            f"Merge=${mg:+7.4f} ({d_mg:+7.4f}) | Settle=${sett:+8.4f} ({d_sett:+7.4f}) | "
            f"Net=${net:+7.4f} (delta={d_net:+7.4f}) | WorstW=${worst:+.4f}"
        )
        for d in res["details"][:10]:
            print(
                f"      -> win#{d['win_idx']:3d} {d['slug']}: flat={d['shares_flat']:.2f}sh "
                f"fee=${d['fee']:.4f} base_net=${d['base_net']:+.4f} -> new_net=${d['new_net']:+.4f} "
                f"(delta=${d['new_net']-d['base_net']:+.4f})"
            )
        if len(res["details"]) > 10:
            print(f"      ... ({len(res['details']) - 10} more filled windows)")

    print("\n--- PRE-FLATTEN BENCHMARK: ENTRY-PRICE FLOOR (NO ENTRY BELOW THRESHOLD) ---")
    for floor in [0.25, 0.30, 0.35]:
        drop_ws = [w for w in windows if w["fills_count"] > 0 and (w["first_fill_px_micro"] / 1e6) < floor]
        lost_mg = sum(w["mg_micro"] for w in drop_ws) / 1e6
        saved_sett = -sum(w["sett_micro"] for w in drop_ws) / 1e6
        net_delta = -sum(w["net_micro"] for w in drop_ws) / 1e6
        new_net = base_net + net_delta
        print(
            f"  Floor < ${floor:.2f}: dropped_w={len(drop_ws):2d} | lost_Merge=${-lost_mg:+7.4f} "
            f"| saved_Settle=${saved_sett:+7.4f} | new_Net=${new_net:+7.4f} (delta=${net_delta:+7.4f})"
        )

    return 0


if __name__ == "__main__":
    sys.exit(main())
