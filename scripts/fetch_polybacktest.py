#!/usr/bin/env python3
"""
PolyBackTest API v4 Downloader & Replay Converter
Fetches historical Polymarket crypto up/down markets and L2 snapshots,
converting them into canonical polymarket-as-rust capture JSONL format.
"""

import argparse
import json
import os
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional
import requests
from dotenv import dotenv_values

BASE_URL = "https://api.polybacktest.com/v4"
DEFAULT_DELAY_SEC = 1.15  # Free tier allows max 1 req/sec; 1.15s gives safe margin

def get_api_key(cli_key: Optional[str]) -> str:
    if cli_key:
        return cli_key
    env = dotenv_values(".env")
    if "POLYBACKTEST_API_KEY" in env and env["POLYBACKTEST_API_KEY"]:
        return env["POLYBACKTEST_API_KEY"].strip()
    env_var = os.environ.get("POLYBACKTEST_API_KEY")
    if env_var:
        return env_var.strip()
    raise ValueError(
        "No API key provided. Set POLYBACKTEST_API_KEY in .env or pass --api-key"
    )

class PolyBackTestClient:
    def __init__(self, api_key: str, min_delay: float = DEFAULT_DELAY_SEC):
        self.api_key = api_key
        self.min_delay = min_delay
        self.last_req_time = 0.0
        self.session = requests.Session()
        self.session.headers.update({"X-API-Key": self.api_key})

    def _throttle(self):
        elapsed = time.time() - self.last_req_time
        if elapsed < self.min_delay:
            time.sleep(self.min_delay - elapsed)

    def _get(self, endpoint: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        url = f"{BASE_URL}/{endpoint.lstrip('/')}"
        max_retries = 5
        for attempt in range(max_retries):
            self._throttle()
            self.last_req_time = time.time()
            try:
                r = self.session.get(url, params=params, timeout=30)
                if r.status_code == 200:
                    return r.json()
                elif r.status_code == 429:
                    retry_after = 2.0
                    try:
                        data = r.json()
                        retry_after = float(data.get("details", {}).get("retry_after", 2.0)) + 0.5
                    except Exception:
                        pass
                    print(f"[429 Rate Limit] Sleeping {retry_after:.1f}s before retry...", file=sys.stderr)
                    time.sleep(retry_after)
                    continue
                elif r.status_code in (502, 503, 504):
                    wait = 2.0 * (attempt + 1)
                    print(f"[{r.status_code} Upstream error] Retrying in {wait}s...", file=sys.stderr)
                    time.sleep(wait)
                    continue
                else:
                    r.raise_for_status()
            except requests.RequestException as e:
                if attempt == max_retries - 1:
                    raise
                print(f"[Network error] {e}. Retrying...", file=sys.stderr)
                time.sleep(2.0 * (attempt + 1))
        raise RuntimeError(f"Failed to fetch {url} after {max_retries} retries")

    def get_plan_limits(self, coin: str = "btc") -> Dict[str, Any]:
        return self._get("billing/plan-limits", params={"coin": coin})

    def list_markets(
        self,
        coin: str = "btc",
        market_type: str = "5m",
        resolved: Optional[bool] = True,
        start_time_ms: Optional[int] = None,
        end_time_ms: Optional[int] = None,
        limit: int = 50,
        offset: int = 0,
    ) -> Dict[str, Any]:
        params: Dict[str, Any] = {
            "coin": coin,
            "market_type": market_type,
            "limit": limit,
            "offset": offset,
            "include_total": "true",
        }
        if resolved is not None:
            params["resolved"] = "true" if resolved else "false"
        if start_time_ms is not None:
            params["start_time"] = start_time_ms
        if end_time_ms is not None:
            params["end_time"] = end_time_ms
        return self._get("markets", params=params)

    def get_market_by_slug(self, slug: str, coin: str = "btc") -> Dict[str, Any]:
        return self._get(f"markets/by-slug/{slug}", params={"coin": coin})

    def get_all_snapshots_for_market(self, market_id: str, coin: str = "btc") -> List[Dict[str, Any]]:
        all_snapshots: List[Dict[str, Any]] = []
        offset = 0
        limit = 1000
        while True:
            params = {
                "coin": coin,
                "limit": limit,
                "offset": offset,
                "include_orderbook": "true",
                "include_total": "false",
            }
            res = self._get(f"markets/{market_id}/snapshots", params=params)
            snaps = res.get("snapshots", [])
            all_snapshots.extend(snaps)
            if not res.get("has_more") or not snaps:
                break
            offset = res.get("next_offset", offset + len(snaps))
        return all_snapshots

def infer_synthetic_trades(
    prev_snap: Dict[str, Any],
    curr_snap: Dict[str, Any],
    fill_mode: str = "strict-cross",
    participation: float = 0.7,
) -> List[tuple[str, str, float, float]]:
    """Infers taker trades between consecutive orderbook snapshots.
    Returns list of (outcome, side, price_usd, size_shares).
    """
    if fill_mode == "none":
        return []

    trades: List[tuple[str, str, float, float]] = []

    for outcome, ob_key in [("Up", "orderbook_up"), ("Down", "orderbook_down")]:
        ob_prev = prev_snap.get(ob_key) or {}
        ob_curr = curr_snap.get(ob_key) or {}
        bids_prev = {round(b["price"], 4): b["size"] for b in ob_prev.get("bids", []) if b.get("price") and b.get("size")}
        asks_prev = {round(a["price"], 4): a["size"] for a in ob_prev.get("asks", []) if a.get("price") and a.get("size")}
        bids_curr = {round(b["price"], 4): b["size"] for b in ob_curr.get("bids", []) if b.get("price") and b.get("size")}
        asks_curr = {round(a["price"], 4): a["size"] for a in ob_curr.get("asks", []) if a.get("price") and a.get("size")}

        best_bid_prev = max(bids_prev.keys()) if bids_prev else None
        best_ask_prev = min(asks_prev.keys()) if asks_prev else None
        best_bid_curr = max(bids_curr.keys()) if bids_curr else None
        best_ask_curr = min(asks_curr.keys()) if asks_curr else None

        # Taker Sell trades (hitting resting bids)
        if best_bid_prev is not None:
            for p, v_prev in bids_prev.items():
                if best_ask_curr is not None and best_ask_curr <= best_bid_prev and p >= best_ask_curr:
                    trades.append((outcome, "Sell", p, v_prev))
                elif fill_mode == "depletion":
                    if best_bid_curr is not None and p > best_bid_curr:
                        trades.append((outcome, "Sell", p, v_prev))
                    elif p == best_bid_prev and best_bid_curr == best_bid_prev:
                        v_curr = bids_curr.get(p, 0.0)
                        if v_curr < v_prev:
                            diff = (v_prev - v_curr) * participation
                            if diff >= 1.0:
                                trades.append((outcome, "Sell", p, diff))

        # Taker Buy trades (hitting resting asks)
        if best_ask_prev is not None:
            for p, v_prev in asks_prev.items():
                if best_bid_curr is not None and best_bid_curr >= best_ask_prev and p <= best_bid_curr:
                    trades.append((outcome, "Buy", p, v_prev))
                elif fill_mode == "depletion":
                    if best_ask_curr is not None and p < best_ask_curr:
                        trades.append((outcome, "Buy", p, v_prev))
                    elif p == best_ask_prev and best_ask_curr == best_ask_prev:
                        v_curr = asks_curr.get(p, 0.0)
                        if v_curr < v_prev:
                            diff = (v_prev - v_curr) * participation
                            if diff >= 1.0:
                                trades.append((outcome, "Buy", p, diff))

    return trades

def convert_market_to_capture(
    market: Dict[str, Any],
    snapshots: List[Dict[str, Any]],
    start_mono_ns: int = 1_000_000_000,
    fill_mode: str = "none",
    participation: float = 0.7,
) -> tuple[List[Dict[str, Any]], int]:
    """Converts a market and its snapshots into canonical JSONL capture envelopes."""
    lines: List[Dict[str, Any]] = []
    start_ts = market["start_time"]
    expiry_ts = market["end_time"]
    strike_price = market.get("btc_price_start")

    # 1. market_discovered event
    desc_payload = {
        "condition_id": market.get("condition_id") or f"0x{market['market_id']}",
        "slug": market.get("slug", f"btc-updown-{market.get('market_type')}-{start_ts}"),
        "question": f"Bitcoin Up or Down {market.get('slug', '')}",
        "version": "CtfV1",
        "neg_risk": False,
        "asset_id_up": market.get("clob_token_up", ""),
        "asset_id_dn": market.get("clob_token_down", ""),
        "tick_size_micro": 10000,
        "min_order_size_micro": 5000000,
        "taker_delay_ms": 50,
        "fee_schedule": {
            "fee_rate_e6": 70000,
            "maker_fee_zero": True,
        },
        "start_ts_ms": start_ts,
        "expiry_ts_ms": expiry_ts,
        "strike_price_usd": strike_price,
    }

    base_mono_ns = start_mono_ns
    lines.append({
        "ts_unix_ms": start_ts,
        "mono_ns": base_mono_ns,
        "event": "market_discovered",
        "payload": desc_payload,
    })

    # Sort snapshots ascending by time
    sorted_snaps = sorted(snapshots, key=lambda s: s["time"])
    current_mono_ns = base_mono_ns + 10_000_000

    for idx, snap in enumerate(sorted_snaps):
        t_ms = snap["time"]
        # Ensure monotonic progression relative to base_mono_ns
        expected_mono = base_mono_ns + max(0, (t_ms - start_ts) * 1_000_000)
        current_mono_ns = max(current_mono_ns + 10_000, expected_mono)

        # 1.5 Infer synthetic taker trades between previous snapshot and current snapshot
        if fill_mode != "none" and idx > 0:
            synth_trades = infer_synthetic_trades(
                sorted_snaps[idx - 1],
                snap,
                fill_mode=fill_mode,
                participation=participation,
            )
            for outcome, side, px, sz in synth_trades:
                current_mono_ns += 2_000
                lines.append({
                    "ts_unix_ms": t_ms,
                    "mono_ns": current_mono_ns,
                    "event": "poly_trade",
                    "payload": {
                        "LastTrade": {
                            "outcome": outcome,
                            "side": side,
                            "price": int(round(px * 1_000_000)),
                            "size": int(round(sz * 1_000_000)),
                            "rx_mono_ns": current_mono_ns,
                            "exch_ts_ms": t_ms,
                        }
                    },
                })

        # 2. Underlying spot tick (Chainlink price represented as BinanceSpotEvent::AggTrade)
        spot_px = snap.get("btc_price")
        if spot_px and spot_px > 0:
            lines.append({
                "ts_unix_ms": t_ms,
                "mono_ns": current_mono_ns,
                "event": "binance_tick",
                "payload": {
                    "AggTrade": {
                        "price_e8": int(round(spot_px * 100_000_000)),
                        "qty_e8": 100_000_000,
                        "is_buyer_maker": False,
                        "exch_ts_ms": t_ms,
                        "rx_mono_ns": current_mono_ns,
                    }
                },
            })
            current_mono_ns += 5_000

        # 3. DOWN order book snapshot
        ob_down = snap.get("orderbook_down")
        if ob_down and isinstance(ob_down, dict):
            bids = [
                {"price": int(round(b["price"] * 1_000_000)), "size": int(round(b["size"] * 1_000_000))}
                for b in ob_down.get("bids", [])
                if b.get("price") is not None and b.get("size") is not None
            ]
            asks = [
                {"price": int(round(a["price"] * 1_000_000)), "size": int(round(a["size"] * 1_000_000))}
                for a in ob_down.get("asks", [])
                if a.get("price") is not None and a.get("size") is not None
            ]
            lines.append({
                "ts_unix_ms": t_ms,
                "mono_ns": current_mono_ns,
                "event": "poly_book",
                "payload": {
                    "BookSnapshot": {
                        "outcome": "Down",
                        "book": {
                            "outcome": "Down",
                            "bids": bids,
                            "asks": asks,
                            "last_update_ns": current_mono_ns,
                            "exchange_ts_ms": t_ms,
                        },
                        "rx_mono_ns": current_mono_ns,
                        "exch_ts_ms": t_ms,
                    }
                },
            })
            current_mono_ns += 5_000

        # 4. UP order book snapshot
        ob_up = snap.get("orderbook_up")
        if ob_up and isinstance(ob_up, dict):
            bids = [
                {"price": int(round(b["price"] * 1_000_000)), "size": int(round(b["size"] * 1_000_000))}
                for b in ob_up.get("bids", [])
                if b.get("price") is not None and b.get("size") is not None
            ]
            asks = [
                {"price": int(round(a["price"] * 1_000_000)), "size": int(round(a["size"] * 1_000_000))}
                for a in ob_up.get("asks", [])
                if a.get("price") is not None and a.get("size") is not None
            ]
            lines.append({
                "ts_unix_ms": t_ms,
                "mono_ns": current_mono_ns,
                "event": "poly_book",
                "payload": {
                    "BookSnapshot": {
                        "outcome": "Up",
                        "book": {
                            "outcome": "Up",
                            "bids": bids,
                            "asks": asks,
                            "last_update_ns": current_mono_ns,
                            "exchange_ts_ms": t_ms,
                        },
                        "rx_mono_ns": current_mono_ns,
                        "exch_ts_ms": t_ms,
                    }
                },
            })
            current_mono_ns += 5_000

    return lines, current_mono_ns

def main():
    parser = argparse.ArgumentParser(description="PolyBackTest API v4 Downloader & Converter")
    parser.add_argument("--api-key", help="PolyBackTest API key (pdm_...)")
    parser.add_argument("--coin", default="btc", choices=["btc", "eth", "sol"], help="Coin (default: btc)")
    parser.add_argument("--market-type", default="5m", choices=["5m", "15m", "1h", "4h", "24h"], help="Market timeframe (default: 5m)")
    parser.add_argument("--slugs", nargs="+", help="Specific market slugs to fetch (e.g. btc-updown-5m-1795361700)")
    parser.add_argument("--limit-markets", type=int, default=None, help="Max markets to download/convert (default: 10 online, all cached offline)")
    parser.add_argument("--days", type=float, default=1.0, help="Window in days backwards from now (default: 1.0)")
    parser.add_argument(
        "--fill-mode",
        default="none",
        choices=["none", "strict-cross", "depletion"],
        help="Synthetic trade inference mode (default: none)",
    )
    parser.add_argument("--trade-participation", type=float, default=0.7, help="Ratio of book depletion attributed to trades in depletion mode (default: 0.7)")
    parser.add_argument("--offline-convert", action="store_true", help="Reconvert cached JSON in data/polybacktest/ without hitting the API")
    parser.add_argument("--data-dir", default="data/polybacktest", help="Directory to cache raw API JSON")
    parser.add_argument("--out-dir", default="logs/captures/polybacktest", help="Directory to output replay JSONL")
    parser.add_argument("--combined-name", help="Optional name for a single combined replay file across all markets")
    args = parser.parse_args()

    raw_cache_dir = Path(args.data_dir) / args.market_type
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    markets_to_convert: List[tuple[Dict[str, Any], Optional[List[Dict[str, Any]]]]] = []

    if args.offline_convert:
        if not raw_cache_dir.exists():
            print(f"[!] Cache directory does not exist: {raw_cache_dir}", file=sys.stderr)
            return
        cache_files = sorted(raw_cache_dir.glob("*.json"))
        slug_filter = {s.strip() for s in args.slugs} if args.slugs else None
        found_slugs = set()

        for cf in cache_files:
            with open(cf, "r", encoding="utf-8") as f:
                data = json.load(f)
            market = data.get("market")
            if not market:
                continue
            slug = market.get("slug", "")
            if slug_filter is not None:
                if slug not in slug_filter:
                    continue
                found_slugs.add(slug)
            markets_to_convert.append((market, data.get("snapshots", [])))

        if slug_filter is not None:
            missing = sorted(slug_filter - found_slugs)
            if missing:
                print(f"[!] Warning: {len(missing)} requested slug(s) not found in cache: {', '.join(missing)}", file=sys.stderr)

        markets_to_convert.sort(key=lambda item: item[0]["start_time"])
        if args.limit_markets is not None:
            markets_to_convert = markets_to_convert[: args.limit_markets]

        print(f"[*] Offline-converting {len(markets_to_convert)} cached {args.market_type} market(s) (fill_mode={args.fill_mode})...")
        if not markets_to_convert:
            print("[!] No cached markets matched in cache directory.")
            return
    else:
        api_key = get_api_key(args.api_key)
        client = PolyBackTestClient(api_key=api_key)
        limit_markets = args.limit_markets if args.limit_markets is not None else 10

        # 1. Verify plan limits
        print(f"[*] Checking plan limits for coin={args.coin}...")
        limits = client.get_plan_limits(coin=args.coin)
        plan_id = limits.get("plan_id", "unknown")
        max_days = limits.get("max_history_days", 31)
        rate_limits = limits.get("rate_limits", {})
        print(f"    Plan: {plan_id} | Max history: {max_days} days | Rate: {rate_limits.get('max_per_second', 1)} req/s")

        # 2. Determine time range
        now_ms = int(time.time() * 1000)
        start_ms = now_ms - int(args.days * 86400 * 1000)
        print(f"[*] Querying markets from last {args.days} day(s) (start_time >= {start_ms})...")

        raw_cache_dir.mkdir(parents=True, exist_ok=True)

        # 3. Page list of markets or fetch specific slugs
        markets_to_fetch: List[Dict[str, Any]] = []
        if args.slugs:
            print(f"[*] Fetching {len(args.slugs)} specific market(s) by slug...")
            for s in args.slugs:
                try:
                    res_m = client.get_market_by_slug(s.strip(), coin=args.coin)
                    markets_to_fetch.append(res_m)
                except Exception as e:
                    print(f"[!] Error fetching market slug {s}: {e}", file=sys.stderr)
        else:
            offset = 0
            batch_size = 50
            while len(markets_to_fetch) < limit_markets:
                res = client.list_markets(
                    coin=args.coin,
                    market_type=args.market_type,
                    resolved=True,
                    start_time_ms=start_ms,
                    limit=batch_size,
                    offset=offset,
                )
                batch = res.get("markets", [])
                if not batch:
                    break
                for m in batch:
                    markets_to_fetch.append(m)
                    if len(markets_to_fetch) >= limit_markets:
                        break
                if not res.get("has_more"):
                    break
                offset = res.get("next_offset", offset + len(batch))

        print(f"[*] Found {len(markets_to_fetch)} resolved {args.market_type} market(s).")
        if not markets_to_fetch:
            print("[!] No resolved markets returned in the given time window.")
            return

        # Sort markets oldest first so replay sequence is chronological
        markets_to_fetch.sort(key=lambda m: m["start_time"])
        markets_to_convert = [(m, None) for m in markets_to_fetch]

    combined_lines: List[Dict[str, Any]] = []
    running_mono_ns = 1_000_000_000

    # 4. Fetch snapshots for each market (or load from cache)
    total_markets = len(markets_to_convert)
    for idx, (market, cached_snapshots) in enumerate(markets_to_convert, 1):
        market_id = market["market_id"]
        slug = market.get("slug", f"market_{market_id}")
        cache_file = raw_cache_dir / f"{market_id}.json"

        if cached_snapshots is not None:
            snapshots = cached_snapshots
        elif cache_file.exists():
            print(f"[{idx}/{total_markets}] Loading cached snapshots for {slug} (ID: {market_id})...")
            with open(cache_file, "r", encoding="utf-8") as f:
                data = json.load(f)
                snapshots = data.get("snapshots", [])
        else:
            print(f"[{idx}/{total_markets}] Downloading snapshots for {slug} (ID: {market_id})...")
            snapshots = client.get_all_snapshots_for_market(market_id, coin=args.coin)
            tmp_file = cache_file.with_suffix(".json.tmp")
            with open(tmp_file, "w", encoding="utf-8") as f:
                json.dump({"market": market, "snapshots": snapshots}, f)
            os.replace(tmp_file, cache_file)
            print(f"    Saved {len(snapshots)} snapshots to cache.")

        # Convert to individual replay JSONL (starting at 1s mono)
        single_market_lines, _ = convert_market_to_capture(
            market,
            snapshots,
            start_mono_ns=1_000_000_000,
            fill_mode=args.fill_mode,
            participation=args.trade_participation,
        )
        single_capture_file = out_dir / f"polybacktest_{args.market_type}_{slug}.jsonl"
        with open(single_capture_file, "w", encoding="utf-8") as f:
            for item in single_market_lines:
                f.write(json.dumps(item, separators=(",", ":")) + "\n")

        # Convert to combined replay JSONL (continuous monotonic timestamp)
        combined_market_lines, running_mono_ns = convert_market_to_capture(
            market,
            snapshots,
            start_mono_ns=running_mono_ns + 100_000_000,
            fill_mode=args.fill_mode,
            participation=args.trade_participation,
        )
        combined_lines.extend(combined_market_lines)

    # If requested or as default, save combined replay file
    combined_name = args.combined_name or f"polybacktest_{args.market_type}_{total_markets}w.jsonl"
    combined_file = out_dir / combined_name
    print(f"[*] Writing combined replay file to {combined_file} ({len(combined_lines)} events)...")
    with open(combined_file, "w", encoding="utf-8") as f:
        for item in combined_lines:
            f.write(json.dumps(item, separators=(",", ":")) + "\n")

    print(f"[+] Done! Successfully created replay file: {combined_file}")
    print(f"    Ready to test with: rtk cargo run -p cli --bin poly-as -- paper --source replay --input {combined_file}")

if __name__ == "__main__":
    main()
