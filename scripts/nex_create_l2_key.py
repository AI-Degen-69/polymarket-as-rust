"""Create or derive NEX sandbox L2 API creds for your own EOA.

Reads local .env only. Never prints secrets. Writes NEX_API_* back to .env.
Run from allowlisted egress (currently 31.168.108.54) with hosts mapped to 51.24.102.79.

  python3 -m pip install -r scripts/requirements.txt
  python3 scripts/nex_create_l2_key.py --nonce 0 [--derive]

Requires in .env:
  POLYMARKET_PRIVATE_KEY=<your EOA key, never share>
  NEX_TRADING_ADDRESS=<same EOA>
  NEX_CLOB_URL=https://nex.preprod.defi.euw2.polymarket.sh
"""
import argparse
import http.client
import json
import os
import tempfile
import time
import urllib.request
from pathlib import Path

from eth_account import Account


def load_dotenv(path: Path) -> dict:
    vals: dict = {}
    if not path.exists():
        return vals
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        vals[k.strip()] = v.strip().strip('"').strip("'")
    return vals


def save_dotenv(path: Path, lines: list[str]) -> None:
    """Write atomically: a failed save leaves the original .env intact."""
    fd, tmp = tempfile.mkstemp(dir=str(path.parent), prefix=".env.", suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as f:
            f.write("\n".join(lines) + "\n")
        os.replace(tmp, path)
    except BaseException:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--nonce", default="0")
    ap.add_argument("--derive", action="store_true",
                    help="use GET /auth/derive-api-key instead of POST /auth/api-key")
    args = ap.parse_args()

    env_path = Path(".env")
    vals = load_dotenv(env_path)

    # Wallet values come from .env only, so shell exports cannot mix two wallets.
    priv = vals.get("POLYMARKET_PRIVATE_KEY", "")
    addr = vals.get("NEX_TRADING_ADDRESS", "")
    host = (vals.get("NEX_CLOB_URL") or "https://nex.preprod.defi.euw2.polymarket.sh").rstrip("/")
    if not priv or not addr:
        print("missing POLYMARKET_PRIVATE_KEY or NEX_TRADING_ADDRESS in local .env (values redacted)")
        return 2
    # sanity without revealing: compare derived address
    derived = Account.from_key(priv).address
    if derived.lower() != addr.strip().lower():
        print("mismatch: POLYMARKET_PRIVATE_KEY does not control NEX_TRADING_ADDRESS (redacted). Fix locally.")
        return 2

    ts = str(int(time.time()))
    payload = {
        "domain": {"name": "ClobAuthDomain", "version": "1", "chainId": 80002},
        "types": {"ClobAuth": [
            {"name": "address", "type": "address"},
            {"name": "timestamp", "type": "string"},
            {"name": "nonce", "type": "uint256"},
            {"name": "message", "type": "string"},
        ]},
        "primaryType": "ClobAuth",
        "message": {
            "address": derived,
            "timestamp": ts,
            "nonce": args.nonce,
            "message": "This message attests that I control the given wallet",
        },
    }
    sig = Account.sign_typed_data(priv, full_message=payload).signature.hex()
    if not sig.startswith("0x"):
        sig = "0x" + sig

    path = "/auth/derive-api-key" if args.derive else "/auth/api-key"
    url = host + path
    method = "GET" if args.derive else "POST"
    req = urllib.request.Request(url, method=method, headers={
        "POLY_ADDRESS": derived,
        "POLY_SIGNATURE": sig,
        "POLY_TIMESTAMP": ts,
        "POLY_NONCE": str(args.nonce),
    })
    try:
        with urllib.request.urlopen(req, timeout=20) as r:
            body = json.loads(r.read().decode())
    except (OSError, ValueError, http.client.HTTPException) as e:
        print(f"key bootstrap failed: {type(e).__name__} (no secrets shown). "
              f"Try --derive if key exists, or confirm egress allowlist + /health 200.")
        return 1

    api_key = body.get("apiKey") if isinstance(body, dict) else None
    secret = body.get("secret") if isinstance(body, dict) else None
    passphrase = body.get("passphrase") if isinstance(body, dict) else None
    creds = (api_key, secret, passphrase)
    if not all(isinstance(v, str) and v for v in creds):
        print("unexpected response shape (keys redacted). Not writing .env.")
        return 1

    # update .env in place, preserving comments and line order
    lines = env_path.read_text(encoding="utf-8").splitlines() if env_path.exists() else []
    updates = {
        "NEX_API_KEY": api_key,
        "NEX_API_SECRET": secret,
        "NEX_API_PASSPHRASE": passphrase,
    }
    out: list[str] = []
    seen: set[str] = set()
    for line in lines:
        if "=" in line and not line.startswith("#"):
            key = line.split("=", 1)[0].strip()
            if key in updates:
                if key not in seen:  # drop duplicate assignments, keep one
                    seen.add(key)
                    out.append(f"{key}={updates[key]}")
                continue
        out.append(line)
    for key, value in updates.items():
        if key not in seen:
            out.append(f"{key}={value}")
    try:
        save_dotenv(env_path, out)
    except OSError as exc:
        print(f"saving .env failed: {type(exc).__name__} (original .env left intact).")
        return 1
    print("wrote NEX_API_KEY/SECRET/PASSPHRASE to local .env (values redacted, not printed). "
          "Re-run sandbox feeds to verify 401 is gone.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
