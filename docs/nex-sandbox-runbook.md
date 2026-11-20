# NEX Sandbox Operator Runbook (Amoy 80002)

Paper-trading venue only. No simulator. Docs are ground truth: `nex-public-connection`,
`nex-connect`, `nex-start-trading`, `nex-websocket-feeds`.

## 0. Prerequisites

Copy template, never commit secrets:

```powershell
Copy-Item .env.example .env
```

Required `.env` values:

```text
NEX_CLOB_URL=https://nex.preprod.defi.euw2.polymarket.sh
NEX_WS_URL=wss://nex-ws.preprod.defi.euw2.polymarket.sh
GAMMA_PREPROD_URL=https://gamma-api-preprod.polymarket.dev
NEX_CHAIN_ID=80002
NEX_INGRESS_IP=51.24.102.79
NEX_ALLOWLISTED_EGRESS_IP=31.168.108.54
NEX_EXCHANGE_ADDRESS=0xe3333700cA9d93003F00f0F71f8515005F6c00Aa
NEX_TRADING_ADDRESS=<amoy-eoa-or-deposit-wallet>
NEX_WALLET_MODE=eoa            # or: deposit-wallet (Safe/Proxy unsupported)
NEX_API_KEY=<sandbox-l2-key>
NEX_API_SECRET=<sandbox-l2-secret>
NEX_API_PASSPHRASE=<sandbox-l2-passphrase>
GAMMA_CLIENT_ID=<gamma-preprod-id>
GAMMA_CLIENT_SECRET=<gamma-preprod-secret>
POLYMARKET_PRIVATE_KEY=<amoy-eoa-key>
AMOY_RPC_URL=<amoy-rpc>
```

Never put sandbox secrets in production `CLOB_API_KEY` / Gamma / relayer vars, and never
point NEX hosts at production. `SandboxProfile` fails closed on any host other than the four
sandbox hosts, on chain IDs other than 80002, and on the three known production exchange
addresses. It checks only addresses and key presence, not key ownership, so keep production
keys out of sandbox settings yourself.

## 1. Allowlist `31.168.108.54/32`

1. NEX registry must list operator egress as `31.168.108.54/32` (verified by live `sandbox health` on 2026-11-20; re-check if NAT changes).
2. Map both NEX hostnames locally to the pinned ingress:
   `nex.preprod.defi.euw2.polymarket.sh -> 51.24.102.79`
   `nex-ws.preprod.defi.euw2.polymarket.sh -> 51.24.102.79`
3. Keep TCP 443 outbound open. Client preserves SNI/Host; only DNS resolution is pinned.
4. Verify:

```powershell
cargo run -p cli --bin poly-as -- sandbox health --require-allowlisted-egress
```

Expected:

```text
Egress public IPv4:      31.168.108.54
Registered allowlist:   31.168.108.54
Allowlist match:        YES
Pinned ingress IPv4:    51.24.102.79
Host nex.preprod... -> ["51.24.102.79"] (pinned=true)
Host nex-ws.preprod... -> ["51.24.102.79"] (pinned=true)
Outbound TCP/443:       true
GET /health:            Some(200) (ok=true)
GATE: PASS  /health=200 from egress 31.168.108.54
```

If egress differs, stop and re-allowlist. Gate fails closed on unpinned DNS, TCP failure,
or non-200 `/health`.

Note: egress is NAT-dependent. Earlier runs saw `79.181.159.149`; live check on 2026-11-20 shows `31.168.108.54`. If `Allowlist match: NO`, update the NEX registry and `NEX_ALLOWLISTED_EGRESS_IP` before trading.

## 2. Credentials

### Gamma preprod

- Uses `GAMMA_CLIENT_ID` / `GAMMA_CLIENT_SECRET` as `x-client-id` / `x-client-secret`.
- Used only for `sandbox markets` discovery.
- Failure mode: `403 {"error":"missing or invalid edge credentials"}` means rotate/confirm
  Gamma edge credentials.
- Fallback on Gamma failure is NEX `GET /clob-markets` (same venue, not a simulator).

```powershell
cargo run -p cli --bin poly-as -- sandbox markets --limit 20
```

### Sandbox L2 trading credentials

- Uses `NEX_API_KEY` / `NEX_API_SECRET` / `NEX_API_PASSPHRASE`.
- Used for authenticated REST (`GET /orders`, `GET /order`) and user WS.
- Gamma credentials do **not** work here.
- Failure mode: `401 {"error":"Unauthorized/Invalid api key"}` means confirm/rotate the
  sandbox L2 credentials.
- EOA/Deposit Wallet only.

## 3. Amoy POL funding and approvals

1. Fund the Amoy trading address (`NEX_TRADING_ADDRESS`) with Amoy POL for approval gas.
2. Fund test pUSD / complete token approvals on Amoy before `validate`.
3. EOAs need `POLYMARKET_PRIVATE_KEY` + `AMOY_RPC_URL` for approval transactions.
4. Sampled sandbox books were empty; pick an asset with `tick > 0`, `min > 0`.
5. Validation never sizes up: `price = tickSize`, `size = minOrderSize`, post-only buy.

## 4. Validate (minimum-size post-only order)

```powershell
cargo run -p cli --bin poly-as -- sandbox book --asset-id <TOKEN_ID>
cargo run -p cli --bin poly-as -- sandbox validate --asset-id <TOKEN_ID> --confirm-sandbox-trade
```

Expected `book`:

```text
(c) BOOK
    Tick / min:             10000 / 5000000
    Empty book:             true/false
    Best bid/ask:           ... / ...
```

Expected `validate`:

```text
(d) VALIDATION ORDER
    Chain / wallet:         80002 / eoa
    Price x size (micro):   <tick> x <min> (one tick / min size)
    Placement success:      true
    Order ID:               <uuid — record this>
    Placement status:       live/open
    Read-back:              status=... price=... size=... matched=...
    Cancel/reconcile:       cancelled / remainder cancelled
```

The command places, reads back by ID, then cancels the remainder. It refuses without
`--confirm-sandbox-trade`, on tick/min metadata mismatch, or book asset mismatch.
Post-only crossing maps to existing CLOB errors; report, do not retry as taker.

## 5. Feeds

```powershell
cargo run -p cli --bin poly-as -- sandbox feeds `
  --up-asset-id <UP_ID> --dn-asset-id <DN_ID> `
  --duration 30s --ws-version subs-v2 `
  --reconnect-after-secs 15
```

Covers both feed families via `--ws-version original|subs-v2`. Caps apply only to `subs-v2`;
original feeds have no application limit. Client sends PING every 10s, expects PONG,
reconnects with exponential backoff, rebuilds snapshots via REST, and reconciles fills via
authenticated REST.

Expected:

```text
(e) LIVE FEEDS
    Market path:            wss://...
    User path:              wss://...
    [health] market session 1 connected (snapshot required)
    [market] snapshot Up bids=... asks=... exch_ms=...
    [market] snapshot Down bids=... asks=... exch_ms=...
    [probe] forcing a real socket reconnect ...
    [health] market READY after snapshot/REST resync
    [gate] quoting_allowed=... (snapshot=... liquidity=... fresh=... user_ready=...)
(f) FEED SUMMARY
    frames=... pongs=... sessions=2 snapshots=6 ready=... reconnects=1 stale=...
    snapshot_rebuild_ok=true book_has_liquidity=... quoting_allowed=false user_reconciled=...
```

Healthy run observed: `sessions=2`, `snapshots=6`, `reconnects=1`,
`snapshot_rebuild_ok=true`, PONG received. `quoting_allowed=false` is normal on an empty
book; all JSONL rows carry `"origin":"sandbox"`, `"network":"nex-amoy-80002"`.

## 6. Post-Whitelist Activation Checklist & Common Pitfalls

Run these steps in order once the NEX allowlist shows your egress IP.

### Pitfall 1 — EOA vs Deposit Address

In `NEX_WALLET_MODE=eoa`, `NEX_TRADING_ADDRESS` and `POLYMARKET_PRIVATE_KEY` must be the same
**EOA whose private key you control**. Never paste a Polymarket Deposit or Proxy address here:
the sandbox signs with that key, and a deposit address has no key.

In `NEX_WALLET_MODE=deposit-wallet`, `NEX_TRADING_ADDRESS` must be the Deposit Wallet address.
`POLYMARKET_PRIVATE_KEY` stays the signing EOA key. The sandbox does not compare the two.

The steps below assume `NEX_WALLET_MODE=eoa`.

### Pitfall 2 — Builder Auth vs CLOB L2 Creds

1Password Builder Auth credentials are for the **Relayer only**. Never put them in
`NEX_API_*` or `CLOB_API_*`. Sandbox L2 creds come from `scripts/nex_create_l2_key.py`.

### Activation steps

1. Verify Amoy POL and pUSD balances for the EOA, and confirm token approvals (§3).
2. Create L2 creds. Start with `--nonce 0`; retry with `--derive` on a nonce conflict:

   ```powershell
   python3 scripts/nex_create_l2_key.py --nonce 0
   python3 scripts/nex_create_l2_key.py --nonce 0 --derive
   ```

3. Check the route and discovery. Pick a clean `PolyV2` asset with `neg_risk=false`:

   ```powershell
   cargo run -p cli --bin poly-as -- sandbox health --require-allowlisted-egress
   cargo run -p cli --bin poly-as -- sandbox markets --limit 20
   ```

4. Place and cancel the minimum-size post-only order on that asset. Confirm **0 open orders**
   after cancel:

   ```powershell
   cargo run -p cli --bin poly-as -- sandbox validate --asset-id <TOKEN_ID> --confirm-sandbox-trade
   ```

5. Run the feeds for ~15s. Check that the authenticated `/ws/user` session connects:

   ```powershell
   cargo run -p cli --bin poly-as -- sandbox feeds `
     --up-asset-id <UP_ID> --dn-asset-id <DN_ID> --duration 15s
   ```

## 7. Do not simulate

If health, discovery, book, validate, or feeds is blocked (allowlist, 401/403, funding,
empty books), report the blocker. Do not build a mock exchange or local fills.
