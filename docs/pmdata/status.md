# `pmdata status`

Prints the persistent quota ledger. Offline, `0` quota, no API key required.

## Syntax

```powershell
poly-as pmdata status
```

No flags.

## Output

```text
PMData Quota Manifest (`data/pmdata/manifest.json`): charged=214 | reserved=0 | remaining=436 / cap=650
Manifest entries (23 total): status={"completed": 20, "not_found": 1, "failed_uncharged": 2}, dataset={"l2": 15, "trades": 6, "onchain_fills": 2}
```

## Reading the Ledger

| Field | Meaning |
| :--- | :--- |
| `charged` | Units committed for completed or ambiguous downloads. |
| `reserved` | Units held by in-flight requests. Non-zero on an idle host means an interrupted run. |
| `remaining` | `cap - charged - reserved`. |
| `cap` | Configured ceiling, never above `650`. |

| Entry status | Meaning | Charged |
| :--- | :--- | :--- |
| `reserved` | Reservation written before the HTTP send. | Held |
| `completed` | Download finished and cached. | Yes |
| `not_found` | Provider returned `404`. | No |
| `forbidden` | Provider returned `403` (typically the 21-day window). | No |
| `failed_uncharged` | Pre-send or uncharged HTTP failure. | No |
| `failed_ambiguous` | Post-send failure or corrupt body. | Yes, conservatively |

`reserved` above `0` on an idle host means an interrupted run left a reservation. Re-running the
same download reclaims it automatically: the leftover reservation becomes `failed_ambiguous` and its
uncharged delta is billed conservatively, so the ledger can never pass `650`. Do not hand-edit
`manifest.json`; it also proves which units were already billed.

## Examples

```powershell
poly-as pmdata status
$env:PMDATA_CACHE_DIR = "D:\pmdata"; poly-as pmdata status   # inspect an alternate ledger
```
