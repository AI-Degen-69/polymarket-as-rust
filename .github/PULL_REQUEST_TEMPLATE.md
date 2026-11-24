## Summary

- Phase / Scope:
- Why this change exists:
- Issue: Closes #<id>

## Atomic Commits / Key Changes

-

## Verification (Station IV Step 0 proof gate — proved or honestly unverified, never silent)

- [ ] Targeted suites for touched modules pass (paste command + result below)
- [ ] Browser gate (UI changes only): endpoint smoke 200, DOM checks, zero console errors, zero failed requests, screenshot last
- [ ] Items that could not be verified (name each + tooling reason):

```
<paste verification output>
```

## Gate Checklist

- [ ] `rtk cargo fmt --all -- --check` exits 0
- [ ] `rtk cargo clippy --workspace --all-targets -- -D warnings` exits 0
- [ ] `rtk cargo test --workspace` exits 0
- [ ] Live/replay `poly-as` trace and JSONL telemetry verified (if applicable)
- [ ] OCR delegation review completed (file list + rule groups + findings, or skip reason)
- [ ] Spec axis: diff matches linked issue/plan (Missing / Added-not-asked / Implemented-wrong)
- [ ] `rust-reviewer` and `silent-failure-hunter` report 0 CRITICAL / 0 HIGH findings
- [ ] Zero `.env` files, secrets, private keys, or `logs/` artifacts in diff

## Human-Readable Verification Scorecard

- Score: `/100`
- Verdict: `PASS` / `PASS WITH NOTES` / `FAIL`
- Key Metrics:

## CodeRabbit (Station IV Step 5 → Station V)

- Trigger: `@coderabbitai review` posted (paste comment link)
- Ack status (exactly one): *review started* / *rate limited (N min)* / *summary-only — no findings expected here; verification rests on the local gates* / *other (quoted)* / *no acknowledgement*

@coderabbitai summary
