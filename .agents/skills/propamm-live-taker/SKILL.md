---
name: propamm-live-taker
description: Dry-run, send, retry, and verify live pAMM taker orders from propamm/taker.py (or builder pamm_taker.rs) through public Titan, including dual Kipseli+Fermi bundles, and confirm on-chain landing. Use for live mainnet taker sends, "did my Titan bundle land", or extending taker.py. For local-builder tests use local-pamm-taker.
---

# Live pAMM taker via Titan

Source of truth: `~/Software/gattaca/propamm` `README.md` and the `taker.py` docstring/argparse (Rust port `src/main.rs` is byte-equivalent). Builder's `crates/rpc/examples/pamm_taker.rs` is the alternative with `--multi-pamm`/`--raw`. Read current flags before running; don't trust remembered ones.

## Procedure

1. **Env.** Install deps with the interpreter that runs the script (`python -m pip install -r requirements.txt`, or `uv`); worktrees may lack `.venv`. Helpers importing `taker` must run from the checkout or put it on `PYTHONPATH`. Use `PYTHONUNBUFFERED=1 python -u` under `process`.
2. **Secrets.** `PROP_AMM_TAKER_PRIVATE_KEY` never enters model context, argv, logs, or memory. Either the user exports it, or recover it (e.g. from atuin) inside a subprocess and print only redacted status. If feeding via stdin, write it immediately and close stdin. Never `--send` with a dummy key.
3. **Endpoints.** `--eth-rpc-url` must support state+block overrides on `eth_call` and report chain id 1. Default Titan is `https://rpc.titanbuilder.xyz/`; omit `--titan-url` for it, and unset a stray `TITAN_RPC_URL=127.0.0.1:1337` (it overrides the default).
4. **Dry-run** without `--send` (`--skip-setup --once`): check signer, balances, allowances, nonce, calldata, tx hash. `--setup-only` sends approval/wrap txs, so it needs approval too.
5. **Live send only after explicit user confirmation** (real funds). Use a current `--eth-price-usd` (Chainlink ETH/USD `0x5f4eC3Df9cbd43714FE2740f5E3616155c5b8419`) and prefer `--stream` (Fermi/Kipseli only, not Bebop) so it pre-sims against quote-stream state overrides. Stream-gated Kipseli has been the reliable path; Fermi one-offs are often accepted and never land.
6. **Retries** run as bounded `process` loops with notifications. Stop the previous loop before switching contract/strategy (nonce contention). Gate on `[trade] state-override sim ok`/submit output, not exit code; `sim reverts, skipping` exits 0 without sending.
7. **Verify landing** after a few blocks with a full-node RPC: signer `latest`/`pending` nonce (most reliable), `eth_getTransactionReceipt` `status=1`, `eth_getTransactionByHash`. Report Titan acceptance and inclusion separately.

## Dual Kipseli+Fermi bundle

One Titan `eth_sendBundle` with both legs: same signer, Kipseli at `pending_nonce`, Fermi at `+1`, raw txs in that order, shared fee fields. Approve WETH/USDC/USDT for both `FERMI_SWAPPER` and `KIPSELI_GUARD`. In stream mode, group overrides by `blockNumber` and require both (Fermi key `FERMI_SWAPPER`; Kipseli stream key `KIPSELI_POOL` but txs go to `KIPSELI_GUARD`), pre-sim each leg against its own override and the shared block context, skip the whole bundle if either reverts, prune old block buckets. Success = both receipts `status=1` in the same block and nonce +2. Keep single-contract behaviour unchanged when adding such modes; Bebop stays out (operator-gated).

## Pitfalls

- Titan 200 + `bundleHash` = acceptance only. Builder-RPC `eth_sendRawTransaction` may return `result: null` on success.
- `execution reverted: SP` = slippage/price protection: stale `--eth-price-usd` or static latest-state `eth_call` without stream overrides. Refresh inputs and re-sim; don't blindly resend.
- Fermi and Kipseli stream frames arrive separately; one fresh override says nothing about the other.
- Titan relay RPC isn't a full node (`-32601` for tx lookups). Public RPCs may return `null` for mined txs; treat as inconclusive and fall back to nonce or the Etherscan address page.
- Public Titan routes to the production fleet, never the local builder; local inclusion is `local-pamm-taker`'s job.
- Inclusion in many built blocks or `AddedToPool` doesn't prove maker composition; need `PammMatch` + `PammFill`.
- `--no-stream --skip-sim` (where supported) only after balances/approvals are independently verified.
- Before publishing taker changes: `python -m py_compile`, `--help`, a no-send dry run, and a scan for keys, API-key RPC URLs and secret-bearing commands. Pushing to `gattaca-com/propamm` may need a fork/PR if the account is read-only.
