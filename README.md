# PropAMM

Reference scripts for trading against propAMM protocols (FermiSwap, Kipseli,
Bebop) through Titan's bundle relay.

Three scripts, same protocol family:

| Script | What it does |
|---|---|
| `quoter.py` | Subscribe to Titan's pAMM state-diff WebSocket and run pre-trade quote sims against the current state override. |
| `taker.py` / `src/main.rs` | Wrap ETH, set approvals, sign + submit swap bundles to Titan. The Python taker also supports dual Kipseli+Fermi bundles. |
| `contracts/KipseliGuard.sol` | Optional permissionless slippage-checking wrapper around the Kipseli pool. |

All three scripts accept `--eth-rpc-url <URL>`. You need a mainnet RPC that
supports `eth_call` with state and block overrides (Alchemy, Infura, self-hosted
Geth/Reth). Public free RPCs typically don't.

`taker.py` and the Rust taker additionally need a mainnet-funded signer EOA —
its private key is read from the `PROP_AMM_TAKER_PRIVATE_KEY` environment
variable (hex, with or without `0x`). The quoter does not need it. Never commit
real private keys, RPC API keys, auth tokens, or funded addresses' secret
material; examples in this repository use placeholders only.

```sh
export PROP_AMM_TAKER_PRIVATE_KEY=0x...
```

## `quoter.py`

```sh
python quoter.py --eth-rpc-url <your-rpc-url> --no-auth --notional-usd 1000
```

Pick a different region:

```sh
python quoter.py --eth-rpc-url <your-rpc-url> --no-auth \
    --ws wss://us.rpc.titanbuilder.xyz/ws/pamm_quote_stream
```

Flags: `--eth-rpc-url URL` `--ws URL` `--binance URL` `--notional-usd N`
`--auth-token TOKEN` `--no-auth` `--print-raw`.

## `taker.py`

```sh
pip install -r requirements.txt
```

One-time setup (wrap ETH, grant ERC20 approvals to the target contract):

```sh
python taker.py --eth-rpc-url <your-rpc-url> --contract fermi --setup-only
```

For dual-bundle mode, setup grants approvals to both FermiSwapper and
KipseliGuard:

```sh
python taker.py --eth-rpc-url <your-rpc-url> --dual-bundle --setup-only
```

Dry-run a single trade (no `--send`):

```sh
python taker.py --eth-rpc-url <your-rpc-url> \
    --contract fermi --skip-setup --once \
    --pair weth/usdc --notional-usd 2
```

Continuous stream-gated sends against Fermi or Kipseli:

```sh
python taker.py --eth-rpc-url <your-rpc-url> \
    --contract fermi --stream --send --skip-setup \
    --pair weth/usdc --notional-usd 2 \
    --min-priority-gwei 5 --interval-secs 3
```

Submit one Titan bundle containing both Kipseli and Fermi swaps. Live
`--dual-bundle --send` requires `--stream` so both swaps are pre-simulated
against Fermi and Kipseli state overrides from the same block context:

```sh
python taker.py --eth-rpc-url <your-rpc-url> \
    --dual-bundle --stream --send --skip-setup --once \
    --pair weth/usdc --notional-usd 1 \
    --min-priority-gwei 5
```

Same against Bebop:

```sh
python taker.py --eth-rpc-url <your-rpc-url> \
    --contract bebop --stream --send --skip-setup \
    --pair weth/usdc --notional-usd 2 \
    --min-priority-gwei 5 --interval-secs 3
```

Flags: `--contract {fermi|bebop|kipseli}` `--dual-bundle`
`--pair {weth/usdc|weth/usdt}` `--notional-usd N` `--slippage-bps N`
`--min-priority-gwei N` `--interval-secs N` `--reserve-eth F` `--target-weth F`
`--send` `--once` `--setup-only` `--skip-setup` `--stream`
`--stream-region {eu|ap|us}` `--titan-url URL`. The ETH/USDC mid used to size
the WETH leg is auto-fetched from Binance and refreshed every second. On a
successful landing the script prints a short `🚀 LANDED` banner with the
block + etherscan link; if a tracked tx's nonce gets consumed by a different
tx the script prints a `[dropped]` line and stops polling that hash.

## `src/main.rs` (Rust taker)

The Rust taker matches the single-contract Python behavior. The `--dual-bundle`
mode is currently Python-only:

```sh
cargo run --release -- --eth-rpc-url <your-rpc-url> \
    --contract fermi --stream --send --skip-setup \
    --pair weth/usdc --notional-usd 2 \
    --min-priority-gwei 5 --interval-secs 3
```

## Stream-gated submission

`--stream` subscribes to Titan's pAMM state-diff
WebSocket at `wss://{eu,ap,us}.rpc.titanbuilder.xyz/ws/pamm_quote_stream`,
pre-simulates each swap via `eth_call` with the stream's `stateOverride` +
block overrides, and skips submission when the sim reverts. With
`--dual-bundle`, the stream waits for Fermi and Kipseli state overrides from the
same block context, pre-simulates both, checks aggregate token balance and gas
budget, and posts them as one Titan bundle.

## Targets

| Contract | Address |
|---|---|
| FermiSwap | `0xb1076fe3ab5e28005c7c323bac5ac06a680d452e` |
| Bebop     | `0x160141a205f5ddcf096ba3f48b7ed21eb52c62ea` |
| KipseliGuard | `0x9a7a5dccc7851c0f141d07c4d608a29b3830548b` (optional wrapper around `0x5cdbe594…d588c`) |

## Troubleshooting

When troubleshooting, it may be useful to widen the default `--slippage-bps`.

## License

MIT — see [LICENSE](LICENSE).
