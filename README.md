# PropAMM

Reference scripts for trading against propAMM protocols (FermiSwap, Kipseli,
Bebop) through Titan's RPC.

Three scripts, same protocol family:

| Script | What it does |
|---|---|
| `quoter.py` | Subscribe to Titan's pAMM state-diff WebSocket and run taker-sized quote sims against the current state override. |
| `taker.py` / `src/main.rs` | Wrap ETH, set approvals, sign + submit swaps to Titan as bundles or raw transactions. Python and Rust ports are byte-equivalent. |
| `contracts/KipseliGuard.sol` | Optional permissionless slippage-checking wrapper around the Kipseli pool. |

All three scripts accept `--eth-rpc-url <URL>`. You need a mainnet RPC that
supports `eth_call` with state and block overrides (Alchemy, Infura, self-hosted
Geth/Reth). Public free RPCs typically don't.

`taker.py` and the Rust taker additionally need a mainnet-funded signer EOA.
Its private key is read from the `PROP_AMM_TAKER_PRIVATE_KEY` environment
variable (hex, with or without `0x`). The quoter does not sign, but it can
derive this address for realistic `eth_call` sims; otherwise pass
`--from-address`.

```sh
export PROP_AMM_TAKER_PRIVATE_KEY=0x...
```

## `quoter.py`

Fermi:

```sh
python quoter.py --eth-rpc-url <your-rpc-url> \
    --contract fermi \
    --pair weth/usdc --notional-usd 1000
```

Kipseli currently streams on the AP endpoint:

```sh
python quoter.py --eth-rpc-url <your-rpc-url> \
    --contract kipseli --stream-region ap \
    --pair weth/usdc --notional-usd 1000
```

Bebop:

```sh
python quoter.py --eth-rpc-url <your-rpc-url> \
    --contract bebop --stream-region us \
    --pair weth/usdc --notional-usd 1000
```

All configured contracts:

```sh
python quoter.py --eth-rpc-url <your-rpc-url> \
    --contract all --pair weth/usdc --notional-usd 2
```

Each successful frame prints one line with sell, buy, spread in bps, and raw
output amounts:

```text
[14:19:41.773] #    7 FermiSwapper block=25230252 sell= 1959.72000 buy= 1959.92000 spread=   +1.00bps buy_out=1020451559227807 sell_out=2000000
```

For Kipseli, the quoter resolves the pool's current `swapImpl()` and calls its
`quote(address,uint256,address)` entry point, so the quote is not capped by the
EOA's token balances.

Flags: `--contract {all|fermi|bebop|kipseli}`
`--pair {weth/usdc|weth/usdt|usdc/usdt}` `--notional-usd N`
`--slippage-bps N` `--from-address ADDRESS` `--eth-rpc-url URL`
`--stream-region {eu|ap|us}` `--ws URL` `--binance URL`
`--auth-token TOKEN` `--no-auth` `--print-raw` `--print-misses` `--once`
`--max-matches N` `--timeout-secs N`.

## `taker.py`

```sh
pip install -r requirements.txt
```

One-time setup (wrap ETH, grant ERC20 approvals to the target contract):

```sh
python taker.py --eth-rpc-url <your-rpc-url> --contract fermi --setup-only
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

Submit the signed transaction with Titan's `eth_sendRawTransaction` instead of
`eth_sendBundle`:

```sh
python taker.py --eth-rpc-url <your-rpc-url> \
    --contract fermi --stream --send --send-mode raw-transaction --skip-setup \
    --pair weth/usdc --notional-usd 2 \
    --min-priority-gwei 5 --interval-secs 3
```

Same against Bebop:

```sh
python taker.py --eth-rpc-url <your-rpc-url> \
    --contract bebop --stream --send --skip-setup \
    --pair weth/usdc --notional-usd 2 \
    --min-priority-gwei 5 --interval-secs 3
```

Flags: `--contract {fermi|bebop|kipseli}` `--pair {weth/usdc|weth/usdt}`
`--notional-usd N` `--slippage-bps N`
`--min-priority-gwei N` `--interval-secs N` `--reserve-eth F` `--target-weth F`
`--send` `--send-mode {bundle|raw-transaction}` `--once` `--setup-only` `--skip-setup` `--stream`
`--stream-region {eu|ap|us}` `--titan-url URL`. The ETH/USDC mid used to size
the WETH leg is auto-fetched from Binance and refreshed every second. On a
successful landing the script prints a short `🚀 LANDED` banner with the
block + etherscan link; if a tracked tx's nonce gets consumed by a different
tx the script prints a `[dropped]` line and stops polling that hash.

## `src/main.rs` (Rust taker)

Same flags, same behavior:

```sh
cargo run --release -- --eth-rpc-url <your-rpc-url> \
    --contract fermi --stream --send --skip-setup \
    --pair weth/usdc --notional-usd 2 \
    --min-priority-gwei 5 --interval-secs 3
```

Use `--send-mode raw-transaction` for Titan's documented
`eth_sendRawTransaction` submission path.

## Stream-gated submission

`--stream` subscribes to Titan's pAMM state-diff
WebSocket at `wss://{eu,ap,us}.rpc.titanbuilder.xyz/ws/pamm_quote_stream`,
pre-simulates each swap via `eth_call` with the stream's `stateOverride` +
block overrides, and skips submission when the sim reverts.

## Targets

| Contract | Address |
|---|---|
| FermiSwap | `0xb1076fe3ab5e28005c7c323bac5ac06a680d452e` |
| BebopSwapper | `0xdb13ad0fcd134e9c48f2fdaea8f6751a0f5349ca` (stream key `0xbc60639345dfa607d73b74e88c2d54d8b8ad7cc3`) |
| KipseliGuard | `0x9a7a5dccc7851c0f141d07c4d608a29b3830548b` (optional wrapper around `0x5cdbe594…d588c`) |

## Troubleshooting

When troubleshooting, it may be useful to widen the default `--slippage-bps`.

## License

MIT — see [LICENSE](LICENSE).
