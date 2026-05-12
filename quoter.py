#!/usr/bin/env python3
"""pAMM quote simulator — subscribe to Titan's state-diff stream and sim quotes.

For each frame, filters the stateDiff to addresses with real storage overrides,
identifies which configured pAMM had its quote contract touched, and runs two
``eth_call`` quote simulations (BUY: USDC->WETH, SELL: WETH->USDC) using the
filtered state override.

A reference ETHUSDC mid is fetched once from Binance at startup to size the
SELL leg; the script is otherwise self-contained — no DB, no polling per frame.

## Examples

The mainnet RPC must support ``eth_call`` with state overrides::

    export ETH_RPC_URL=https://eth-mainnet.example/<key>

Unauthenticated stream with a custom notional::

    python quoter.py --eth-rpc-url "$ETH_RPC_URL" --no-auth --notional-usd 1000

Pick a different region::

    python quoter.py --eth-rpc-url "$ETH_RPC_URL" --no-auth \\
        --ws wss://us.rpc.titanbuilder.xyz/ws/pamm_quote_stream
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
import urllib.request
from datetime import UTC, datetime

import websockets
from eth_utils import to_checksum_address

DEFAULT_WS = "wss://eu.rpc.titanbuilder.xyz/ws/pamm_quote_stream"
DEFAULT_BINANCE = "https://api.binance.com/api/v3/ticker/price?symbol=ETHUSDC"

WETH = to_checksum_address("0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2")
USDC = to_checksum_address("0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48")
FERMI_SWAPPER = to_checksum_address("0xb1076fe3ab5e28005c7c323bac5ac06a680d452e")
KIPSELI_POOL = to_checksum_address("0x5cdbe59400cc2efdcc2b54acca4a99fe00dd588c")

USDC_DECIMALS = 6
WETH_DECIMALS = 18

# Kipseli has no view-only quote; we eth_call its swap() directly with a
# tokenIn balanceOf override on the pool to satisfy its pre-deposit check
# (the live path arrives via KipseliGuard which transferFroms first).
PAMMS: list[dict] = [
    {
        "name":     "FermiSwap",
        "quoter":   FERMI_SWAPPER.lower(),
        "oracle":   "0x1038c87766e36d1925889e6f26d10e0012d50fed",
        "selector": "300aa47f",
        "kind":     "fermiswap",
    },
    {
        "name":     "Kipseli",
        "quoter":   KIPSELI_POOL.lower(),
        "oracle":   "0x8051c111cd6978396e4f81cd81d21b1ae8be5a08",
        "selector": "5a837efd",
        "kind":     "kipseli",
    },
]

# Kipseli's swap transfers tokenOut to this recipient during the sim; any
# non-blacklisted EOA works.
KIPSELI_DEST_PLACEHOLDER = "000000000000000000000000000000000000dead"

ERC20_BALANCE_SLOTS: dict[str, int] = {
    WETH.lower(): 3,
    USDC.lower(): 9,
}


def now() -> str:
    return datetime.now(UTC).strftime("%H:%M:%S.%f")[:-3]


def to_word(value: int, *, signed: bool = False) -> str:
    return value.to_bytes(32, "big", signed=signed).hex()


def addr_word(addr: str) -> str:
    return addr.lower().removeprefix("0x").rjust(64, "0")


def jsonrpc(rpc_url: str, method: str, params: list) -> dict:
    req = json.dumps(
        {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
    ).encode()
    r = urllib.request.urlopen(
        urllib.request.Request(
            rpc_url, data=req, headers={"content-type": "application/json"}
        ),
        timeout=15,
    )
    return json.loads(r.read())


def erc20_balance_slot(rpc_url: str, token: str, holder: str) -> str:
    base = ERC20_BALANCE_SLOTS[token.lower()]
    payload = "0x" + addr_word(holder) + to_word(base)
    return jsonrpc(rpc_url, "web3_sha3", [payload])["result"]


def fetch_binance_mid(url: str) -> float:
    r = urllib.request.urlopen(url, timeout=10)
    return float(json.loads(r.read())["price"])


FRAME_META_KEYS = {"slot", "block_number", "blockNumber", "timestamp"}


def overrides_by_quoter(frame: dict | None) -> dict[str, dict]:
    if not frame:
        return {}
    by_quoter: dict[str, dict] = {}
    for quoter, payload in frame.items():
        if quoter in FRAME_META_KEYS:
            continue
        if not isinstance(quoter, str) or not isinstance(payload, dict):
            continue
        addr_map = payload.get("state_override")
        if not isinstance(addr_map, dict):
            addr_map = payload.get("stateOverride")
        if not isinstance(addr_map, dict):
            continue
        filtered: dict = {}
        for addr, spec in addr_map.items():
            if not isinstance(addr, str) or not isinstance(spec, dict):
                continue
            sd = spec.get("stateDiff")
            if not isinstance(sd, dict) or not sd:
                continue
            filtered[addr.lower()] = {
                "stateDiff": {
                    (slot.lower() if isinstance(slot, str) else slot): val
                    for slot, val in sd.items()
                }
            }
        if filtered:
            by_quoter[quoter.lower()] = filtered
    return by_quoter


def pamm_quoter_touched(pamm: dict, by_quoter: dict[str, dict]) -> tuple[bool, int]:
    inner = by_quoter.get(pamm["quoter"].lower())
    if not inner:
        return False, 0
    return True, sum(len(spec.get("stateDiff", {})) for spec in inner.values())


def encode_quote_calldata(
    pamm: dict, *, token_in: str, token_out: str, amount_in: int
) -> str:
    if pamm["kind"] == "fermiswap":
        return (
            "0x"
            + pamm["selector"]
            + addr_word(token_in)
            + addr_word(token_out)
            + to_word(amount_in, signed=True)
        )
    if pamm["kind"] == "kipseli":
        return (
            "0x"
            + pamm["selector"]
            + addr_word(token_in)
            + to_word(amount_in)
            + addr_word(token_out)
            + addr_word(KIPSELI_DEST_PLACEHOLDER)
        )
    raise ValueError(f"unknown pamm kind: {pamm['kind']!r}")


def decode_quote_result(
    pamm: dict, raw: bytes, amount_in_requested: int
) -> tuple[str | None, int, int]:
    if pamm["kind"] == "fermiswap":
        if len(raw) < 64:
            return f"SHORT({len(raw)}b)", 0, 0
        return (
            None,
            int.from_bytes(raw[0:32], "big", signed=False),
            int.from_bytes(raw[32:64], "big", signed=False),
        )
    if pamm["kind"] == "kipseli":
        if len(raw) < 32:
            return f"SHORT({len(raw)}b)", 0, 0
        return None, amount_in_requested, int.from_bytes(raw[0:32], "big", signed=False)
    raise ValueError(f"unknown pamm kind: {pamm['kind']!r}")


def call_quote(
    rpc_url: str,
    pamm: dict,
    *,
    token_in: str,
    token_out: str,
    amount_in: int,
    in_decimals: int,
    out_decimals: int,
    overrides: dict | None,
) -> tuple[str, float | None]:
    data = encode_quote_calldata(
        pamm, token_in=token_in, token_out=token_out, amount_in=amount_in
    )
    params: list = [{"to": pamm["quoter"], "data": data}, "latest"]
    if pamm["kind"] == "kipseli":
        slot = erc20_balance_slot(rpc_url, token_in, pamm["quoter"])
        token_lc = token_in.lower()
        overrides = dict(overrides or {})
        inner = overrides.get(token_lc, {})
        state_diff = {**inner.get("stateDiff", {}), slot: "0x" + to_word(1 << 200)}
        overrides[token_lc] = {**inner, "stateDiff": state_diff}
    if overrides:
        params.append(overrides)

    r = jsonrpc(rpc_url, "eth_call", params)
    if "error" in r:
        msg = r["error"].get("message", "(no message)")
        return f"REVERT({msg[:60]})", None

    result = r.get("result", "0x")
    if not result or result == "0x":
        return "EMPTY", None

    raw = bytes.fromhex(result.removeprefix("0x"))
    err, amount_in_returned, amount_out = decode_quote_result(pamm, raw, amount_in)
    if err is not None:
        return err, None

    in_h = amount_in_returned / (10**in_decimals)
    out_h = amount_out / (10**out_decimals)
    if in_h <= 0 or out_h <= 0:
        return f"ZERO(in={in_h},out={out_h})", None

    price = in_h / out_h if token_in.lower() == USDC.lower() else out_h / in_h
    return "OK", price


async def listen(
    rpc_url: str,
    ws_url: str,
    binance_url: str,
    notional_usd: float,
    auth_token: str | None,
    print_raw: bool,
) -> None:
    print(f"[{now()}] rpc      = {rpc_url}")
    print(f"[{now()}] ws       = {ws_url}")
    print(f"[{now()}] notional_usd = {notional_usd}")
    for p in PAMMS:
        print(
            f"[{now()}] pamm     = {p['name']:<10}  "
            f"quoter={p['quoter']}  oracle={p['oracle']}"
        )

    try:
        ref_mid = fetch_binance_mid(binance_url)
    except Exception as e:
        print(f"[{now()}] Binance fetch failed: {e}", file=sys.stderr)
        sys.exit(1)
    size_eth = notional_usd / ref_mid
    amount_in_buy = int(notional_usd * (10**USDC_DECIMALS))
    amount_in_sell = int(size_eth * (10**WETH_DECIMALS))
    print(
        f"[{now()}] binance ETHUSDC = {ref_mid:.2f}  =>  "
        f"size_eth = {size_eth:.6f} ETH (sell leg)"
    )

    # FermiSwap is expected to ZERO without a fresh oracle override.
    print(f"\n[{now()}] === control (no overrides) ===")
    for pamm in PAMMS:
        s_b, p_b = call_quote(
            rpc_url, pamm,
            token_in=USDC, token_out=WETH,
            amount_in=amount_in_buy,
            in_decimals=USDC_DECIMALS, out_decimals=WETH_DECIMALS,
            overrides=None,
        )
        s_s, p_s = call_quote(
            rpc_url, pamm,
            token_in=WETH, token_out=USDC,
            amount_in=amount_in_sell,
            in_decimals=WETH_DECIMALS, out_decimals=USDC_DECIMALS,
            overrides=None,
        )
        print(f"  {pamm['name']:<10}  BUY  {s_b}  price={p_b}")
        print(f"  {pamm['name']:<10}  SELL {s_s}  price={p_s}")

    print(f"\n[{now()}] connecting to WS...")
    try:
        headers = {"Authorization": auth_token} if auth_token else None
        ws = await websockets.connect(ws_url, open_timeout=10, additional_headers=headers)
    except Exception as e:
        print(f"[{now()}] WS connect failed: {e}")
        sys.exit(1)
    print(f"[{now()}] connected, streaming\n")

    n = 0
    async for raw in ws:
        n += 1
        ts = now()
        try:
            frame = json.loads(raw)
            if print_raw:
                print(frame)
        except json.JSONDecodeError as e:
            print(f"[{ts}] #{n} PARSE ERROR: {e}")
            continue
        if not isinstance(frame, dict):
            print(f"[{ts}] #{n} unexpected top-level: {type(frame).__name__}")
            continue

        by_quoter = overrides_by_quoter(frame)

        touched: list[tuple[dict, int, dict]] = []
        for pamm in PAMMS:
            hit, slot_count = pamm_quoter_touched(pamm, by_quoter)
            if hit:
                touched.append((pamm, slot_count, by_quoter[pamm["quoter"].lower()]))

        if not touched:
            slot = frame.get("slot")
            slot_str = f" slot={slot}" if slot is not None else ""
            seen = ",".join(sorted(by_quoter.keys())) if by_quoter else "-"
            print(
                f"[{ts}] #{n:>5}  no configured quoter"
                f" (quoters={len(by_quoter):>2}: {seen}){slot_str}"
            )
            continue

        for pamm, slot_count, overrides in touched:
            s_buy, p_buy = call_quote(
                rpc_url, pamm,
                token_in=USDC, token_out=WETH,
                amount_in=amount_in_buy,
                in_decimals=USDC_DECIMALS, out_decimals=WETH_DECIMALS,
                overrides=overrides,
            )
            s_sell, p_sell = call_quote(
                rpc_url, pamm,
                token_in=WETH, token_out=USDC,
                amount_in=amount_in_sell,
                in_decimals=WETH_DECIMALS, out_decimals=USDC_DECIMALS,
                overrides=overrides,
            )

            bid_str = f"{p_sell:8.2f}" if p_sell is not None else f"{s_sell:>8s}"
            ask_str = f"{p_buy:8.2f}"  if p_buy  is not None else f"{s_buy:>8s}"
            if p_buy is not None and p_sell is not None and p_sell > 0:
                mid = 0.5 * (p_buy + p_sell)
                spread_bps = 10000.0 * (p_buy - p_sell) / mid
                spread_str = f"{spread_bps:+7.2f}bps"
            else:
                spread_str = "    n/a"

            print(
                f"[{ts}] #{n:>5}  {pamm['name']:<10}  "
                f"({slot_count:>3} slots)  "
                f"bid={bid_str}  ask={ask_str}  spread={spread_str}"
            )


def main() -> None:
    p = argparse.ArgumentParser(description="pAMM quote simulator")
    p.add_argument("--eth-rpc-url", required=True,
                   help="Mainnet RPC URL for eth_call quote sims (must support state overrides)")
    p.add_argument("--ws", default=DEFAULT_WS,
                   help=f"pAMM state-diff WS URL (default: {DEFAULT_WS})")
    p.add_argument("--binance", default=DEFAULT_BINANCE,
                   help="Binance ticker URL for the SELL-leg sizing reference")
    p.add_argument("--notional-usd", type=float, default=100.0,
                   help="USD notional for both legs (default: 100)")
    p.add_argument("--auth-token", default=None,
                   help="Authorization header value for the WS (omit for unauthenticated)")
    p.add_argument("--no-auth", action="store_true",
                   help="Do not send an Authorization header")
    p.add_argument("--print-raw", action="store_true",
                   help="Print each raw pAMM wire frame before the quote summary")
    args = p.parse_args()
    auth_token = None if args.no_auth else args.auth_token
    try:
        asyncio.run(
            listen(args.eth_rpc_url, args.ws, args.binance, args.notional_usd, auth_token, args.print_raw)
        )
    except KeyboardInterrupt:
        print(f"\n[{now()}] interrupted")


if __name__ == "__main__":
    main()
