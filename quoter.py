#!/usr/bin/env python3
"""pAMM state-stream simulator.

This mirrors taker.py sizing, contract addresses, and stream-key handling
without signing or sending. Fermi, Bebop, and Kipseli use quote entry points
so simulated sizes are not limited by the caller's token balances.

Examples:

Fermi:

    python quoter.py \
        --eth-rpc-url "$ETH_RPC_URL" \
        --contract fermi \
        --pair weth/usdc \
        --notional-usd 1000

Kipseli:

    python quoter.py \
        --eth-rpc-url "$ETH_RPC_URL" \
        --contract kipseli \
        --stream-region ap \
        --pair weth/usdc \
        --notional-usd 1000

Bebop:

    python quoter.py \
        --eth-rpc-url "$ETH_RPC_URL" \
        --contract bebop \
        --stream-region us \
        --pair weth/usdc \
        --notional-usd 1000
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import urllib.request
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import Enum
from urllib.parse import urlsplit
from typing import Optional

import websockets
from eth_abi import encode as abi_encode
from eth_account import Account
from eth_utils import keccak, to_checksum_address
from web3 import AsyncHTTPProvider, AsyncWeb3

BINANCE_TICKER = "https://api.binance.com/api/v3/ticker/price?symbol=ETHUSDC"

WETH = to_checksum_address("0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2")
USDC = to_checksum_address("0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48")
USDT = to_checksum_address("0xdac17f958d2ee523a2206206994597c13d831ec7")
FERMI_SWAPPER = to_checksum_address("0xb1076fe3ab5e28005c7c323bac5ac06a680d452e")
BEBOP = to_checksum_address("0xbc60639345dfa607d73b74e88c2d54d8b8ad7cc3")
BEBOP_SWAPPER = to_checksum_address("0xdb13ad0fcd134e9c48f2fdaea8f6751a0f5349ca")
KIPSELI_GUARD = to_checksum_address("0x9a7a5dccc7851c0f141d07c4d608a29b3830548b")
KIPSELI_POOL = to_checksum_address("0x5cdbe59400cc2efdcc2b54acca4a99fe00dd588c")

STABLE_DECIMALS = 6

BEACON_GENESIS_TS = 1606824023
SECONDS_PER_SLOT = 12

DEFAULT_FROM = to_checksum_address("0x000000000000000000000000000000000000dead")


def _selector(sig: str) -> bytes:
    return keccak(text=sig)[:4]


SEL_FERMI_QUOTE = _selector("quoteAmounts(address,address,int256)")
SEL_BEBOP_QUOTE = _selector("quote(address,address,uint256)")
SEL_KIPSELI_QUOTE = _selector("quote(address,uint256,address)")
SEL_KIPSELI_SWAP_IMPL = _selector("swapImpl()")

CUSTOM_ERRORS = {
    "0x666a2814": "StaleUpdate()",
    "0x90b8ec18": "TransferFailed()",
}


class Contract(str, Enum):
    FERMI = "fermi"
    BEBOP = "bebop"
    KIPSELI = "kipseli"

    @property
    def label(self) -> str:
        return {
            Contract.FERMI: "FermiSwapper",
            Contract.BEBOP: "Bebop",
            Contract.KIPSELI: "Kipseli",
        }[self]

    @property
    def address(self) -> str:
        return {
            Contract.FERMI: FERMI_SWAPPER,
            Contract.BEBOP: BEBOP_SWAPPER,
            Contract.KIPSELI: KIPSELI_GUARD,
        }[self]

    @property
    def stream_key(self) -> str:
        return {
            Contract.FERMI: FERMI_SWAPPER,
            Contract.BEBOP: BEBOP,
            Contract.KIPSELI: KIPSELI_POOL,
        }[self]


class Pair(str, Enum):
    WETH_USDC = "weth/usdc"
    WETH_USDT = "weth/usdt"
    USDC_USDT = "usdc/usdt"


class Stable(str, Enum):
    USDC = "usdc"
    USDT = "usdt"

    @property
    def addr(self) -> str:
        return USDC if self is Stable.USDC else USDT

    @property
    def sym(self) -> str:
        return "USDC" if self is Stable.USDC else "USDT"


class Direction(str, Enum):
    STABLE_TO_WETH = "s2w"
    WETH_TO_STABLE = "w2s"


class StreamRegion(str, Enum):
    EU = "eu"
    AP = "ap"
    US = "us"

    @property
    def ws_url(self) -> str:
        return f"wss://{self.value}.rpc.titanbuilder.xyz/ws/pamm_quote_stream"


@dataclass(frozen=True)
class SwapPlan:
    contract: Contract
    label: str
    call_address: str
    token_in: str
    token_out: str
    base_token: str
    quote_token: str
    amount_in: int
    min_out: int
    calldata: bytes


@dataclass(frozen=True)
class SimResult:
    plan: SwapPlan
    status: str
    price: Optional[float]
    amount_out: Optional[int]


def now() -> str:
    return datetime.now(UTC).strftime("%H:%M:%S.%f")[:-3]


def redacted_url(url: str) -> str:
    try:
        parts = urlsplit(url)
    except ValueError:
        return "(invalid url)"
    if not parts.scheme or not parts.netloc:
        return "(redacted)"
    return f"{parts.scheme}://{parts.netloc}/..."


def usd_to_units(usd: float, decimals: int) -> int:
    return int(usd * 10**decimals)


def fetch_binance_mid(url: str = BINANCE_TICKER) -> float:
    r = urllib.request.urlopen(url, timeout=10)
    return float(json.loads(r.read())["price"])


def encode_call(sel: bytes, types: list, values: list) -> bytes:
    return sel + abi_encode(types, values)


def cd_fermi_quote(token_in: str, token_out: str, amount_specified: int) -> bytes:
    return encode_call(
        SEL_FERMI_QUOTE,
        ["address", "address", "int256"],
        [token_in, token_out, amount_specified],
    )


def cd_bebop_quote(token_in: str, token_out: str, amount_in: int) -> bytes:
    return encode_call(
        SEL_BEBOP_QUOTE,
        ["address", "address", "uint256"],
        [token_in, token_out, amount_in],
    )


def cd_kipseli_quote(token_in: str, amount_in: int, token_out: str) -> bytes:
    return encode_call(
        SEL_KIPSELI_QUOTE,
        ["address", "uint256", "address"],
        [token_in, amount_in, token_out],
    )


def token_decimals(token: str) -> int:
    return 18 if token.lower() == WETH.lower() else STABLE_DECIMALS


def pair_tokens(pair: Pair) -> tuple[str, str]:
    if pair is Pair.WETH_USDC:
        return WETH, USDC
    if pair is Pair.WETH_USDT:
        return WETH, USDT
    return USDC, USDT


def price_from_amounts(
    token_in: str,
    token_out: str,
    amount_in: int,
    amount_out: int,
    base_token: str,
    quote_token: str,
) -> float:
    in_h = amount_in / 10 ** token_decimals(token_in)
    out_h = amount_out / 10 ** token_decimals(token_out)
    if in_h <= 0 or out_h <= 0:
        raise ValueError("zero amount")
    token_in_lc = token_in.lower()
    token_out_lc = token_out.lower()
    base_lc = base_token.lower()
    quote_lc = quote_token.lower()
    if token_in_lc == base_lc and token_out_lc == quote_lc:
        return out_h / in_h
    if token_in_lc == quote_lc and token_out_lc == base_lc:
        return in_h / out_h
    raise ValueError("tokens do not match pair")


def quote_side(plan: SwapPlan) -> Optional[str]:
    token_in_lc = plan.token_in.lower()
    token_out_lc = plan.token_out.lower()
    base_lc = plan.base_token.lower()
    quote_lc = plan.quote_token.lower()
    if token_in_lc == base_lc and token_out_lc == quote_lc:
        return "sell"
    if token_in_lc == quote_lc and token_out_lc == base_lc:
        return "buy"
    return None


def format_rpc_error(err) -> str:
    if not isinstance(err, dict):
        return str(err)[:120]
    msg = err.get("message") or str(err)
    data = err.get("data")
    if isinstance(data, dict):
        data = data.get("data") or data.get("result")
    if isinstance(data, str) and data.startswith("0x"):
        name = CUSTOM_ERRORS.get(data[:10].lower())
        if name:
            return f"{name} data={data[:74]}"
        return f"{msg[:90]} data={data[:74]}"
    return msg[:120]


def parse_private_key_address() -> Optional[str]:
    pk = os.environ.get("PROP_AMM_TAKER_PRIVATE_KEY")
    if not pk:
        return None
    pk = pk.strip()
    if pk.startswith("0x"):
        pk = pk[2:]
    try:
        return Account.from_key(bytes.fromhex(pk)).address
    except Exception as e:
        sys.exit(f"failed to derive address from PROP_AMM_TAKER_PRIVATE_KEY: {e}")


def resolve_from_address(value: Optional[str]) -> tuple[str, bool]:
    if value:
        return to_checksum_address(value), False
    derived = parse_private_key_address()
    if derived:
        return derived, False
    return DEFAULT_FROM, True


async def read_address(w3: AsyncWeb3, to: str, data: bytes) -> str:
    response = await w3.provider.make_request(
        "eth_call", [{"to": to, "data": "0x" + data.hex()}, "latest"]
    )
    if response.get("error"):
        raise RuntimeError(format_rpc_error(response["error"]))
    result = response.get("result") or "0x"
    raw = bytes.fromhex(result.removeprefix("0x"))
    if len(raw) < 32:
        raise RuntimeError(f"short address response ({len(raw)} bytes)")
    return to_checksum_address("0x" + raw[-20:].hex())


async def quote_call_addresses(
    w3: AsyncWeb3, contracts: list[Contract]
) -> dict[Contract, str]:
    addresses = {contract: contract.address for contract in contracts}
    if Contract.KIPSELI in contracts:
        addresses[Contract.KIPSELI] = await read_address(
            w3, KIPSELI_POOL, SEL_KIPSELI_SWAP_IMPL
        )
    return addresses


def frame_block_number(frame: dict) -> Optional[int]:
    value = frame.get("blockNumber")
    if value is None:
        value = frame.get("block_number")
    return value if isinstance(value, int) else None


def frame_timestamp_secs(frame: dict) -> Optional[int]:
    slot = frame.get("slot")
    if isinstance(slot, int):
        return BEACON_GENESIS_TS + slot * SECONDS_PER_SLOT
    ts = frame.get("timestamp")
    if isinstance(ts, int):
        return ts // 1_000_000_000
    return None


def state_overrides_by_stream_key(frame: dict) -> dict[str, dict]:
    by_key: dict[str, dict] = {}
    for key, value in frame.items():
        if not isinstance(key, str) or not isinstance(value, dict):
            continue
        state_override = value.get("stateOverride")
        if state_override is None:
            state_override = value.get("state_override")
        if isinstance(state_override, dict):
            by_key[key.lower()] = state_override
    return by_key


def _enum_arg(cls, name: str):
    def parse(s: str):
        try:
            return cls(s.lower())
        except ValueError:
            valid = "|".join(m.value for m in cls)
            raise argparse.ArgumentTypeError(
                f"invalid {name} {s!r} (choices: {valid})"
            )

    return parse


def selected_contracts(value: str) -> list[Contract]:
    if value == "all":
        return list(Contract)
    return [Contract(value)]


def build_amounts(pair: Pair, direction: Direction, notional_usd: float, eth_price_usd: float):
    stable_pair = pair is Pair.USDC_USDT
    if stable_pair:
        src, dst = (
            (Stable.USDC, Stable.USDT)
            if direction is Direction.STABLE_TO_WETH
            else (Stable.USDT, Stable.USDC)
        )
        units = usd_to_units(notional_usd, STABLE_DECIMALS)
        return src.addr, dst.addr, units, units, f"{src.sym}->{dst.sym}", stable_pair

    stable = Stable.USDC if pair is Pair.WETH_USDC else Stable.USDT
    stable_units = usd_to_units(notional_usd, STABLE_DECIMALS)
    weth_units = int(usd_to_units(notional_usd, 18) / eth_price_usd)
    if direction is Direction.STABLE_TO_WETH:
        return stable.addr, WETH, stable_units, weth_units, f"{stable.sym}->WETH", stable_pair
    return WETH, stable.addr, weth_units, stable_units, f"WETH->{stable.sym}", stable_pair


async def build_swap_plan(
    contract: Contract,
    call_address: str,
    pair: Pair,
    direction: Direction,
    notional_usd: float,
    eth_price_usd: float,
    slippage_bps: int,
) -> SwapPlan:
    token_in, token_out, amount_in, expected_out, label, stable_pair = build_amounts(
        pair, direction, notional_usd, eth_price_usd
    )
    base_token, quote_token = pair_tokens(pair)

    bps = 10_000
    slip_lo = max(10_000 - slippage_bps, 0)
    min_out = expected_out * slip_lo // bps

    if contract is Contract.FERMI:
        if stable_pair:
            amount_specified = amount_in
        elif direction is Direction.STABLE_TO_WETH:
            stable_units = usd_to_units(notional_usd, STABLE_DECIMALS)
            amount_specified = stable_units
        else:
            stable_units = usd_to_units(notional_usd, STABLE_DECIMALS)
            amount_specified = -stable_units
        calldata = cd_fermi_quote(token_in, token_out, amount_specified)
    elif contract is Contract.BEBOP:
        calldata = cd_bebop_quote(token_in, token_out, amount_in)
    else:
        calldata = cd_kipseli_quote(token_in, amount_in, token_out)

    return SwapPlan(
        contract=contract,
        label=label,
        call_address=call_address,
        token_in=token_in,
        token_out=token_out,
        base_token=base_token,
        quote_token=quote_token,
        amount_in=amount_in,
        min_out=min_out,
        calldata=calldata,
    )


async def call_swap(
    w3: AsyncWeb3,
    from_address: str,
    plan: SwapPlan,
    state_override: dict,
    block_number: int,
    timestamp_secs: Optional[int],
) -> SimResult:
    call = {
        "from": from_address,
        "to": plan.call_address,
        "data": "0x" + plan.calldata.hex(),
    }
    block_overrides = {"number": hex(block_number)}
    if timestamp_secs is not None:
        block_overrides["time"] = hex(timestamp_secs)
    params = [call, "latest", state_override, block_overrides]
    response = await w3.provider.make_request("eth_call", params)
    if response.get("error"):
        return SimResult(
            plan, f"REVERT({format_rpc_error(response['error'])})", None, None
        )

    result = response.get("result") or "0x"
    raw = bytes.fromhex(result.removeprefix("0x"))
    if not raw:
        return SimResult(plan, "OK(empty)", None, None)
    if plan.contract is Contract.FERMI:
        if len(raw) < 64:
            return SimResult(plan, f"SHORT({len(raw)}b)", None, None)
        amount_in = int.from_bytes(raw[0:32], "big")
        amount_out = int.from_bytes(raw[32:64], "big")
    else:
        if len(raw) < 32:
            return SimResult(plan, f"SHORT({len(raw)}b)", None, None)
        amount_in = plan.amount_in
        amount_out = int.from_bytes(raw[0:32], "big")
    try:
        price = price_from_amounts(
            plan.token_in,
            plan.token_out,
            amount_in,
            amount_out,
            plan.base_token,
            plan.quote_token,
        )
    except ValueError:
        return SimResult(plan, f"ZERO(in={amount_in},out={amount_out})", None, amount_out)
    return SimResult(plan, f"OK(out={amount_out})", price, amount_out)


def spread_bps(sell_price: float, buy_price: float) -> float:
    mid = 0.5 * (sell_price + buy_price)
    return 10_000.0 * (buy_price - sell_price) / mid if mid else 0.0


def format_quote_line(
    sequence: int,
    contract: Contract,
    block_number: int,
    results: list[SimResult],
) -> str:
    by_side = {quote_side(result.plan): result for result in results}
    buy = by_side.get("buy")
    sell = by_side.get("sell")
    buy_ok = buy is not None and buy.status.startswith("OK") and buy.price is not None
    sell_ok = sell is not None and sell.status.startswith("OK") and sell.price is not None
    if buy_ok and sell_ok:
        return (
            f"[{now()}] #{sequence:>5} {contract.label:<11} block={block_number} "
            f"sell={sell.price:11.5f} buy={buy.price:11.5f} "
            f"spread={spread_bps(sell.price, buy.price):+8.2f}bps "
            f"buy_out={buy.amount_out} sell_out={sell.amount_out}"
        )
    sell_text = (
        f"{sell.price:11.5f}" if sell_ok else f"{sell.status if sell else 'MISSING':>24}"
    )
    buy_text = (
        f"{buy.price:11.5f}" if buy_ok else f"{buy.status if buy else 'MISSING':>24}"
    )
    return (
        f"[{now()}] #{sequence:>5} {contract.label:<11} block={block_number} "
        f"sell={sell_text} buy={buy_text} spread=     n/a"
    )


async def process_frame(
    w3: AsyncWeb3,
    frame: dict,
    contracts: list[Contract],
    quote_addresses: dict[Contract, str],
    from_address: str,
    pair: Pair,
    notional_usd: float,
    eth_price_usd: float,
    slippage_bps: int,
    sequence: int,
    print_misses: bool,
) -> bool:
    block_number = frame_block_number(frame)
    if block_number is None:
        return False
    timestamp_secs = frame_timestamp_secs(frame)
    by_key = state_overrides_by_stream_key(frame)
    touched = [
        contract
        for contract in contracts
        if contract.stream_key.lower() in by_key
    ]
    if not touched:
        if print_misses:
            seen = ",".join(sorted(by_key.keys())) if by_key else "-"
            print(f"[{now()}] #{sequence:>5} no configured contract (keys={seen})")
        return False

    any_success = False
    for contract in touched:
        state_override = by_key[contract.stream_key.lower()]
        results: list[SimResult] = []
        for direction in (Direction.STABLE_TO_WETH, Direction.WETH_TO_STABLE):
            plan = await build_swap_plan(
                contract,
                quote_addresses[contract],
                pair,
                direction,
                notional_usd,
                eth_price_usd,
                slippage_bps,
            )
            result = await call_swap(
                w3, from_address, plan, state_override, block_number, timestamp_secs
            )
            if contract is Contract.FERMI and "StaleUpdate()" in result.status:
                print(
                    f"[{now()}] #{sequence:>5} {contract.label:<11} "
                    f"block={block_number} stale update, waiting for complete quote state"
                )
                break
            results.append(result)
        if len(results) != 2:
            continue
        print(format_quote_line(sequence, contract, block_number, results))
        if all(r.status.startswith("OK") for r in results):
            any_success = True
    return any_success


async def listen(args: argparse.Namespace) -> None:
    contracts = selected_contracts(args.contract)
    from_address, placeholder_from = resolve_from_address(args.from_address)
    w3 = AsyncWeb3(AsyncHTTPProvider(args.eth_rpc_url))

    try:
        chain_id = await w3.eth.chain_id
    except Exception as e:
        sys.exit(f"failed to connect to RPC: {e}")

    try:
        quote_addresses = await quote_call_addresses(w3, contracts)
    except Exception as e:
        sys.exit(f"failed to resolve quote call target: {e}")

    try:
        eth_price_usd = fetch_binance_mid(args.binance)
    except Exception as e:
        sys.exit(f"failed to fetch ETH/USDC mid from Binance: {e}")

    print(f"[{now()}] rpc          = {redacted_url(args.eth_rpc_url)}")
    print(f"[{now()}] chain_id     = {chain_id}")
    print(f"[{now()}] ws           = {args.ws}")
    print(f"[{now()}] from         = {from_address}")
    if placeholder_from:
        print(
            f"[{now()}] warning      = --from-address not set and "
            "PROP_AMM_TAKER_PRIVATE_KEY not set; calls may revert"
        )
    print(f"[{now()}] pair         = {args.pair.value}")
    print(f"[{now()}] notional_usd = {args.notional_usd}")
    print(f"[{now()}] slippage_bps = {args.slippage_bps}")
    print(f"[{now()}] binance ETHUSDC = {eth_price_usd:.2f}")
    for contract in contracts:
        print(
            f"[{now()}] contract     = {contract.label:<11} "
            f"call={quote_addresses[contract].lower()} "
            f"stream_key={contract.stream_key.lower()}"
        )

    headers = None if args.no_auth or not args.auth_token else {"Authorization": args.auth_token}
    print(f"[{now()}] connecting to WS...")
    async with websockets.connect(
        args.ws, open_timeout=10, additional_headers=headers
    ) as ws:
        print(f"[{now()}] connected, streaming")
        sequence = 0
        matched = 0
        async for raw in ws:
            sequence += 1
            if isinstance(raw, bytes):
                continue
            try:
                frame = json.loads(raw)
            except json.JSONDecodeError as e:
                print(f"[{now()}] #{sequence:>5} parse error: {e}")
                continue
            if args.print_raw:
                print(frame)
            if not isinstance(frame, dict):
                print(f"[{now()}] #{sequence:>5} unexpected top-level frame")
                continue
            if await process_frame(
                w3,
                frame,
                contracts,
                quote_addresses,
                from_address,
                args.pair,
                args.notional_usd,
                eth_price_usd,
                args.slippage_bps,
                sequence,
                args.print_misses,
            ):
                matched += 1
                if args.once or (args.max_matches and matched >= args.max_matches):
                    return


async def amain() -> None:
    args = parse_args()
    if args.timeout_secs:
        async with asyncio.timeout(args.timeout_secs):
            await listen(args)
    else:
        await listen(args)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="pAMM state-stream simulator using taker.py sizing"
    )
    p.add_argument(
        "--contract",
        choices=["all"] + [c.value for c in Contract],
        default="all",
        help="Contract to simulate (all|fermi|bebop|kipseli).",
    )
    p.add_argument(
        "--pair",
        type=_enum_arg(Pair, "pair"),
        default=Pair.WETH_USDC,
        help="Trading pair: weth/usdc, weth/usdt, or usdc/usdt.",
    )
    p.add_argument(
        "--notional-usd",
        type=float,
        default=100.0,
        help="USD notional per simulated leg.",
    )
    p.add_argument(
        "--slippage-bps",
        type=int,
        default=50,
        help="Slippage tolerance used for amountCheck / minOut.",
    )
    p.add_argument(
        "--from-address",
        default=None,
        help="EOA used as eth_call from/recipient. Defaults to "
             "PROP_AMM_TAKER_PRIVATE_KEY's address, then 0x...dead.",
    )
    p.add_argument(
        "--eth-rpc-url",
        required=True,
        help="Mainnet RPC URL for eth_call sims. Must support state and block overrides.",
    )
    p.add_argument(
        "--stream-region",
        type=_enum_arg(StreamRegion, "stream-region"),
        default=StreamRegion.EU,
        help="Region for the state-diff WebSocket (eu|ap|us).",
    )
    p.add_argument(
        "--ws",
        default=None,
        help="Override the state-diff WebSocket URL.",
    )
    p.add_argument(
        "--binance",
        default=BINANCE_TICKER,
        help="Binance ticker URL for ETH/USDC sizing.",
    )
    p.add_argument(
        "--auth-token",
        default=None,
        help="Authorization header value for the WS.",
    )
    p.add_argument(
        "--no-auth",
        action="store_true",
        help="Do not send an Authorization header.",
    )
    p.add_argument(
        "--print-raw",
        action="store_true",
        help="Print each raw pAMM wire frame before the sim summary.",
    )
    p.add_argument(
        "--print-misses",
        action="store_true",
        help="Print stream frames that do not match the selected contract.",
    )
    p.add_argument(
        "--once",
        action="store_true",
        help="Exit after the first matching state-diff frame with a successful sim.",
    )
    p.add_argument(
        "--max-matches",
        type=int,
        default=0,
        help="Exit after this many matching frames (0 means unlimited).",
    )
    p.add_argument(
        "--timeout-secs",
        type=float,
        default=0,
        help="Exit after this many seconds (0 means unlimited).",
    )
    args = p.parse_args()
    args.ws = args.ws or args.stream_region.ws_url
    return args


def main() -> None:
    try:
        asyncio.run(amain())
    except TimeoutError:
        print(f"[{now()}] timeout")
    except KeyboardInterrupt:
        print(f"\n[{now()}] interrupted")


if __name__ == "__main__":
    main()
