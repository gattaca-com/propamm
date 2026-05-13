#!/usr/bin/env python3
"""pAMM taker — wraps ETH, sets approvals, sends a swap bundle to Titan.

Targets FermiSwapper (default), Bebop, or KipseliGuard.

Reads the signer private key from ``PROP_AMM_TAKER_PRIVATE_KEY`` (hex, with or
without ``0x``). The mainnet RPC URL and Titan URL are passed via ``--eth-rpc-url``
and ``--titan-url``. The mainnet RPC must support ``eth_call`` with state and
block overrides (Alchemy / Infura / self-hosted).

## Examples

::

    export PROP_AMM_TAKER_PRIVATE_KEY=0x...

One-time setup (wrap ETH and grant ERC20 approvals to the target contract)::

    python taker.py --eth-rpc-url "$ETH_RPC_URL" \\
        --contract kipseli --setup-only

Dual-bundle setup grants approvals to both FermiSwapper and KipseliGuard::

    python taker.py --eth-rpc-url "$ETH_RPC_URL" \\
        --dual-bundle --setup-only

Single-shot dry-run (no ``--send``) — prints the calldata and tx hash that
would be submitted but does not POST to Titan::

    python taker.py --eth-rpc-url "$ETH_RPC_URL" \\
        --contract fermi --stream --skip-setup --once \\
        --pair weth/usdc --notional-usd 2

Continuous stream-gated $2 WETH/USDC swaps against Kipseli via Titan::

    python taker.py --eth-rpc-url "$ETH_RPC_URL" \\
        --contract kipseli --stream --send --skip-setup \\
        --pair weth/usdc --notional-usd 2 \\
        --min-priority-gwei 5 --interval-secs 3

Same against Fermi::

    python taker.py --eth-rpc-url "$ETH_RPC_URL" \\
        --contract fermi --stream --send --skip-setup \\
        --pair weth/usdc --notional-usd 2 \\
        --min-priority-gwei 5 --interval-secs 3

One bundle containing Kipseli + Fermi swaps. Live ``--dual-bundle --send``
requires ``--stream`` so both swaps are pre-simulated against matching block
state overrides::

    python taker.py --eth-rpc-url "$ETH_RPC_URL" \
        --dual-bundle --stream --send --skip-setup --once \
        --pair weth/usdc --notional-usd 1 \
        --min-priority-gwei 5

Same against Bebop::

    python taker.py --eth-rpc-url "$ETH_RPC_URL" \\
        --contract bebop --stream --send --skip-setup \\
        --pair weth/usdc --notional-usd 2 \\
        --min-priority-gwei 5 --interval-secs 3
"""

import argparse
import asyncio
import json
import os
import sys
import urllib.request
from enum import Enum
from typing import Optional, Tuple

import aiohttp
import websockets
from eth_abi import encode as abi_encode
from eth_account import Account
from eth_utils import keccak, to_checksum_address
from web3 import AsyncHTTPProvider, AsyncWeb3

TITAN_RPC_DEFAULT = "https://rpc.titanbuilder.xyz/"
BINANCE_TICKER = "https://api.binance.com/api/v3/ticker/price?symbol=ETHUSDC"

WETH = to_checksum_address("0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2")
USDC = to_checksum_address("0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48")
USDT = to_checksum_address("0xdac17f958d2ee523a2206206994597c13d831ec7")
FERMI_SWAPPER = to_checksum_address("0xb1076fe3ab5e28005c7c323bac5ac06a680d452e")
BEBOP = to_checksum_address("0x160141a205f5ddcf096ba3f48b7ed21eb52c62ea")
# Optional permissionless slippage-checking wrapper around the Kipseli pool;
# callers may bypass it and call KIPSELI_POOL directly.
KIPSELI_GUARD = to_checksum_address("0x9a7a5dccc7851c0f141d07c4d608a29b3830548b")
KIPSELI_POOL = to_checksum_address("0x5cdbe59400cc2efdcc2b54acca4a99fe00dd588c")

STABLE_DECIMALS = 6
BEBOP_EXPIRY_SECS = 120


def _selector(sig: str) -> bytes:
    return keccak(text=sig)[:4]


SEL_BALANCE_OF = _selector("balanceOf(address)")
SEL_ALLOWANCE = _selector("allowance(address,address)")
SEL_APPROVE = _selector("approve(address,uint256)")
SEL_DEPOSIT = _selector("deposit()")
SEL_FERMI_SWAP = _selector(
    "fermiSwapWithAllowances(address,address,int256,uint256,address)"
)
SEL_BEBOP_SWAP = _selector("swap(address,address,uint256,uint256,uint256)")
SEL_KIPSELI_SWAP = _selector("swap(address,uint256,address,uint256)")


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
            Contract.BEBOP: BEBOP,
            Contract.KIPSELI: KIPSELI_GUARD,
        }[self]

    @property
    def gas_limit(self) -> int:
        return 600_000 if self is Contract.KIPSELI else 300_000

    @property
    def stream_key(self) -> Optional[str]:
        return {
            Contract.FERMI: FERMI_SWAPPER,
            Contract.KIPSELI: KIPSELI_POOL,
            Contract.BEBOP: BEBOP,
        }[self]


class Pair(str, Enum):
    WETH_USDC = "weth/usdc"
    WETH_USDT = "weth/usdt"


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


class StateStream:
    """Latest-value channel mirroring `tokio::sync::watch` semantics."""

    def __init__(self) -> None:
        self._value = None
        self._version = 0
        self._seen = 0
        self._event = asyncio.Event()

    def set(self, value) -> None:
        self._value = value
        self._version += 1
        self._event.set()

    async def changed(self) -> None:
        while self._seen == self._version:
            self._event.clear()
            await self._event.wait()

    def borrow_and_update(self):
        self._seen = self._version
        return self._value


BOX_INNER = 95
ETHERSCAN_TX = "https://etherscan.io/tx/"


def print_landed_banner(label: str, tx_hash: str, nonce: int, block: int, ok: bool) -> None:
    if not ok:
        print(f"[reverted] {label} nonce={nonce} block={block} tx={tx_hash}")
        return
    border = "\033[32m"
    word_color = "\033[1;32m"
    reset = "\033[0m"
    word = "LANDED"
    bare1 = f"  🚀  {word}  {label}  block={block}  nonce={nonce}"
    prefix = "  ↳ "
    url = f"{ETHERSCAN_TX}{tx_hash}"
    max_url = BOX_INNER - len(prefix)
    if len(url) > max_url:
        url = url[: max_url - 3] + "..."
    bare2 = prefix + url
    # The rocket glyph renders as 2 visual columns but counts as 1 char,
    # so pad line1 to one less than line2.
    padded1 = bare1.ljust(BOX_INNER - 1).replace(word, f"{word_color}{word}{reset}", 1)
    padded2 = bare2.ljust(BOX_INNER)
    bar = "━" * BOX_INNER
    print(f"{border}┏{bar}┓{reset}")
    print(f"{border}┃{reset}{padded1}{border}┃{reset}")
    print(f"{border}┃{reset}{padded2}{border}┃{reset}")
    print(f"{border}┗{bar}┛{reset}")


class TxMonitor:
    """Polls submitted tx hashes for receipts and prints a banner on landing.

    Also detects stale-nonce hashes — when the chain's latest nonce has
    advanced past a tracked tx's nonce without including it, the hash will
    never land (a different tx at that nonce won inclusion). We drop it and
    print a `[dropped]` line so the user knows.
    """

    def __init__(self, w3: AsyncWeb3, sender: str, poll_secs: float = 1.0) -> None:
        self._w3 = w3
        self._sender = sender
        self._poll_secs = poll_secs
        self._pending: dict[str, dict] = {}

    def track(self, tx_hash: str, label: str, nonce: int) -> None:
        self._pending.setdefault(tx_hash, {"label": label, "nonce": nonce})

    async def run(self) -> None:
        while True:
            await asyncio.sleep(self._poll_secs)
            if not self._pending:
                continue
            try:
                latest_count = await self._w3.eth.get_transaction_count(
                    self._sender, "latest"
                )
            except Exception:
                latest_count = None
            for h in list(self._pending):
                try:
                    resp = await self._w3.provider.make_request(
                        "eth_getTransactionReceipt", [h]
                    )
                    receipt = resp.get("result")
                except Exception:
                    receipt = None
                if receipt:
                    meta = self._pending.pop(h, None)
                    if meta is None:
                        continue
                    status = int(receipt.get("status", "0x0"), 16)
                    block = int(receipt.get("blockNumber", "0x0"), 16)
                    print_landed_banner(meta["label"], h, meta["nonce"], block, status == 1)
                elif latest_count is not None and self._pending[h]["nonce"] < latest_count:
                    meta = self._pending.pop(h)
                    print(
                        f"[dropped] {meta['label']} nonce={meta['nonce']} "
                        f"chain_at={latest_count} tx={h}"
                    )


def usd_to_units(usd: float, decimals: int) -> int:
    return int(usd * 10**decimals)


def fetch_binance_mid() -> float:
    r = urllib.request.urlopen(BINANCE_TICKER, timeout=10)
    return float(json.loads(r.read())["price"])


async def refresh_binance_mid(args, refresh_secs: float = 1.0) -> None:
    while True:
        await asyncio.sleep(refresh_secs)
        try:
            args.eth_price_usd = await asyncio.to_thread(fetch_binance_mid)
            print(f"[binance] eth/usdc = {args.eth_price_usd:.2f}")
        except Exception as e:
            print(f"[binance] refresh failed: {e}")


def encode_call(sel: bytes, types: list, values: list) -> bytes:
    return sel + abi_encode(types, values)


def cd_balance_of(account: str) -> bytes:
    return encode_call(SEL_BALANCE_OF, ["address"], [account])


def cd_allowance(owner: str, spender: str) -> bytes:
    return encode_call(SEL_ALLOWANCE, ["address", "address"], [owner, spender])


def cd_approve(spender: str, value: int) -> bytes:
    return encode_call(SEL_APPROVE, ["address", "uint256"], [spender, value])


def cd_fermi_swap(
    token_in: str, token_out: str, amount_specified: int, amount_check: int, recipient: str
) -> bytes:
    return encode_call(
        SEL_FERMI_SWAP,
        ["address", "address", "int256", "uint256", "address"],
        [token_in, token_out, amount_specified, amount_check, recipient],
    )


def cd_bebop_swap(
    token_in: str, token_out: str, amount_in: int, min_amount_out: int, expiry: int
) -> bytes:
    return encode_call(
        SEL_BEBOP_SWAP,
        ["address", "address", "uint256", "uint256", "uint256"],
        [token_in, token_out, amount_in, min_amount_out, expiry],
    )


def cd_kipseli_swap(
    token_in: str, amount_in: int, token_out: str, min_out: int
) -> bytes:
    return encode_call(
        SEL_KIPSELI_SWAP,
        ["address", "uint256", "address", "uint256"],
        [token_in, amount_in, token_out, min_out],
    )


def _raw_tx(signed) -> bytes:
    raw = getattr(signed, "raw_transaction", None)
    if raw is None:
        raw = signed.rawTransaction
    return bytes(raw)


async def read_uint256(w3: AsyncWeb3, to: str, data: bytes) -> int:
    result = await w3.eth.call({"to": to, "data": data})
    return int.from_bytes(result, "big")


async def print_balances(w3: AsyncWeb3, me: str) -> None:
    eth = await w3.eth.get_balance(me)
    weth = await read_uint256(w3, WETH, cd_balance_of(me))
    usdc = await read_uint256(w3, USDC, cd_balance_of(me))
    usdt = await read_uint256(w3, USDT, cd_balance_of(me))
    print(f"balances: ETH={eth} WETH={weth} USDC={usdc}(1e6) USDT={usdt}(1e6)")


async def estimate_eip1559(w3: AsyncWeb3) -> Tuple[int, int]:
    block = await w3.eth.get_block("latest")
    base_fee = block.get("baseFeePerGas") or 0
    try:
        max_priority = await w3.eth.max_priority_fee
    except Exception:
        max_priority = 1_500_000_000
    return base_fee * 2 + max_priority, max_priority


async def send_with_account(w3: AsyncWeb3, account, tx: dict) -> dict:
    tx = dict(tx)
    tx.setdefault("from", account.address)
    tx.setdefault("type", 2)
    tx.setdefault("value", 0)
    if "nonce" not in tx:
        tx["nonce"] = await w3.eth.get_transaction_count(account.address, "pending")
    if "chainId" not in tx:
        tx["chainId"] = await w3.eth.chain_id
    if "maxFeePerGas" not in tx:
        max_fee, max_prio = await estimate_eip1559(w3)
        tx["maxFeePerGas"] = max_fee
        tx["maxPriorityFeePerGas"] = max_prio
    if "gas" not in tx:
        tx["gas"] = await w3.eth.estimate_gas(tx)
    signed = account.sign_transaction(tx)
    tx_hash = await w3.eth.send_raw_transaction(_raw_tx(signed))
    return await w3.eth.wait_for_transaction_receipt(tx_hash)


async def setup(w3: AsyncWeb3, account, args) -> None:
    me = account.address
    spenders = (
        (
            (FERMI_SWAPPER, Contract.FERMI.label),
            (KIPSELI_GUARD, Contract.KIPSELI.label),
        )
        if args.dual_bundle
        else ((args.contract.address, args.contract.label),)
    )
    weth_target = 100 * 10**18
    stable_target = 100_000 * 10**STABLE_DECIMALS
    for spender, label in spenders:
        for token, sym, target in (
            (WETH, "WETH", weth_target),
            (USDC, "USDC", stable_target),
            (USDT, "USDT", stable_target),
        ):
            allowance = await read_uint256(w3, token, cd_allowance(me, spender))
            if allowance >= target:
                print(f"[setup] {sym} already approved -> {label}")
                continue
            print(f"[setup] approving {sym} -> {label}")
            receipt = await send_with_account(
                w3, account, {"to": token, "data": cd_approve(spender, target)}
            )
            print(f"[setup]   {sym} approve mined block={receipt.get('blockNumber')}")

    weth_bal = await read_uint256(w3, WETH, cd_balance_of(me))
    target_wei = int(args.target_weth * 1e18)
    if weth_bal >= target_wei:
        return
    eth_bal = await w3.eth.get_balance(me)
    reserve_wei = int(args.reserve_eth * 1e18)
    if eth_bal <= reserve_wei:
        raise RuntimeError(f"ETH {eth_bal} <= reserve {reserve_wei}, cannot wrap")
    need = target_wei - weth_bal
    available = eth_bal - reserve_wei
    if available < need:
        raise RuntimeError(
            f"ETH available {available} < need {need} to reach target WETH"
        )
    wrap_amt = max(min(available, need * 2), need)
    print(f"[setup] wrapping {wrap_amt} wei ETH -> WETH")
    receipt = await send_with_account(
        w3, account, {"to": WETH, "value": wrap_amt, "data": SEL_DEPOSIT}
    )
    print(f"[setup]   wrap mined block={receipt.get('blockNumber')}")


def build_signed_tx(
    account,
    chain_id: int,
    nonce: int,
    gas_limit: int,
    max_fee: int,
    max_priority: int,
    to: str,
    data: bytes,
) -> Tuple[str, str]:
    tx = {
        "chainId": chain_id,
        "nonce": nonce,
        "gas": gas_limit,
        "maxFeePerGas": max_fee,
        "maxPriorityFeePerGas": max_priority,
        "to": to,
        "value": 0,
        "data": data,
        "type": 2,
    }
    signed = account.sign_transaction(tx)
    raw = _raw_tx(signed)
    return "0x" + raw.hex(), "0x" + keccak(raw).hex()


def trade_params(args, iter_: int):
    stable = Stable.USDC if args.pair is Pair.WETH_USDC else Stable.USDT
    direction = (
        Direction.STABLE_TO_WETH if iter_ % 2 == 0 else Direction.WETH_TO_STABLE
    )

    stable_units = usd_to_units(args.notional_usd, STABLE_DECIMALS)
    weth_units = int(usd_to_units(args.notional_usd, 18) / args.eth_price_usd)
    if direction is Direction.STABLE_TO_WETH:
        token_in, token_out = stable.addr, WETH
        amount_in, expected_out = stable_units, weth_units
        label = f"{stable.sym}->WETH"
    else:
        token_in, token_out = WETH, stable.addr
        amount_in, expected_out = weth_units, stable_units
        label = f"WETH->{stable.sym}"

    bps = 10_000
    slip = args.slippage_bps
    slip_lo = max(10_000 - slip, 0)
    slip_hi = 10_000 + slip
    min_out = expected_out * slip_lo // bps
    return (
        direction,
        token_in,
        token_out,
        amount_in,
        stable_units,
        weth_units,
        slip_lo,
        slip_hi,
        min_out,
        label,
    )


async def build_swap_calldata(
    w3: AsyncWeb3,
    contract: Contract,
    direction: Direction,
    token_in: str,
    token_out: str,
    amount_in: int,
    min_out: int,
    stable_units: int,
    weth_units: int,
    slip_lo: int,
    slip_hi: int,
    recipient: str,
) -> bytes:
    bps = 10_000
    if contract is Contract.FERMI:
        # Fermi's amountSpecified is signed: positive = exact tokenIn, negative
        # = exact tokenOut. Trade is always denominated in stable units, so flip
        # the sign on WETH-input legs to mean "exact stable output".
        if direction is Direction.STABLE_TO_WETH:
            amount_specified = stable_units
            amount_check = weth_units * slip_lo // bps
        else:
            amount_specified = -stable_units
            amount_check = weth_units * slip_hi // bps
        return cd_fermi_swap(
            token_in, token_out, amount_specified, amount_check, recipient
        )
    if contract is Contract.BEBOP:
        pending = await w3.eth.get_block("pending")
        expiry = pending["timestamp"] + BEBOP_EXPIRY_SECS
        return cd_bebop_swap(token_in, token_out, amount_in, min_out, expiry)
    return cd_kipseli_swap(token_in, amount_in, token_out, min_out)


async def next_state_frame(state: StateStream):
    await state.changed()
    frame = state.borrow_and_update()
    while frame is None:
        await state.changed()
        frame = state.borrow_and_update()
    return frame


async def state_sim_ok(
    w3: AsyncWeb3,
    me: str,
    to: str,
    calldata: bytes,
    frame,
    prefix: str,
) -> bool:
    block_number, timestamp_secs, state_override = frame
    call = {
        "from": me,
        "to": to,
        "data": "0x" + calldata.hex(),
    }
    block_overrides = {
        "number": hex(block_number),
        "time": hex(timestamp_secs),
    }
    params = [call, "latest", state_override, block_overrides]
    response = await w3.provider.make_request("eth_call", params)
    if response.get("error"):
        print(f"{prefix} state-override sim reverts, skipping: {response['error']}")
        return False
    print(f"{prefix} state-override sim ok @ block {block_number}")
    return True


async def dual_preflight_ok(
    w3: AsyncWeb3,
    me: str,
    direction: Direction,
    token_in: str,
    stable_units: int,
    weth_units: int,
    slip_hi: int,
    max_fee: int,
) -> bool:
    gas_budget = (Contract.KIPSELI.gas_limit + Contract.FERMI.gas_limit) * max_fee
    eth_balance = await w3.eth.get_balance(me)
    if eth_balance < gas_budget:
        print(f"[dual] ETH {eth_balance} < gas budget {gas_budget}, skipping")
        return False
    if direction is Direction.STABLE_TO_WETH:
        stable_balance = await read_uint256(w3, token_in, cd_balance_of(me))
        needed = stable_units * 2
        if stable_balance < needed:
            print(
                f"[dual] stable balance {stable_balance} < dual input {needed}, skipping"
            )
            return False
    else:
        weth_balance = await read_uint256(w3, WETH, cd_balance_of(me))
        fermi_max_input = weth_units * slip_hi // 10_000
        needed = weth_units + fermi_max_input
        if weth_balance < needed:
            print(
                f"[dual] WETH balance {weth_balance} < dual max input {needed}, skipping"
            )
            return False
    return True


async def send_titan_bundle(
    http: aiohttp.ClientSession, titan_url: str, raw_txs: list, prefix: str
) -> None:
    # Titan accepts `blockNumber: 0x0` as "include in any block within validity".
    body = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "eth_sendBundle",
        "params": [{"txs": raw_txs, "blockNumber": "0x0"}],
    }
    async with http.post(titan_url, json=body) as resp:
        text = await resp.text()
        print(f"{prefix} titan status={resp.status} body={text}")


async def trade_once(
    w3: AsyncWeb3,
    http: aiohttp.ClientSession,
    account,
    args,
    chain_id: int,
    iter_: int,
    state: Optional[StateStream],
    watcher: Optional[TxMonitor],
) -> None:
    me = account.address
    nonce = await w3.eth.get_transaction_count(me, "pending")
    max_fee, max_priority = await estimate_eip1559(w3)
    max_priority = max(max_priority, args.min_priority_gwei * 1_000_000_000)
    max_fee = max(max_fee, max_priority * 3)

    (
        direction,
        token_in,
        token_out,
        amount_in,
        stable_units,
        weth_units,
        slip_lo,
        slip_hi,
        min_out,
        label,
    ) = trade_params(args, iter_)
    calldata = await build_swap_calldata(
        w3,
        args.contract,
        direction,
        token_in,
        token_out,
        amount_in,
        min_out,
        stable_units,
        weth_units,
        slip_lo,
        slip_hi,
        me,
    )

    if state is not None:
        frame = await next_state_frame(state)
        if not await state_sim_ok(
            w3, me, args.contract.address, calldata, frame, "[trade]"
        ):
            return

    raw_tx, tx_hash = build_signed_tx(
        account,
        chain_id,
        nonce,
        args.contract.gas_limit,
        max_fee,
        max_priority,
        args.contract.address,
        calldata,
    )
    print(
        f"[trade] {label} nonce={nonce} amount_in={amount_in} min_out={min_out} "
        f"max_fee={max_fee} max_prio={max_priority} tx_hash={tx_hash}"
    )
    if not args.send:
        return
    await send_titan_bundle(http, args.titan_url, [raw_tx], "[trade]")
    if watcher is not None:
        watcher.track(tx_hash, label, nonce)


async def trade_dual_once(
    w3: AsyncWeb3,
    http: aiohttp.ClientSession,
    account,
    args,
    chain_id: int,
    iter_: int,
    state: Optional[StateStream],
    watcher: Optional[TxMonitor],
) -> None:
    me = account.address
    nonce = await w3.eth.get_transaction_count(me, "pending")
    max_fee, max_priority = await estimate_eip1559(w3)
    max_priority = max(max_priority, args.min_priority_gwei * 1_000_000_000)
    max_fee = max(max_fee, max_priority * 3)

    (
        direction,
        token_in,
        token_out,
        amount_in,
        stable_units,
        weth_units,
        slip_lo,
        slip_hi,
        min_out,
        label,
    ) = trade_params(args, iter_)
    kipseli_calldata = await build_swap_calldata(
        w3,
        Contract.KIPSELI,
        direction,
        token_in,
        token_out,
        amount_in,
        min_out,
        stable_units,
        weth_units,
        slip_lo,
        slip_hi,
        me,
    )
    fermi_calldata = await build_swap_calldata(
        w3,
        Contract.FERMI,
        direction,
        token_in,
        token_out,
        amount_in,
        min_out,
        stable_units,
        weth_units,
        slip_lo,
        slip_hi,
        me,
    )

    if state is not None:
        (
            block_number,
            timestamp_secs,
            fermi_override,
            kipseli_override,
        ) = await next_state_frame(state)
        kipseli_ok = await state_sim_ok(
            w3,
            me,
            Contract.KIPSELI.address,
            kipseli_calldata,
            (block_number, timestamp_secs, kipseli_override),
            "[dual] kipseli",
        )
        fermi_ok = await state_sim_ok(
            w3,
            me,
            Contract.FERMI.address,
            fermi_calldata,
            (block_number, timestamp_secs, fermi_override),
            "[dual] fermi",
        )
        if not (kipseli_ok and fermi_ok):
            return

    if args.send and not await dual_preflight_ok(
        w3, me, direction, token_in, stable_units, weth_units, slip_hi, max_fee
    ):
        return

    raw_kipseli, kipseli_tx_hash = build_signed_tx(
        account,
        chain_id,
        nonce,
        Contract.KIPSELI.gas_limit,
        max_fee,
        max_priority,
        Contract.KIPSELI.address,
        kipseli_calldata,
    )
    raw_fermi, fermi_tx_hash = build_signed_tx(
        account,
        chain_id,
        nonce + 1,
        Contract.FERMI.gas_limit,
        max_fee,
        max_priority,
        Contract.FERMI.address,
        fermi_calldata,
    )
    print(
        f"[dual] {label} nonce={nonce}/{nonce + 1} amount_in={amount_in} "
        f"min_out={min_out} max_fee={max_fee} max_prio={max_priority} "
        f"kipseli_tx_hash={kipseli_tx_hash} fermi_tx_hash={fermi_tx_hash}"
    )
    if not args.send:
        return
    await send_titan_bundle(
        http, args.titan_url, [raw_kipseli, raw_fermi], "[dual]"
    )
    if watcher is not None:
        watcher.track(kipseli_tx_hash, f"{label} Kipseli", nonce)
        watcher.track(fermi_tx_hash, f"{label} Fermi", nonce + 1)


async def run_state_stream(
    region: StreamRegion, contract: str, state: StateStream
) -> None:
    url = region.ws_url
    contract_lc = contract.lower()
    while True:
        try:
            async with websockets.connect(url) as ws:
                print(f"[stream] connected: {url}")
                async for msg in ws:
                    if isinstance(msg, bytes):
                        continue
                    try:
                        v = json.loads(msg)
                    except ValueError:
                        continue
                    if not isinstance(v, dict):
                        continue
                    block_number = v.get("blockNumber")
                    if not isinstance(block_number, int):
                        continue
                    ts = v.get("timestamp")
                    timestamp_secs = (ts // 1_000_000_000) if isinstance(ts, int) else 0
                    for k, val in v.items():
                        if (
                            k.lower() == contract_lc
                            and isinstance(val, dict)
                            and "stateOverride" in val
                        ):
                            state.set(
                                (block_number, timestamp_secs, val["stateOverride"])
                            )
                            break
        except Exception as e:
            print(f"[stream] disconnected: {e}")
        await asyncio.sleep(5)


async def run_dual_state_stream(region: StreamRegion, state: StateStream) -> None:
    url = region.ws_url
    fermi_lc = FERMI_SWAPPER.lower()
    kipseli_lc = KIPSELI_POOL.lower()
    by_block = {}
    while True:
        try:
            async with websockets.connect(url) as ws:
                print(f"[stream] connected: {url}")
                async for msg in ws:
                    if isinstance(msg, bytes):
                        continue
                    try:
                        v = json.loads(msg)
                    except ValueError:
                        continue
                    if not isinstance(v, dict):
                        continue
                    block_number = v.get("blockNumber")
                    if not isinstance(block_number, int):
                        continue
                    ts = v.get("timestamp")
                    timestamp_secs = (
                        (ts // 1_000_000_000) if isinstance(ts, int) else 0
                    )
                    block_state = by_block.setdefault(
                        block_number, {"timestamp_secs": timestamp_secs}
                    )
                    for k, val in v.items():
                        if not isinstance(k, str) or not isinstance(val, dict):
                            continue
                        state_override = val.get("stateOverride")
                        if not isinstance(state_override, dict):
                            continue
                        k_lc = k.lower()
                        if k_lc == fermi_lc:
                            block_state["fermi"] = state_override
                        elif k_lc == kipseli_lc:
                            block_state["kipseli"] = state_override
                    if "fermi" in block_state and "kipseli" in block_state:
                        state.set(
                            (
                                block_number,
                                block_state["timestamp_secs"],
                                block_state["fermi"],
                                block_state["kipseli"],
                            )
                        )
                    for old_block in list(by_block):
                        if old_block < block_number - 2:
                            del by_block[old_block]
        except Exception as e:
            print(f"[stream] disconnected: {e}")
        await asyncio.sleep(5)


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


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="pAMM taker: wrap ETH, approve, and send a swap bundle to Titan."
    )
    p.add_argument(
        "--contract",
        type=_enum_arg(Contract, "contract"),
        default=Contract.FERMI,
        help="Target pAMM contract (fermi|bebop|kipseli).",
    )
    p.add_argument(
        "--dual-bundle",
        action="store_true",
        help="Submit one Titan bundle containing both Kipseli and Fermi swaps.",
    )
    p.add_argument(
        "--notional-usd", type=float, default=1.0,
        help="USD notional per trade (stable side).",
    )
    p.add_argument(
        "--pair",
        type=_enum_arg(Pair, "pair"),
        default=Pair.WETH_USDC,
        help="Trading pair: weth/usdc or weth/usdt.",
    )
    p.add_argument(
        "--slippage-bps", type=int, default=50,
        help="Slippage tolerance applied to amountCheck / minOut.",
    )
    p.add_argument(
        "--min-priority-gwei", type=int, default=1,
        help="Floor for max_priority_fee_per_gas.",
    )
    p.add_argument(
        "--interval-secs", type=int, default=12,
        help="Sleep between sends in continuous mode (seconds).",
    )
    p.add_argument(
        "--reserve-eth", type=float, default=0.05,
        help="ETH to keep unwrapped for gas during setup.",
    )
    p.add_argument(
        "--target-weth", type=float, default=0.02,
        help="Wrap ETH if WETH balance falls below this.",
    )
    p.add_argument(
        "--send", action="store_true",
        help="POST bundles to Titan. Without this, runs as a dry-run.",
    )
    p.add_argument(
        "--once", action="store_true", help="Run one iteration then exit.",
    )
    p.add_argument(
        "--setup-only", action="store_true",
        help="Run wrap + approvals then exit.",
    )
    p.add_argument(
        "--skip-setup", action="store_true",
        help="Skip wrap + approval setup transactions.",
    )
    p.add_argument(
        "--stream", action="store_true",
        help="Subscribe to Titan's pAMM state-diff WebSocket and pre-simulate "
             "each swap against the latest state override; skip submission when "
             "the sim reverts.",
    )
    p.add_argument(
        "--stream-region",
        type=_enum_arg(StreamRegion, "stream-region"),
        default=StreamRegion.EU,
        help="Region for the state-diff WebSocket (eu|ap|us).",
    )
    p.add_argument(
        "--eth-rpc-url", required=True,
        help="Mainnet RPC URL for view calls + state-override sims. Must support "
             "eth_call with state and block overrides.",
    )
    p.add_argument(
        "--titan-url", default=TITAN_RPC_DEFAULT,
        help="Titan bundle RPC URL for eth_sendBundle.",
    )
    args = p.parse_args()
    if args.setup_only and args.skip_setup:
        p.error("--setup-only conflicts with --skip-setup")
    if args.dual_bundle and args.contract is Contract.BEBOP:
        p.error(
            "--dual-bundle submits Kipseli + Fermi; Bebop is not part of this mode"
        )
    if args.dual_bundle and args.send and not args.stream:
        p.error("--dual-bundle --send requires --stream")
    return args


async def amain() -> None:
    args = parse_args()
    pk = os.environ.get("PROP_AMM_TAKER_PRIVATE_KEY")
    if not pk:
        sys.exit("PROP_AMM_TAKER_PRIVATE_KEY not set")
    pk = pk.strip()
    if pk.startswith("0x"):
        pk = pk[2:]
    account = Account.from_key(bytes.fromhex(pk))
    me = account.address

    w3 = AsyncWeb3(AsyncHTTPProvider(args.eth_rpc_url))
    chain_id = await w3.eth.chain_id
    if chain_id != 1:
        print(f"warning: chain_id={chain_id}, expected 1 (mainnet)")

    try:
        args.eth_price_usd = fetch_binance_mid()
    except Exception as e:
        sys.exit(f"failed to fetch ETH/USDC mid from Binance: {e}")
    asyncio.create_task(refresh_binance_mid(args))

    label = "Kipseli+Fermi" if args.dual_bundle else args.contract.label
    print(f"signer={me} chain_id={chain_id} contract={label}")
    await print_balances(w3, me)

    if not args.skip_setup:
        await setup(w3, account, args)
        await print_balances(w3, me)
        if args.setup_only:
            return

    state: Optional[StateStream] = None
    if args.stream:
        state = StateStream()
        if args.dual_bundle:
            asyncio.create_task(run_dual_state_stream(args.stream_region, state))
        else:
            key = args.contract.stream_key
            if key is None:
                raise RuntimeError(f"--stream not supported for {args.contract.label}")
            asyncio.create_task(run_state_stream(args.stream_region, key, state))

    watcher: Optional[TxMonitor] = None
    if args.send:
        watcher = TxMonitor(w3, me)
        asyncio.create_task(watcher.run())

    timeout = aiohttp.ClientTimeout(total=5)
    async with aiohttp.ClientSession(timeout=timeout) as http:
        iter_ = 0
        while True:
            try:
                if args.dual_bundle:
                    await trade_dual_once(
                        w3, http, account, args, chain_id, iter_, state, watcher
                    )
                else:
                    await trade_once(
                        w3, http, account, args, chain_id, iter_, state, watcher
                    )
            except Exception as e:
                print(f"[iter {iter_}] error: {e}")
            iter_ += 1
            if args.once:
                return
            await asyncio.sleep(args.interval_secs)


def main() -> None:
    try:
        asyncio.run(amain())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
