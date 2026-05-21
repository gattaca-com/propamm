//! pAMM taker — wraps ETH, sets approvals, sends a swap bundle to Titan.
//!
//! Targets FermiSwapper (default), Bebop, or KipseliGuard.
//!
//! Env: `PROP_AMM_TAKER_PRIVATE_KEY` (signer hex, mainnet-funded EOA),
//! `ETH_RPC_URL` (required; must support state + block overrides on
//! `eth_call`), optional `TITAN_RPC_URL`.
//!
//! ## Examples
//!
//! The mainnet RPC must support `eth_call` with state and block overrides
//! (Alchemy/Infura/self-hosted). Pass it via `--eth-rpc-url` or `ETH_RPC_URL`.
//! Export the credentials once for the shell so they're picked up by every
//! invocation below:
//! ```text
//!   export PROP_AMM_TAKER_PRIVATE_KEY=0x...
//!   export ETH_RPC_URL=https://eth-mainnet.example/<key>
//! ```
//!
//! One-time setup (wrap ETH and grant ERC20 approvals to the target contract):
//! ```text
//!   cargo run --release -- \
//!       --eth-rpc-url "$ETH_RPC_URL" \
//!       --contract kipseli --setup-only
//! ```
//!
//! Single-shot dry-run (no `--send`) — prints the calldata and tx hash that
//! would be submitted but does not POST to Titan:
//! ```text
//!   cargo run --release -- \
//!       --eth-rpc-url "$ETH_RPC_URL" \
//!       --contract fermi --stream --skip-setup --once \
//!       --pair weth/usdc --notional-usd 2 --eth-price-usd 2290
//! ```
//!
//! Continuous stream-gated $2 WETH/USDC swaps against Kipseli via Titan:
//! ```text
//!   cargo run --release -- \
//!       --eth-rpc-url "$ETH_RPC_URL" \
//!       --contract kipseli --stream --send --skip-setup \
//!       --pair weth/usdc --notional-usd 2 \
//!       --min-priority-gwei 5 --interval-secs 3 --eth-price-usd 2290
//! ```
//!
//! Same against Fermi:
//! ```text
//!   cargo run --release -- \
//!       --eth-rpc-url "$ETH_RPC_URL" \
//!       --contract fermi --stream --send --skip-setup \
//!       --pair weth/usdc --notional-usd 2 \
//!       --min-priority-gwei 5 --interval-secs 3 --eth-price-usd 2290
//! ```
//!
//! Bebop is not on the public pAMM stream — run without `--stream`:
//! ```text
//!   cargo run --release -- \
//!       --eth-rpc-url "$ETH_RPC_URL" \
//!       --contract bebop --send --skip-setup \
//!       --pair weth/usdc --notional-usd 2 \
//!       --min-priority-gwei 5 --eth-price-usd 2290
//! ```

use std::{env, time::Duration};

use alloy_consensus::{SignableTransaction, TxEip1559, TxEnvelope};
use alloy_eips::eip2718::Encodable2718;
use alloy_network::{EthereumWallet, TransactionBuilder, TxSignerSync};
use alloy_primitives::{Address, B256, Bytes, I256, TxKind, U256, address, hex, keccak256};
use alloy_provider::{Provider, ProviderBuilder};
use alloy_rpc_types::{BlockId, TransactionRequest};
use alloy_signer_local::PrivateKeySigner;
use alloy_sol_types::{SolCall, sol};
use anyhow::{Context, Result, bail};
use clap::{Parser, ValueEnum};
use futures::StreamExt;
use serde_json::{Value, json};
use tokio::sync::watch;
use tokio_tungstenite::{connect_async, tungstenite::Message};

const TITAN_RPC_DEFAULT: &str = "https://rpc.titanbuilder.xyz/";

const WETH: Address = address!("c02aaa39b223fe8d0a0e5c4f27ead9083c756cc2");
const USDC: Address = address!("a0b86991c6218b36c1d19d4a2e9eb0ce3606eb48");
const USDT: Address = address!("dac17f958d2ee523a2206206994597c13d831ec7");
const FERMI_SWAPPER: Address = address!("b1076fe3ab5e28005c7c323bac5ac06a680d452e");
const BEBOP: Address = address!("160141a205f5ddcf096ba3f48b7ed21eb52c62ea");
// Optional permissionless slippage-checking wrapper around the Kipseli pool;
// callers may bypass it and call KIPSELI_POOL directly.
const KIPSELI_GUARD: Address = address!("9a7a5dccc7851c0f141d07c4d608a29b3830548b");
const KIPSELI_POOL: Address = address!("5cdbe59400cc2efdcc2b54acca4a99fe00dd588c");

const STABLE_DECIMALS: u32 = 6;
const BEBOP_EXPIRY_SECS: u64 = 120;

type StateStreamFrame = (u64, u64, Value);
type StateStreamRx = watch::Receiver<Option<StateStreamFrame>>;
type StateStreamTx = watch::Sender<Option<StateStreamFrame>>;

sol! {
    interface IERC20 {
        function balanceOf(address account) external view returns (uint256);
        function allowance(address owner, address spender) external view returns (uint256);
        function approve(address spender, uint256 value) external returns (bool);
    }
    interface IWETH9 { function deposit() external payable; }

    interface IFermiSwapper {
        function fermiSwapWithAllowances(
            address tokenIn,
            address tokenOut,
            int256 amountSpecified,
            uint256 amountCheck,
            address recipient,
        ) external returns (uint256 amountIn, uint256 amountOut);
    }
    interface IBebop {
        function swap(
            address tokenIn,
            address tokenOut,
            uint256 amountIn,
            uint256 minAmountOut,
            uint256 expiry,
        ) external payable;
    }
    interface IKipseliGuard {
        function swap(
            address tokenIn,
            uint256 amountIn,
            address tokenOut,
            uint256 minOut,
        ) external returns (uint256 received);
    }
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum Contract {
    Fermi,
    Bebop,
    Kipseli,
}

impl Contract {
    fn label(self) -> &'static str {
        match self {
            Self::Fermi => "FermiSwapper",
            Self::Bebop => "Bebop",
            Self::Kipseli => "Kipseli",
        }
    }
    fn address(self) -> Address {
        match self {
            Self::Fermi => FERMI_SWAPPER,
            Self::Bebop => BEBOP,
            Self::Kipseli => KIPSELI_GUARD,
        }
    }
    fn gas_limit(self) -> u64 {
        match self {
            Self::Kipseli => 600_000,
            _ => 300_000,
        }
    }

    /// Key under which this contract's state-diff frames are indexed on Titan's
    /// public stream. For Kipseli the stream keys on the pool, not the guard
    /// wrapper we call. `None` means the contract is not on the public feed.
    fn stream_key(self) -> Option<Address> {
        match self {
            Self::Fermi => Some(FERMI_SWAPPER),
            Self::Kipseli => Some(KIPSELI_POOL),
            Self::Bebop => None,
        }
    }
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum Pair {
    #[value(name = "weth/usdc")]
    WethUsdc,
    #[value(name = "weth/usdt")]
    WethUsdt,
}

#[derive(Clone, Copy)]
enum Stable {
    Usdc,
    Usdt,
}

impl Stable {
    fn addr(self) -> Address {
        match self {
            Self::Usdc => USDC,
            Self::Usdt => USDT,
        }
    }
    fn sym(self) -> &'static str {
        match self {
            Self::Usdc => "USDC",
            Self::Usdt => "USDT",
        }
    }
}

#[derive(Clone, Copy)]
enum Direction {
    StableToWeth,
    WethToStable,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum StreamRegion {
    Eu,
    Ap,
    Us,
}

impl StreamRegion {
    fn ws_url(self) -> &'static str {
        match self {
            Self::Eu => "wss://eu.rpc.titanbuilder.xyz/ws/pamm_quote_stream",
            Self::Ap => "wss://ap.rpc.titanbuilder.xyz/ws/pamm_quote_stream",
            Self::Us => "wss://us.rpc.titanbuilder.xyz/ws/pamm_quote_stream",
        }
    }
}

#[derive(Parser, Debug)]
#[command(about = "pAMM taker: wrap ETH, approve, and send a swap bundle to Titan.")]
struct Cli {
    /// Target pAMM contract.
    #[arg(long, value_enum, default_value_t = Contract::Fermi)]
    contract: Contract,

    /// USD notional per trade (stable side).
    #[arg(long, default_value_t = 1.0)]
    notional_usd: f64,

    /// Trading pair (WETH against the chosen stable).
    #[arg(long, value_enum, default_value_t = Pair::WethUsdc)]
    pair: Pair,

    /// Slippage tolerance applied to amountCheck / minOut.
    #[arg(long, default_value_t = 30)]
    slippage_bps: u32,

    /// Reference ETH price (USD), used to size the WETH leg.
    #[arg(long, default_value_t = 2290)]
    eth_price_usd: u64,

    /// Floor for max_priority_fee_per_gas.
    #[arg(long, default_value_t = 1)]
    min_priority_gwei: u128,

    /// Sleep between sends in continuous mode (seconds).
    #[arg(long, default_value_t = 12)]
    interval_secs: u64,

    /// ETH to keep unwrapped for gas during setup.
    #[arg(long, default_value_t = 0.05)]
    reserve_eth: f64,

    /// Wrap ETH if WETH balance falls below this.
    #[arg(long, default_value_t = 0.02)]
    target_weth: f64,

    /// POST bundles to Titan. Without this, runs as a dry-run.
    #[arg(long)]
    send: bool,

    /// Run one iteration then exit.
    #[arg(long)]
    once: bool,

    /// Run wrap + approvals then exit.
    #[arg(long, conflicts_with = "skip_setup")]
    setup_only: bool,

    /// Skip wrap + approval setup transactions.
    #[arg(long)]
    skip_setup: bool,

    /// Subscribe to Titan's pAMM state-diff WebSocket and pre-simulate each
    /// swap against the latest state override; skip submission when the sim
    /// reverts.
    #[arg(long)]
    stream: bool,

    /// Region for the state-diff WebSocket.
    #[arg(long, value_enum, default_value_t = StreamRegion::Eu)]
    stream_region: StreamRegion,

    /// Mainnet RPC URL for view calls + state-override sims. Must support
    /// `eth_call` with state and block overrides (e.g., Alchemy, Infura,
    /// self-hosted Geth/Reth).
    #[arg(long)]
    eth_rpc_url: String,

    /// Titan bundle RPC URL for `eth_sendBundle`.
    #[arg(long, default_value = TITAN_RPC_DEFAULT)]
    titan_url: String,
}

fn usd_to_units(usd: f64, decimals: u32) -> U256 {
    U256::from((usd * 10f64.powi(decimals as i32)) as u128)
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Cli::parse();
    let pk =
        env::var("PROP_AMM_TAKER_PRIVATE_KEY").context("PROP_AMM_TAKER_PRIVATE_KEY not set")?;
    let signer: PrivateKeySigner = pk.trim().trim_start_matches("0x").parse()?;
    let me = signer.address();

    let provider = ProviderBuilder::new()
        .wallet(EthereumWallet::from(signer.clone()))
        .connect_http(args.eth_rpc_url.parse()?);
    let chain_id = provider.get_chain_id().await?;
    if chain_id != 1 {
        println!("warning: chain_id={chain_id}, expected 1 (mainnet)");
    }

    println!(
        "signer={me} chain_id={chain_id} contract={}",
        args.contract.label()
    );
    print_balances(&provider, me).await?;

    if !args.skip_setup {
        setup(&provider, me, &args).await?;
        print_balances(&provider, me).await?;
        if args.setup_only {
            return Ok(());
        }
    }

    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()?;
    let interval = Duration::from_secs(args.interval_secs);
    let mut stream_rx: Option<StateStreamRx> = if args.stream {
        let key = args
            .contract
            .stream_key()
            .with_context(|| format!("--stream not supported for {}", args.contract.label()))?;
        let (tx, rx) = watch::channel(None);
        tokio::spawn(run_state_stream(args.stream_region, key, tx));
        Some(rx)
    } else {
        None
    };
    let mut iter: u64 = 0;
    loop {
        let stable = match args.pair {
            Pair::WethUsdc => Stable::Usdc,
            Pair::WethUsdt => Stable::Usdt,
        };
        let dir = if iter.is_multiple_of(2) {
            Direction::StableToWeth
        } else {
            Direction::WethToStable
        };
        if let Err(e) = trade_once(
            &provider,
            &http,
            &signer,
            &args.titan_url,
            &args,
            chain_id,
            me,
            stable,
            dir,
            stream_rx.as_mut(),
        )
        .await
        {
            println!("[iter {iter}] error: {e:#}");
        }
        iter += 1;
        if args.once {
            return Ok(());
        }
        tokio::time::sleep(interval).await;
    }
}

async fn print_balances<P: Provider>(provider: &P, me: Address) -> Result<()> {
    let eth = provider.get_balance(me).await?;
    let weth = read_view(provider, WETH, IERC20::balanceOfCall { account: me }).await?;
    let usdc = read_view(provider, USDC, IERC20::balanceOfCall { account: me }).await?;
    let usdt = read_view(provider, USDT, IERC20::balanceOfCall { account: me }).await?;
    println!("balances: ETH={eth} WETH={weth} USDC={usdc}(1e6) USDT={usdt}(1e6)");
    Ok(())
}

async fn read_view<P: Provider, C: SolCall>(
    provider: &P,
    to: Address,
    call: C,
) -> Result<C::Return> {
    let req = TransactionRequest::default()
        .with_to(to)
        .with_input(call.abi_encode());
    let bytes = provider.call(req).await?;
    Ok(C::abi_decode_returns(&bytes)?)
}

async fn setup<P: Provider>(provider: &P, me: Address, args: &Cli) -> Result<()> {
    let spender = args.contract.address();
    let label = args.contract.label();
    let weth_target = U256::from(100u128) * U256::from(10u128.pow(18));
    let stable_target = U256::from(100_000u128) * U256::from(10u128.pow(STABLE_DECIMALS));
    for (token, sym, target) in [
        (WETH, "WETH", weth_target),
        (USDC, "USDC", stable_target),
        (USDT, "USDT", stable_target),
    ] {
        let allowance = read_view(
            provider,
            token,
            IERC20::allowanceCall { owner: me, spender },
        )
        .await?;
        if allowance >= target {
            println!("[setup] {sym} already approved");
            continue;
        }
        println!("[setup] approving {sym} -> {label}");
        let calldata = IERC20::approveCall {
            spender,
            value: target,
        }
        .abi_encode();
        let req = TransactionRequest::default()
            .with_to(token)
            .with_input(calldata);
        let receipt = provider.send_transaction(req).await?.get_receipt().await?;
        println!(
            "[setup]   {sym} approve mined block={:?}",
            receipt.block_number
        );
    }

    let weth_bal = read_view(provider, WETH, IERC20::balanceOfCall { account: me }).await?;
    let target_wei = U256::from((args.target_weth * 1e18) as u128);
    if weth_bal >= target_wei {
        return Ok(());
    }
    let eth_bal = provider.get_balance(me).await?;
    let reserve_wei = U256::from((args.reserve_eth * 1e18) as u128);
    if eth_bal <= reserve_wei {
        bail!("ETH {eth_bal} <= reserve {reserve_wei}, cannot wrap");
    }
    let need = target_wei - weth_bal;
    let available = eth_bal - reserve_wei;
    if available < need {
        bail!("ETH available {available} < need {need} to reach target WETH");
    }
    let wrap_amt = available.min(need * U256::from(2)).max(need);
    println!("[setup] wrapping {wrap_amt} wei ETH -> WETH");
    let req = TransactionRequest::default()
        .with_to(WETH)
        .with_value(wrap_amt)
        .with_input(IWETH9::depositCall {}.abi_encode());
    let receipt = provider.send_transaction(req).await?.get_receipt().await?;
    println!("[setup]   wrap mined block={:?}", receipt.block_number);
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn trade_once<P: Provider>(
    provider: &P,
    http: &reqwest::Client,
    signer: &PrivateKeySigner,
    titan_url: &str,
    args: &Cli,
    chain_id: u64,
    me: Address,
    stable: Stable,
    dir: Direction,
    stream_rx: Option<&mut StateStreamRx>,
) -> Result<()> {
    let nonce = provider.get_transaction_count(me).pending().await?;
    let fees = provider.estimate_eip1559_fees().await?;
    let max_priority = fees
        .max_priority_fee_per_gas
        .max(args.min_priority_gwei * 1_000_000_000);
    let max_fee = fees.max_fee_per_gas.max(max_priority * 3);

    let stable_units = usd_to_units(args.notional_usd, STABLE_DECIMALS);
    let weth_units = usd_to_units(args.notional_usd, 18) / U256::from(args.eth_price_usd);
    let (token_in, token_out, amount_in, expected_out, label) = match dir {
        Direction::StableToWeth => (
            stable.addr(),
            WETH,
            stable_units,
            weth_units,
            format!("{}->WETH", stable.sym()),
        ),
        Direction::WethToStable => (
            WETH,
            stable.addr(),
            weth_units,
            stable_units,
            format!("WETH->{}", stable.sym()),
        ),
    };
    let bps = U256::from(10_000);
    let slip = args.slippage_bps as u64;
    let slip_lo = U256::from(10_000u64.saturating_sub(slip));
    let slip_hi = U256::from(10_000u64.saturating_add(slip));
    let min_out = expected_out * slip_lo / bps;

    let calldata = match args.contract {
        // Fermi's `amountSpecified` is signed: positive = exact tokenIn, negative = exact
        // tokenOut. We always denominate the trade in stable units, so flip the sign on
        // WETH-input legs to mean "exact stable output".
        Contract::Fermi => {
            let stable_i = I256::try_from(stable_units).expect("notional fits");
            let (amount_specified, amount_check) = match dir {
                Direction::StableToWeth => (stable_i, weth_units * slip_lo / bps),
                Direction::WethToStable => (-stable_i, weth_units * slip_hi / bps),
            };
            IFermiSwapper::fermiSwapWithAllowancesCall {
                tokenIn: token_in,
                tokenOut: token_out,
                amountSpecified: amount_specified,
                amountCheck: amount_check,
                recipient: me,
            }
            .abi_encode()
        }
        Contract::Bebop => {
            let pending = provider
                .get_block(BlockId::pending())
                .await?
                .context("no pending block")?;
            let expiry = U256::from(pending.header.timestamp + BEBOP_EXPIRY_SECS);
            IBebop::swapCall {
                tokenIn: token_in,
                tokenOut: token_out,
                amountIn: amount_in,
                minAmountOut: min_out,
                expiry,
            }
            .abi_encode()
        }
        Contract::Kipseli => IKipseliGuard::swapCall {
            tokenIn: token_in,
            amountIn: amount_in,
            tokenOut: token_out,
            minOut: min_out,
        }
        .abi_encode(),
    };

    if let Some(rx) = stream_rx {
        let (block_number, timestamp_secs, state_override) = loop {
            rx.changed().await?;
            if let Some(frame) = rx.borrow_and_update().clone() {
                break frame;
            }
        };
        let call = json!({
            "from": me,
            "to": args.contract.address(),
            "data": format!("0x{}", hex::encode(&calldata)),
        });
        let block_overrides = json!({
            "number": format!("0x{block_number:x}"),
            "time": format!("0x{timestamp_secs:x}"),
        });
        let params = json!([call, "latest", state_override, block_overrides]);
        match provider
            .raw_request::<_, Bytes>("eth_call".into(), params)
            .await
        {
            Ok(_) => println!("[trade] state-override sim ok @ block {block_number}"),
            Err(e) => {
                println!("[trade] state-override sim reverts, skipping: {e}");
                return Ok(());
            }
        }
    }

    let (raw_tx, tx_hash) = build_signed_tx(
        signer,
        chain_id,
        nonce,
        args.contract.gas_limit(),
        max_fee,
        max_priority,
        args.contract.address(),
        calldata,
    )?;
    println!(
        "[trade] {label} nonce={nonce} amount_in={amount_in} min_out={min_out} \
         max_fee={max_fee} max_prio={max_priority} tx_hash={tx_hash:#x}"
    );
    if !args.send {
        return Ok(());
    }
    // Titan accepts `blockNumber: 0x0` as "include in any block within validity".
    let body = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "eth_sendBundle",
        "params": [{ "txs": [raw_tx], "blockNumber": "0x0" }],
    });
    let resp = http.post(titan_url).json(&body).send().await?;
    println!(
        "[trade] titan status={} body={}",
        resp.status(),
        resp.text().await?
    );
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn build_signed_tx(
    signer: &PrivateKeySigner,
    chain_id: u64,
    nonce: u64,
    gas_limit: u64,
    max_fee: u128,
    max_priority: u128,
    to: Address,
    input: Vec<u8>,
) -> Result<(String, B256)> {
    let mut tx = TxEip1559 {
        chain_id,
        nonce,
        gas_limit,
        max_fee_per_gas: max_fee,
        max_priority_fee_per_gas: max_priority,
        to: TxKind::Call(to),
        value: U256::ZERO,
        input: Bytes::from(input),
        access_list: Default::default(),
    };
    let sig = signer.sign_transaction_sync(&mut tx)?;
    let raw = TxEnvelope::from(tx.into_signed(sig)).encoded_2718();
    let tx_hash = keccak256(&raw);
    Ok((format!("0x{}", hex::encode(&raw)), tx_hash))
}

async fn run_state_stream(region: StreamRegion, contract: Address, tx: StateStreamTx) {
    let url = region.ws_url();
    let contract_lc = format!("{contract:#x}").to_lowercase();
    loop {
        let r: Result<()> = async {
            let (ws, _) = connect_async(url).await?;
            println!("[stream] connected: {url}");
            let (_, mut reader) = ws.split();
            while let Some(msg) = reader.next().await {
                let Message::Text(text) = msg? else { continue };
                let Ok(v) = serde_json::from_str::<Value>(&text) else {
                    continue;
                };
                let Some(obj) = v.as_object() else { continue };
                let Some(block_number) = obj
                    .get("blockNumber")
                    .or_else(|| obj.get("block_number"))
                    .and_then(|b| b.as_u64())
                else {
                    continue;
                };
                let timestamp_secs = obj
                    .get("timestamp")
                    .and_then(|t| t.as_u64())
                    .map(|ns| ns / 1_000_000_000)
                    .unwrap_or(0);
                if let Some(entry) = obj.iter().find(|(k, _)| k.to_lowercase() == contract_lc)
                    && let Some(so) = entry
                        .1
                        .get("stateOverride")
                        .or_else(|| entry.1.get("state_override"))
                {
                    let _ = tx.send(Some((block_number, timestamp_secs, so.clone())));
                }
            }
            Ok(())
        }
        .await;
        if let Err(e) = r {
            println!("[stream] disconnected: {e:#}");
        }
        tokio::time::sleep(Duration::from_secs(5)).await;
    }
}
