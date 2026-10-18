# Golden-cross dip recovery: implementation

Copy the ZIP's `TradingMage/app/` files into your existing `TradingMage/app/` and the two root scripts into the root. Keep your existing environment file and dependencies. The ZIP includes all supplied application modules; existing strategy implementations, research, and Alpha Vantage persistent news caching are retained.

## Run a simulation

From the TradingMage root:

```bash
./run_local_endpoint.sh run \
	strategy_name=golden_cross_dip_recovery_4h \
	start=2026-06-01T04:00:00-04:00 \
	end=2026-10-01T20:00:00-04:00 \
	position_size=6600 \
	slow_weight=0.5 cross_weight=0.5 \
	max_exposure_multiple=3
```

To produce charts:

```bash
./run_local_endpoint.sh plot \
	strategy_name=golden_cross_dip_recovery_4h \
	start=2026-06-01T04:00:00-04:00 \
	end=2026-10-01T20:00:00-04:00 \
	position_size=6600 \
	entries_and_exits=true \
	lux_algo_symbols=false \
	alpha_vantage_symbols=false \
	indicators=true
```

The endpoints remain `GET /backtest/run` and `GET /backtest/plot`; the strategy name selects the new implementation. You may supply `tickers=AAPL,MSFT` to limit the run. News markers use the existing persistent cache when enabled. The local runner explicitly disables live order submission even if your environment file enables it.

Additional parameters:

| Parameter | Default | Meaning |
|---|---:|---|
| `position_size` | `POSITION_SIZE_1H` | Base dollars per qualifying entry |
| `slow_weight` | 0.5 | Contribution of slow-average proximity |
| `cross_weight` | 0.5 | Contribution of golden-cross proximity; weights must sum to 1 |
| `max_exposure_multiple` | 3 | Maximum per-stock market value relative to base dollars |
| `invalidation_atr_multiple` | 1 | Allowed completed-close distance below slow average, in ATRs |
| `fast_moving_average_type` | ema | ema or sma |
| `fast_moving_average_period` | 9 | Fast average period |
| `slow_moving_average_type` | sma | ema or sma |
| `slow_moving_average_period` | 200 | Slow average period |
| `ATR_period` | 14 | Wilder ATR period |
| `indicators` | false | Show the two moving-average lines on plots |

Legacy `exit_strategy`, liquidation-factor overrides, nonzero `warmup_sessions`, `liquidate_before_market_close=true`, and `run_exit_strategy=false` are rejected for this strategy. The simulation performs an indicator-only historical warmup, with no hidden pre-period purchases. Positions are retained at the end; JSON reports `open_positions`, `realized_pnl`, `unrealized_pnl`, and `entry_diagnostics`.

## Strategy behavior

A completed native Alpaca 4H candle permits entries when `fast_moving_average > slow_moving_average` and its close is at least `slow_moving_average - ATR * invalidation_atr_multiple`. A distinct 4H buy/buy+ event triggers a purchase. Losing eligibility blocks additions without selling; eligibility can resume when both conditions hold again. A 4H bearish_exit closes the full long position, with priority over simultaneous buys. Short-side methods are inactive placeholders.

The two sizing fractions are `ATR / (ATR + abs(execution_price - reference_price))`. Their weighted sum multiplies the base amount. The golden-cross reference is the interpolated intersection of the moving averages, not the stock's candle close. Repeated entries accumulate; available capacity is capped at three base amounts by default. Price appreciation beyond the cap prevents further purchases without forcing sales.

Indicator data includes available extended-hours bars from the configured Alpaca feed, uses interval-start timestamps plus four hours for completion, and is shared between live and simulation. No TradingView OHLC or future bars substitute for missing history. Lookback expands backward until a prior cross with sufficient prehistory is found or the earliest requested history boundary (1970) is reached. Crosses must occur after at least five times the larger moving-average period in observed bars (1,000 bars with the defaults). This conservative stabilization rule prevents startup artifacts from becoming entry references; insufficient history or an unqualified initial cross produces a skip. API errors stop the run rather than making up a cross. Native Alpaca candle boundaries can differ from a TradingView chart's configured session boundaries; the buy signal remains TradingView's 4H signal while averages come from Alpaca.

Fast average: solid orange (`#FF8C00`, width 1.8). Slow average: solid blue (`#1F77B4`, width 1.3). Long entries: purple (`#7B2CBF`); short entries: teal (`#00A6A6`). TradingView buy/sell markers remain green/red. No crossover markers are added. The other three category switches keep their existing meanings.

## Live configuration

The selected live dispatch strategy defaults to `golden_cross_dip_recovery_4h`, using the existing `strategy4_1h_anchor` trading credentials and `POSITION_SIZE_1H`. Market data uses the existing `MARKET_DATA_API`. Old live dispatch and its background trailing-stop managers are bypassed while the new strategy is selected. No startup migration or liquidation of existing holdings is performed.

Actual order submission is **disabled by default**, including exits. Keep it disabled while evaluating simulations:

```dotenv
GOLDEN_CROSS_LIVE_ORDERS_ENABLED=false
GOLDEN_CROSS_SLOW_WEIGHT=0.5
GOLDEN_CROSS_CROSS_WEIGHT=0.5
GOLDEN_CROSS_MAX_EXPOSURE_MULTIPLE=3
GOLDEN_CROSS_INVALIDATION_ATR_MULTIPLE=1
GOLDEN_CROSS_FAST_MOVING_AVERAGE_TYPE=ema
GOLDEN_CROSS_FAST_MOVING_AVERAGE_PERIOD=9
GOLDEN_CROSS_SLOW_MOVING_AVERAGE_TYPE=sma
GOLDEN_CROSS_SLOW_MOVING_AVERAGE_PERIOD=200
GOLDEN_CROSS_ATR_PERIOD=14
```

These are read in app.py like the existing environment settings. No new API keys are required. Set the submission flag to true only when ready to enable execution. Do not use the local backtest runner to enable live trading; it forces that flag off.

The broker adapter submits day limit orders at the current ask (buy) or bid (sell), uses supported fractional quantities rounded down, counts unfilled buys toward capacity, and serializes decisions under a renewable Redis account/symbol lock. A bearish exit cancels pending buys before submitting a full-quantity sell. Persistent exit intent is reconciled by the background monitor until flat, including after expired or rejected day orders. A submitted limit order is not a guaranteed immediate fill. Default order sessions are regular, pre-market and post-market; unavailable fractional extended-hours eligibility or closed sessions produce explicit skips/waits. Overnight execution is not enabled. Available extended-hours **indicator data** is still included.

Simulation entries use the existing executable ask/bid model from completed one-minute bars and assume immediate fills. It does not model live limit-order queuing, partial fills or rejection. This is a material difference to consider when interpreting results.

## Validation

Run from the root:

```bash
python3 -m unittest discover -s tests -v
python3 -m compileall -q app
bash -n run_local_endpoint.sh
```

Tests use synthetic OHLC, fake Redis/brokers, and the actual simulation accounting. Tests exercise sizing, cross interpolation, completion/no-lookahead, historical search, exposure clipping, duplicate events, fractional accumulation, whole-position exits, legacy dispatch, endpoint forwarding, and category-independent plots. Offline import shims are restricted to tests when optional runtime dependencies are absent; they do not validate real FastAPI/Alpaca/Redis compatibility. No authenticated broker orders or real historical market-data runs were performed in this workspace.

Final verification: 32 tests passed. Independent review findings were fixed with regression tests: exit-fill retry race, insufficient crossover prehistory, per-symbol exit failure isolation, shared simulation flags, and delayed-exit chart timestamps. Python compilation, shell syntax, and rendered indicator chart checks passed.

## Per-timeframe Lux Algo plot filters

`/backtest/plot` accepts `lux_algo_symbols1m`, `lux_algo_symbols3m`, `lux_algo_symbols5m`, `lux_algo_symbols15m`, `lux_algo_symbols30m`, `lux_algo_symbols45m`, `lux_algo_symbols1h`, `lux_algo_symbols2h`, `lux_algo_symbols4h`, and `lux_algo_symbols1d`. Each explicit value overrides the broad `lux_algo_symbols` switch; omitted values inherit it. For only 4H signals, use `lux_algo_symbols=false lux_algo_symbols4h=true`. Filtering applies only to charts and never changes trades.
