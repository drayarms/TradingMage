# TradingMage: golden_cross_dip_recovery_4h

## Purpose and scope

Add one long-only investment strategy to the supplied TradingMage branch. Preserve existing strategy implementations and their simulation support. Select the new strategy as the sole active live dispatch strategy, with order submission disabled initially for simulation validation. Reuse the existing strategy4_1h_anchor Alpaca client and credentials. No migration or special handling of legacy holdings is required; do not close them during installation. Once enabled, normal account position and exposure checks apply. No deployment or broker orders are part of implementation.

Keep run_local_endpoint.sh and show_backtest_charts.py at the project root; application modules stay under app/. Use tabs for Python indentation.

## Shared strategy decisions

Use generic fast_moving_average and slow_moving_average names, initially EMA9 and SMA200 on completed 4H Alpaca OHLC candles. Calculate Wilder ATR14 from the same candles. Include available extended-hours data and use identical timestamp boundaries and completion rules for live and simulation. Never use an unfinished candle or future data at a signal's decision time. Verify candle alignment against the existing data configuration before wiring the strategy.

A bullish regime is eligible when fast > slow and completed close >= slow - invalidation_atr_multiple * ATR. The invalidation multiple defaults to 1. This is a current-condition filter: entries become eligible again when both conditions hold. Losing eligibility does not close an existing position.

A golden cross occurs when the preceding completed fast-minus-slow difference is <= 0 and the current difference is > 0. For differences d0 and d1, compute t = -d0 / (d1 - d0), and approximate intersection price by interpolating the slow moving average between those bars. Use it only after the confirming bar completes. Record the latest such cross; death crosses do not erase its historical record.

Search progressively backward through historical bars to establish a valid indicator warmup and the latest prior golden cross. Never search forward past the decision timestamp. Stop at the provider's available-history boundary; missing cross, insufficient data, or nonpositive ATR blocks entry with an explicit reason. Cache reusable historical context without freezing an incomplete bar. EMA initialization needs sufficient prehistory; a 200-bar slice alone must not be treated as exact historical EMA context.

## Entries, sizing, and exits

A distinct 4H buy or buy+ event triggers an entry only while eligible. Repeated qualifying events may accumulate shares; duplicate deliveries of one event must not create another order. Process bearish exits before buys when both occur in one event, and do not reenter on that event. Other timeframes do not trigger orders for this strategy.

For current execution/reference price P, slow average S, last golden-cross intersection G, and ATR A:

F_slow = A / (A + abs(P - S))

F_cross = A / (A + abs(P - G))

entry_amount = base_amount * (slow_weight * F_slow + cross_weight * F_cross)

Base amount defaults to POSITION_SIZE_1H for live trading; simulation accepts position_size as its equivalent. Weights default to 0.5 each and must be finite, nonnegative, and sum to 1. Expose sizing weights and max_exposure_multiple through endpoint parameters and corresponding live configuration. Exposure cap initially equals 3 * base_amount, an experimental default rather than an optimized result.

Remaining entry capacity is cap minus current long position market value minus unfilled pending buy exposure. Clip orders to remaining capacity; skip when none remains. Price appreciation above the cap blocks additions but does not force selling. Allow supported fractional shares, respect broker precision/minimums, and do not silently increase an entry by rounding. Serialize decisions per account and symbol so concurrent requests cannot bypass the cap. Use the same capacity and sizing rules in simulation.

Only a 4H bearish_exit liquidates the whole accumulated long position. Cancel/reconcile pending strategy buys before closing so they cannot reopen it after liquidation. No short entries or short exits; retain explicit inactive placeholders for future bearish behavior. No trailing stops, ATR liquidation, end-of-day liquidation, or mandatory backtest-end liquidation for this strategy. Report open holdings and unrealized PnL separately from realized PnL.

## Application integration

strategies.py owns shared indicator, crossover, eligibility, and sizing logic. app.py owns broker execution, selected live dispatch, and the initial disabled submission flag. Existing strategies remain available but do not receive live execution dispatch while the new one is selected. Existing background trailing-stop managers must not manage this strategy's positions.

Register golden_cross_dip_recovery_4h with backtester.py and support it through /backtest/run and /backtest/plot using strategy_name. Preserve legacy endpoint defaults for other strategies; reject incompatible liquidation/exit overrides for the new strategy with a clear explanation. Reuse existing accumulation and trade-record infrastructure after verifying fractional quantities and cost basis. Preserve strategy-neutral research modules and Alpha Vantage persistent cache behavior.

## Plotting and diagnostics

Keep entries_and_exits, lux_algo_symbols, and alpha_vantage_symbols toggles; add independent indicators toggle for the fast and slow moving-average lines. No golden-cross/death-cross markers. Indicator lines must use the exact as-of series used by the strategy and be labeled with their configured type and period. Preserve meaningful marker colors, comprehensive legend, and news markers at price.

Expose entry diagnostics: reference prices, ATR, fractions, weights, proposed amount, capacity, actual quantity, and skip reason. Historical warmup is excluded from requested-period transactions and plotted output except for its calculated indicator context.

## Validation and deliverables

Test crossover interpolation, sizing at zero/large gaps, eligibility boundaries, missing history, completed-bar timing and absence of lookahead, repeated entries, exposure clipping including pending orders, duplicate events, full exits, and absence of intraday forced exits. Check live/simulation decision parity using fixed OHLC and signals without broker credentials or real orders. Verify old strategies still dispatch in simulation and that live submission remains disabled by default. Render a representative chart to check independent toggles and indicator alignment. Validate Python syntax and runner shell syntax.

Deliver updated application files in the agreed folder layout, with runnable simulation examples and a short change/validation note. No supplied files have been modified at the design stage.
