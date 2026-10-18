# Golden Cross Dip Recovery Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox syntax for tracking.

**Goal:** Add golden_cross_dip_recovery_4h consistently to live dispatch, simulations, and selectable indicator plots without removing existing strategies.

**Architecture:** Put deterministic calculations in a focused app/golden_cross_dip_recovery.py module called through Strategies. Use one completed-bar/history adapter for both execution paths. Keep broker execution in app.py and simulated position accounting and ticker plotting in backtester.py.

**Tech Stack:** Existing Python, FastAPI, pandas, Alpaca REST, Redis, matplotlib, and shell runner; unittest for isolated tests without credentials.

**Spec:** ../specs/2026-10-09-golden-cross-dip-recovery-design.md

## Global Constraints

- Retain legacy strategies and simulation behavior; only the new strategy receives selected live dispatch.
- Reuse strategy4_1h_anchor credentials; live order submission defaults to disabled.
- App modules remain under app/; runner and chart viewer remain at root. Use tabs for Python indentation.
- Completed 4H candles only, including available extended hours; fast EMA9, slow SMA200, Wilder ATR14.
- Initial weights 0.5/0.5, cap 3 times base amount, invalidation threshold 1 ATR.
- No short trading, trailing stops, daily liquidation, or forced end-of-test sale for this strategy.
- Preserve Alpha Vantage persistent cache and independent plot categories.

## Review Focus

- Delayed signals and incomplete bars must not access future indicator values (Task 1).
- Provider pagination, missing history, and API failures must block ungrounded entries rather than create synthetic crosses (Task 2).
- Concurrent or duplicate events and pending buys must not bypass exposure caps (Task 4).
- Fractional broker restrictions must produce an explicit skip/defer result rather than larger orders (Task 4).
- Conflicting legacy endpoint flags and live background managers must not change the new exit rule (Tasks 3 and 4).

## Task 1: Deterministic shared strategy calculations

**Files:** Create app/golden_cross_dip_recovery.py and tests/test_golden_cross_dip_recovery.py; modify app/strategies.py. Assemble working copies from the supplied attachments under TradingMage/ using canonical filenames, preserving originals.

**Interfaces:** StrategySettings dataclass exposes slow_weight, cross_weight, max_exposure_multiple, invalidation_atr_multiple, fast_type, fast_period, slow_type, slow_period, atr_period. build_indicator_frame(bars: DataFrame, settings: StrategySettings) -> DataFrame produces completed_at, fast_moving_average, slow_moving_average, atr, golden_cross_price. evaluate_entry(price: float, base_amount: float, snapshot: dict, current_exposure: float, pending_buy_exposure: float, settings: StrategySettings) -> dict returns eligibility, reason, fractions, desired_amount, clipped_amount, quantity. Strategies.evaluate_golden_cross_dip_recovery delegates to this engine. Bearish entry/exit placeholders return inactive decisions.

- [ ] Write tests asserting equal reference prices yield full base amount, the approved example yields $1760, far-away prices reduce amounts, capacity clips quantities, and invalid/NaN settings reject input.
- [ ] Run unittest discovery and verify missing engine causes failure.
- [ ] Implement settings validation, Wilder seeding, EMA/SMA computation, cross interpolation t=-d0/(d1-d0), and sizing. Test fast <= slow and close < slow-ATR rejection; equality at threshold remains eligible.
- [ ] Add tests proving a cross is available only after completion, later bars never change earlier snapshots, death crosses block entries without erasing cross history, and zero/nonfinite ATR skips.
- [ ] Run the calculation suite and commit in an isolated local working repository if no supplied Git repository exists.

## Task 2: Shared historical context and progressive lookback

**Files:** Create app/golden_cross_history.py and tests/test_golden_cross_history.py; modify app/trade_records.py.

**Interfaces:** GoldenCrossHistory(api, settings, timeframe).get_frame(ticker: str, start: datetime, end: datetime) -> DataFrame obtains paginated 4H OHLC and indicator context. snapshot(ticker: str, as_of: datetime) -> dict returns latest fully completed row at or before as_of. Both live and simulation consume this adapter. API and clock are injectable for tests.

- [ ] Write fake-provider tests for insufficient initial context, discovery of an older cross, oldest-history exhaustion, pagination, timestamps with timezones, and provider errors. Assert no request ends after the decision time for a snapshot.
- [ ] Run tests and verify the adapter is missing.
- [ ] Inspect existing Alpaca timeframe/candle timestamp settings; reuse provider-native 4H bars and define completed_at from the actual bar start plus duration. Include available extended-hours bars, use chronological de-duplication, and progressively expand history to provider inception when necessary. Fetch additional prehistory for EMA stabilization rather than assuming 200 bars is sufficient. Report no-history/no-cross/error explicitly.
- [ ] Cache completed rows per symbol/settings, refresh new data, and never cache an incomplete bar as final. Require valid finite OHLC and monotonic timestamps.
- [ ] Run history and calculation suites; confirm recorded completion boundaries match fixed provider fixtures; commit.

## Task 3: Simulation integration and endpoint parameters

**Files:** Modify app/backtester.py and app/app.py; create tests/test_golden_cross_simulation.py.

**Interfaces:** Add BackTester._process_golden_cross_dip_recovery_event(strategy_name, state, config, event, position_size) -> None and register the named strategy. BackTester.run receives slow_weight=0.5, cross_weight=0.5, max_exposure_multiple=3.0, invalidation_atr_multiple=1.0 plus moving-average settings. /backtest/run and /backtest/plot forward these parameters with unchanged legacy defaults.

- [ ] Write fixtures with multiple 4H buys and one bearish_exit; assert accumulation, cap clipping, fractional cost basis, full liquidation, no non-4H trades, and no reentry when a single event contains exit and buy flags.
- [ ] Verify failures before dispatch is added.
- [ ] Use indicator-only historical warmup; process only reporting-period trade signals. Obtain execution quote first and size at its executable buy price, using the same quote for simulated fill to prevent cap overshoot from quote mismatch. Reuse _open_or_add_position and _close_position after checking their quantity/accounting rules.
- [ ] Deduplicate stable event identities. Retain open positions at backtest end and expose realized/unrealized results separately. Block legacy ATR exit paths, daily liquidation, and entry-validation-only modes incompatible with this strategy; return explanatory validation errors.
- [ ] Add endpoint forwarding/default tests, legacy dispatch regression tests, and live/simulation engine parity fixtures. Keep old strategy run behavior unchanged.
- [ ] Run all unit suites and commit.

## Task 4: Live selection, broker execution, and pending exposure

**Files:** Modify app/app.py and app/strategies.py; create app/golden_cross_execution.py and tests/test_golden_cross_live.py.

**Interfaces:** GoldenCrossExecutor(api, redis_client, history, settings, base_amount, enabled=False).process(event: dict) -> dict returns explicit skipped/submitted/closed results. A Redis lock serializes account/symbol decisions; a deterministic client_order_id identifies orders. app.py selects golden_cross_dip_recovery_4h and aliases its API to ALPACA_APIS['strategy4_1h_anchor']; configuration flag GOLDEN_CROSS_LIVE_ORDERS_ENABLED defaults false.

- [ ] Write fake-broker tests proving default disabled dispatch submits no orders, legacy strategies receive no live dispatch, duplicates do not purchase twice, lock contention skips/defer, and two overlapping events cannot exceed the cap.
- [ ] Verify failure before executor exists.
- [ ] Reconcile actual long positions, open buys, remaining unfilled quantities, and broker order state under the lock. Calculate pending exposure conservatively; skip if it cannot be determined. Check stable broker client_order_id before retry after a crash; reserve/reconcile Redis event status instead of marking failed attempts completed.
- [ ] Check broker fractional eligibility and supported execution sessions; submit precise fractional quantities when supported. Outside supported sessions defer/skip explicitly, without changing indicator coverage or silently rounding up. No broker calls during tests.
- [ ] For bearish_exit, cancel pending strategy buys, confirm cancellations/fills, reload position, then close full long quantity; retry unresolved close state safely. Opposite account positions produce an explicit unsupported-position result rather than initiating short trading.
- [ ] Prevent run_live_trailing_stop_exit_monitor and legacy webhook order calls from managing this strategy. Do not migrate or automatically liquidate pre-existing positions at startup.
- [ ] Run mocked execution, parity, and legacy tests; commit.

## Task 5: Indicator plots, runner examples, and deliverables

**Files:** Modify app/backtester.py, app/app.py, and run_local_endpoint.sh; create tests/test_golden_cross_plots.py and GOLDEN_CROSS_README.md. Preserve show_backtest_charts.py and unrelated modules unless a verified compatibility fix is necessary.

**Interfaces:** Add indicators: bool = False to /backtest/plot and BackTester.build_backtest_chart_zip. Pass calculated frame through to _plot_ticker_candlesticks; display generic average names with configured EMA9/SMA200 labels. Plot uses the same completed/as-of indicator series as decisions.

- [ ] Write tests for all four independent category switches, indicator alignment, exclusion of warmup rows, and absence of crossover markers. Assert changing plot flags cannot change simulation orders.
- [ ] Verify tests fail before indicator plotting is wired.
- [ ] Draw only the two indicator lines with meaningfully labeled legend entries. Retain existing news sentiment cache, at-price markers, and semantic color conventions.
- [ ] Add executable runner examples for run and plot with strategy_name=golden_cross_dip_recovery_4h, position_size=6600, configurable weights/cap, and independent categories. Document disabled live flag, missing-cross skips, holdings valuation, and supported broker session limitations.
- [ ] Run unittest discovery, Python compileall, and bash -n on the runner. Render a representative synthetic chart and visually inspect it. Record that no authenticated live or market-data verification occurred if credentials are unavailable.
- [ ] Review changes against every spec section; commit final changes and package TradingMage/ with canonical layout, tests, docs, and no credentials. Save edited existing files as replacements for their supplied Library identities and new deliverables as new files; provide the ZIP and concise validation evidence.

## Execution

Native execution in this session is recommended: the five tasks share indicator, execution, and state interfaces. No subagents are required. This plan has been checked against the approved design; implementation begins after the required plan review.
