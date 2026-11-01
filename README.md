# TradingMage: completed-candle exits and market filters

This archive contains full replacement files, not a patch. Copy app/ files into your existing app/ directory and the runner into the project root. Other project files remain in place. Delete the obsolete app/alpha_vantage_news.py file. ALPHA_VANTAGE_KEY and ALPHA_VANTAGE_API_KEY are no longer required and can be removed from your environment. No stored historical data is deleted automatically.

Scope: /backtest/run and /backtest/plot for golden_cross_dip_recovery_4h. Existing live order processing is not extended with these endpoint-only exit options.

## Endpoint arguments

- range_bollinger_gap_similarity_ratio=0.6 (finite, greater than 0 and at most 1) sets the lower band-width similarity ratio; the upper ratio is its reciprocal.
- extreme_price_stretch_min_bollinger_gap_atr_multiple=3 (finite and positive) sets the minimum Bollinger width in ATRs required for an extreme stretch. The separate extreme_price_stretch_atr_multiple=2 still sets the price distance from the band midpoint.

- exit_on_confirmatory_candlestick=true now replaces the old 1h/4h signal-pair exit. Longs close after two consecutive red completed 4h candles with a 1h/4h sell/sell+ received during either candle. Shorts mirror this with green candles and buy/buy+. Both candles must complete after position entry, and both the signal timestamp and its receipt must follow entry. An arrival exactly at completion belongs to that closing candle. Exit evaluation occurs at second-candle completion, without waiting for another signal. Dojis break the sequence. Signals received later do not cause retroactive exits. Same-side additions do not reset the original entry time.
- shade_ranging_area=true (default) draws pale lemon-green shading between the Bollinger bands during qualifying ranges. false hides only range shading and does not disable the entry filter.
- extreme_price_stretch_atr_multiple=2 (default, finite and positive) controls the bullish/bearish stretch threshold. Stretch shading is pale purple/orange. Shading is shown with indicators=true. Stretch shading spans from the bands' midpoint toward the candle's extreme wick, including the outer band.
- entry_trigger=buy_sell_signal or turning_point remains available. The new entry filters apply to initial entries and additions in either mode.

## Range and entry filters

All market-state annotations use completed 4h candles. Band proximity is measured with the completed candle's close; live entry-price thresholds use the simulated execution price and the latest completed 4h context available at signal receipt. Legacy sizing and regime context are preserved.

A range requires ATR / (upper band - lower band) at most range_max_atr_to_bollinger_gap_ratio (default 0.5), with no lower ratio bound, a valid band-width comparison, and no band clinging. On an upper proximity close (> upper - 0.2 ATR), compare current width with the width at the latest lower proximity close (< lower + 0.2 ATR), and conversely. Width ratios must be between range_bollinger_gap_similarity_ratio and its reciprocal inclusive (default 0.6 to 1/0.6). A successful comparison persists between proximity events; ATR ratio and clinging are reevaluated every completed candle. No opposite touch means no established range.

Clinging means two consecutive green candles with either high above its own upper band, or two consecutive red candles with either low below its own lower band. Any clinging disqualifies that candle from the range classification. A long is blocked above upper - 0.5 ATR during a range; a short is blocked below lower + 0.5 ATR.

Extreme stretches additionally require (upper band - lower band) >= extreme_price_stretch_min_bollinger_gap_atr_multiple * ATR (default 3). This requirement applies to entry blocking, completed-candle labels, shading and peak/trough reference creation. Long entries are blocked when slow MA < upper band and entry price >= midpoint + n ATR. Shorts are blocked when slow MA > lower band and entry price <= midpoint - n ATR. Completed candles meeting the same tests using their closes are labeled extreme stretches.

The latest extreme bullish candle's high is the obvious peak; the latest extreme bearish candle's low is the obvious trough. Longs below that peak are blocked while current completed DEMA9 minus DEMA9 at the latest lower proximity candle is positive. Shorts above that trough are blocked while the equivalent slope from the latest upper proximity candle is negative. The peak resets on the next lower proximity event; the trough resets on the next upper proximity event. Subsequent stretch candles replace the corresponding reference. Without a valid proximity DEMA9 reference, this slope filter does not block entries.

Cross exits, turning-point exits, signal exits, entry confirmation, sizing weights and exposure caps otherwise retain their existing behavior. The previously supplied do_not_enter_after_opposite_turning_point argument is absent from the last delivered source recovered for this release; it is not added by this change.

## Alpha Vantage removal

Fetching, cache-client initialization, chart markers/legend, result news fields, and endpoint options include_news_sentiment / alpha_vantage_symbols have been removed. Remove those options from commands. The obsolete client module should be deleted as described above.

## Validation

Offline tests exercise both directions, candle completion timing, delayed signals, dojis, endpoint forwarding, range persistence, clinging, stretch/peak/trough boundaries, causal prefix invariance and current filter context for delayed entries. Charts were rendered and inspected with synthetic data. No external-data performance backtest was run here.

Use run_market_filters_test.sh from your existing project root after copying the replacement files. It uses your latest supplied sizing settings and the May 30–August 4 test window. Change dates or entry_trigger as desired.

## Startup correction

Restored BackTester initialization before accessing its research helper. Alpha Vantage remains removed. An offline regression test executes the real initialization statements in startup order without an Alpha Vantage key. 145 offline tests pass.

Latest parameter update validation: 151 offline tests pass, including endpoint and simulation forwarding, invalid values, custom range ratios, and the inclusive stretch-width boundary for longs and shorts.

## Independent long/short entry filters

All six arguments default to true:

- long_range_entry_filter_enabled
- short_range_entry_filter_enabled
- long_extreme_stretch_entry_filter_enabled
- short_extreme_stretch_entry_filter_enabled
- long_post_peak_entry_filter_enabled
- short_post_trough_entry_filter_enabled

false disables only that directional entry filter. Detection, reference tracking and shading continue. Other eligibility checks, sizing and exits remain active. Both signal and turning-point entries use these switches, including additions. DEMA9 slope signs remain unchanged: positive blocks longs after a peak; negative blocks shorts after a trough.

Range, bullish-stretch and bearish-stretch shades have been darkened and opacity raised from 0.30 to 0.45. The range ATR/gap lower bound remains removed.

Directional-switch update validation: 156 offline tests pass, including independent switches, both endpoint forwarding paths, simulation settings forwarding and preservation of regime gates.

The endpoint argument range_max_atr_to_bollinger_gap_ratio=0.5 sets the maximum ATR/Bollinger-width ratio. This is separate from range_bollinger_gap_similarity_ratio=0.6, which compares two band widths. The existing extreme_price_stretch_min_bollinger_gap_atr_multiple=3 continues to require Bollinger width >= 3 ATR for bullish/bearish stretch detection and filtering.

## Range and cross strategies together

Both /backtest/run and /backtest/plot now accept:

- cross_strategy_enabled=true
- range_strategy_enabled=true
- disabled_strategy_virtual_positions_enabled=true
- range_sizing_low_atr_price_ratio=0.01
- range_sizing_high_atr_price_ratio=0.05
- range_sizing_min_entry_fraction=0.25
- range_sizing_max_entry_fraction=1.0

These defaults enable both actual simulated strategy books. A disabled strategy continues virtually when disabled_strategy_virtual_positions_enabled=true. Its qualifying entries participate in opposite-side precedence and replacement in the same ticker. Virtual trades and open positions are returned separately and excluded from actual PnL/exposure. Set the virtual flag false to test one strategy in full isolation. Cross strategy_mode and entry_trigger continue to apply only to the cross book. The range book trades both directions using its own range rules, without fast/slow cross gates or the six cross-entry filter switches.

Ownership: cross and range can own separate same-side quantities in one ticker. Every qualifying entry/addition resolves opposite exposure against both active books, including disabled-strategy virtual books, using the replacement rules below. The entry_strategy_precedence setting governs simultaneous opposing opportunities. A strategy exit closes only its own quantity. The existing max_exposure_multiple cap applies independently per strategy and ticker, so combined exposure may be greater than in a cross-only run.

Range entries evaluate every completed 1m price update and each incoming signal (all loaded timeframes), using only the latest completed 4h indicators. Entry long when ranging and market price < lower Bollinger + 0.2 ATR; entry short when ranging and price > upper Bollinger - 0.2 ATR. Range entries in 4h candle 10 allow another same-side range entry in candle 12. This spacing persists across exits and covers additions, counting native 4h candle steps rather than wall-clock hours.

Range exits do not require the market to remain ranging. Longs exit near the current upper band or near its projected next value; shorts mirror this at the lower band. Projection = current band + (current band - band two completed candles earlier)/2. Current ATR is used for the same 0.2 ATR proximity test; current price is held constant for projection. No extra stop or forced exit upon range termination is introduced. No same-timestamp range reentry after a range exit.

Range sizing: let v = ATR / execution price, and u = clamp((v - low_ratio)/(high_ratio - low_ratio), 0, 1). Entry fraction = max_fraction + u*(min_fraction - max_fraction). Defaults give 100% of base at <=1% normalized ATR, 62.5% at 3%, and 25% at >=5%, before clipping to the owner's exposure cap. This produces a desired entry amount rather than guaranteed execution; existing simulated quote/fill constraints remain active.

Results: strategy_results.cross and strategy_results.range contain independently attributed actual PnL histories. direction_results retains golden_cross/death_cross for cross trades and adds range_long/range_short. trade_events and open_positions identify strategy_type. virtual_trade_events / virtual_open_positions are explicitly marked is_virtual=true. Overall/ticker PnL and gross exposure include actual books only. Charts include separate cross/range PnL images; range long/short entries use thick purple/teal tri_up/tri_down markers, and range exits use magenta stars. Existing cross filled-triangle/square markers remain.

This release is backtest-only; it does not place live broker orders. Live multi-strategy ownership would require a broker fill ledger and reconciliation of each strategy's quantity against the ticker's combined net holding.

Run bash run_range_cross_test.sh after copying all replacement app files. This command uses both strategies, your latest supplied date window/filter settings, and the accepted range sizing defaults. To test cross-only, use range_strategy_enabled=false; range-only uses cross_strategy_enabled=false. The virtual setting still applies unless explicitly disabled.

Validation: 179 offline tests pass, including new range, portfolio, endpoint, chart and UTC execution-index regression tests. Synthetic marker charts were rendered and inspected. No external market-data performance backtest was run in this environment.

Latest display/priority update: repeated trade annotations use dollar amounts only and EXIT for both strategy types; markers and the legend distinguish ownership. At each timestamp, process market updates and all exits first, then range entries (including virtual range entries), then cross entries. The default range precedence governs simultaneous opposing entries; cross precedence can be selected. 182 offline tests pass, including priority, virtual priority, exit-before-entry and chart annotation regression checks.


## Range width and maximum open positions

Entry annotations show only the dollar amount, at font size 7.

`range_min_bollinger_gap_to_peak_since_turning_point_ratio=0.7` blocks range entries when the latest completed 4h Bollinger width is below 70% of the widest completed width since the latest confirmed 4h maximum or minimum. The turning candle is included. Entries are blocked until the first confirmed turning point. Detection works regardless of reversal plot settings. Equality passes; range exits and cross entries are unchanged.

Every run reports maximum actual gross open market value per stock and the simultaneous overall maximum, with timestamps and portfolio composition at that maximum. Long and short market values are absolute and added together; virtual holdings are excluded. Individual stock maxima need not occur together and are not summed to obtain the portfolio maximum. Sampling uses completed 1m market updates and event times, including values immediately before exits and after entries. This is not intraminute tick exposure, buying power or margin usage.

The runner prints this report and saves `local_backtest_output/max_open_positions_report.txt`. Plot ZIPs contain `max_open_positions_report.txt`, also copied into `backtest_charts`. JSON results include `max_open_positions_exposure` and `max_open_positions_report`. The report extractor reads only the report prefix of JSON responses rather than loading large history arrays.

Latest validation: 193 offline tests pass; Python compilation and shell syntax checks pass. No external market-data performance run was executed here. Replace all supplied app files and run_local_endpoint.sh together, since the exposure report uses two new modules.


## Qualifying opposite-entry replacement

`entry_strategy_precedence=range` (default) or `cross` controls which strategy evaluates entries first at an identical timestamp. An executed qualifying entry blocks opposing entries of the other strategy only at that timestamp. A later qualifying opposite entry closes the other strategy's position immediately, then opens its own position. This replaces the persistent opposite-position entry block. Same-side positions remain independent; opposite positions of the same strategy are not automatically flipped.

Cross candidates pass all entry filters and positive sizing before replacement is attempted. Range candidates must pass range classification, band proximity, width filter, spacing and positive sizing. Failed liquidation prevents the incoming entry. Replacement close events carry `replaced_by_qualifying_cross_entry` or `replaced_by_qualifying_range_entry` as exit_reason. Existing entry/exit rules otherwise remain intact.

Disabled books still participate virtually when disabled_strategy_virtual_positions_enabled=true, including precedence and replacement. Set it false for full isolation. Precedence is based on exact engine timestamps, not grouping the entire 1h or 4h candle. A later candidate may replace a position even if it arrives within the same candle.

Replacement release validation: 198 offline tests pass, including both precedence choices, later replacement, virtual replacement, failed liquidation and rejected-candidate preservation. Python compilation and shell syntax checks pass.


## Bollinger cusp requirement for extreme stretches

Extreme bullish stretch additionally requires at least one increase in consecutive lower-band gradients since the latest confirmed 4h fast-MA turning point: (lower[t]-lower[t-1]) > (lower[t-1]-lower[t-2]). Extreme bearish stretch requires the mirrored upper-band decrease: (upper[t]-upper[t-1]) < (upper[t-1]-upper[t-2]). These are strict signed comparisons, not absolute slope comparisons; equality does not qualify.

Both gradients must lie entirely within the cycle. The confirmed turning candle is the first sample, so the earliest comparison is two completed 4h candles later. Each side latches independently until the next confirmed fast-MA maximum or minimum resets both. Before the first confirmed point neither cusp can qualify. Crosses alone do not reset these cusp cycles. Missing band samples interrupt adjacent-gradient comparisons. Detection is independent of chart toggles and uses completed candles only.

The condition applies to shading, formation of new obvious peak/trough references and execution-time extreme-stretch entry filtering. Existing peak/trough reference reset rules remain unchanged. Range classification and range entry rules are unchanged. No new endpoint argument is required: retain the current command, including entry_strategy_precedence and all existing stretch parameters. Indicator JSON includes lower_bollinger_cusp_confirmed and upper_bollinger_cusp_confirmed (0/1).

Cusp release validation: 205 offline tests pass, including signed comparisons, equality, cycle reset, pre-cycle exclusion, prefix causality, independent detection, shading classifications and entry filtering. Python compilation passes. No external market-data performance run was executed.
