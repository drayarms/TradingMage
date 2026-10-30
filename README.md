# TradingMage: completed-candle exits and market filters

This archive contains full replacement files, not a patch. Copy app/ files into your existing app/ directory and the runner into the project root. Other project files remain in place. Delete the obsolete app/alpha_vantage_news.py file. ALPHA_VANTAGE_KEY and ALPHA_VANTAGE_API_KEY are no longer required and can be removed from your environment. No stored historical data is deleted automatically.

Scope: /backtest/run and /backtest/plot for golden_cross_dip_recovery_4h. Existing live order processing is not extended with these endpoint-only exit options.

## Endpoint arguments

- exit_on_confirmatory_candlestick=true now replaces the old 1h/4h signal-pair exit. Longs close after two consecutive red completed 4h candles with a 1h/4h sell/sell+ received during either candle. Shorts mirror this with green candles and buy/buy+. Both candles must complete after position entry, and both the signal timestamp and its receipt must follow entry. An arrival exactly at completion belongs to that closing candle. Exit evaluation occurs at second-candle completion, without waiting for another signal. Dojis break the sequence. Signals received later do not cause retroactive exits. Same-side additions do not reset the original entry time.
- shade_ranging_area=true (default) draws pale lemon-green shading between the Bollinger bands during qualifying ranges. false hides only range shading and does not disable the entry filter.
- extreme_price_stretch_atr_multiple=2 (default, finite and positive) controls the bullish/bearish stretch threshold. Stretch shading is pale purple/orange. Shading is shown with indicators=true. Stretch shading spans from the bands' midpoint toward the candle's extreme wick, including the outer band.
- entry_trigger=buy_sell_signal or turning_point remains available. The new entry filters apply to initial entries and additions in either mode.

## Range and entry filters

All market-state annotations use completed 4h candles. Band proximity is measured with the completed candle's close; live entry-price thresholds use the simulated execution price and the latest completed 4h context available at signal receipt. Legacy sizing and regime context are preserved.

A range requires ATR / (upper band - lower band) between 0.4 and 0.6 inclusive, a valid band-width comparison, and no band clinging. On an upper proximity close (> upper - 0.2 ATR), compare current width with the width at the latest lower proximity close (< lower + 0.2 ATR), and conversely. Width ratios must be between 0.75 and 1/0.75 inclusive. A successful comparison persists between proximity events; ATR ratio and clinging are reevaluated every completed candle. No opposite touch means no established range.

Clinging means two consecutive green candles with either high above its own upper band, or two consecutive red candles with either low below its own lower band. Any clinging disqualifies that candle from the range classification. A long is blocked above upper - 0.5 ATR during a range; a short is blocked below lower + 0.5 ATR.

Long entries are blocked when slow MA < upper band and entry price >= midpoint + n ATR. Shorts are blocked when slow MA > lower band and entry price <= midpoint - n ATR. Completed candles meeting the same tests using their closes are labeled extreme stretches.

The latest extreme bullish candle's high is the obvious peak; the latest extreme bearish candle's low is the obvious trough. Longs below that peak are blocked while current completed DEMA9 minus DEMA9 at the latest lower proximity candle is positive. Shorts above that trough are blocked while the equivalent slope from the latest upper proximity candle is negative. The peak resets on the next lower proximity event; the trough resets on the next upper proximity event. Subsequent stretch candles replace the corresponding reference. Without a valid proximity DEMA9 reference, this slope filter does not block entries.

Cross exits, turning-point exits, signal exits, entry confirmation, sizing weights and exposure caps otherwise retain their existing behavior. The previously supplied do_not_enter_after_opposite_turning_point argument is absent from the last delivered source recovered for this release; it is not added by this change.

## Alpha Vantage removal

Fetching, cache-client initialization, chart markers/legend, result news fields, and endpoint options include_news_sentiment / alpha_vantage_symbols have been removed. Remove those options from commands. The obsolete client module should be deleted as described above.

## Validation

Offline tests exercise both directions, candle completion timing, delayed signals, dojis, endpoint forwarding, range persistence, clinging, stretch/peak/trough boundaries, causal prefix invariance and current filter context for delayed entries. Charts were rendered and inspected with synthetic data. No external-data performance backtest was run here.

Use run_market_filters_test.sh from your existing project root after copying the replacement files. It uses your latest supplied sizing settings and the May 30–August 4 test window. Change dates or entry_trigger as desired.

## Startup correction

Restored BackTester initialization before accessing its research helper. Alpha Vantage remains removed. An offline regression test executes the real initialization statements in startup order without an Alpha Vantage key. 145 offline tests pass.
