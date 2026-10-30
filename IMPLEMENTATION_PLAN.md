# Confirmed 4h candle exits and market filters

Approved behavior: replace confirmation-pair exits with two consecutive same-color completed 4h candles and a matching received 1h/4h signal during either interval. Both candles and the qualifying signal must be post-entry; exit at the second completion. Preserve other exits and entry triggers.

1. Add causal frame annotations for range state, DEMA9, band proximity, extreme stretches and resettable peak/trough references. Test boundaries and prefix invariance.
2. Apply range, stretch and post-extreme entry blocks to both signal and turning-point entry paths. Keep sizing and other validity gates intact.
3. Schedule confirmation-candle exit evaluations at every completed 4h candle; bucket only signals received within those candles. Test both directions, post-entry restrictions, dojis, boundaries and no signals from the future.
4. Wire shade_ranging_area=true and extreme_price_stretch_atr_multiple=2 through both endpoints and settings. Export diagnostic state and draw pale range/stretch areas independently of signal visibility.
5. Remove Alpha Vantage integration, collection, plotting and endpoint options; include deletion instructions for the obsolete module and key.
6. Run offline regression tests, compile, inspect rendered shading, and deliver full changed files plus a run command. No market-data performance claim without an external rerun.
