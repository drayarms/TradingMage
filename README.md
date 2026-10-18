Replace the five included files in TradingMage/app and restart the endpoint runner.

Corrected long exits: completed 4H death cross OR any 4H bullish_exit OR 1H bullish_exit whose signal price is strictly above latest completed 4H fast average + bullish_exit_above_fast_average_atr_multiple * latest completed 4H ATR. Default multiplier 1. Entire long position is closed. Equality does not trigger the 1H condition. Bearish exit signals alone no longer close long positions. Entry sizing, eligibility, buy/buy+ timeframe and death-cross behavior are unchanged.

Use bullish_exit_above_fast_average_atr_multiple=1 in run or plot commands instead of the former bearish parameter. Live environment name: GOLDEN_CROSS_BULLISH_EXIT_ABOVE_FAST_AVERAGE_ATR_MULTIPLE. Live submission remains disabled by default; authenticated execution remains untested.

Webhook selection gives bullish_exit priority for the active golden-cross strategy, including payloads with multiple signals. Other strategies and red/green buy/sell, orange bearish-exit, blue bullish-exit and magenta strategy-exit chart colors are preserved.

57 local tests pass plus Python compilation. Tests cover strict thresholds, raw exit flags, rejected bearish signals and completed death crosses. This package does not include a historical rerun.
