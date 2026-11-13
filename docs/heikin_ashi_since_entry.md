# Heikin-Ashi color filter since the previous fill

An additional filter, enabled by default, applies separately to each ticker, strategy book (cross, normal range, post-stretch range) and side while that position remains open.

- Long additions are blocked if every completed native 1h HA candle since the last actual same-book long fill is red.
- Short additions are blocked if every completed native 1h HA candle since the last actual same-book short fill is green.
- A green candle or doji clears the long restriction; a red candle or doji clears the short restriction. Wicks and Bollinger proximity do not matter for this filter.
- Only hourly candles closing strictly after the previous fill and no later than the proposed new fill are considered. With no newly completed hourly candle, another entry is blocked.
- Every actual entry fill, including an addition or extended-hours fill, restarts the observation interval. Order submission or rejection does not restart it. Pending entries are rechecked before filling.
- Full closure clears this filter for that book and ticker. A newly opened position does not inherit the previous position's color restriction. Other tickers, books and sides do not share this state.

Existing 4h entry spacing remains in effect. The existing configurable wickless-run filter and liquidation rule also remain independent. Each position type has its own boolean endpoint argument, defaulting to `true` on both run and plot:

- `do_not_enter_cross_after_only_opposing_heikin_ashi_since_last_entry`
- `do_not_enter_range_after_only_opposing_heikin_ashi_since_last_entry`
- `do_not_enter_range_stretch_after_only_opposing_heikin_ashi_since_last_entry`

Set a switch to `false` to disable only this filter for that position type. Wickless-run entry and liquidation switches remain independent; see [the full switch table](heikin_ashi_entry_exit_filter.md).
