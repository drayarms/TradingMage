# Per-strategy completed 1h Heikin-Ashi filters

Both `/backtest/run` and `/backtest/plot` accept nine independent booleans. Every switch defaults to `true`.

| Position type | Wickless-run liquidation | Wickless-run entry guard | Color-since-last-fill entry guard |
|---|---|---|---|
| Cross | `exit_cross_on_opposing_heikin_ashi_run` | `do_not_enter_cross_on_opposing_heikin_ashi_run` | `do_not_enter_cross_after_only_opposing_heikin_ashi_since_last_entry` |
| Normal range | `exit_range_on_opposing_heikin_ashi_run` | `do_not_enter_range_on_opposing_heikin_ashi_run` | `do_not_enter_range_after_only_opposing_heikin_ashi_since_last_entry` |
| Post-stretch range | `exit_range_stretch_on_opposing_heikin_ashi_run` | `do_not_enter_range_stretch_on_opposing_heikin_ashi_run` | `do_not_enter_range_stretch_after_only_opposing_heikin_ashi_since_last_entry` |

The three former shared switches have been removed. Each book now checks only its own switches, including disabled-strategy virtual positions. The endpoint label `range_stretch` maps to the existing internal/reporting book `post_stretch_range`.

## Wickless-run rules

`heikin_ashi_filter_candles` remains one shared positive integer (default `2`). Longs are opposed by n red HA candles with no upper wick; shorts by n green HA candles with no lower wick. Wick equality allows relative floating-point tolerance of 1e-10. Doji candles and candles with the specified wick interrupt the run.

A run qualifies only if its first candle satisfies the existing strict, one-sided Bollinger proximity rule:

- Red run: first HA low < 4h lower Bollinger + 0.2 × 4h ATR.
- Green run: first HA high > 4h upper Bollinger − 0.2 × 4h ATR.

The check uses the latest completed 4h context available when the first hourly HA candle closes, including exact simultaneous completion. Prices beyond the band qualify. The origin is the start of the entire uninterrupted run; its qualification remains frozen as the run grows and bands change. Missing/nonfinite context or nonpositive ATR cannot qualify a run.

The entry switch blocks entries and additions in its own book, including pending-order submission and fill revalidation. Unlike earlier versions, crosses and post-stretch ranges can each enable this entry guard too. The independent exit switch liquidates positions in its own book and bypasses the three-gradient veto. Execution calendars, fresh prices, extended-session limits and liquidity rules still apply.

## Color since the last fill

See [the separate color-filter rules](heikin_ashi_since_entry.md). This filter does not require wicklessness or Bollinger proximity and uses its own switch for each position type.

## Example script

`run_range_cross_test.sh` contains the current configuration, including both new [streak-end fast-MA exits](heikin_ashi_streak_fast_exit.md). `bollinger_proximity_atr_multiple` now configures the Bollinger proximity equations above (default 0.2).

All HA decisions use completed native hourly candles with historical warmup, independently of chart selectors or displayed candle type. No synthetic closed-market candles are created.
