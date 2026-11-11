# Completed 1h Heikin-Ashi filter

Both `/backtest/run` and `/backtest/plot` accept:

| Argument | Default | Meaning |
|---|---|---|
| `heikin_ashi_filter_candles` | `2` | Positive integer number of consecutive completed native 1h candles. |
| `do_not_enter_on_opposing_heikin_ashi_run` | `true` | Guard normal range entries only, including additions and pending fills. |
| `exit_on_opposing_heikin_ashi_run` | `true` | Liquidate opposing positions when the run qualifies. |

The entry filter applies only to normal range entries, including additions and pending order fills, when `do_not_enter_on_opposing_heikin_ashi_run=true`. Cross and post-stretch range entries are exempt. It blocks longs after n red HA candles, each without an upper wick, and shorts after n green HA candles, each without a lower wick. Doji candles or candles with the specified wick interrupt the run. Wick equality allows a relative floating-point tolerance of 1e-10.

When liquidation is enabled, the same conditions close existing positions in all three books, bypassing the three-gradient veto. Execution hours, fresh quotes, liquidity and extended-session limit fills still apply. Pending liquidation retains this priority until executed. The entry and liquidation switches are independent. Setting liquidation to false leaves the normal range entry guard enabled unless its own switch is also false.

Hourly data is fetched independently of all plotting selectors and chart candle type. HA recursion includes historical warmup, and a candle becomes available only at its completion time. Closed-market periods do not create synthetic candles. The last n available completed native hourly candles define a run.

Append to your existing command:

```bash
  heikin_ashi_filter_candles=2 \
  do_not_enter_on_opposing_heikin_ashi_run=true \
  exit_on_opposing_heikin_ashi_run=true
```

## Proximity of the first candle

A wickless run qualifies only if its first candle is close to the relevant 4h band, using the latest completed 4h context available when that first hourly candle closes:

- Red run: first HA low < lower Bollinger + 0.2 × 4h ATR.
- Green run: first HA high > upper Bollinger − 0.2 × 4h ATR.

These are the same strict, one-sided thresholds used for normal range proximity; values beyond the band qualify too. The test uses the actual HA low/high. Its result is frozen for the entire uninterrupted wickless same-color run. It does not roll forward to the latest n candles or change as later bands change. A wick or color interruption resets the run. Missing/nonfinite 4h context or nonpositive ATR cannot qualify a run.

This qualification is shared by the optional normal-range entry guard and optional liquidation of all three position types. Existing switches, candle count and execution rules remain unchanged.
