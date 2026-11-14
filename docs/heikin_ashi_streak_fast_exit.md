# Fast-MA exits after a favorable 1h HA streak ends

Both run and plot accept:

| Argument | Default | Scope |
|---|---|---|
| `exit_range_near_fast_ma_after_heikin_ashi_streak` | `true` | Normal range positions |
| `exit_range_stretch_near_fast_ma_after_heikin_ashi_streak` | `true` | Post-stretch range positions |
| `fast_ma_proximity_atr_multiple` | `0.1` | This fast-MA exit proximity |
| `bollinger_proximity_atr_multiple` | `0.2` | Existing Bollinger proximity checks |

Multiples must be finite and nonnegative. Zero removes the ATR allowance. Cross positions do not use this new exit rule.

For a short range position, a red HA streak ends when a completed green hourly HA candle appears. For a long range position, a green streak ends at a completed red candle. One favorable candle is sufficient; dojis neither start nor interrupt a streak. No wick or origin-band condition applies to this new rule, and `heikin_ashi_filter_candles` is unrelated to its length.

The streak may already be in progress when a position is filled, but its ending candle must close strictly after the latest actual entry/add fill. Every new fill resets eligibility; an ending candle at the exact fill time does not qualify. Completed 1h data is used independently of chart type/timeframes.

Once an eligible streak ends, its trigger remains armed until a new fill or full closure, even if another streak starts. Each later executable opportunity checks current minute-price approximation against the latest completed 4h fast MA and ATR:

- Short: `price < fast MA + fast_ma_proximity_atr_multiple × ATR`.
- Long: `price > fast MA − fast_ma_proximity_atr_multiple × ATR`.

These strict, one-sided comparisons match existing range proximity semantics; price beyond the threshold qualifies too. No future projection of the fast MA is used. Existing range exits remain available independently.

This exit respects the universal three-gradient veto. Once proximity has triggered an exit, blocked exits are remembered and released when permitted, using the existing deferred-exit policy without requiring another streak or price touch. A later actual addition cancels any stale streak-based exit intent/order and resets the reference. Execution calendars, fresh quotes, limit orders, and liquidity rules remain enforced.

`bollinger_proximity_atr_multiple` replaces the previous 0.2 constant in normal range entries, current/projected Bollinger range exits, band-proximity zone calculations, and first-candle Bollinger qualification for the separate wickless HA rules. Defaults preserve the old thresholds.

`run_range_cross_test.sh` contains the user's latest command settings, both new switches enabled, and both multiples explicitly supplied. Its supplied end offset `-04:00` is preserved (19:00 New York standard time on November 12).
