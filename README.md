# TradingMage cross-entry and exit filters

Full replacement files for an existing TradingMage project. Copy the included app/ files into your project app/ directory and the shell scripts into the project root. Keep the rest of your project. This is not a standalone application.

Run your updated command from the project root:

```bash
bash ./run_range_cross_test.sh
```

## Cross entries

Cross entries and additions must be outside ranging markets and both extreme price-stretch zones. Longs require fast 4h MA above slow 4h MA, or fast below slow with a positive net fast-MA change since the latest confirmed turning point. Shorts mirror this. Turning points only become available after confirmation; their fast-MA reference is taken from the actual fast MA, independently of the marker source.

Aligned entries retain existing sizing. Countertrend entries reverse the distance and elapsed-time components while retaining the slow-MA component, weights and exposure cap. Existing side-specific cross references are retained.

Same-side successful cross fills must be at least two native 4h candle indices apart, leaving at least one candle between entries. This applies to additions and survives position exits. Range spacing is separate.

`entry_trigger=buy_sell_signal` is the default. Explicit `none` evaluates entries on completed eligible minute bars; `turning_point` remains available. `require_entry_confirmation=true` checks only price above fast MA for longs or below fast MA for shorts. `enforce_atr_invalidation=false` is the default; true enables the previous completed-4h-close ATR boundary check.

Legacy directional range, stretch and post-turn filter flags remain accepted for compatibility but cannot override the new cross rules. They are removed from the supplied command. Pending cross limit orders recheck current eligibility, spacing and sizing before filling; quantity can shrink but cannot grow.

## Additional cross entry filters

Use `cross_entry_bollinger_gap_atr_divisor=2` (default, finite and positive). Every cross entry/add requires strictly `ATR < (upper Bollinger - lower Bollinger) / divisor`.

After a bullish stretch, long entries require the most recent completed 4h candle's high strictly above its own upper Bollinger. After a bearish stretch, shorts require its low strictly below its own lower Bollinger. A range resets the remembered stretch, and an opposite stretch replaces it. Hugging is an exception for that candle and does not erase the remembered zone. Existing active range/stretch exclusions still apply.

The latest completed 4h candle must also have a body maximum greater than at least one of the previous two body maxima for a long, or a body minimum below at least one previous two body minima for a short. Equality fails; two completed predecessors are required.

## Universal exit veto and remembered exits

Extreme bullish and bearish stretch candles cannot also be classified as ranges.

All exits in both strategy books are blocked while the upper Bollinger, lower Bollinger and fast MA changes from the candle with the narrowest Bollinger gap since the latest confirmed 4h moving-average turning point are all positive for longs or all negative for shorts. Search includes the turning-point evaluation candle and subsequent completed candles. Each indicator uses its own value on that same minimum-gap candle; ties use the most recent candle. No unconfirmed point or incomplete candle is visible. The fast-MA gradient for countertrend entries still starts at the turning point. Zero or unavailable gradients do not veto.

The first blocked exit stays attached to that specific position. When the veto clears, it executes without a new trigger at the first permitted execution opportunity, using current quotes and the existing calendar/extended-hours limit model. The veto is checked again before delayed fills. Closing clears the remembered intent and cancels pending additions belonging to that book/ticker; it cannot reopen or close a later position accidentally. Reports include `gradient_blocked_exit_intents` and block/cancellation events.

Range long positions also exit immediately after a confirmed native 4h fast/slow golden cross, and range shorts after a death cross, subject to the universal veto and execution constraints. These triggers are independent of `cross_exit_method`; existing other exit rules remain available.

This release targets the backtest run/plot endpoints, not a live continuous-entry scheduler.

## Validation

See docs/progress.md for verification results and docs/design.md for the approved detailed specification. No authenticated historical-data backtest or live orders were run here. Tests use deterministic fixtures.

The archive includes the latest supplied source files plus the changed history module, tests and design notes. Unchanged older modules recovered solely to run tests are not supplied as replacements. Run tests in your full project using `python -m unittest discover -s tests`.

## Post-stretch range class

`range_strategy_enabled=true` also enables a separately owned/reported `post_stretch_range` class. During the first two completed native 4h candles after the last extreme stretch candle, when currently in a range, a bullish stretch permits short entry above the fast MA and a bearish stretch permits long entry below the fast MA. Equality does not qualify. This class replaces ordinary range band-proximity and minimum-width-reference entry checks with that fast-MA condition.

When both classes qualify, post-stretch range has priority, including while its order is pending. Both classes share normal range sizing, same-side fill spacing, and the range exposure cap. Opposite positions being replaced do not consume replacement capacity. Existing cross-versus-range precedence still applies. New-class delayed entries must still satisfy their window and price condition when filled.

The new class retains its ownership and liberal exits for the position lifetime: exit near the current target Bollinger or any of its one-, two-, or three-candle projections, using only the existing completed two-candle slope. Ordinary range retains its one-candle projection. Native range cross exits, calendar/limit execution and the universal gradient veto/remembered release still apply. JSON includes separate strategy and directional results; charts use diamonds for this class's entries and filled-plus markers for exits, preserving existing direction colors. No command changes required.

## Yellow range transition zones

`range_transition_zones_enabled=true` (default) includes the yellow zones in analysis. Set it to `false` to disable their classification, shading, entry restrictions and zone-triggered exits. Other range/stretch/cross rules and the three-gradient exit veto remain active.

Classification uses only completed 4h Bollinger values. Keep `range_transition_band_speed_multiple=2` for n. The net movements still start at the most recent minimum-gap candle since the confirmed turning point:

- Bullish transition: lower band has declined, upper band has risen at least n times that decline, and the upper band's latest completed-candle increment is positive and strictly greater than its preceding increment.
- Bearish transition: upper band has risen, lower band has declined at least n times that rise, and the lower band's latest increment is negative with strictly greater absolute magnitude than its preceding increment (for example -3 to -4).

Recent increments are band[t] - band[t-1] and band[t-1] - band[t-2], using native completed 4h candles. Equal or unavailable increments do not qualify. Zones remain range-only and cannot overlap an extreme stretch. Market price no longer creates or changes a yellow zone. The obsolete `range_transition_price_speed_multiple` (m) remains accepted with its previous validation for compatibility but is ignored; it is omitted from the updated runner.

Both transition types remain yellow. Only the bullish-transition trading restrictions remain: all three books prohibit shorts and trigger short exits there, subject to the universal veto and remembered release. Bearish-transition zones no longer impose a long-entry ban or force long exits. Opposite extreme-stretch zones no longer independently force range or post-stretch range exits. All other entry and exit rules remain active.

Cross aligned entry restrictions and the `entry_trigger=buy_sell_signal` default remain unchanged. Reports include compressed `range_transition_zones` intervals for chart overlays. No live orders or authenticated historical backtest were run here.
