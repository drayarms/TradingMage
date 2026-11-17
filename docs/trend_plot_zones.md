# Display-only 4h trend overlays

The range/trend engine and chart overlays share causal native 4h classifications. Zone flags drive trend decisions independently of chart visibility. All observations use completed candles and causally available turning-point confirmations.

## Heikin-Ashi streak condition

`trend_heikin_ashi_streak_condition_enabled` defaults to true. Calculate Heikin-Ashi candles from native completed regular 4h OHLC, independently of the displayed candlestick timeframe/type and all separate 1h strategy filters.

A long trend starts once an uninterrupted green 4h HA streak reaches `trend_heikin_ashi_streak_min_candles` (default **2**) and `abs(fast MA - slow MA) <= trend_heikin_ashi_streak_max_ma_gap_atr_multiple × ATR` (default **1.2**). A short trend starts under the same gap condition during an uninterrupted red streak of at least that many candles. At startup the fast MA must also be strictly above its value at the **first candle of the entire uninterrupted green streak** for longs, or strictly below its value at the first candle of the entire red streak for shorts. This uses signed net change, not absolute magnitude. Wicks do not matter. HA close > HA open is green, HA close < HA open is red, and equality is a doji.

A qualifying trend can start anywhere within an ongoing streak when the gap first qualifies. Once started, it continues regardless of subsequent MA-gap widening or unavailable gap/ATR inputs until that streak is interrupted. An opposite-color candle, doji or missing HA open/close ends it. The interrupting opposite-color candle starts counting toward the opposite streak. A new streak must satisfy the minimum count and gap condition again. The fast-MA sign check applies only at startup and is not rechecked during an active streak. A flat/missing first-candle MA cannot qualify startup. Even if the minimum count is configured to 1, the first streak candle has zero slope, so startup needs a later candle. No MA ordering, cloud alignment or turning point is needed for this branch.

Startup requires finite fast MA, slow MA and strictly positive finite 4h ATR. The minimum count is a positive integer; the ATR multiple is positive and finite. This condition replaces the former gradient branch completely; its endpoint argument and slope classification have been removed. Cloud crossing slopes remain part of the separate cloud condition.

## Cloud condition

`trend_cloud_condition_enabled` defaults to true. The existing cloud rules remain unchanged:

- Long: max(aligned span A, aligned span B) < current lower Bollinger, and fast MA and lower Bollinger are both above their values at the latest observed candle where the aligned cloud maximum crossed from above to below the lower Bollinger.
- Short: min(aligned span A, aligned span B) > current upper Bollinger, and fast MA and upper Bollinger are both below their values at the latest observed candle where the aligned cloud minimum crossed from below to above the upper Bollinger.

The crossing candle itself has zero slope and cannot qualify. Each fresh favorable crossing resets that direction's cloud reference. A cloud already on the favorable side at the first available aligned observation cannot establish a crossing. A touch followed by movement across counts as a crossing.

### Startup checks

`trend_cloud_fast_ma_band_gap_fraction` defaults to 0.4 and accepts nonnegative finite numbers. If the previous completed 4h period was not already in the same cloud-based trend, also require:

- Long: fast MA >= upper Bollinger - fraction × Bollinger gap, AND upper Bollinger is strictly above its value at the latest confirmed turning point's evaluation candle.
- Short: fast MA <= lower Bollinger + fraction × Bollinger gap, AND lower Bollinger is strictly below its value at that turning-point candle.

Both extra checks govern startup only. No confirmed turn means a new cloud trend cannot start. Existing crossing-anchored slopes still apply while the cloud stays on its original qualifying side. A failed normal cloud trend must pass startup checks again before resuming. Cloud state is independent of whether the HA streak branch qualifies.

### Continuation after a reverse cloud crossing

If an active long cloud trend's aligned cloud maximum crosses back to/above the lower Bollinger, continue solely while every completed native 4h Heikin-Ashi candle is green with no lower wick. If an active short cloud trend's aligned cloud minimum crosses back to/below the upper Bollinger, continue solely while every completed native 4h HA candle is red with no upper wick. Check the reverse-crossing candle itself.

Earlier cloud/MA/Bollinger slope requirements no longer constrain this continuation. The first wrong-color candle, wick, doji or missing HA value ends continuation and disarms that direction until a fresh favorable cloud crossing. A later wickless candle alone cannot restart it. Native 4h HA is calculated from regular OHLC independently of displayed candles and separate 1h trading filters. Wick checks allow only floating-point tolerance.

## Ichimoku alignment

Use regular-OHLC 9/26/52-period spans with 26-native-candle displacement. At completed candle t, compare other indicators with spans calculated at t-26, matching their visual alignment on the chart. Apply this alignment to cloud comparisons and crossing references unless explicitly specified otherwise. Never use newly calculated spans displayed 26 candles into the future.

## Universal peak and trough zones

`shade_universal_peak_trough_zones` defaults to true, independently of `shade_trend_zones`. `universal_zone_gradient_candles` is a positive integer, default **2**. At each completed native 4h candle t, with n equal to this argument:

```
latest gradient   = band[t] - band[t-n]
previous gradient = band[t-n] - band[t-2n]
```

Both universal zones require **both** Bollinger conditions simultaneously:

- Lower Bollinger: previous gradient < 0 AND latest gradient > previous gradient (for example -2 → -1).
- Upper Bollinger: previous gradient > 0 AND latest gradient < previous gradient (for example +2 → +1).

In addition, let `delta = current fast MA - fast MA at the last confirmed turning point's evaluation candle`:

- **Universal peak:** delta > 0 AND delta >= `universal_zone_min_fast_ma_change_atr_multiple` × current completed 4h ATR.
- **Universal trough:** delta < 0 AND -delta >= that multiple × current completed 4h ATR.

The new multiple defaults to **2**, is finite and nonnegative, and accepts an exact equality at the threshold. It applies to the signed change in MA value (delta y), not change per candle or the Bollinger acceleration interval. Use the latest completed 4h ATR, not the ATR at the reference. A zero multiple retains strict slope signs while disabling the magnitude floor; missing, zero, negative or nonfinite ATR cannot qualify either zone. Both existing Bollinger conditions and their previous-gradient sign constraints remain required. Neither latest band gradient needs to cross zero.

Comparisons are on the signed number line, never absolute magnitude. Equal gradients and zero previous gradients fail. Both full intervals require finite samples; incomplete history cannot qualify. The rolling samples may extend before the last MA turning point and are not truncated or reset by turning points. The Bollinger samples remain rolling, but the additional fast-MA net slope uses the latest causally confirmed turning-point reference. No confirmed reference, a flat fast MA, or a missing fast-MA value prevents both zones. Newly confirmed turns reset only the MA reference, without changing the rolling Bollinger intervals.

Reevaluate on every completed 4h candle, without latching: a zone ends as soon as its condition fails. They have no dependency on trend direction, candle color, MA order or other zones. Peak and trough classifications are now mutually exclusive because their fast-MA slope signs oppose each other; either can overlap existing fills/trend patterns without priority. Peaks use dark grey (#555555) crosses at half their previous width/height (twice the hatch density); troughs use dark grey separated short dashes at half their previous stroke length. Both span the full 4h Bollinger gap, have matching legend entries, and respect the `indicators` master visibility switch. These flags also govern trend exits/additions; shading and `indicators` control visibility only.

## Rendering and configuration

Enabled cloud and HA streak branches are ORed independently for each direction. If both switches are false, neither trend is classified. `shade_trend_zones=false` disables classification/preparation and shading; `indicators=false` hides overlays using the existing master switch.

| Trend | Color | Pattern |
|---|---|---|
| Long | Turquoise (#16BDB4) | Dots |
| Short | Pink (#E83E8C) | Dots |

Regions span the full native 4h lower-to-upper Bollinger gap, starting when the classified candle completes and holding until the next completed observation. They are clipped to the plot window on the compressed trading-session axis. Transparent backgrounds retain existing range/stretch zones and clouds. If both directions qualify, a sparser pink dot pattern keeps both visible. Legend keys match the regions.

Likely-start calculations, shading, legend entries, dashed patterns and all four `likely_trend_start_*` endpoint arguments have been removed. They no longer suppress strong-trend shading. Remove those arguments from older commands; `run_range_cross_test.sh` has already been updated.

The example script retains these display settings:

```bash
  shade_trend_zones=true \
  trend_cloud_fast_ma_band_gap_fraction=0.4 \
  trend_cloud_condition_enabled=true \
  trend_heikin_ashi_streak_condition_enabled=true \
  trend_heikin_ashi_streak_min_candles=2 \
  trend_heikin_ashi_streak_max_ma_gap_atr_multiple=1.2 \
  shade_universal_peak_trough_zones=true \
  universal_zone_gradient_candles=2 \
  universal_zone_min_fast_ma_change_atr_multiple=2
```

The example script enables both cloud and HA streak trend conditions and the universal peak/trough overlays, matching the supplied command. Other settings remain unchanged.
