# TradingMage cross-entry revamp

Status: agreed behavior consolidated for final design review; implementation has not started.

## Purpose and scope

Replace the cross book's entry policy with mandatory market-zone exclusions, trend-aligned or turning-point-gradient eligibility, direction-dependent sizing, and trigger-free operation with controlled addition frequency. Preserve the range strategy's detection, entries, sizing and exits. Preserve existing cross exits, portfolio replacement/precedence rules, calendar gates, and regular/extended-hours fill policies.

Base: latest recovered tradingmage_candle_exits_and_market_filters.zip, version 13. Work remains simulation-focused. Production code will use tabs.

## Approach

Use the existing shared cross evaluators and simulation/portfolio execution path. The user chose replacement of the old cross-entry restrictions, rather than layering the new policy over the old post-peak/trough policy. Layering would reject otherwise valid new entries. A separate strategy/book would introduce an unnecessary additional owner and would not implement the requested revamp of the existing cross book.

Keep the entry evaluators pure; maintain successful-fill spacing in the portfolio. Add causal completed-4h turning-point context to the existing historical frames. Reuse the existing market-state detection and historical order machinery.

## Approved entry rules

All cross entries and additions must pass the following:

1. The cross book is enabled (or participating virtually under the existing virtual-book configuration), strategy_mode permits the side, and there is valid completed 4h indicator context and sufficient history.
2. The current region is not ranging and neither extreme bullish nor extreme bearish price stretch qualifies. These exclusions are mandatory for either side. Reuse the existing range/stretch definitions, cusp history and configured thresholds; do not change the range classifier, shading or range-book behavior. Stretch tests use current candidate execution price against the completed 4h context, as the existing execution-time stretch filter does, and test both stretch directions regardless of entry side.
3. Let F be the latest completed 4h fast MA and S the corresponding slow MA. A long qualifies if F>S, or if F<S and its gradient since the last confirmed moving-average turning point is positive. A short qualifies if F<S, or if F>S and that gradient is negative. F=S qualifies neither side.
4. When require_entry_confirmation=true, current candidate execution price must be strictly above F for longs or strictly below F for shorts. Remove the previous-candle rising/falling requirement. False disables only this price-versus-fast confirmation.
5. enforce_atr_invalidation defaults to false. When true, retain the existing completed-4h-close boundary: close>=S-invalidation_atr_multiple*ATR for longs; close<=S+invalidation_atr_multiple*ATR for shorts. Preserve the existing invalidation_atr_multiple default of 1.0.
6. Positive sizing and remaining per-book/per-ticker exposure capacity are required.
7. A prior successful same-side cross entry/addition must be at least two completed native 4h candle indexes earlier. Entry at candle 10 permits the next at candle 12. Track separately by ticker and side, retain across exits, and keep independent of range-entry spacing. Opposite-side history does not reset a side's spacing.
8. Existing own-position direction, opposite-book replacement, simultaneous precedence, dated sessions, quote freshness and fill requirements must pass.

The old long/short range-proximity, extreme-stretch bypass switches, and post-peak/post-trough cross filters do not govern the revamped policy. Hard range exclusion replaces the old near-band-only cross range filter. Both post-peak/trough cross restrictions are replaced, per the user's latest clarification.

## Gradient and historical context

Implementation choice for review: use the most recently confirmed 4h fast-MA turning point already used by the current strategy, rather than changing the turning-point detector or its plotting. Evaluate the fast MA at that point's evaluation candle and at the current completed candle. The sign of their difference gives the requested positive/negative gradient; division by positive elapsed candle count would not change this eligibility test.

A point becomes usable only at its confirmed_at timestamp. At the point's own evaluation candle, net change is zero. Without a usable previous point, the countertrend branch is ineligible, while the trend-aligned branch can still qualify. Use the most recently confirmed point even if a fast/slow cross has occurred since it; the request specifies the last turning point, not a cross reset for this gradient. No later point or incomplete candle may influence an earlier entry.

## Sizing

Use the existing base amount B, exposure cap and configurable weights ws, wc, wt. Preserve the existing directional entry-cross reference: the long reference is the latest qualifying upward fast/upper-Bollinger crossing of slow MA; the short reference is the latest qualifying downward fast/lower-Bollinger crossing. Reference value and candle-age construction remain unchanged. This avoids introducing an unapproved change to which historical cross supplies distance and age. A valid side-specific entry-cross reference and age are sufficient; do not additionally require a separate old golden_cross_price/death_cross_price when that sizing reference is available. Warmup readiness must reflect the reference actually used by the revamped policy.

Let A=completed 4h ATR, P=candidate execution price, C=the existing side-specific entry-cross price, d=abs(P-C), a=completed native 4h candles since that reference, and T=cross_point_time_decay_candles (existing default 20).

The slow-MA component stays fs=A/(A+abs(P-S)) in both branches.

| Component | Trend-aligned branch | Countertrend branch |
| --- | --- | --- |
| Cross distance | fc=A/(A+d); multiply by 0.4 when d>A | fc=d/(A+d); no 0.4 penalty |
| Cross age | ft=T/(T+a) | ft=a/(T+a) |

Desired amount = B*(ws*fs+wc*fc+wt*ft). Clip to the existing remaining exposure capacity; quantity is clipped amount/P. These fractions are bounded and countertrend allocation rises with vertical and horizontal cross distance. The user approved these formulas and keeping the slow component, weights and cap unchanged.

Normal branch means long with F>S or short with F<S. Countertrend branch means long with F<S and positive gradient, or short with F>S and negative gradient.

## Trigger modes and scheduling

entry_trigger accepts none, buy_sell_signal and turning_point, with none as the new default.

- none: evaluate cross candidates on every completed eligible 1m market update, using only then-available completed 4h context. Successful-fill spacing controls repeated entries/additions.
- buy_sell_signal: retain 1h buy/buy+ long triggers and 1h sell/sell+ short triggers; still apply all new eligibility and spacing requirements.
- turning_point: retain confirmed 4h minimum long triggers and maximum short triggers; still apply all new eligibility and spacing requirements.

Preserve the existing execution phase order: pending orders are processed in their existing phase, then exit phases precede newly evaluated entry candidates at that timestamp. This is not a new promise that every pending entry waits for all same-timestamp exits. Preserve range/cross ordering under entry_strategy_precedence; do not move the shared pending phase and thereby change range behavior. Existing cross positions keep their side until an existing exit closes them; do not introduce an automatic opposite-side flip.

Implementation choice: with none, no position, confirmation disabled and both sides mathematically eligible, prefer the trend-aligned side. With the user's require_entry_confirmation=true, price relative to F ordinarily makes the side unique. Preserve direction-mode restrictions.

## Fill spacing and pending orders

Record spacing only after a successful fill, using the latest completed native 4h candle index at that fill. Rejected, cancelled, or expired orders do not consume spacing. Open and add fills both consume it. Do not reset it on exits, day boundaries, crosses or turning points.

Apply spacing before submission and check it again before a pending cross entry fills. Before any pending cross entry/addition fills, rerun the new cross eligibility rules using the latest then-completed 4h context, current turning-point gradient, and submitted limit as execution price. Recheck both zone exclusions, side/mode, optional confirmation/invalidation, sizing and spacing. Implementation choice: cancel a cross entry that no longer qualifies; a later qualifying opportunity may submit a new order. Never increase submitted quantity at fill; reduce it if current sizing/cap permits less. A stored signal/turning-point trigger does not require a second trigger solely because its eligible order fills later. Preserve existing normal limit reach/volume/expiry/replacement constraints and exit supersession. Apply this revalidation only to cross entry orders, including virtual ones; do not change range or exit order semantics. Range spacing and replacement behavior remain unchanged.

## Endpoint and command compatibility

Both /backtest/run and /backtest/plot must expose entry_trigger=none and enforce_atr_invalidation=false and pass them through the existing settings route. Results must retain the effective settings and add entry diagnostics identifying normal/countertrend sizing, reference, gradient, and spacing rejections.

Keep the six old directional entry-filter arguments accepted for backward command compatibility but explicitly deprecated/ignored for cross eligibility. Do not let false values bypass mandatory zone exclusions. Remove the six obsolete filter lines from the supplied updated command to avoid misleading users about their effect:

- long_range_entry_filter_enabled / short_range_entry_filter_enabled
- long_extreme_stretch_entry_filter_enabled / short_extreme_stretch_entry_filter_enabled
- long_post_peak_entry_filter_enabled / short_post_trough_entry_filter_enabled

Preserve all other arguments in the user's command. In particular, range thresholds and strategy toggles still control existing detection/range behavior; require_entry_confirmation=true now means price-versus-fast only.

## Updated command

```bash
#!/usr/bin/env bash
bash ./run_local_endpoint.sh plot \
	strategy_name=golden_cross_dip_recovery_4h \
	strategy_mode=both \
	cross_strategy_enabled=true \
	range_strategy_enabled=true \
	disabled_strategy_virtual_positions_enabled=true \
	entry_strategy_precedence=range \
	range_sizing_low_atr_price_ratio=0.01 \
	range_sizing_high_atr_price_ratio=0.05 \
	range_sizing_min_entry_fraction=0.25 \
	range_sizing_max_entry_fraction=1.0 \
	entry_trigger=none \
	enforce_atr_invalidation=false \
	cross_exit_method=fast_moving_average \
	fast_moving_average_reversed=true \
	fast_moving_average_reversal_timeframe=4h \
	start=2026-05-30T04:00:00-04:00 \
	end=2026-10-30T20:00:00-04:00 \
	position_size=6600 \
	exit_after_turning_point=true \
	exit_on_confirmatory_candlestick=true \
	shade_ranging_area=true \
	range_max_atr_to_bollinger_gap_ratio=0.5 \
	range_bollinger_gap_similarity_ratio=0.6 \
	range_min_bollinger_gap_to_peak_since_turning_point_ratio=0.8 \
	extreme_price_stretch_min_bollinger_gap_atr_multiple=3 \
	extreme_price_stretch_atr_multiple=2 \
	slow_average_to_price_weight=0.1 \
	cross_point_to_price_weight=0.6 \
	cross_point_to_current_moment_weight=0.3 \
	require_entry_confirmation=true \
	bullish_exit_above_fast_average_atr_multiple=1 \
	bearish_exit_below_fast_average_atr_multiple=0.75 \
	bollinger_period=20 \
	bollinger_std_dev=2 \
	lux_algo_symbols=false \
	lux_algo_symbols1m=false \
	lux_algo_symbols5m=false \
	lux_algo_symbols15m=false \
	lux_algo_symbols1h=true \
	lux_algo_symbols4h=true \
	indicators=true
```

## Verification required before delivery

- Long/short eligibility truth table, equality, positive/negative/zero/missing turning-point gradient, completed-bar and prefix causality.
- Mandatory range and both stretch exclusions despite obsolete bypass switches; original range classifier and range-book decisions unchanged.
- require_entry_confirmation checks price only; latest-candle MA slope cannot veto an otherwise qualifying entry.
- ATR invalidation off/on boundaries and parameter propagation through both endpoints.
- Existing normal sizing including the >1 ATR penalty; reversed fractions, bounds, monotonicity, weights and exposure caps. A valid side-specific band/fast cross reference must suffice even without a separate old fast-only cross price.
- Cross fill in candle 10 blocks 10 and 11 and permits 12, independently per ticker/side; persists across exit/reentry, applies to adds, and respects native candle indexes over weekends/gaps.
- Pending and virtual fills update spacing, unsuccessful orders do not, and delayed fills cannot bypass any mandatory entry condition. Test a submitted cross order cancelled when a later completed candle becomes ranging/extreme, gradient/confirmation fails, or capacity vanishes. Range pending-order behavior and phase order remain unchanged.
- All three trigger modes, continuous entry without signal arrival, deterministic flat-side selection, no automatic own-book flip.
- Regression tests for existing exits, range entries/sizing/spacing, precedence/replacement, regular/extended session execution, holidays, pending limits and result serialization.
- Full available test suite, Python compilation, shell syntax for the updated command, and a review of the changed files.

No historical profitability or execution accuracy improvement will be claimed from synthetic tests. The resulting historical comparison must run with the user's data access.
