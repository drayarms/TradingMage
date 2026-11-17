# Range and trend strategy family

The public family is `strategy_name=range_trend_4h`. The HTTP run/plot endpoints no longer accept cross-only configuration, and reject the old family name rather than executing its old cross book. Historical cross modules and the private backtester adapter remain in the source overlay only as dependencies/regression reference for established indicator and range behavior; the new family and live dispatcher do not call cross-entry or cross-exit policies.

## Trend rules

Use latest completed native 4h indicators and current completed one-minute market-price approximation. Calculate trend and universal flags for trading even when `shade_trend_zones`, `shade_universal_peak_trough_zones`, or `indicators` hides them. Cloud and HA-streak definitions are unchanged: each enabled branch can establish a direction, and that direction ends when neither enabled branch identifies it. The shared branch settings are independent of visibility.

- Long entry: the latest universal zone is a trough, the first completed green native 4h HA candle has just interrupted one or more red candles, no short trend is active, and `abs(price-fast MA) < abs(price-upper Bollinger)`.
- Short entry mirrors this: latest universal zone is a peak, first red native 4h HA after one or more green candles, no long trend, and price strictly closer to fast MA than lower Bollinger.
- A matching trend is not required. The universal zone may still be active; multiple distinct reversals can qualify after the same zone until the opposite universal zone occurs. HA dojis interrupt the color sequence and do not themselves count as a dip/lift. Wicks do not matter.
- The completed reversal remains eligible until the next native 4h candle completes. Recheck distance using each current completed one-minute market-price approximation; do not wait for a new 4h close for proximity. One actual fill per reversal, including after a full close. Missing the distance condition at the reversal close does not consume it. No retrospective fills for missed live minutes.
- Per-side trend fills use `position_size / 2**existing_fill_count` dollars at their executable fill price: 6600, 3300, 1650, etc. Count only actual open trend fills on that side, excluding normal/stretch range books, the other side, failed orders and pending orders. Closing the entire side resets sizing. Additions do not reset band-visit history. No artificial position-count/exposure cap is imposed.
- Long exits: price **strictly above** upper Bollinger + `trend_exit_bollinger_atr_multiple` × ATR; universal peak while **not** in a long trend; a worse upper-band visit; or the contracted opposing-band condition below. Short exits mirror these. Trend ending or the opposite trend starting alone no longer exits a holding.
- A band visit uses the established one-sided proximity test: long upper-band visit when `price > upper - bollinger_proximity_atr_multiple * ATR`; short lower-band visit when `price < lower + multiple * ATR`. Track the **first observed market price** of each visit while the side is open. A new visit requires leaving and reentering the zone. Exit all long trend fills if the new visit's first price is strictly lower than the previous visit's first price; exit shorts if strictly higher. Equality does not exit; fluctuations within one visit do not count as separate approaches. Tracking starts at the first fill and clears on full close.
- Contracted opposing-band exit: long near/below the lower band, or near/below its one-candle projection; short near/above the upper band, or near/above its one-candle projection; **and** `current gap < widest gap / trend_exit_bollinger_gap_contraction_divisor`. The widest gap spans the second-to-last confirmed turning point's evaluation candle through the current completed candle, including intervening history. This exit waits for two confirmed turning points; it never uses an unconfirmed point. New confirmations advance the reference causally.
- Projection follows the existing normal-range formula: `projected_band = band[t] + (band[t] - band[t-2]) / 2`, one native 4h candle ahead, compared with the current minute-price approximation and the same Bollinger proximity margin. It extrapolates the band, not price.

`trend_strategy_enabled` defaults to true; `trend_exit_bollinger_atr_multiple` defaults to 1 (finite and nonnegative); `trend_exit_bollinger_gap_contraction_divisor` defaults to 2 (finite and positive). LuxAlgo triggers, old cross confirmation rules and three-gradient exit suppression do not govern the trend book. Both enabled trend-classification branches remain ORed. If both directions are classified as trends, their opposing-trend entry guards block both sides; existing holdings still follow the new exits above.

Evaluate exits before new entries/additions at a timestamp, including 4h completions without a new minute bar. A book cannot reenter on the same timestamp it closed. Regular-hours simulation uses executable bid/ask approximations; extended hours uses DAY limits touched by a subsequent complete one-minute bar with sufficient positive volume. Pending entries are revalidated against current conditions at fill time. An unfilled trend order is canceled as soon as its completed HA reversal window ends, even if its limit has not been touched, releasing any precedence reservation. Session/holiday handling remains Alpaca calendar based, bounded to supported extended hours. Positions are retained overnight; this is not overnight-hours order submission.

### Opposing universal-zone entry cooldown

`trend_entry_opposing_universal_zone_cooldown_candles` defaults to **2** and accepts nonnegative integers. It blocks **all trend entries**, including initial entries and additions:

- Long: block while within n completed native 4h candles of the last completed universal **peak** candle.
- Short: block while within n completed native 4h candles of the last completed universal **trough** candle.

Every active opposing-zone candle resets the counter to zero, so the reference is its last completed candle, not its first. After that reference, the next n native candles are blocked; candle n+1 permits entry if the other rules qualify. For n=2 the elapsed counts 0, 1 and 2 are blocked and 3 is allowed. Count observed native candles rather than elapsed clock hours, so nights/weekends do not consume additional candles. No previous observed opposing zone means no cooldown restriction. A value of 0 disables this additional guard; existing universal-zone exit rules still apply.

This guard runs before precedence replacements, applies to disabled-strategy trend shadow books too, and is rechecked before a pending entry fills. It does not close an existing position or change range-entry/exit rules. Shared frame preparation computes the same causal counter for backtesting and live evaluation, independently of chart shading. Live configuration can set it in `RANGE_TREND_SETTINGS_JSON`.

## Range rules and precedence

Normal range and post-stretch range retain their entry, sizing, HA guards, spacing and exits, including three-gradient suppression and remembered range exits. Range sizing still has its established range exposure limit; trend sizing does not share that limit. The two range classes count together for family precedence.

`entry_strategy_precedence` accepts:

| Value | Behavior |
|---|---|
| `range` (default) | Existing normal/stretch range holdings block trend entries/additions. A qualifying range entry closes trend holdings before proceeding. |
| `trend` | Existing trend holdings block both range classes. A qualifying trend entry closes range holdings before proceeding. |
| `none` | Independent books may hold opposing positions in the same asset, without replacing one another. |

Precedence applies regardless of position side, including same-side holdings in different families. Replacement exits bypass gradient veto, but still obey execution/session/liquidity constraints. If replacement cannot complete, the new entry waits. Pending preferred entries reserve precedence until filled or canceled. Disabled-strategy shadow positions, controlled by `disabled_strategy_virtual_positions_enabled`, are excluded from actual PnL/net inventory and cannot block, close, or consume liquidity from enabled books.

All reports retain each strategy's independent inventory/cost basis and PnL. Overall PnL sums those books, including hedged holdings, rather than overwriting one side. Reports expose signed `net_positions` in shares. Gross strategy exposure and net market exposure are distinct values; net zero can still have gross virtual exposure. The chart ZIP includes overall and per-book PnL. No end-of-run forced liquidation is introduced.

## Net capital and virtual-ledger comparison

Capital reporting defaults to net inventory across **enabled** strategies. At each observation, offset long and short shares **within each ticker**, value the residual at its current market price, then sum the absolute ticker values. Long AAPL and short AMD do not offset one another. Disabled-strategy shadow books and pending/unfilled orders do not count in either view. Gross virtual-ledger exposure still values every enabled book before offsets, so fully hedged holdings can show zero net capital and positive ledger exposure.

The existing `max_open_positions_report.txt` now shows the **net-position** and **virtual-ledger gross** simultaneous peaks side by side, with independent first-observed times, each stock's independent peak, holdings at each portfolio peak, the other view's value at the same moment, daily peaks, and daily-peak statistics. Neither portfolio peak is the sum of separate stock peaks. Observe at the existing pre-exit and post-entry-batch boundaries using completed-minute prices; do not invent transient exposure from sequential bookkeeping inside one decision batch.

API outputs:

- `capital_exposure_basis="net_positions"` identifies the default capital calculation.
- `max_open_positions_exposure.overall` and `.by_ticker` now use **net** values. Explicit `.net_positions` and `.virtual_ledger` contain the two complete peak views. This changes the meaning of the previously unqualified peak fields; consumers requiring the old gross values should read `.virtual_ledger`.
- `overall_pnl_history` retains `gross_open_exposure` and `net_open_exposure`; adds `virtual_ledger_open_exposure`, `capital_open_exposure` (net), and `net_long_open_exposure`/`net_short_open_exposure`.
- `daily_max_open_exposure` includes `daily_max_net_open_exposure`, `daily_max_capital_open_exposure` (net), and the preserved `daily_max_gross_open_exposure`. Daily maxima in the two columns can occur at different moments.
- `daily_max_open_exposure_summary` uses net capital for its default mean, sample standard deviation, maximum and minimum; explicit `.net_positions` and `.virtual_ledger` preserve both sets. Statistics use days with positive exposure in the respective view, with zero-safe empty results and a separate observed-day count.

The chart ZIP includes both the text report and `capital_exposure_summary.json`. The local runner copies both alongside the charts and prints the existing text report; the run endpoint returns the report first so the runner can extract it without reading large history arrays. No additional endpoint argument is required.

These are net target inventory **market values**, before broker margin/buying-power requirements, rounding and execution differences. They do not model cash balances, short-sale collateral or actual broker fills. Independent strategy PnL, entries, exits and sizing limits remain strategy-ledger calculations; offsetting inventory changes aggregate capital reporting rather than erasing holdings or trading rules.

## Trade markers

Every strategy initial entry/addition is turquoise (#16BDB4); every exit is pink (#E83E8C). Shape is the same for entry and exit:

| Book and side | Matplotlib marker |
|---|---|
| Trend long | `^` triangle_up |
| Trend short | `v` triangle_down |
| Normal range long | `2` tri_up |
| Normal range short | `1` tri_down |
| Post-stretch range long | `d` thin diamond |
| Post-stretch range short | `s` square |

LuxAlgo signal markers retain their separate semantics.

## Updated command

`run_range_cross_test.sh` retains the supplied dates, range settings, indicator choices, and 4h regular chart settings. It enables both trend and range strategies, uses `entry_strategy_precedence=none`, sets the trend stretch-exit multiple to 2.2, and removes cross-only arguments. It includes `trend_exit_bollinger_gap_contraction_divisor=2`, `trend_entry_opposing_universal_zone_cooldown_candles=2` and `universal_zone_min_fast_ma_change_atr_multiple=2`, requiring a positive fast-MA change of at least 2× current 4h ATR for universal peaks and a negative change of at least that magnitude for troughs. Both existing Bollinger conditions remain required.

Removed public arguments include `cross_strategy_enabled`, cross HA flags, `cross_exit_method`, `cross_entry_bollinger_gap_atr_divisor`, `enforce_atr_invalidation`, `entry_trigger`, `require_entry_confirmation`, old cross weighting parameters, and old cross signal/turning-point exit settings. Fast/slow MA definitions, ATR, Bollingers, native turning-point analysis and range HA switches remain supported.

## Live virtual ledger and net execution

Live wiring is included, but **order submission defaults to disabled**. No deployment, credentials, account changes or real orders are performed by this source update. Tests use fake brokers plus deterministic market data; this release has not been verified against a connected Alpaca paper/live account.

New live configuration:

```bash
ACTIVE_LIVE_STRATEGY=range_trend_4h
RANGE_TREND_LIVE_ORDERS_ENABLED=false
RANGE_TREND_TICKERS=AAPL,AMD
RANGE_TREND_SETTINGS_JSON='{"strategy_mode":"both","entry_strategy_precedence":"none","trend_strategy_enabled":true,"range_strategy_enabled":true,"trend_entry_opposing_universal_zone_cooldown_candles":2,"trend_exit_bollinger_gap_contraction_divisor":2}'
RANGE_TREND_ANALYSIS_JSON='{"trend_cloud_condition_enabled":true,"trend_heikin_ashi_streak_condition_enabled":true,"universal_zone_gradient_candles":2,"universal_zone_min_fast_ma_change_atr_multiple":2}'
```

The existing `strategy4_1h_anchor` broker account/API is the shared account. Settings above are application environment settings, separate from backtest endpoint parameters. The monitor uses an explicit watchlist, persisted webhook-registered symbols, or existing 1h/4h stream symbols if no watchlist exists. No LuxAlgo trigger is required for the trend decision. Only the latest fresh completed minute is evaluated, not missed historical entries after a restart.

Redis namespace is `range_trend_4h:<account-id>:`. It stores each symbol's virtual books, last actual virtual fills, range spacing, consumed HA reversal identities, first-price Bollinger visit state, actual trend fill counts, remembered gradient-blocked range exits, and net target. Virtual ledger fills are strategy accounting events at bid/ask; **they are not broker fill confirmations**. They remain distinct from broker inventory and broker PnL. The monitor reconciles the enabled books' signed share sum to actual broker holdings, explicitly reporting pending/partial/unfilled/rejected orders. Slippage, rounding, borrowing costs and delayed/failed broker orders can make real account PnL differ from virtual PnL.

Net orders use a renewable account lock, fresh quotes, supported sessions, DAY limit orders and deterministic persisted client IDs. Submission-timeout recovery looks up the same ID rather than creating duplicate orders. A target change waits for cancellation/terminal confirmation; actual holdings are refreshed afterward. Position reversals flatten the broker position before opening the opposite side. Short targets use whole shares; unsupported fractional extended-hours deltas wait for a core session. Tradability/shortability and broker account restrictions are respected; actual buying-power constraints still come from Alpaca.

Existing unmanaged holdings or unrelated open orders are not silently taken over. Use exclusively managed symbols in the selected shared account; manual/other-program trading in an owned symbol is outside this executor's inventory model. A submission whose existence cannot be resolved after a target change fails closed (`awaiting_uncertain_order_resolution`) rather than guessing. Do not erase Redis ledger/order state while positions or orders remain outstanding.

Before activating orders, apply the source files and validate the configuration against the intended paper account. Turning the enable flag on is an operator deployment choice; this update leaves it off.
