# Range strategy alongside cross strategy

Approved: cross_strategy_enabled=true and range_strategy_enabled=true independently control actual simulated trades; disabled_strategy_virtual_positions_enabled=true keeps disabled strategies trading virtually for the per-ticker opposite-side guard. Same-side strategy books coexist and exit only their own quantities. Range entries win same-time opposing requests.

Range decisions use every completed 1m price and incoming signal arrival, with latest completed 4h indicators. Enter near lower/upper band in a range; exit long/short near current or one-candle-projected opposite band even if the range later ends. Projection uses (band[i]-band[i-2])/2. Preserve existing .2 ATR proximity. Entry spacing compares native 4h candle indices, minimum difference two, per ticker and range side, including successful additions. Use normalized ATR/price linear sizing: 100% at <=1%, 25% at >=5%, with endpoint parameters. Existing exposure cap applies per owner and ticker.

1. Test pure proximity, projection, normalized sizing and shared guard defaults and validation.
2. Add separate strategy states using existing quote/fill/accounting methods. Keep disabled books outside actual PnL/exposure. Maintain ordered actual/virtual audit events.
3. Group the timeline by timestamp: market updates, range exits, cross exits, range entries, then cross entries. No future data, and no same-timestamp range reentry after its exit. Check shared guard before each owner's entry, including additions.
4. Aggregate enabled books for overall/ticker PnL; return per-strategy reports and separate cross/range direction histories. Carry strategy_type and is_virtual on events and positions.
5. Wire endpoint/settings controls. Range markers: Matplotlib tri_up/tri_down (1/2), thick purple/teal strokes; magenta star exits. Preserve cross shapes/colors. Include separate strategy PnL chart images.
6. Run regressions, synthetic full simulations covering virtual blocking and same-side ownership, render chart markers, and deliver full files plus test command. This release targets backtests, not live broker execution.
