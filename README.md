Copy the complete app files over their matching files in your current TradingMage project. This release builds on the revised exits/entry references and realtime progress releases from this conversation. Includes full files, not a patch.

Both /backtest/run and /backtest/plot now accept:
  entry_trigger=buy_sell_signal (default): existing1h buy/buy+ long and sell/sell+ short entry triggers.
  entry_trigger=turning_point: confirmed4h minimum triggers evaluation for long; confirmed4h maximum triggers evaluation for short.

Append entry_trigger=turning_point to your existing ./run_local_endpoint.sh plot command. To return to previous behavior use entry_trigger=buy_sell_signal or omit it.

Turningpoint entries execute at the evaluation candle completion timestamp, never its earlier plotted timestamp. Execution quotes use the same completed-minute simulated bid/ask machinery. Detection always uses4h regardless of fast_moving_average_reversal_timeframe, indicators, or fast_moving_average_reversed plotting flags.

Strategy mode (golden_cross/death_cross/both), actual fast/slow regime gate, require_entry_confirmation, sizing and exposure cap all remain active. A marker is an opportunity, not a guaranteed fill. Matching markers can add to an existing position subject to these gates; opposite markers do not flip a position. No buy/sell entry or addition occurs in turningpoint mode. Raw buy/sell and exit signals still drive the existing exit rules, including confirmation-pair exits; a synthetic marker does not count as a buy/sell confirmation signal.

Cross and signal exits are processed before marker entries at the same timestamp. A close prevents immediate same-timestamp re-entry. Duplicate marker entries are ignored. Trade records include entry_trigger and, for marker entries, turning_point_evaluated_at.

Realtime progress is preserved. No short-extension gate, portfolio giveback guard, or emergency stop from the prior analysis has been introduced. Scope: backtest run/plot; live execution mode is not modified by this endpoint option.

Validation:131 tests pass; production files compile. Tests cover default/validation, both endpoints, long/minimum and short/maximum entries, hidden or differently timed plots, confirmation timing, default signal suppression, duplicates, exit priority and existing entry gates. No external market-data backtest was run here.
