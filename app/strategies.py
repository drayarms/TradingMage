import logging
import time
from datetime import datetime

logger = logging.getLogger("tv-webhook")

class Strategies:
	def __init__(self, trading_view_webhook_helpers, trade_records):
		self.tvw_helpers = trading_view_webhook_helpers
		self.r = trading_view_webhook_helpers.require_redis()
		self.trade_records = trade_records
		self.SMALLEST_SHARE_SIZE = 0.25
		

	def is_tf_relative_to_last_higher_tf(
		self,
		ticker,
		signal,
		timeframe,
		current_tf,
		higher_tf,
		relation="opposite",
	):
		"""
		Evaluates the relationship between the current signal's timeframe and the
		most recent signal from a higher timeframe for a given ticker.

		This function determines whether the current signal side (buy/sell) is either:
			- opposite to the most recent higher timeframe signal, or
			- the same as the most recent higher timeframe signal

		Signal normalization:
			- "buy" and "buy+" are treated as "buy"
			- "sell" and "sell+" are treated as "sell"

		Parameters:
			ticker (str):
				The ticker symbol (e.g., "AAPL").

			signal (str):
				The incoming signal (e.g., "buy", "buy+", "sell", "sell+").

			timeframe (str):
				The timeframe of the incoming signal (e.g., "1m", "5m", "1h").

			current_tf (str):
				The required timeframe for this check to apply (e.g., "1h").

			higher_tf (str):
				The higher timeframe to compare against (e.g., "4h").

			relation (str, optional):
				The relationship to evaluate:
					- "opposite" → returns True if sides differ
					- "same" → returns True if sides match
				Default is "opposite".

		Returns:
			bool:
				True if the specified relationship condition is met, False otherwise.

		Behavior:
			- Returns False if:
				- timeframe does not match current_tf
				- signal cannot be normalized to buy/sell
				- no higher timeframe signal exists
				- higher timeframe signal is invalid
			- Otherwise evaluates the relationship between the two signals.

		Example:
			- current_tf="1h", higher_tf="4h", relation="opposite"
			  → True if latest 1h signal is opposite of latest 4h signal
		"""

		tf = self.tvw_helpers.normalize_tf(timeframe)
		if tf != current_tf:
			return False

		sym = str(ticker or "").upper().strip()
		current_side = self.tvw_helpers.normalize_signal(signal)

		if current_side not in {"buy", "sell"}:
			return False

		last_alert = self.tvw_helpers.get_nth_last_alert(sym, higher_tf, 1)
		if last_alert is None:
			return False

		_, last_fields = last_alert
		last_side = self.tvw_helpers.normalize_signal(last_fields.get("signal"))

		if last_side not in {"buy", "sell"}:
			return False

		if relation == "opposite":
			return current_side != last_side

		if relation == "same":
			return current_side == last_side

		return False


	def get_latest_valid_same_side_signal(self, ticker, side, higher_tf, max_scan=500):
		"""
		Return the OHLCV context for the most recent signal on the specified higher
		timeframe whose normalized side matches the side under consideration.

		Signal normalization:
			- "buy" and "buy+" are treated as "buy"
			- "sell" and "sell+" are treated as "sell"

		Validity constraint:
			A candidate same-side signal is considered valid only if there is no more
			recent signal on that same higher timeframe with the opposite normalized side
			between the candidate and now.

		How it works:
			- Reads recent entries from the Redis stream for the given ticker and
			  higher timeframe.
			- Scans backward from newest to oldest.
			- If the first relevant higher-timeframe signal encountered is opposite-side,
			  returns None because any older same-side signal is invalidated.
			- If the first relevant higher-timeframe signal encountered is same-side,
			  returns its OHLCV context.

		Parameters:
			ticker (str):
				The ticker symbol, e.g. "AAPL".

			side (str):
				The side under consideration, e.g. "buy", "buy+", "sell", or "sell+".

			higher_tf (str):
				The higher timeframe to inspect, e.g. "15m", "1h", or "4h".

			max_scan (int, optional):
				The maximum number of recent stream entries to inspect.
				Default is 500.

		Returns:
			Optional[dict]:
				A dictionary containing the matched higher-timeframe signal context if a
				valid same-side signal is found, otherwise None.

				Returned fields:
					- id
					- ticker
					- timeframe
					- signal
					- normalized_signal
					- bar_close_time_eastern
					- open
					- high
					- low
					- close
					- volume
					- price
		"""
		sym = str(ticker or "").upper().strip()
		target_side = self.tvw_helpers.normalize_signal(side)
		tf = self.tvw_helpers.normalize_tf(higher_tf)

		if target_side not in {"buy", "sell"}:
			logger.info(
				"Invalid side passed to get_latest_valid_same_side_signal: %r",
				side,
			)
			return None

		if not tf:
			logger.info(
				"Invalid higher_tf passed to get_latest_valid_same_side_signal: %r",
				higher_tf,
			)
			return None

		opposite_side = "sell" if target_side == "buy" else "buy"
		stream_key = self.tvw_helpers.stream_key(tf, sym)

		try:
			entries = self.r.xrevrange(stream_key, count=max_scan)
		except Exception:
			logger.exception("Failed reading %r stream for %r", tf, sym)
			return None

		if not entries:
			logger.info("No %r entries found for %r", tf, sym)
			return None

		for entry_id, fields in entries:
			entry_side = self.tvw_helpers.normalize_signal(fields.get("signal"))

			if entry_side not in {"buy", "sell"}:
				continue

			# If the first relevant higher-timeframe signal encountered going backward
			# is the opposite side, then any older same-side candidate is invalid.
			if entry_side == opposite_side:
				logger.info(
					"No valid %r %r context for %r because newer opposite-side signal exists: %r",
					tf,
					target_side,
					sym,
					entry_id,
				)
				return None

			# First relevant signal encountered is same-side, so it is the most recent valid one.
			if entry_side == target_side:
				bar_close_time_eastern = fields.get("bar_close_time_eastern")

				return {
					"id": entry_id,
					"ticker": fields.get("symbol") or sym,
					"timeframe": self.tvw_helpers.normalize_tf(fields.get("timeframe") or tf),
					"signal": fields.get("signal"),
					"normalized_signal": entry_side,
					"bar_close_time_eastern": bar_close_time_eastern,
					"open": self.tvw_helpers.safe_float(fields.get("open")),
					"high": self.tvw_helpers.safe_float(fields.get("high")),
					"low": self.tvw_helpers.safe_float(fields.get("low")),
					"close": self.tvw_helpers.safe_float(fields.get("close")),
					"volume": self.tvw_helpers.safe_float(fields.get("volume")),
					"price": self.tvw_helpers.safe_float(fields.get("price")),
				}

		return None


	def lower_tf_confirms_mid_tf_opposite_of_higher_tf(
		self,
		ticker,
		signal,
		timeframe,
		valid_lower_tfs,
		mid_tf,
		higher_tf,
	):
		"""
		Returns True if:
			- the current timeframe is one of the allowed lower timeframes
			- the current signal side matches the most recent signal side on mid_tf
			- the most recent signal side on mid_tf is opposite of the most recent
			  signal side on higher_tf

		Signal normalization:
			- "buy" and "buy+" are treated as "buy"
			- "sell" and "sell+" are treated as "sell"

		Parameters:
			ticker (str):
				The ticker symbol, e.g. "AAPL".

			signal (str):
				The current incoming signal, e.g. "buy", "buy+", "sell", or "sell+".

			timeframe (str):
				The timeframe of the incoming signal.

			valid_lower_tfs (Iterable[str]):
				The set or list of lower timeframes allowed for this confirmation check,
				e.g. {"1m", "5m", "15m"}.

			mid_tf (str):
				The intermediate timeframe whose latest signal must match the current
				signal, e.g. "1h".

			higher_tf (str):
				The higher timeframe whose latest signal must be opposite of mid_tf,
				e.g. "4h".

		Returns:
			bool:
				True if:
					- normalized current timeframe is in valid_lower_tfs
					- normalized current signal matches the latest mid_tf signal
					- latest mid_tf signal is opposite of latest higher_tf signal

				False otherwise.

		Example:
			- valid_lower_tfs={"1m", "5m", "15m"}, mid_tf="1h", higher_tf="4h"
			  returns True when:
				- current signal is on 1m, 5m, or 15m
				- current signal matches the latest 1h signal
				- latest 1h signal is opposite of latest 4h signal
		"""
		tf = self.tvw_helpers.normalize_tf(timeframe)
		allowed_lower_tfs = {
			self.tvw_helpers.normalize_tf(item) for item in (valid_lower_tfs or [])
		}
		mid_tf_norm = self.tvw_helpers.normalize_tf(mid_tf)
		higher_tf_norm = self.tvw_helpers.normalize_tf(higher_tf)

		if tf not in allowed_lower_tfs:
			return False

		sym = str(ticker or "").upper().strip()
		current_side = self.tvw_helpers.normalize_signal(signal)

		if current_side not in {"buy", "sell"}:
			return False

		if not mid_tf_norm or not higher_tf_norm:
			logger.info(
				"Invalid timeframe configuration in lower_tf_confirms_mid_tf_opposite_of_higher_tf: mid_tf=%r higher_tf=%r",
				mid_tf,
				higher_tf,
			)
			return False

		last_mid_alert = self.tvw_helpers.get_nth_last_alert(sym, mid_tf_norm, 1)
		if last_mid_alert is None:
			return False

		last_higher_alert = self.tvw_helpers.get_nth_last_alert(sym, higher_tf_norm, 1)
		if last_higher_alert is None:
			return False

		_, last_mid_fields = last_mid_alert
		_, last_higher_fields = last_higher_alert

		last_mid_signal = self.tvw_helpers.normalize_signal(last_mid_fields.get("signal"))
		last_higher_signal = self.tvw_helpers.normalize_signal(last_higher_fields.get("signal"))

		if last_mid_signal not in {"buy", "sell"}:
			logger.info(
				"lower-tf confirms %r vs %r: INVALID last_mid_signal=%r for ticker=%r",
				mid_tf_norm,
				higher_tf_norm,
				last_mid_signal,
				sym,
			)
			return False

		if last_higher_signal not in {"buy", "sell"}:
			logger.info(
				"lower-tf confirms %r vs %r: INVALID last_higher_signal=%r for ticker=%r",
				mid_tf_norm,
				higher_tf_norm,
				last_higher_signal,
				sym,
			)
			return False

		is_true = (
			(current_side == last_mid_signal) and
			(last_mid_signal != last_higher_signal)
		)

		logger.info(
			"lower-tf confirms %r vs %r: ticker=%r tf=%r current=%r last_mid=%r last_higher=%r result=%r",
			mid_tf_norm,
			higher_tf_norm,
			sym,
			tf,
			current_side,
			last_mid_signal,
			last_higher_signal,
			is_true,
		)

		return is_true		



	def has_opposite_signal_since_last_valid_same_side_higher_tf(
		self,
		ticker,
		signal,
		opposite_tf,
		anchor_tf,
		max_scan_opposite_tf=1000,
		max_scan_anchor_tf=500,
	):
		"""
		Return True if there exists an opposite-side signal on opposite_tf that occurred
		after the most recent valid same-side signal on anchor_tf for the given ticker
		and incoming signal side.

		Signal normalization:
			- "buy" and "buy+" are treated as "buy"
			- "sell" and "sell+" are treated as "sell"

		Definitions:
			- Anchor signal:
				The most recent valid same-side signal on anchor_tf whose normalized side
				matches the incoming signal side, as determined by
				get_latest_valid_same_side_signal(...).

			- Opposite-side signal:
				A signal on opposite_tf whose normalized side is opposite to the incoming
				signal side.

		Behavior:
			- If the incoming signal cannot be normalized to buy/sell, return False.
			- If no valid same-side anchor signal exists on anchor_tf, return False.
			- If the anchor signal lacks a parseable bar_close_time_eastern, return False.
			- Scan recent opposite_tf alerts for the ticker.
			- Return True if any opposite-side opposite_tf signal has
			  bar_close_time_eastern strictly later than the anchor signal time.
			- Otherwise return False.

		Parameters:
			ticker (str):
				The ticker symbol, e.g. "AAPL".

			signal (str):
				The current incoming signal, e.g. "buy", "buy+", "sell", or "sell+".

			opposite_tf (str):
				The timeframe to scan for opposite-side signals, e.g. "1h".

			anchor_tf (str):
				The higher timeframe used to find the most recent valid same-side anchor
				signal, e.g. "4h".

			max_scan_anchor_tf (int, optional):
				The maximum number of recent anchor_tf entries to inspect when locating the
				anchor signal. Default is 500.

			max_scan_opposite_tf (int, optional):
				The maximum number of recent opposite_tf entries to inspect.
				Default is 1000.

		Returns:
			bool:
				True if an opposite-side signal on opposite_tf exists after the anchor
				same-side signal on anchor_tf; False otherwise.

		Example:
			- opposite_tf="1h", anchor_tf="4h"
			  returns True if an opposite-side 1h signal exists after the most recent
			  valid same-side 4h signal.
		"""
		sym = str(ticker or "").upper().strip()
		target_side = self.tvw_helpers.normalize_signal(signal)
		opposite_tf_norm = self.tvw_helpers.normalize_tf(opposite_tf)
		anchor_tf_norm = self.tvw_helpers.normalize_tf(anchor_tf)

		if target_side not in {"buy", "sell"}:
			logger.info(
				"Invalid signal passed to has_opposite_signal_since_last_valid_same_side_higher_tf: %r",
				signal,
			)
			return False

		if not opposite_tf_norm or not anchor_tf_norm:
			logger.info(
				"Invalid timeframe configuration in has_opposite_signal_since_last_valid_same_side_higher_tf: opposite_tf=%r anchor_tf=%r",
				opposite_tf,
				anchor_tf,
			)
			return False

		latest_valid_same_side_anchor_signal = self.get_latest_valid_same_side_signal(
			sym,
			target_side,
			anchor_tf_norm,
			max_scan=max_scan_anchor_tf,
		)

		if not latest_valid_same_side_anchor_signal:
			logger.info("No same-side valid %r anchor found for %r", anchor_tf_norm, sym)
			return False

		anchor_time_str = latest_valid_same_side_anchor_signal.get("bar_close_time_eastern")
		if not anchor_time_str:
			logger.info(
				"Anchor %r signal for %r is missing bar_close_time_eastern",
				anchor_tf_norm,
				sym,
			)
			return False

		try:
			anchor_time = datetime.fromisoformat(anchor_time_str)
		except Exception:
			logger.exception(
				"Failed parsing anchor %r time for %r: %r",
				anchor_tf_norm,
				sym,
				anchor_time_str,
			)
			return False

		opposite_side = "sell" if target_side == "buy" else "buy"
		stream_key = self.tvw_helpers.stream_key(opposite_tf_norm, sym)

		try:
			entries = self.r.xrevrange(stream_key, count=max_scan_opposite_tf)
		except Exception:
			logger.exception("Failed reading %r stream for %r", opposite_tf_norm, sym)
			return False

		if not entries:
			logger.info("No %r entries found for %r", opposite_tf_norm, sym)
			return False

		for entry_id, fields in entries:
			entry_side = self.tvw_helpers.normalize_signal(fields.get("signal"))
			if entry_side != opposite_side:
				continue

			entry_time_str = fields.get("bar_close_time_eastern")
			if not entry_time_str:
				continue

			try:
				entry_time = datetime.fromisoformat(entry_time_str)
			except Exception:
				logger.exception(
					"Failed parsing %r signal time for %r entry_id=%r value=%r",
					opposite_tf_norm,
					sym,
					entry_id,
					entry_time_str,
				)
				continue

			if entry_time > anchor_time:
				logger.info(
					"Found opposite-side %r signal after anchor %r for %r: opposite_side=%r entry_id=%r entry_time=%r anchor_time=%r",
					opposite_tf_norm,
					anchor_tf_norm,
					sym,
					opposite_side,
					entry_id,
					entry_time_str,
					anchor_time_str,
				)
				return True

		return False


	def get_progressive_entry_size(self, strategy_name, ticker, side, base_num_shares, smallest_share_size):
		"""
		Computes progressively smaller same-side entry sizes for an open position
		lifecycle, resetting after a full close.

		Sizing rule:
			entry_sequence_count = 0 -> next size = base_num_shares
			entry_sequence_count = 1 -> next size = base_num_shares / 2
			entry_sequence_count = 2 -> next size = base_num_shares / 4
			entry_sequence_count = 3 -> next size = base_num_shares / 8
			...

		If the computed size is less than smallest share size, returns 0.

		Parameters:
			strategy_name (str): Strategy name.
			ticker (str): Ticker symbol.
			side (str): "long" or "short".
			base_num_shares (float): Original unscaled entry size.
			smallest_share_size (float): Smallest num shares that can be bought/shorted

		Returns:
			float: Computed execution quantity, or 0 if below smallest share size.
		"""
		side = str(side or "").strip().lower()

		if side not in {"long", "short"}:
			return 0.0

		try:
			base_num_shares = float(base_num_shares)
		except Exception:
			return 0.0

		if base_num_shares <= 0:
			return 0.0

		position = None
		try:
			position = self.trade_records.get_position(strategy_name, ticker)
		except Exception:
			logger.exception(
				"Failed retrieving position for progressive sizing: strategy=%r ticker=%r side=%r",
				strategy_name,
				ticker,
				side,
			)
			return 0.0

		entry_sequence_count = 0

		if position is not None:
			position_side = str(position.get("side") or "").strip().lower()
			position_qty = float(position.get("num_shares") or 0.0)
			try:
				stored_sequence = max(0, int(float(position.get("entry_sequence_count") or 0)))
			except Exception:
				stored_sequence = 0

			if position_qty > 0 and position_side == side:
				entry_sequence_count = stored_sequence

		execution_qty = base_num_shares / (2 ** entry_sequence_count)

		if execution_qty < smallest_share_size:
			return 0.0

		return execution_qty


	def entry_strategy1(self, strategy_name, simulation_only, date, signal, prices, ticker, timeframe, NUM_SHARES, alpaca_api):

		tf = self.tvw_helpers.normalize_tf(timeframe)
		if tf not in {"1m", "15m"}:
			return None

		num_shares = NUM_SHARES
		max_scan_1h = 1000
		max_scan_4h = 500		

		latest_valid_4h_same_side_signal = self.get_latest_valid_same_side_signal(ticker, signal, "4h", max_scan_4h)
		if not latest_valid_4h_same_side_signal:
			logger.info("No valid 4h context")
			return None

		# Extra constraint only for 1m entries:
		# block entry if an opposite-side 1h signal exists after the anchor 4h signal.
		if tf == "1m":
			if self.has_opposite_signal_since_last_valid_same_side_higher_tf(ticker, signal, "1h", "4h", max_scan_1h, max_scan_4h):
				logger.info(
					"Blocked Strategy 1 1m entry for %r because an opposite-side 1h signal exists after the anchor 4h signal",
					ticker,
				)
				return None

		open_price = latest_valid_4h_same_side_signal["open"]
		close_price = latest_valid_4h_same_side_signal["close"]

		if open_price is None or close_price is None:
			return None

		_4h_hi = max(open_price, close_price)
		_4h_lo = min(open_price, close_price)
		signal_4h_len = _4h_hi - _4h_lo

		max_deviation_from_4hr_peak_to_4hr_len_ratio = 0.75

		market_price = prices.get(ticker, {}).get("market")
		if market_price is None:
			return None

		current_side = self.tvw_helpers.normalize_signal(signal)

		if current_side == "buy":
			#if market_price < (_4h_hi - (max_deviation_from_4hr_peak_to_4hr_len_ratio * signal_4h_len)):
			return self.place_long_order(simulation_only, strategy_name, ticker, date, prices, num_shares, alpaca_api)

		if current_side == "sell":
			#if market_price > (_4h_lo + (max_deviation_from_4hr_peak_to_4hr_len_ratio * signal_4h_len)):
			return self.place_short_order(simulation_only, strategy_name, ticker, date, prices, num_shares, alpaca_api)

		logger.info("No trade condition met for Strategy 1 for %r", ticker)
		return None


	def exit_strategy1(self, strategy_name, simulation_only, date, signal, prices, ticker, timeframe, alpaca_api):

		logger.info(
			"exit_strategy1 check: strategy=%r ticker=%r timeframe=%r raw_signal=%r normalized_signal=%r",
			strategy_name,
			ticker,
			timeframe,
			signal,
			self.tvw_helpers.normalize_signal(signal),
		)

		tf = self.tvw_helpers.normalize_tf(timeframe)
		if tf not in {"1h", "1m", "5m", "15m"}:
			return None		

		is_1h_opposite_of_last_4h = self.is_tf_relative_to_last_higher_tf(ticker, signal, timeframe, "1h", "4h", "opposite")
		tf_confirms_1h_opposite_of_4h = self.lower_tf_confirms_mid_tf_opposite_of_higher_tf(ticker, signal, tf, {"1m", "5m", "15m"}, "1h", "4h")		

		is_opposite = is_1h_opposite_of_last_4h or tf_confirms_1h_opposite_of_4h
		logger.info("exit_strategy1 opposite-check for %r => %r", ticker, is_opposite)

		if not is_opposite:
			return None

		redis_position = None
		try:
			redis_position = self.trade_records.get_position(strategy_name, ticker)
			logger.info("exit_strategy1 Redis position for %r/%r => %r", strategy_name, ticker, redis_position)
		except Exception:
			logger.exception("exit_strategy1 failed to fetch Redis position for strategy=%r ticker=%r", strategy_name, ticker)

		alpaca_position = None
		try:
			alpaca_position = alpaca_api.get_position(ticker)
			logger.info(
				"exit_strategy1 Alpaca position for %r => qty=%r side=%r avg_entry_price=%r",
				ticker,
				getattr(alpaca_position, "qty", None),
				getattr(alpaca_position, "side", None),
				getattr(alpaca_position, "avg_entry_price", None),
			)
		except Exception:
			logger.exception("exit_strategy1 failed to fetch Alpaca position for %r", ticker)

		redis_position_qty = 0.0
		redis_position_side = None
		if redis_position is not None:
			try:
				redis_position_qty = float(redis_position.get("num_shares") or 0.0)
			except Exception:
				redis_position_qty = 0.0
			redis_position_side = str(redis_position.get("side") or "").strip().lower()

		alpaca_position_qty = 0.0
		if alpaca_position is not None:
			try:
				alpaca_position_qty = float(alpaca_position.qty)
			except Exception:
				alpaca_position_qty = 0.0

		redis_num_shares = abs(redis_position_qty)
		alpaca_num_shares = abs(alpaca_position_qty)

		last_1h_alert = self.tvw_helpers.get_nth_last_alert(ticker, "1h", 1)
		if last_1h_alert is None:
			logger.info("No 1h signal found for %r", ticker)
			return None

		_, last_1h_fields = last_1h_alert
		signal_1h = self.tvw_helpers.normalize_signal(last_1h_fields.get("signal"))

		logger.info(
			"exit_strategy1 signal context: ticker=%r signal_1h=%r redis_side=%r redis_qty=%r alpaca_qty=%r",
			ticker,
			signal_1h,
			redis_position_side,
			redis_num_shares,
			alpaca_num_shares,
		)

		if signal_1h == "buy":
			#if redis_position_side == "short" and redis_num_shares > 0:
			if simulation_only and redis_position_side == "short" and redis_num_shares > 0:
				logger.info(
					"exit_strategy1 Redis bookkeeping cover for %r using redis_num_shares=%r",
					ticker,
					redis_num_shares,
				)
				try:
					self.trade_records.create_trade_record(
						strategy_name,
						ticker,
						date,
						prices.get(ticker, {}).get("market"),
						redis_num_shares,
						"cover",
						False,
					)
				except Exception:
					logger.exception(
						"exit_strategy1 Redis bookkeeping cover failed for strategy=%r ticker=%r qty=%r",
						strategy_name,
						ticker,
						redis_num_shares,
					)

			if alpaca_position_qty < 0 and alpaca_num_shares > 0:
				logger.info(
					"exit_strategy1 Alpaca cover for %r using alpaca_num_shares=%r",
					ticker,
					alpaca_num_shares,
				)
				return self.cover_short_order(
					simulation_only,
					strategy_name,
					ticker,
					date,
					prices,
					alpaca_num_shares,
					alpaca_api,
					do_redis_bookkeeping=not simulation_only,
				)

			logger.info(
				"exit_strategy1 no Alpaca short position to cover for %r; alpaca_qty=%r",
				ticker,
				alpaca_position_qty,
			)
			return None

		elif signal_1h == "sell":
			#if redis_position_side == "long" and redis_num_shares > 0:
			if simulation_only and redis_position_side == "long" and redis_num_shares > 0:
				logger.info(
					"exit_strategy1 Redis bookkeeping sell for %r using redis_num_shares=%r",
					ticker,
					redis_num_shares,
				)
				try:
					self.trade_records.create_trade_record(
						strategy_name,
						ticker,
						date,
						prices.get(ticker, {}).get("market"),
						redis_num_shares,
						"sell",
						False,
					)
				except Exception:
					logger.exception(
						"exit_strategy1 Redis bookkeeping sell failed for strategy=%r ticker=%r qty=%r",
						strategy_name,
						ticker,
						redis_num_shares,
					)

			if alpaca_position_qty > 0 and alpaca_num_shares > 0:
				logger.info(
					"exit_strategy1 Alpaca sell for %r using alpaca_num_shares=%r",
					ticker,
					alpaca_num_shares,
				)
				return self.sell_long_order(
					simulation_only,
					strategy_name,
					ticker,
					date,
					prices,
					alpaca_num_shares,
					alpaca_api,
					do_redis_bookkeeping=not simulation_only,
				)

			logger.info(
				"exit_strategy1 no Alpaca long position to sell for %r; alpaca_qty=%r",
				ticker,
				alpaca_position_qty,
			)
			return None

		else:
			logger.info("Latest 1h signal is invalid/unknown for %r: %r", ticker, signal_1h)
			return None							
			

	def entry_strategy2(self, strategy_name, simulation_only, date, signal, prices, ticker, timeframe, NUM_SHARES, alpaca_api):

		tf = self.tvw_helpers.normalize_tf(timeframe)
		if tf != "1m":
			return None

		num_shares = NUM_SHARES

		last_1m_alert = self.tvw_helpers.get_nth_last_alert(ticker, tf, 1)
		last_15m_alert = self.tvw_helpers.get_nth_last_alert(ticker, "15m", 1)

		if last_1m_alert is None or last_15m_alert is None:
			logger.info("Strategy skipped: missing alert context for %r", ticker)
			return None

		_, last_1m_fields = last_1m_alert
		_, last_15m_fields = last_15m_alert

		last_1m_signal = self.tvw_helpers.normalize_signal(last_1m_fields.get("signal"))
		last_15m_signal = self.tvw_helpers.normalize_signal(last_15m_fields.get("signal"))

		if last_1m_signal not in {"buy", "sell"} or last_15m_signal not in {"buy", "sell"}:
			logger.info(
				"Strategy skipped: invalid signal context for %r | 1m=%r 15m=%r",
				ticker,
				last_1m_signal,
				last_15m_signal,
			)
			return None

		# Block entry if an opposite-side 5m signal occurred after the
		# last valid same-side 15m anchor signal.
		if last_15m_signal == "buy" and last_1m_signal == "buy":
			if self.has_opposite_signal_since_last_valid_same_side_higher_tf(
				ticker, "buy", "5m", "15m", 1000, 500
			):
				logger.info("Blocked Strategy 2 long entry for %r due to opposite 5m after anchor 15m", ticker)
				return None
			return self.place_long_order(simulation_only, strategy_name, ticker, date, prices, num_shares, alpaca_api)

		if last_15m_signal == "sell" and last_1m_signal == "sell":
			if self.has_opposite_signal_since_last_valid_same_side_higher_tf(
				ticker, "sell", "5m", "15m", 1000, 500
			):
				logger.info("Blocked Strategy 2 short entry for %r due to opposite 5m after anchor 15m", ticker)
				return None
			return self.place_short_order(simulation_only, strategy_name, ticker, date, prices, num_shares, alpaca_api)
		logger.info(
			"No trade condition met for %r | 1m=%r 15m=%r",
			ticker,
			last_1m_signal,
			last_15m_signal,
		)

		return None


	def exit_strategy2(self, strategy_name, simulation_only, date, signal, prices, ticker, timeframe, alpaca_api):

		logger.info(
			"exit_strategy2 check: strategy=%r ticker=%r timeframe=%r raw_signal=%r normalized_signal=%r",
			strategy_name,
			ticker,
			timeframe,
			signal,
			self.tvw_helpers.normalize_signal(signal),
		)

		is_5m_opposite_of_last_15m = self.is_tf_relative_to_last_higher_tf(ticker, signal, timeframe, "5m", "15m", "opposite")		

		logger.info("exit_strategy2 opposite-check for %r => %r", ticker, is_5m_opposite_of_last_15m)

		if not is_5m_opposite_of_last_15m:
			return None

		redis_position = None
		try:
			redis_position = self.trade_records.get_position(strategy_name, ticker)
			logger.info("exit_strategy2 Redis position for %r/%r => %r", strategy_name, ticker, redis_position)
		except Exception:
			logger.exception("exit_strategy2 failed to fetch Redis position for strategy=%r ticker=%r", strategy_name, ticker)

		alpaca_position = None
		try:
			alpaca_position = alpaca_api.get_position(ticker)
			logger.info(
				"exit_strategy2 Alpaca position for %r => qty=%r side=%r avg_entry_price=%r",
				ticker,
				getattr(alpaca_position, "qty", None),
				getattr(alpaca_position, "side", None),
				getattr(alpaca_position, "avg_entry_price", None),
			)
		except Exception:
			logger.exception("exit_strategy2 failed to fetch Alpaca position for %r", ticker)

		redis_position_qty = 0.0
		redis_position_side = None
		if redis_position is not None:
			try:
				redis_position_qty = float(redis_position.get("num_shares") or 0.0)
			except Exception:
				redis_position_qty = 0.0
			redis_position_side = str(redis_position.get("side") or "").strip().lower()

		alpaca_position_qty = 0.0
		if alpaca_position is not None:
			try:
				alpaca_position_qty = float(alpaca_position.qty)
			except Exception:
				alpaca_position_qty = 0.0

		redis_num_shares = abs(redis_position_qty)
		alpaca_num_shares = abs(alpaca_position_qty)

		last_5m_alert = self.tvw_helpers.get_nth_last_alert(ticker, "5m", 1)
		if last_5m_alert is None:
			logger.info("No 5m signal found for %r", ticker)
			return None

		_, last_5m_fields = last_5m_alert
		signal_5m = self.tvw_helpers.normalize_signal(last_5m_fields.get("signal"))

		logger.info(
			"exit_strategy2 signal context: ticker=%r signal_5m=%r redis_side=%r redis_qty=%r alpaca_qty=%r",
			ticker,
			signal_5m,
			redis_position_side,
			redis_num_shares,
			alpaca_num_shares,
		)

		if signal_5m == "buy":
			#if redis_position_side == "short" and redis_num_shares > 0:
			if simulation_only and redis_position_side == "short" and redis_num_shares > 0:
				logger.info(
					"exit_strategy2 Redis bookkeeping cover for %r using redis_num_shares=%r",
					ticker,
					redis_num_shares,
				)
				try:
					self.trade_records.create_trade_record(
						strategy_name,
						ticker,
						date,
						prices.get(ticker, {}).get("market"),
						redis_num_shares,
						"cover",
						False,
					)
				except Exception:
					logger.exception(
						"exit_strategy2 Redis bookkeeping cover failed for strategy=%r ticker=%r qty=%r",
						strategy_name,
						ticker,
						redis_num_shares,
					)

			if alpaca_position_qty < 0 and alpaca_num_shares > 0:
				logger.info(
					"exit_strategy2 Alpaca cover for %r using alpaca_num_shares=%r",
					ticker,
					alpaca_num_shares,
				)
				return self.cover_short_order(
					simulation_only,
					strategy_name,
					ticker,
					date,
					prices,
					alpaca_num_shares,
					alpaca_api,
					do_redis_bookkeeping=not simulation_only,
				)

			logger.info(
				"exit_strategy2 no Alpaca short position to cover for %r; alpaca_qty=%r",
				ticker,
				alpaca_position_qty,
			)
			return None

		elif signal_5m == "sell":
			#if redis_position_side == "long" and redis_num_shares > 0:
			if simulation_only and redis_position_side == "long" and redis_num_shares > 0:
				logger.info(
					"exit_strategy2 Redis bookkeeping sell for %r using redis_num_shares=%r",
					ticker,
					redis_num_shares,
				)
				try:
					self.trade_records.create_trade_record(
						strategy_name,
						ticker,
						date,
						prices.get(ticker, {}).get("market"),
						redis_num_shares,
						"sell",
						False,
					)
				except Exception:
					logger.exception(
						"exit_strategy2 Redis bookkeeping sell failed for strategy=%r ticker=%r qty=%r",
						strategy_name,
						ticker,
						redis_num_shares,
					)

			if alpaca_position_qty > 0 and alpaca_num_shares > 0:
				logger.info(
					"exit_strategy2 Alpaca sell for %r using alpaca_num_shares=%r",
					ticker,
					alpaca_num_shares,
				)
				return self.sell_long_order(
					simulation_only,
					strategy_name,
					ticker,
					date,
					prices,
					alpaca_num_shares,
					alpaca_api,
					do_redis_bookkeeping=not simulation_only,
				)

			logger.info(
				"exit_strategy2 no Alpaca long position to sell for %r; alpaca_qty=%r",
				ticker,
				alpaca_position_qty,
			)
			return None

		else:
			logger.info("Latest 5m signal is invalid/unknown for %r: %r", ticker, signal_5m)
			return None	


	def entry_strategy3(self, strategy_name, simulation_only, date, signal, prices, ticker, timeframe, NUM_SHARES, alpaca_api):
	
		tf = self.tvw_helpers.normalize_tf(timeframe)
		if tf != "1h":
			return None

		num_shares = NUM_SHARES

		last_1h_alert = self.tvw_helpers.get_nth_last_alert(ticker, tf, 1)

		if last_1h_alert is None:
			logger.info("Strategy skipped: missing alert context for %r", ticker)
			return None

		_, last_1h_fields = last_1h_alert

		last_1h_signal = self.tvw_helpers.normalize_signal(last_1h_fields.get("signal"))

		if last_1h_signal == "buy":
			return self.place_long_order(simulation_only, strategy_name, ticker, date, prices, num_shares, alpaca_api)

		if last_1h_signal == "sell":
			return self.place_short_order(simulation_only, strategy_name, ticker, date, prices, num_shares, alpaca_api)

		logger.info(
			"No trade condition met for %r | 1h=%r",
			ticker, last_1h_signal
		)
		return None


	def exit_strategy3(self, strategy_name, simulation_only, date, signal, prices, ticker, timeframe, alpaca_api):

		logger.info(
			"exit_strategy3 check: strategy=%r ticker=%r timeframe=%r raw_signal=%r normalized_signal=%r",
			strategy_name,
			ticker,
			timeframe,
			signal,
			self.tvw_helpers.normalize_signal(signal),
		)

		tf = self.tvw_helpers.normalize_tf(timeframe)
		if tf not in {"15m", "1m", "5m"}:
			return None

		is_15m_opposite_of_last_1h = self.is_tf_relative_to_last_higher_tf(ticker, signal, timeframe, "15m", "1h", "opposite")

		lower_tf_confirms_15m_opposite_of_1h = self.lower_tf_confirms_mid_tf_opposite_of_higher_tf(ticker, signal, timeframe, {"1m", "5m"}, "15m", "1h")

		should_exit = is_15m_opposite_of_last_1h or lower_tf_confirms_15m_opposite_of_1h

		logger.info(
			"exit_strategy3 exit-check for %r => direct_15m=%r delayed_lower_tf=%r final=%r",
			ticker,
			is_15m_opposite_of_last_1h,
			lower_tf_confirms_15m_opposite_of_1h,
			should_exit,
		)

		if not should_exit:
			return None

		redis_position = None
		try:
			redis_position = self.trade_records.get_position(strategy_name, ticker)
			logger.info("exit_strategy3 Redis position for %r/%r => %r", strategy_name, ticker, redis_position)
		except Exception:
			logger.exception("exit_strategy3 failed to fetch Redis position for strategy=%r ticker=%r", strategy_name, ticker)

		alpaca_position = None
		try:
			alpaca_position = alpaca_api.get_position(ticker)
			logger.info(
				"exit_strategy3 Alpaca position for %r => qty=%r side=%r avg_entry_price=%r",
				ticker,
				getattr(alpaca_position, "qty", None),
				getattr(alpaca_position, "side", None),
				getattr(alpaca_position, "avg_entry_price", None),
			)
		except Exception:
			logger.exception("exit_strategy3 failed to fetch Alpaca position for %r", ticker)

		redis_position_qty = 0.0
		redis_position_side = None
		if redis_position is not None:
			try:
				redis_position_qty = float(redis_position.get("num_shares") or 0.0)
			except Exception:
				redis_position_qty = 0.0
			redis_position_side = str(redis_position.get("side") or "").strip().lower()

		alpaca_position_qty = 0.0
		if alpaca_position is not None:
			try:
				alpaca_position_qty = float(alpaca_position.qty)
			except Exception:
				alpaca_position_qty = 0.0

		redis_num_shares = abs(redis_position_qty)
		alpaca_num_shares = abs(alpaca_position_qty)

		last_15m_alert = self.tvw_helpers.get_nth_last_alert(ticker, "15m", 1)
		if last_15m_alert is None:
			logger.info("No 15m signal found for %r", ticker)
			return None

		_, last_15m_fields = last_15m_alert
		signal_15m = self.tvw_helpers.normalize_signal(last_15m_fields.get("signal"))

		logger.info(
			"exit_strategy3 signal context: ticker=%r signal_15m=%r redis_side=%r redis_qty=%r alpaca_qty=%r",
			ticker,
			signal_15m,
			redis_position_side,
			redis_num_shares,
			alpaca_num_shares,
		)

		if signal_15m == "buy":
			#if redis_position_side == "short" and redis_num_shares > 0:
			if simulation_only and redis_position_side == "short" and redis_num_shares > 0:
				logger.info(
					"exit_strategy3 Redis bookkeeping cover for %r using redis_num_shares=%r",
					ticker,
					redis_num_shares,
				)
				try:
					self.trade_records.create_trade_record(
						strategy_name,
						ticker,
						date,
						prices.get(ticker, {}).get("market"),
						redis_num_shares,
						"cover",
						False,
					)
				except Exception:
					logger.exception(
						"exit_strategy3 Redis bookkeeping cover failed for strategy=%r ticker=%r qty=%r",
						strategy_name,
						ticker,
						redis_num_shares,
					)

			if alpaca_position_qty < 0 and alpaca_num_shares > 0:
				logger.info(
					"exit_strategy3 Alpaca cover for %r using alpaca_num_shares=%r",
					ticker,
					alpaca_num_shares,
				)
				return self.cover_short_order(
					simulation_only,
					strategy_name,
					ticker,
					date,
					prices,
					alpaca_num_shares,
					alpaca_api,
					do_redis_bookkeeping=not simulation_only,
				)

			logger.info(
				"exit_strategy3 no Alpaca short position to cover for %r; alpaca_qty=%r",
				ticker,
				alpaca_position_qty,
			)
			return None

		elif signal_15m == "sell":
			#if redis_position_side == "long" and redis_num_shares > 0:
			if simulation_only and redis_position_side == "long" and redis_num_shares > 0:
				logger.info(
					"exit_strategy3 Redis bookkeeping sell for %r using redis_num_shares=%r",
					ticker,
					redis_num_shares,
				)
				try:
					self.trade_records.create_trade_record(
						strategy_name,
						ticker,
						date,
						prices.get(ticker, {}).get("market"),
						redis_num_shares,
						"sell",
						False,
					)
				except Exception:
					logger.exception(
						"exit_strategy3 Redis bookkeeping sell failed for strategy=%r ticker=%r qty=%r",
						strategy_name,
						ticker,
						redis_num_shares,
					)

			if alpaca_position_qty > 0 and alpaca_num_shares > 0:
				logger.info(
					"exit_strategy3 Alpaca sell for %r using alpaca_num_shares=%r",
					ticker,
					alpaca_num_shares,
				)
				return self.sell_long_order(
					simulation_only,
					strategy_name,
					ticker,
					date,
					prices,
					alpaca_num_shares,
					alpaca_api,
					do_redis_bookkeeping=not simulation_only,
				)

			logger.info(
				"exit_strategy3 no Alpaca long position to sell for %r; alpaca_qty=%r",
				ticker,
				alpaca_position_qty,
			)
			return None

		else:
			logger.info("Latest 15m signal is invalid/unknown for %r: %r", ticker, signal_15m)
			return None		


	def place_order(
		self,
		simulation_only,
		strategy_name,
		ticker,
		date,
		prices,
		num_shares,
		alpaca_api,
		order_type,
		do_redis_bookkeeping=True,
	):
		order_type = str(order_type or "").strip().lower()
		ticker = str(ticker or "").upper().strip()

		if order_type not in {"long", "short", "sell", "cover"}:
			logger.info("Invalid order_type=%r for ticker=%r", order_type, ticker)
			return None

		broker_side = None
		if order_type in {"long", "cover"}:
			broker_side = "buy"
		elif order_type in {"short", "sell"}:
			broker_side = "sell"

		price = prices.get(ticker, {}).get("market")
		if price is None:
			logger.info("No market price available for %r order_type=%r", ticker, order_type)
			return None

		if order_type == "short" and not self.tvw_helpers.is_symbol_shortable(alpaca_api, ticker):
			logger.info("Ticker %r is not shortable; skipping short order", ticker)
			return None

		is_regular_hours = self.tvw_helpers._is_regular_hours_et()

		if not is_regular_hours:
			if order_type in {"long", "cover"}:
				ask = prices.get(ticker, {}).get("ask")
				if ask is None:
					logger.info("No ask price available for off-hours %r order_type=%r", ticker, order_type)
					return None
				price = ask + 0.01

			elif order_type in {"short", "sell"}:
				bid = prices.get(ticker, {}).get("bid")
				if bid is None:
					logger.info("No bid price available for off-hours %r order_type=%r", ticker, order_type)
					return None
				price = bid - 0.01

		if price <= 0:
			logger.info("Invalid computed price=%r for ticker=%r order_type=%r", price, ticker, order_type)
			return None

		try:
			num_shares = float(num_shares)
		except Exception:
			logger.info("Invalid num_shares=%r for ticker=%r order_type=%r", num_shares, ticker, order_type)
			return None

		if num_shares <= 0:
			logger.info("Non-positive num_shares=%r for ticker=%r order_type=%r", num_shares, ticker, order_type)
			return None

		redis_position = None
		try:
			redis_position = self.trade_records.get_position(strategy_name, ticker)
			logger.info(
				"Redis position before order: strategy=%r ticker=%r order_type=%r redis_position=%r",
				strategy_name,
				ticker,
				order_type,
				redis_position,
			)
		except Exception:
			logger.exception(
				"Failed retrieving Redis position before order: strategy=%r ticker=%r order_type=%r",
				strategy_name,
				ticker,
				order_type,
			)

		alpaca_position = None
		try:
			alpaca_position = alpaca_api.get_position(ticker)
			alpaca_position_qty = float(alpaca_position.qty)
			logger.info(
				"Alpaca position before order: ticker=%r qty=%r side=%r avg_entry_price=%r",
				ticker,
				getattr(alpaca_position, "qty", None),
				getattr(alpaca_position, "side", None),
				getattr(alpaca_position, "avg_entry_price", None),
			)
		except Exception:
			alpaca_position = None
			alpaca_position_qty = 0.0			
			logger.exception(
				"Failed retrieving Alpaca position before order for ticker=%r order_type=%r",
				ticker,
				order_type,
			)

		execution_qty = num_shares

		def submit_to_alpaca_and_wait_for_fill(timeout_seconds=10, poll_interval=0.5):
			order_type_name = "market" if is_regular_hours else "limit"
			submitted_order = None

			for attempt in range(1, 4):
				try:
					if is_regular_hours:
						submitted_order = alpaca_api.submit_order(
							symbol=ticker,
							qty=execution_qty,
							side=broker_side,
							type="market",
							time_in_force="day",
						)
					else:
						submitted_order = alpaca_api.submit_order(
							symbol=ticker,
							qty=execution_qty,
							side=broker_side,
							type="limit",
							time_in_force="day",
							limit_price=price,
						)

					logger.info(
						"Alpaca %s order submitted: strategy=%r ticker=%r order_type=%r qty=%r order_id=%r",
						order_type_name,
						strategy_name,
						ticker,
						order_type,
						execution_qty,
						getattr(submitted_order, "id", None),
					)
					break

				except Exception:
					logger.exception(
						"Failed submitting Alpaca %s order: strategy=%r ticker=%r order_type=%r qty=%r attempt=%r",
						order_type_name,
						strategy_name,
						ticker,
						order_type,
						execution_qty,
						attempt,
					)
					time.sleep(0.5)

			if submitted_order is None:
				return None

			order_id = getattr(submitted_order, "id", None)
			if not order_id:
				logger.info("Submitted Alpaca order missing order id for %r", ticker)
				return None

			deadline = time.time() + timeout_seconds
			terminal_bad_statuses = {
				"canceled",
				"expired",
				"rejected",
				"suspended",
				"stopped",
			}

			while time.time() < deadline:
				try:
					order = alpaca_api.get_order(order_id)
					status = str(getattr(order, "status", "") or "").lower()

					logger.info(
						"Alpaca order status check: ticker=%r order_id=%r status=%r filled_qty=%r avg_fill_price=%r",
						ticker,
						order_id,
						status,
						getattr(order, "filled_qty", None),
						getattr(order, "filled_avg_price", None),
					)

					if status == "filled":
						return order

					if status in terminal_bad_statuses:
						return None

				except Exception:
					logger.exception("Failed polling Alpaca order status: ticker=%r order_id=%r", ticker, order_id)

				time.sleep(poll_interval)

			logger.info(
				"Alpaca order not filled before timeout: ticker=%r order_id=%r timeout_seconds=%r",
				ticker,
				order_id,
				timeout_seconds,
			)
			return None

		if order_type in {"long", "short"}:
			execution_qty = self.get_progressive_entry_size(
				strategy_name,
				ticker,
				order_type,
				num_shares,
				self.SMALLEST_SHARE_SIZE
			)

			if execution_qty < self.SMALLEST_SHARE_SIZE:
				logger.info(
					"Progressive entry size below %r shares; skipping entry: strategy=%r ticker=%r order_type=%r base_qty=%r computed_qty=%r",
					self.SMALLEST_SHARE_SIZE,
					strategy_name,
					ticker,
					order_type,
					num_shares,
					execution_qty,
				)
				return None

		if execution_qty <= 0:
			logger.info(
				"Computed execution_qty is non-positive: strategy=%r ticker=%r order_type=%r base_qty=%r execution_qty=%r",
				strategy_name,
				ticker,
				order_type,
				num_shares,
				execution_qty,
			)
			return None

		if simulation_only: # No Alpaca execution
			if do_redis_bookkeeping:
				return self.trade_records.create_trade_record(
					strategy_name,
					ticker,
					date,
					price,
					execution_qty,
					order_type,
					True,
				)
			return None

		filled_order = submit_to_alpaca_and_wait_for_fill() # Alpaca execution

		if filled_order is None:
			logger.info(
				"Skipping Redis bookkeeping because Alpaca order was not filled: strategy=%r ticker=%r order_type=%r qty=%r",
				strategy_name,
				ticker,
				order_type,
				execution_qty,
			)
			return None

		time.sleep(0.5)
		try:
			position = alpaca_api.get_position(ticker)
			logger.info(
				"Post-fill Alpaca position check: ticker=%r qty=%r side=%r",
				ticker,
				getattr(position, "qty", None),
				getattr(position, "side", None),
			)
		except Exception:
			logger.info("No Alpaca position found after fill for ticker=%r order_type=%r", ticker, order_type)

		# If we are here, the Alpaca order got successfully filled
		fill_price = getattr(filled_order, "filled_avg_price", None)
		if fill_price is None:
			fill_price = price

		if do_redis_bookkeeping:
			return self.trade_records.create_trade_record(
				strategy_name,
				ticker,
				date,
				fill_price,
				execution_qty,
				order_type,
				True,
			)

		return None


	def place_long_order(self, simulation_only, strategy_name, ticker, date, prices, num_shares, alpaca_api):
		return self.place_order(
			simulation_only,
			strategy_name,
			ticker,
			date,
			prices,
			num_shares,
			alpaca_api,
			"long",
			True,
		)

	def place_short_order(self, simulation_only, strategy_name, ticker, date, prices, num_shares, alpaca_api):
		return self.place_order(
			simulation_only,
			strategy_name,
			ticker,
			date,
			prices,
			num_shares,
			alpaca_api,
			"short",
			True,
		)

	def sell_long_order(self, simulation_only, strategy_name, ticker, date, prices, num_shares, alpaca_api, do_redis_bookkeeping=True):
		return self.place_order(
			simulation_only,
			strategy_name,
			ticker,
			date,
			prices,
			num_shares,
			alpaca_api,
			"sell",
			do_redis_bookkeeping,
		)

	def cover_short_order(self, simulation_only, strategy_name, ticker, date, prices, num_shares, alpaca_api, do_redis_bookkeeping=True):
		return self.place_order(
			simulation_only,
			strategy_name,
			ticker,
			date,
			prices,
			num_shares,
			alpaca_api,
			"cover",
			do_redis_bookkeeping,
		)	
