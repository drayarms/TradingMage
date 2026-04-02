import logging
import time
from datetime import datetime

logger = logging.getLogger("tv-webhook")

class Strategies:
	def __init__(self, trading_view_webhook_helpers, trade_records):
		self.tvw_helpers = trading_view_webhook_helpers
		self.r = trading_view_webhook_helpers.require_redis()
		self.trade_records = trade_records
		

	def get_latest_valid_4h_same_side_signal(self, ticker, side, max_scan=500):
		"""
		Return the OHLCV context for the most recent 4h signal whose side matches the
		side under consideration, treating:
			- buy and buy+ as the same side ("buy")
			- sell and sell+ as the same side ("sell")

		Constraint:
			If a 4h signal of the opposite side exists between that candidate signal and now,
			then return None.

		Assumptions:
			- 4h alerts are stored in the Redis stream for this ticker/timeframe.
			- Redis stream order reflects alert arrival order.
			- bar_close_time_eastern is stored as an ISO Eastern timestamp.

		Parameters:
			ticker (str): Ticker symbol, e.g. "AAPL"
			side (str): Current side under consideration: buy, sell, buy+, or sell+
			max_scan (int): Max number of recent 4h entries to inspect

		Returns:
			Optional[dict]: Matching 4h signal context, or None if not found / invalidated.
		"""
		sym = str(ticker or "").upper().strip()
		target_side = self.tvw_helpers.normalize_signal(side)

		if target_side not in {"buy", "sell"}:
			logger.info("Invalid side passed to get_latest_valid_4h_same_side_signal: %r", side)
			return None

		opposite_side = "sell" if target_side == "buy" else "buy"
		stream_key = self.tvw_helpers.stream_key("4h", sym)

		try:
			entries = self.r.xrevrange(stream_key, count=max_scan)
		except Exception:
			logger.exception("Failed reading 4h stream for %r", sym)
			return None

		if not entries:
			logger.info("No 4h entries found for %r", sym)
			return None

		for entry_id, fields in entries:
			entry_side = self.tvw_helpers.normalize_signal(fields.get("signal"))

			if entry_side not in {"buy", "sell"}:
				continue

			# If the first relevant 4h signal encountered going backward is the opposite side,
			# then a same-side candidate is invalid because an opposite-side signal exists
			# between that older candidate and now.
			if entry_side == opposite_side:
				logger.info(
					"No valid 4h %r context for %r because newer opposite-side 4h signal exists: %r",
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
					"timeframe": self.tvw_helpers.normalize_tf(fields.get("timeframe") or "4h"),
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


	def lower_tf_confirms_1h_opposite_of_4h(self, ticker, signal, timeframe):
		"""
		Returns True if:
			- current timeframe is 1m, 5m, or 15m
			- current signal side matches the most recent 1h signal side
			- the most recent 1h signal side is opposite of the most recent 4h signal side
		"""
		tf = self.tvw_helpers.normalize_tf(timeframe)
		if tf not in {"1m", "5m", "15m"}:
			return False

		sym = str(ticker or "").upper().strip()
		current_side = self.tvw_helpers.normalize_signal(signal)

		if current_side not in {"buy", "sell"}:
			return False

		last_1h_alert = self.tvw_helpers.get_nth_last_alert(sym, "1h", 1)
		if last_1h_alert is None:
			return False

		last_4h_alert = self.tvw_helpers.get_nth_last_alert(sym, "4h", 1)
		if last_4h_alert is None:
			return False

		_, last_1h_fields = last_1h_alert
		_, last_4h_fields = last_4h_alert

		last_1h_signal = self.tvw_helpers.normalize_signal(last_1h_fields.get("signal"))
		last_4h_signal = self.tvw_helpers.normalize_signal(last_4h_fields.get("signal"))

		if last_1h_signal not in {"buy", "sell"}:
			logger.info(
				"lower-tf confirms 1h vs 4h: INVALID last_1h_signal=%r for ticker=%r",
				last_1h_signal,
				sym,
			)
			return False

		if last_4h_signal not in {"buy", "sell"}:
			logger.info(
				"lower-tf confirms 1h vs 4h: INVALID last_4h_signal=%r for ticker=%r",
				last_4h_signal,
				sym,
			)
			return False

		is_true = (current_side == last_1h_signal) and (last_1h_signal != last_4h_signal)

		logger.info(
			"lower-tf confirms 1h vs 4h: ticker=%r tf=%r current=%r last_1h=%r last_4h=%r result=%r",
			sym,
			tf,
			current_side,
			last_1h_signal,
			last_4h_signal,
			is_true,
		)

		return is_true


	def is_1h_opposite_of_last_4h(self, ticker, signal, timeframe):
		"""
		Returns True if:
			- timeframe is 1h
			- signal side is opposite of the most recent 4h signal side

		Signal normalization:
			buy, buy+ -> buy
			sell, sell+ -> sell
		"""

		# Must be 1h signal
		tf = self.tvw_helpers.normalize_tf(timeframe)
		if tf != "1h":
			return False

		sym = str(ticker or "").upper().strip()

		# Normalize incoming signal
		current_side = self.tvw_helpers.normalize_signal(signal)
		if current_side not in {"buy", "sell"}:
			return False

		# Get last 4h alert
		last_4h_alert = self.tvw_helpers.get_nth_last_alert(sym, "4h", 1)
		if last_4h_alert is None:
			return False

		_, last_4h_fields = last_4h_alert

		last_4h_signal = self.tvw_helpers.normalize_signal(
			last_4h_fields.get("signal")
		)

		if last_4h_signal not in {"buy", "sell"}:
			logger.info(
				"4h vs 1h check: INVALID last_4h_signal=%r for ticker=%r",
				last_4h_signal,
				sym,
			)			
			return False

		# Check opposite
		#return (
			#(current_side == "buy" and last_4h_signal == "sell") or
			#(current_side == "sell" and last_4h_signal == "buy")
		#)
		is_opposite = (
			(current_side == "buy" and last_4h_signal == "sell") or
			(current_side == "sell" and last_4h_signal == "buy")
		)

		logger.info(
			"4h vs 1h check: ticker=%r last_4h_signal=%r current_signal=%r result=%r",
			sym,
			last_4h_signal,
			current_side,
			is_opposite
		)

		return is_opposite				


	def is_5m_opposite_of_last_15m(self, ticker, signal, timeframe):
		"""
		Return True if:
			- the current signal timeframe is 5m
			- the current signal side is opposite of the most recent 15m signal side

		Signal normalization:
			buy, buy+ -> buy
			sell, sell+ -> sell
		"""
		tf = self.tvw_helpers.normalize_tf(timeframe)
		if tf != "5m":
			return False

		sym = str(ticker or "").upper().strip()
		current_side = self.tvw_helpers.normalize_signal(signal)

		if current_side not in {"buy", "sell"}:
			return False

		last_15m_alert = self.tvw_helpers.get_nth_last_alert(sym, "15m", 1)
		if last_15m_alert is None:
			return False

		_, last_15m_fields = last_15m_alert
		last_15m_side = self.tvw_helpers.normalize_signal(last_15m_fields.get("signal"))

		if last_15m_side not in {"buy", "sell"}:
			return False

		return current_side != last_15m_side


	def has_opposite_1h_signal_since_last_same_side_4h(self, ticker, signal, max_scan_4h=500, max_scan_1h=1000):
		"""
		Return True if there exists an opposite-side 1h signal that occurred after the
		most recent valid same-side 4h signal for the given ticker/signal side.

		Definitions:
			- Same-side 4h signal:
				The most recent valid 4h signal whose normalized side matches the incoming
				signal side, as determined by get_latest_valid_4h_same_side_signal(...).
			- Opposite-side 1h signal:
				A 1h signal whose normalized side is opposite to the incoming signal side.

		Behavior:
			- If no valid same-side 4h anchor exists, return False.
			- If the anchor 4h signal lacks a parseable bar_close_time_eastern, return False.
			- Scan recent 1h alerts for the ticker.
			- Return True if any opposite-side 1h signal has bar_close_time_eastern strictly
			  later than the anchor 4h signal time.
			- Otherwise return False.

		Parameters:
			ticker (str): Ticker symbol, e.g. "AAPL".
			signal (str): Current incoming signal, e.g. "buy", "buy+", "sell", or "sell+".
			max_scan_4h (int): Max number of recent 4h entries to inspect when locating the anchor.
			max_scan_1h (int): Max number of recent 1h entries to inspect.

		Returns:
			bool: True if an opposite-side 1h signal exists after the anchor same-side 4h signal;
			False otherwise.
		"""
		sym = str(ticker or "").upper().strip()
		target_side = self.tvw_helpers.normalize_signal(signal)

		if target_side not in {"buy", "sell"}:
			logger.info(
				"Invalid signal passed to has_opposite_1h_signal_since_last_same_side_4h: %r",
				signal,
			)
			return False

		anchor_4h = self.get_latest_valid_4h_same_side_signal(sym, signal, max_scan=max_scan_4h)
		if not anchor_4h:
			logger.info("No same-side valid 4h anchor found for %r", sym)
			return False

		anchor_time_str = anchor_4h.get("bar_close_time_eastern")
		if not anchor_time_str:
			logger.info("Anchor 4h signal for %r is missing bar_close_time_eastern", sym)
			return False

		try:
			anchor_time = datetime.fromisoformat(anchor_time_str)
		except Exception:
			logger.exception("Failed parsing anchor 4h time for %r: %r", sym, anchor_time_str)
			return False

		opposite_side = "sell" if target_side == "buy" else "buy"
		stream_key = self.tvw_helpers.stream_key("1h", sym)

		try:
			entries = self.r.xrevrange(stream_key, count=max_scan_1h)
		except Exception:
			logger.exception("Failed reading 1h stream for %r", sym)
			return False

		if not entries:
			logger.info("No 1h entries found for %r", sym)
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
					"Failed parsing 1h signal time for %r entry_id=%r value=%r",
					sym,
					entry_id,
					entry_time_str,
				)
				continue

			if entry_time > anchor_time:
				logger.info(
					"Found opposite-side 1h signal after anchor 4h for %r: opposite_side=%r entry_id=%r entry_time=%r anchor_time=%r",
					sym,
					opposite_side,
					entry_id,
					entry_time_str,
					anchor_time_str,
				)
				return True

		return False		


	def entry_strategy1(self, strategy_name, simulation_only, date, signal, prices, ticker, timeframe, NUM_SHARES, alpaca_api):

		tf = self.tvw_helpers.normalize_tf(timeframe)
		if tf not in {"1m", "15m"}:
			return None

		num_shares = NUM_SHARES

		signal_4h = self.get_latest_valid_4h_same_side_signal(ticker, signal)
		if not signal_4h:
			logger.info("No valid 4h context")
			return None

		# Extra constraint only for 1m entries:
		# block entry if an opposite-side 1h signal exists after the anchor 4h signal.
		if tf == "1m":
			if self.has_opposite_1h_signal_since_last_same_side_4h(ticker, signal):
				logger.info(
					"Blocked Strategy 1 1m entry for %r because an opposite-side 1h signal exists after the anchor 4h signal",
					ticker,
				)
				return None

		open_price = signal_4h["open"]
		close_price = signal_4h["close"]

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


	"""
	def entry_strategy1(self, strategy_name, simulation_only, date, signal, prices, ticker, timeframe, NUM_SHARES, alpaca_api):
		
		if timeframe != "1m" and timeframe != "15m":
			return None

		num_shares = NUM_SHARES

		signal_4h = self.get_latest_valid_4h_same_side_signal(ticker, signal)

		if not signal_4h:
			logger.info("No valid 4h context")
			return None

		open_price = signal_4h["open"]
		close_price = signal_4h["close"]

		if open_price is None or close_price is None:
			return None		
		
		_4h_hi = max(open_price, close_price)	
		_4h_lo = min(open_price, close_price)
		signal_4h_len = _4h_hi - _4h_lo

		max_deviation_from_4hr_peak_to_4hr_len_ratio = 0.75
		
		market_price = prices.get(ticker, {}).get("market")
		if market_price is None:
			return None

		if (
			(
				(timeframe != "1m") and 
				(self.has_opposite_1h_signal_since_last_same_side_4h(ticker, signal))
			) or
			(timeframe != "15m")
		):
			if self.tvw_helpers.normalize_signal(signal) == "buy":
				if market_price < (_4h_hi - (max_deviation_from_4hr_peak_to_4hr_len_ratio * signal_4h_len)):
					return self.place_long_order(simulation_only, strategy_name, ticker, date, prices, num_shares, alpaca_api)

			if self.tvw_helpers.normalize_signal(signal) == "sell":
				if market_price > (_4h_lo + (max_deviation_from_4hr_peak_to_4hr_len_ratio * signal_4h_len)):
					return self.place_short_order(simulation_only, strategy_name, ticker, date, prices, num_shares, alpaca_api)

		logger.info("No trade condition met for Strategy 1 for %r", ticker)
		return None		
	"""

	"""
	def entry_strategy1(self, strategy_name, simulation_only, date, signal, prices, ticker, timeframe, NUM_SHARES, alpaca_api):
		
		if timeframe != "1m":
			return None

		num_shares = NUM_SHARES

		signal_4h = self.get_latest_valid_4h_same_side_signal(ticker, signal)

		if not signal_4h:
			logger.info("No valid 4h context")
			return None

		open_price = signal_4h["open"]
		close_price = signal_4h["close"]

		if open_price is None or close_price is None:
			return None		
		
		_4h_hi = max(open_price, close_price)	
		_4h_lo = min(open_price, close_price)
		signal_4h_len = _4h_hi - _4h_lo

		max_deviation_from_4hr_peak_to_4hr_len_ratio = 0.75
		
		market_price = prices.get(ticker, {}).get("market")
		if market_price is None:
			return None

		if self.tvw_helpers.normalize_signal(signal) == "buy":
			if market_price < (_4h_hi - (max_deviation_from_4hr_peak_to_4hr_len_ratio * signal_4h_len)):
				return self.place_long_order(simulation_only, strategy_name, ticker, date, prices, num_shares, alpaca_api)

		if self.tvw_helpers.normalize_signal(signal) == "sell":
			if market_price > (_4h_lo + (max_deviation_from_4hr_peak_to_4hr_len_ratio * signal_4h_len)):
				return self.place_short_order(simulation_only, strategy_name, ticker, date, prices, num_shares, alpaca_api)

		logger.info("No trade condition met for Strategy 1 for %r", ticker)
		return None		
	"""

	"""
	def exit_strategy1(self, strategy_name, simulation_only, date, signal, prices, ticker, timeframe, alpaca_api):

		if self.is_1h_opposite_of_last_4h(ticker, signal, timeframe):

			try:
				position = alpaca_api.get_position(ticker)
			except Exception:
				return None 

			position_qty = float(position.qty)
			NUM_SHARES = abs(position_qty)	

			last_1h_alert = self.tvw_helpers.get_nth_last_alert(ticker, "1h", 1)
			
			if last_1h_alert is None:
				logger.info("No 1h signal found for %r", ticker)
				return None

			_, last_1h_fields = last_1h_alert

			signal_1h = self.tvw_helpers.normalize_signal(last_1h_fields.get("signal"))

			if signal_1h == "buy" and position_qty < 0:
				return self.cover_short_order(simulation_only, strategy_name, ticker, date, prices, NUM_SHARES, alpaca_api)

			elif signal_1h == "sell" and position_qty > 0:
				return self.sell_long_order(simulation_only, strategy_name, ticker, date, prices, NUM_SHARES, alpaca_api)

			else:
				logger.info("Latest 1h signal is invalid/unknown for %r: %r", ticker, signal_1h)
	"""


	def exit_strategy1(self, strategy_name, simulation_only, date, signal, prices, ticker, timeframe, alpaca_api):

		logger.info(
			"exit_strategy1 check: strategy=%r ticker=%r timeframe=%r raw_signal=%r normalized_signal=%r",
			strategy_name,
			ticker,
			timeframe,
			signal,
			self.tvw_helpers.normalize_signal(signal),
		)

		is_opposite = self.is_1h_opposite_of_last_4h(ticker, signal, timeframe) or self.lower_tf_confirms_1h_opposite_of_4h(ticker, signal, timeframe)
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
			if redis_position_side == "short" and redis_num_shares > 0:
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
				)

			logger.info(
				"exit_strategy1 no Alpaca short position to cover for %r; alpaca_qty=%r",
				ticker,
				alpaca_position_qty,
			)
			return None

		elif signal_1h == "sell":
			if redis_position_side == "long" and redis_num_shares > 0:
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

		last_1m_alert = self.tvw_helpers.get_nth_last_alert(ticker, timeframe, 1)
		last_15m_alert = self.tvw_helpers.get_nth_last_alert(ticker, "15m", 1)

		if last_1m_alert is None or last_15m_alert is None:
			logger.info("Strategy skipped: missing alert context for %r", ticker)
			return None

		_, last_1m_fields = last_1m_alert
		_, last_15m_fields = last_15m_alert

		last_1m_signal = self.tvw_helpers.normalize_signal(last_1m_fields.get("signal"))
		last_15m_signal = self.tvw_helpers.normalize_signal(last_15m_fields.get("signal"))

		if last_15m_signal == "buy" and last_1m_signal == "buy":
			return self.place_long_order(simulation_only, strategy_name, ticker, date, prices, num_shares, alpaca_api)

		if last_15m_signal == "sell" and last_1m_signal == "sell":
			return self.place_short_order(simulation_only, strategy_name, ticker, date, prices, num_shares, alpaca_api)

		logger.info(
			"No trade condition met for %r | 1m=%r 15m=%r",
			ticker, last_1m_signal, last_15m_signal
		)
		return None


	"""
	def exit_strategy2(self, strategy_name, simulation_only, date, signal, prices, ticker, timeframe, alpaca_api):
				
		if self.is_5m_opposite_of_last_15m(ticker, signal, timeframe):

			try:
				position = alpaca_api.get_position(ticker)
			except Exception:
				return None 

			position_qty = float(position.qty)
			NUM_SHARES = abs(position_qty)			

			last_5m_alert = self.tvw_helpers.get_nth_last_alert(ticker, "5m", 1)
			
			if last_5m_alert is None:
				logger.info("No 5m signal found for %r", ticker)
				return None

			_, last_5m_fields = last_5m_alert

			signal_5m = self.tvw_helpers.normalize_signal(last_5m_fields.get("signal"))

			if signal_5m == "buy" and position_qty < 0:
				return self.cover_short_order(simulation_only, strategy_name, ticker, date, prices, NUM_SHARES, alpaca_api)
			if signal_5m == "sell" and position_qty > 0:
				return self.sell_long_order(simulation_only, strategy_name, ticker, date, prices, NUM_SHARES, alpaca_api)				
	"""

	def exit_strategy2(self, strategy_name, simulation_only, date, signal, prices, ticker, timeframe, alpaca_api):

		logger.info(
			"exit_strategy2 check: strategy=%r ticker=%r timeframe=%r raw_signal=%r normalized_signal=%r",
			strategy_name,
			ticker,
			timeframe,
			signal,
			self.tvw_helpers.normalize_signal(signal),
		)

		is_opposite = self.is_5m_opposite_of_last_15m(ticker, signal, timeframe)
		logger.info("exit_strategy2 opposite-check for %r => %r", ticker, is_opposite)

		if not is_opposite:
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
			if redis_position_side == "short" and redis_num_shares > 0:
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
				)

			logger.info(
				"exit_strategy2 no Alpaca short position to cover for %r; alpaca_qty=%r",
				ticker,
				alpaca_position_qty,
			)
			return None

		elif signal_5m == "sell":
			if redis_position_side == "long" and redis_num_shares > 0:
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

	"""
	def place_order(self, simulation_only, strategy_name, ticker, date, prices, num_shares, alpaca_api, order_type):

		side = None
		if order_type in {"long", "cover"}:
			side = 'buy'
		elif order_type in {"short", "sell"}:
			side = 'sell'
		else:
			return None

		price = prices.get(ticker, {}).get("market")
		if price is None:
			return None	

		if order_type == "short" and not self.tvw_helpers.is_symbol_shortable(alpaca_api, ticker):
			return None		# Only place short if security is shortable	

		is_regular_hours = self.tvw_helpers._is_regular_hours_et()

		if not is_regular_hours:
			if order_type in {"long", "cover"}:
				ask = prices.get(ticker, {}).get("ask")
				if ask is None:
					return None
				price = ask + 0.01

			elif order_type in {"short", "sell"}:
				bid = prices.get(ticker, {}).get("bid")
				if bid is None:
					return None
				price = bid - 0.01				

		if price <= 0:
			return None

		record = self.trade_records.create_trade_record(
			strategy_name,
			ticker,
			date,
			price,
			num_shares,
			order_type
		)
		logger.info("Executing %r order for %r", order_type, ticker)

		if not simulation_only:
			if is_regular_hours:
				for _ in range(3):
					try:
						alpaca_api.submit_order(
    						symbol=ticker,
    						qty=num_shares,  
    						side=side,
    						type='market',
    						time_in_force='day',
						)	
						break
					except Exception:		
						logger.exception("Failed to execute %r order for %r", order_type, ticker)
						time.sleep(0.5)
			else:
				for _ in range(3):
					try:
						alpaca_api.submit_order(
    						symbol=ticker,
    						qty=num_shares,  
    						side=side,
    						type='limit',
    						time_in_force='day',
    						limit_price=price
						)
						break	
					except Exception:		
						logger.exception("Failed to execute %r order for %r", order_type, ticker)	
						time.sleep(0.5)			

		return record	
	"""

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
			logger.info(
				"Alpaca position before order: ticker=%r qty=%r side=%r avg_entry_price=%r",
				ticker,
				getattr(alpaca_position, "qty", None),
				getattr(alpaca_position, "side", None),
				getattr(alpaca_position, "avg_entry_price", None),
			)
		except Exception:
			logger.exception(
				"Failed retrieving Alpaca position before order for ticker=%r order_type=%r",
				ticker,
				order_type,
			)

		execution_qty = num_shares

		if order_type in {"long", "short"} and redis_position is not None:
			redis_side = str(redis_position.get("side") or "").strip().lower()
			redis_open_qty = float(redis_position.get("num_shares", 0) or 0)

			if redis_open_qty > 0 and redis_side == order_type:
				execution_qty = num_shares * 0.25

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

		record = None

		if do_redis_bookkeeping:
			try:
				logger.info(
					"Recording strategy trade before broker execution: strategy=%r ticker=%r order_type=%r base_qty=%r execution_qty=%r price=%r can_add_to_existing_position=%r",
					strategy_name,
					ticker,
					order_type,
					num_shares,
					execution_qty,
					price,
					True,
				)

				record = self.trade_records.create_trade_record(
					strategy_name,
					ticker,
					date,
					price,
					execution_qty,
					order_type,
					True,
				)

				logger.info(
					"Recorded strategy trade successfully: strategy=%r ticker=%r order_type=%r execution_qty=%r price=%r",
					strategy_name,
					ticker,
					order_type,
					execution_qty,
					price,
				)

			except Exception:
				logger.exception(
					"Failed to record strategy trade before broker execution: strategy=%r ticker=%r order_type=%r execution_qty=%r price=%r",
					strategy_name,
					ticker,
					order_type,
					execution_qty,
					price,
				)
				return None
		else:
			logger.info(
				"Skipping Redis bookkeeping in place_order: strategy=%r ticker=%r order_type=%r execution_qty=%r",
				strategy_name,
				ticker,
				order_type,
				execution_qty,
			)

		logger.info(
			"About to submit Alpaca order: strategy=%r ticker=%r order_type=%r broker_side=%r base_qty=%r execution_qty=%r price=%r regular_hours=%r simulation_only=%r",
			strategy_name,
			ticker,
			order_type,
			broker_side,
			num_shares,
			execution_qty,
			price,
			is_regular_hours,
			simulation_only,
		)

		if not simulation_only:
			if is_regular_hours:
				for attempt in range(1, 4):
					try:
						alpaca_api.submit_order(
							symbol=ticker,
							qty=execution_qty,
							side=broker_side,
							type="market",
							time_in_force="day",
						)
						logger.info(
							"Alpaca market order submitted successfully: strategy=%r ticker=%r order_type=%r broker_side=%r execution_qty=%r attempt=%r",
							strategy_name,
							ticker,
							order_type,
							broker_side,
							execution_qty,
							attempt,
						)
						break
					except Exception:
						logger.exception(
							"Failed to execute Alpaca market order: strategy=%r ticker=%r order_type=%r broker_side=%r execution_qty=%r attempt=%r",
							strategy_name,
							ticker,
							order_type,
							broker_side,
							execution_qty,
							attempt,
						)
						time.sleep(0.5)
			else:
				for attempt in range(1, 4):
					try:
						alpaca_api.submit_order(
							symbol=ticker,
							qty=execution_qty,
							side=broker_side,
							type="limit",
							time_in_force="day",
							limit_price=price,
						)
						logger.info(
							"Alpaca limit order submitted successfully: strategy=%r ticker=%r order_type=%r broker_side=%r execution_qty=%r limit_price=%r attempt=%r",
							strategy_name,
							ticker,
							order_type,
							broker_side,
							execution_qty,
							price,
							attempt,
						)
						break
					except Exception:
						logger.exception(
							"Failed to execute Alpaca limit order: strategy=%r ticker=%r order_type=%r broker_side=%r execution_qty=%r limit_price=%r attempt=%r",
							strategy_name,
							ticker,
							order_type,
							broker_side,
							execution_qty,
							price,
							attempt,
						)
						time.sleep(0.5)

		return record


	"""
	def place_long_order(self, simulation_only, strategy_name, ticker, date, prices, num_shares, alpaca_api):
		return self.place_order(simulation_only, strategy_name, ticker, date, prices, num_shares, alpaca_api, "long")

	def place_short_order(self, simulation_only, strategy_name, ticker, date, prices, num_shares, alpaca_api):
		return self.place_order(simulation_only, strategy_name, ticker, date, prices, num_shares, alpaca_api, "short")

	def sell_long_order(self, simulation_only, strategy_name, ticker, date, prices, num_shares, alpaca_api):
		return self.place_order(simulation_only, strategy_name, ticker, date, prices, num_shares, alpaca_api, "sell")

	def cover_short_order(self, simulation_only, strategy_name, ticker, date, prices, num_shares, alpaca_api):
		return self.place_order(simulation_only, strategy_name, ticker, date, prices, num_shares, alpaca_api, "cover")
	"""


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

	def sell_long_order(self, simulation_only, strategy_name, ticker, date, prices, num_shares, alpaca_api):
		return self.place_order(
			simulation_only,
			strategy_name,
			ticker,
			date,
			prices,
			num_shares,
			alpaca_api,
			"sell",
			False,
		)

	def cover_short_order(self, simulation_only, strategy_name, ticker, date, prices, num_shares, alpaca_api):
		return self.place_order(
			simulation_only,
			strategy_name,
			ticker,
			date,
			prices,
			num_shares,
			alpaca_api,
			"cover",
			False,
		)	
