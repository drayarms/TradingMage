import logging
import time

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
			return False

		# Check opposite
		return (
			(current_side == "buy" and last_4h_signal == "sell") or
			(current_side == "sell" and last_4h_signal == "buy")
		)		


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
			

	def entry_strategy2(self, strategy_name, simulation_only, date, signal, prices, ticker, timeframe, NUM_SHARES, alpaca_api):
		
		if timeframe != "1m":
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


	def place_long_order(self, simulation_only, strategy_name, ticker, date, prices, num_shares, alpaca_api):
		return self.place_order(simulation_only, strategy_name, ticker, date, prices, num_shares, alpaca_api, "long")

	def place_short_order(self, simulation_only, strategy_name, ticker, date, prices, num_shares, alpaca_api):
		return self.place_order(simulation_only, strategy_name, ticker, date, prices, num_shares, alpaca_api, "short")

	def sell_long_order(self, simulation_only, strategy_name, ticker, date, prices, num_shares, alpaca_api):
		return self.place_order(simulation_only, strategy_name, ticker, date, prices, num_shares, alpaca_api, "sell")

	def cover_short_order(self, simulation_only, strategy_name, ticker, date, prices, num_shares, alpaca_api):
		return self.place_order(simulation_only, strategy_name, ticker, date, prices, num_shares, alpaca_api, "cover")


