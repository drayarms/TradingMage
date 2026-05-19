import logging
import time
from datetime import datetime
import math
from decimal import Decimal, ROUND_HALF_UP

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



	def get_signal_based_progressive_entry_size(
		self,
		ticker,
		side,
		entry_tf,
		anchor_tf,
		base_num_shares,
		smallest_share_size,
		max_scan_entry_tf=1000,
		max_scan_anchor_tf=500,
	):
		"""
		Determines progressive entry sizing based on the ordinal position of the
		current qualifying entry signal since the latest valid same-side anchor
		timeframe signal.

		Unlike position-based progressive sizing, this method does not infer the
		next entry step from current Alpaca exposure or Redis bookkeeping state.
		Instead, it reconstructs the sequence directly from historical signals.

		Core concept:
			- Every qualifying same-side entry timeframe signal after the latest
			  valid same-side anchor signal contributes to the progressive sequence.
			- The current signal's ordinal position within that sequence determines
			  the position size using progressive halving logic.

		Sizing rule:
			qualifying_signal_count = 1 -> size = base_num_shares
			qualifying_signal_count = 2 -> size = base_num_shares / 2
			qualifying_signal_count = 3 -> size = base_num_shares / 4
			qualifying_signal_count = 4 -> size = base_num_shares / 8
			...

		Qualification requirements:
			A signal qualifies if:
				- it belongs to the specified entry timeframe
				- its normalized side matches the requested side
				- it occurred after the latest valid same-side anchor signal

			Signal normalization:
				- "buy" and "buy+" are treated as "buy"
				- "sell" and "sell+" are treated as "sell"

			Anchor behavior:
				The anchor signal is determined using
				get_latest_valid_same_side_signal(), meaning:
					- the anchor must match the requested side
					- any newer opposite-side anchor invalidates older anchors

			Short position handling:
				Because Alpaca does not support fractional short shares,
				short execution quantities are floored to whole integers.

			Parameters:
				ticker (str):
					Ticker symbol, e.g. "AAPL".

				side (str):
					Desired trade direction signal:
						"buy", "buy+", "sell", or "sell+".

				entry_tf (str):
					The entry timeframe being evaluated,
					e.g. "1m", "5m", or "15m".

				anchor_tf (str):
					The higher timeframe anchor used to establish the
					current directional regime.

				base_num_shares (float):
					The original unscaled entry size.

				smallest_share_size (float):
					Minimum allowable execution quantity.

				max_scan_entry_tf (int, optional):
					Maximum number of entry timeframe signals to inspect.
					Default is 1000.

				max_scan_anchor_tf (int, optional):
					Maximum number of anchor timeframe signals to inspect.
					Default is 500.

			Returns:
				float:
					The computed progressive execution quantity.

					Returns 0.0 if:
						- no valid anchor exists
						- the signal side is invalid
						- the computed size falls below smallest_share_size
						- no qualifying signals are found
						- required timestamps cannot be parsed

			Example:
				If:
					base_num_shares = 10

				and the qualifying entry signals since the current anchor are:
					1st qualifying signal -> 10 shares
					2nd qualifying signal -> 5 shares
					3rd qualifying signal -> 2.5 shares
					4th qualifying signal -> 1.25 shares

				then the current signal's ordinal position determines
				the execution quantity.
		"""	
		sym = str(ticker or "").upper().strip()
		target_side = self.tvw_helpers.normalize_signal(side)
		entry_tf_norm = self.tvw_helpers.normalize_tf(entry_tf)
		anchor_tf_norm = self.tvw_helpers.normalize_tf(anchor_tf)

		if target_side not in {"buy", "sell"}:
			return 0.0

		try:
			base_num_shares = float(base_num_shares)
		except Exception:
			return 0.0

		if base_num_shares <= 0:
			return 0.0

		anchor_signal = self.get_latest_valid_same_side_signal(
			sym,
			target_side,
			anchor_tf_norm,
			max_scan=max_scan_anchor_tf,
		)

		if not anchor_signal:
			return 0.0

		anchor_time_str = anchor_signal.get("bar_close_time_eastern")
		if not anchor_time_str:
			return 0.0

		try:
			anchor_time = datetime.fromisoformat(anchor_time_str)
		except Exception:
			logger.exception("Failed parsing anchor time for signal-based sizing: %r", anchor_time_str)
			return 0.0

		stream_key = self.tvw_helpers.stream_key(entry_tf_norm, sym)

		try:
			entries = self.r.xrevrange(stream_key, count=max_scan_entry_tf)
		except Exception:
			logger.exception("Failed reading %r stream for signal-based sizing: %r", entry_tf_norm, sym)
			return 0.0

		qualifying_count = 0

		for entry_id, fields in entries:
			entry_side = self.tvw_helpers.normalize_signal(fields.get("signal"))

			if entry_side not in {"buy", "sell"}:
				continue

			entry_time_str = fields.get("bar_close_time_eastern")
			if not entry_time_str:
				continue

			try:
				entry_time = datetime.fromisoformat(entry_time_str)
			except Exception:
				logger.exception(
					"Failed parsing entry signal time for %r entry_id=%r value=%r",
					sym,
					entry_id,
					entry_time_str,
				)
				continue

			if entry_time < anchor_time:
				break

			if entry_side != target_side:
				return 0.0				

			# At this point, signal is qualifying because it is same side to anchor and occurred later than anchor
			qualifying_count += 1

		if qualifying_count < 1:
			return 0.0

		execution_qty = base_num_shares / (2 ** (qualifying_count - 1))

		if target_side == "sell":
			execution_qty = math.floor(execution_qty)

		if execution_qty < smallest_share_size:
			return 0.0

		logger.info(
			"signal-based progressive sizing: ticker=%r side=%r entry_tf=%r anchor_tf=%r qualifying_count=%r execution_qty=%r",
			sym,
			target_side,
			entry_tf_norm,
			anchor_tf_norm,
			qualifying_count,
			execution_qty,
		)

		return execution_qty


	def is_latest_anchor_opposite_of_open_position(self, ticker, anchor_tf, alpaca_position_qty):
		"""
		Return True if the latest anchor timeframe signal is opposite
		of the currently open Alpaca position side.
		"""
		try:
			alpaca_position_qty = float(alpaca_position_qty or 0.0)
		except Exception:
			return False

		if alpaca_position_qty == 0:
			return False

		position_side = "long" if alpaca_position_qty > 0 else "short"
		expected_anchor_side = "buy" if position_side == "long" else "sell"
		opposite_anchor_side = "sell" if expected_anchor_side == "buy" else "buy"

		last_anchor_alert = self.tvw_helpers.get_nth_last_alert(ticker, anchor_tf, 1)
		if last_anchor_alert is None:
			return False

		_, last_anchor_fields = last_anchor_alert
		last_anchor_signal = self.tvw_helpers.normalize_signal(last_anchor_fields.get("signal"))

		is_true = last_anchor_signal == opposite_anchor_side

		logger.info(
			"anchor-position exit check: ticker=%r anchor_tf=%r position_side=%r latest_anchor_signal=%r result=%r",
			ticker,
			anchor_tf,
			position_side,
			last_anchor_signal,
			is_true,
		)

		return is_true		
			

	def entry_strategy1(self, strategy_name, entry_tf, intermediary_tf, anchor_tf, simulation_only, date, signal, prices, ticker, timeframe, NUM_SHARES, alpaca_api):
		"""
		Strategy relies on latest signals of three different timeframes; an anchor timeframe (highest timeframe), an entry timeframe (lowerst timeframe)
		and an intermediary timeframe. A trade is taken upon the entry timeframe, if the latest anchor timeframe is the same side as the
		entry timeframe, and if there is no intermediary timeframe signal of the opposite side between the anchor and the entry. 

		Parameters:
			strategy_name (str): Strategy name.
			entry_tf (str): Entry timeframe (lowest timeframe). 
			intermediary_tf (str): Intermediary timeframe.
			anchor_tf (str): Anchor timeframe (hihgest timeframe)
			simulation_only (bool): True if we only want Redis simulation and no Alpaca execution. False if we want both.
			date (str): Eastern time.
			signal (str): "buy", "sell", "buy+" or "sell+".
			prices (dict): Market, ask, and bid prices for ticker symbol.
			ticker (str): Ticker symbol.
			timeframe (str): Timeframe of signal.
			NUM_SHARES (float): Number of shares to be traded.
			alpaca_api

		Returns:
			place_long_order() or place_short_order() or None
		"""
		tf = self.tvw_helpers.normalize_tf(timeframe)
		if tf != entry_tf:
			return None

		last_entry_tf_alert = self.tvw_helpers.get_nth_last_alert(ticker, tf, 1)
		last_anchor_tf_alert = self.tvw_helpers.get_nth_last_alert(ticker, anchor_tf, 1)

		if last_entry_tf_alert is None or last_anchor_tf_alert is None:
			logger.info("Strategy skipped: missing alert context for %r", ticker)
			return None

		_, last_entry_tf_fields = last_entry_tf_alert
		_, last_anchor_tf_fields = last_anchor_tf_alert

		last_entry_tf_signal = self.tvw_helpers.normalize_signal(last_entry_tf_fields.get("signal"))
		last_anchor_tf_signal = self.tvw_helpers.normalize_signal(last_anchor_tf_fields.get("signal"))

		if last_entry_tf_signal not in {"buy", "sell"} or last_anchor_tf_signal not in {"buy", "sell"}:
			logger.info(
				"Strategy skipped: invalid signal context for %r | %r=%r %r=%r",
				ticker,
				entry_tf,
				last_entry_tf_signal,
				intermediary_tf,
				last_anchor_tf_signal,
			)
			return None

		# Block entry if an opposite-side intermediary tf signal occurred after the anchor
		if last_anchor_tf_signal == "buy" and last_entry_tf_signal == "buy":
			if self.has_opposite_signal_since_last_valid_same_side_higher_tf(
				ticker, "buy", intermediary_tf, anchor_tf, 1000, 500
			):
				logger.info("Blocked Strategy %r long entry for %r due to opposite %r after anchor %r", strategy_name, ticker, intermediary_tf, anchor_tf)
				return None

			num_shares = self.get_signal_based_progressive_entry_size(
				ticker=ticker,
				side=signal,
				entry_tf=entry_tf,
				anchor_tf=anchor_tf,
				base_num_shares=NUM_SHARES,
				smallest_share_size=self.SMALLEST_SHARE_SIZE,
			)

			if num_shares <= 0:
				logger.info(
					"Strategy skipped: signal-based progressive size is zero for %r strategy=%r signal=%r entry_tf=%r anchor_tf=%r",
					ticker,
					strategy_name,
					signal,
					entry_tf,
					anchor_tf,
				)
				return None		
							
			return self.place_long_order(simulation_only, strategy_name, ticker, date, prices, num_shares, alpaca_api)

		if last_anchor_tf_signal == "sell" and last_entry_tf_signal == "sell":
			if self.has_opposite_signal_since_last_valid_same_side_higher_tf(
				ticker, "sell", intermediary_tf, anchor_tf, 1000, 500
			):
				logger.info("Blocked Strategy %r short entry for %r due to opposite %r after anchor %r", strategy_name, ticker, intermediary_tf, anchor_tf)
				return None

			num_shares = self.get_signal_based_progressive_entry_size(
				ticker=ticker,
				side=signal,
				entry_tf=entry_tf,
				anchor_tf=anchor_tf,
				base_num_shares=NUM_SHARES,
				smallest_share_size=self.SMALLEST_SHARE_SIZE,
			)

			if num_shares <= 0:
				logger.info(
					"Strategy skipped: signal-based progressive size is zero for %r strategy=%r signal=%r entry_tf=%r anchor_tf=%r",
					ticker,
					strategy_name,
					signal,
					entry_tf,
					anchor_tf,
				)
				return None	

			return self.place_short_order(simulation_only, strategy_name, ticker, date, prices, num_shares, alpaca_api)

		logger.info(
			"No trade condition met for %r | entry_tf=%r entry_signal=%r anchor_tf=%r anchor_signal=%r",
			ticker,
			entry_tf,
			last_entry_tf_signal,
			anchor_tf,
			last_anchor_tf_signal,
		)		

		return None	


	def exit_strategy1(self, strategy_name, lower_timeframes, intermediary_tf, anchor_tf, simulation_only, date, signal, prices, ticker, timeframe, alpaca_api):
		"""
		Exit if the current intermediary timeframe signal is opposite of the latest anchor timeframe signal,
		if a lower timeframe signal occured that is same side as the last exit tf (which may have occurred overnight), and opposite side to the anchor tf,
		if an anchor tf signal occured, opposite side of the entry tf.
		Note that intermediaty timeframe and exit timeframe are the same thing.

		Parameters:
			strategy_name (str): Strategy name.
			lower_timeframes (Set): All timeframes for which we can get a potential signal, that are lower than intermediary timeframe.
			intermediary_tf (str): Intermediary timeframe.
			anchor_tf (str): Anchor timeframe (hihgest timeframe)
			simulation_only (bool): True if we only want Redis simulation and no Alpaca execution. False if we want both.
			date (str): Eastern time.
			signal (str): "buy", "sell", "buy+" or "sell+".
			prices (dict): Market, ask, and bid prices for ticker symbol.
			ticker (str): Ticker symbol.
			timeframe (str): Timeframe of signal.
			alpaca_api

		Returns:
			sell_long_order() or cover_short_order() or None
		"""				
		logger.info(
			"exit check: strategy=%r intermediary_tf=%r anchor_tf=%r ticker=%r timeframe=%r raw_signal=%r normalized_signal=%r",
			strategy_name,
			intermediary_tf,
			anchor_tf,
			ticker,
			timeframe,
			signal,
			self.tvw_helpers.normalize_signal(signal),
		)

		exit_timeframes = lower_timeframes | {intermediary_tf} # union
		tf = self.tvw_helpers.normalize_tf(timeframe)
		if tf not in exit_timeframes:
			return None

		redis_position_side = None
		redis_num_shares = 0.0
		alpaca_position = None
		alpaca_position_qty = 0.0

		if simulation_only:
			try:
				redis_position = self.trade_records.get_position(strategy_name, ticker)
			except Exception:
				logger.exception(
					"exit %r failed to fetch Redis position for %r",
					strategy_name,
					ticker,
				)
				return None

			if not redis_position:
				return None

			redis_position_side = str(redis_position.get("side") or "").strip().lower()
			redis_num_shares = abs(float(redis_position.get("num_shares") or 0.0))

			if redis_position_side not in {"long", "short"} or redis_num_shares <= 0:
				return None

		else:
			try:
				alpaca_position = alpaca_api.get_position(ticker)
				alpaca_position_qty = float(getattr(alpaca_position, "qty", 0.0) or 0.0)
			except Exception:
				return None

			if abs(alpaca_position_qty) <= 0:
				return None


		# There is a qualifying open position, so proceed with exit condition checks.

		position_qty_for_anchor_check = alpaca_position_qty

		if simulation_only:
			if redis_position_side == "long":
				position_qty_for_anchor_check = redis_num_shares
			elif redis_position_side == "short":
				position_qty_for_anchor_check = -redis_num_shares		

		is_intermediary_tf_opposite_of_last_anchor_tf = self.is_tf_relative_to_last_higher_tf(ticker, signal, timeframe, intermediary_tf, anchor_tf, "opposite")

		lower_tf_confirms_intermediary_opposite_of_anchor = self.lower_tf_confirms_mid_tf_opposite_of_higher_tf(ticker, signal, timeframe, lower_timeframes, intermediary_tf, anchor_tf)

		anchor_opposite_open_position = self.is_latest_anchor_opposite_of_open_position(
			ticker,
			anchor_tf,
			position_qty_for_anchor_check,
		)

		should_exit = (
			is_intermediary_tf_opposite_of_last_anchor_tf
			or lower_tf_confirms_intermediary_opposite_of_anchor
			or anchor_opposite_open_position
		)

		logger.info(
			"exit %r opposite-check for %r => intermediary opp anchor: %r  lower tf confirms intermediary opp anchor: %r  anchor opp open position: %r",
			strategy_name,
			ticker,
			is_intermediary_tf_opposite_of_last_anchor_tf,
			lower_tf_confirms_intermediary_opposite_of_anchor,
			anchor_opposite_open_position,
		)

		if not should_exit:
			return None


		alpaca_num_shares = abs(alpaca_position_qty)		


		last_intermediary_tf_alert = self.tvw_helpers.get_nth_last_alert(ticker, intermediary_tf, 1)
		if last_intermediary_tf_alert is None:
			logger.info("No %r signal found for %r", intermediary_tf, ticker)
			return None

		_, last_intermediary_tf_fields = last_intermediary_tf_alert
		signal_intermediary_tf = self.tvw_helpers.normalize_signal(last_intermediary_tf_fields.get("signal"))

		logger.info(
			"exit_strategy1 signal context: ticker=%r intermediary_tf_signal=%r redis_side=%r redis_qty=%r alpaca_qty=%r",
			ticker,
			signal_intermediary_tf,
			redis_position_side,
			redis_num_shares,
			alpaca_num_shares,
		)

		if signal_intermediary_tf not in {"buy", "sell"}:
			logger.info(
				"Latest intermediary_tf signal is invalid/unknown for %r: %r",
				ticker,
				signal_intermediary_tf,
			)
			return None

		# At this point, should_exit is already True.
		# So liquidation should be based on the actual open position side,
		# not on the latest intermediary signal side.

		if simulation_only:
			if redis_position_side == "short" and redis_num_shares > 0:
				logger.info(
					"exit %r Redis bookkeeping cover for %r using redis_num_shares=%r",
					strategy_name,
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
						"exit Redis bookkeeping cover failed for strategy=%r ticker=%r qty=%r",
						strategy_name,
						ticker,
						redis_num_shares,
					)
				return None

			if redis_position_side == "long" and redis_num_shares > 0:
				logger.info(
					"exit %r Redis bookkeeping sell for %r using redis_num_shares=%r",
					strategy_name,
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
						"exit Redis bookkeeping sell failed for strategy=%r ticker=%r qty=%r",
						strategy_name,
						ticker,
						redis_num_shares,
					)
				return None

			logger.info(
				"exit %r no Redis position to liquidate for %r; redis_side=%r redis_qty=%r",
				strategy_name,
				ticker,
				redis_position_side,
				redis_num_shares,
			)
			return None

		if alpaca_position_qty < 0 and alpaca_num_shares > 0:
			logger.info(
				"exit %r Alpaca cover for %r using alpaca_num_shares=%r",
				strategy_name,
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
				do_redis_bookkeeping=True,
			)

		if alpaca_position_qty > 0 and alpaca_num_shares > 0:
			logger.info(
				"exit %r Alpaca sell for %r using alpaca_num_shares=%r",
				strategy_name,
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
				do_redis_bookkeeping=True,
			)

		logger.info(
			"exit %r no Alpaca position to liquidate for %r; alpaca_qty=%r",
			strategy_name,
			ticker,
			alpaca_position_qty,
		)
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

			price = float(
				Decimal(str(price)).quantize(
					Decimal("0.01"),
					rounding=ROUND_HALF_UP,
				)
			)	
			logger.info(
				"Rounded off-hours limit price: ticker=%r order_type=%r limit_price=%r",
				ticker,
				order_type,
				price,
			)						

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

			try:
				open_orders = alpaca_api.list_orders(status="open", limit=500)

				for existing_order in open_orders:
					existing_symbol = getattr(existing_order, "symbol", None)
					existing_side = str(getattr(existing_order, "side", "") or "").lower()
					existing_status = str(getattr(existing_order, "status", "") or "").lower()
					existing_type = str(getattr(existing_order, "type", "") or "").lower()
					existing_order_id = getattr(existing_order, "id", None)

					if existing_symbol != ticker or existing_side != broker_side:
						continue

					if existing_status not in {"new", "accepted", "partially_filled"}:
						continue

					if existing_type == "market":
						logger.info(
							"Existing market exit order already present; skipping duplicate submission: "
							"ticker=%r side=%r status=%r order_id=%r",
							ticker,
							broker_side,
							existing_status,
							existing_order_id,
						)
						return None

					if (
						is_regular_hours
						and order_type in {"sell", "cover"}
						and existing_type != "market"
					):
						logger.info(
							"Canceling existing non-market exit order before regular-hours market liquidation: "
							"ticker=%r side=%r type=%r status=%r order_id=%r",
							ticker,
							broker_side,
							existing_type,
							existing_status,
							existing_order_id,
						)

						alpaca_api.cancel_order(existing_order_id)

						time.sleep(1.0)
						continue

					logger.info(
						"Existing open Alpaca order already present; skipping duplicate submission: "
						"ticker=%r side=%r type=%r status=%r order_id=%r",
						ticker,
						broker_side,
						existing_type,
						existing_status,
						existing_order_id,
					)
					return None

			except Exception:
				logger.exception(
					"Failed checking/canceling existing Alpaca open orders for ticker=%r",
					ticker,
				)			

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
						return {
							"terminal_failure": True,
							"status": status,
						}						

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

		#if order_type in {"long", "short"}:
			#if simulation_only:
				#execution_qty = self.get_progressive_entry_size(
					#strategy_name,
					#ticker,
					#order_type,
					#num_shares,
					#self.SMALLEST_SHARE_SIZE,
				#)
			#else:
				#execution_qty = self.get_live_progressive_entry_size_from_alpaca(
					#alpaca_position_qty,
					#order_type,
					#num_shares,
					#self.SMALLEST_SHARE_SIZE,
				#)

		# Enforce Alpaca constraint FIRST
		if order_type == "short":
			execution_qty = math.floor(execution_qty)

		# Then enforce min size
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

		#if execution_qty < self.SMALLEST_SHARE_SIZE:
			#logger.info(
				#"Execution qty below minimum threshold for %r: %r",
				#ticker,
				#execution_qty,
			#)
			#return None			

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

		# Ensures execution attempt is complete (with either a success or failure) before next attempt fires
		pending_exit_key = None
		exit_guard_acquired = False

		if order_type in {"sell", "cover"}:
			pending_exit_key = f"tv:pending_exit:{strategy_name}:{ticker}"

			try:
				exit_guard_acquired = bool(
					self.r.set(
						pending_exit_key,
						"1",
						nx=True,
						ex=120,
					)
				)
			except Exception:
				logger.exception(
					"Failed acquiring pending exit guard: strategy=%r ticker=%r order_type=%r",
					strategy_name,
					ticker,
					order_type,
				)
				return None

			if not exit_guard_acquired:
				logger.info(
					"Skipping duplicate exit attempt because pending exit guard exists: strategy=%r ticker=%r order_type=%r",
					strategy_name,
					ticker,
					order_type,
				)
				return None			


		filled_order = submit_to_alpaca_and_wait_for_fill() # Alpaca order

		clear_exit_guard = False

		if (
			filled_order is not None
			and not (
				isinstance(filled_order, dict)
				and filled_order.get("terminal_failure")
			)
		):
			clear_exit_guard = True
		elif is_regular_hours:
			clear_exit_guard = True

		if clear_exit_guard and exit_guard_acquired and pending_exit_key:
			try:
				self.r.delete(pending_exit_key)
			except Exception:
				logger.exception(
					"Failed clearing pending exit guard: strategy=%r ticker=%r key=%r",
					strategy_name,
					ticker,
					pending_exit_key,
				)					

		if filled_order is None:
			logger.info(
				"Skipping Redis bookkeeping because Alpaca order was not filled: strategy=%r ticker=%r order_type=%r qty=%r",
				strategy_name,
				ticker,
				order_type,
				execution_qty,
			)
			return None

		if isinstance(filled_order, dict) and filled_order.get("terminal_failure"):
			logger.info(
				"Alpaca order reached terminal failure: strategy=%r ticker=%r order_type=%r status=%r",
				strategy_name,
				ticker,
				order_type,
				filled_order.get("status"),
			)

			return None			

		# If we are here, the Alpaca order got successfully filled
		fill_price = getattr(filled_order, "filled_avg_price", None)
		if fill_price is None:
			fill_price = price

		time.sleep(0.5)

		alpaca_position_qty_after_fill = 0.0
		alpaca_avg_entry_price_after_fill = None

		try:
			position = alpaca_api.get_position(ticker)
			alpaca_position_qty_after_fill = float(getattr(position, "qty", 0.0) or 0.0)
			alpaca_avg_entry_price_after_fill = getattr(position, "avg_entry_price", None)

			logger.info(
				"Post-fill Alpaca position check: ticker=%r qty=%r side=%r avg_entry_price=%r",
				ticker,
				getattr(position, "qty", None),
				getattr(position, "side", None),
				alpaca_avg_entry_price_after_fill,
			)
		except Exception:
			logger.info(
				"No Alpaca position found after fill for ticker=%r order_type=%r; treating position as flat",
				ticker,
				order_type,
			)

		if do_redis_bookkeeping:
			return self.trade_records.record_live_alpaca_fill(
				strategy_name=strategy_name,
				ticker=ticker,
				date=date,
				fill_price=fill_price,
				filled_qty=execution_qty,
				order_type=order_type,
				alpaca_position_qty=alpaca_position_qty_after_fill,
				alpaca_avg_entry_price=alpaca_avg_entry_price_after_fill,
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
