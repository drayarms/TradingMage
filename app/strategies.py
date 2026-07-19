import logging
import time
from datetime import datetime
import math
from decimal import Decimal, ROUND_HALF_UP
from concurrent.futures import ThreadPoolExecutor

logger = logging.getLogger("tv-webhook")

class Strategies:
	def __init__(self, trading_view_webhook_helpers, trade_records):
		self.tvw_helpers = trading_view_webhook_helpers
		self.r = trading_view_webhook_helpers.require_redis()
		self.trade_records = trade_records
		self.SMALLEST_SHARE_SIZE = 0.25
		self.order_monitor_executor = ThreadPoolExecutor(max_workers=20)
		

	def is_tf_relative_to_last_higher_tf(
		self,
		ticker,
		signal,
		timeframe,
		current_tf,
		higher_tf,
		relation="opposite",
		simulation=False,
		backtester=None,
		state=None
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

			simulation (Boolean): True if mode is simualtion and False if live.
			backtester (Backtester): Instance of class Backtester.
			state 

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
		current_tf_norm = self.tvw_helpers.normalize_tf(current_tf)

		if tf != current_tf_norm:
			return False

		sym = str(ticker or "").upper().strip()
		current_side = self.tvw_helpers.normalize_signal(signal)

		if current_side not in {"buy", "sell"}:
			return False

		current_alert = None
		if simulation:
			current_alert = backtester.get_nth_last_alert(state, sym, tf, 1)
		else:
			current_alert = self.tvw_helpers.get_nth_last_alert(sym, tf, 1)

		if current_alert is None:
			return False

		_, current_fields = current_alert

		if not self.is_confirmation_signal(current_fields):
			if not simulation:
				logger.info(
					"tf-relative check skipped: current %r signal for %r is not confirmation: signal=%r signal_role=%r",
					tf,
					sym,
					current_fields.get("signal"),
					current_fields.get("signal_role"),
				)
			return False

		latest_higher_signal = None

		if simulation:
			latest_higher_signal = backtester.get_latest_confirmation_directional_signal(
				state,
				sym,
				higher_tf,
				max_scan=500,
			)
		else:
			latest_higher_signal = self.get_latest_confirmation_directional_signal(
				sym,
				higher_tf,
				max_scan=500,
			)		

		if latest_higher_signal is None:
			return False

		last_side = latest_higher_signal["side"]

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
			if not self.is_confirmation_signal(fields):
				continue	
						
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
		simulation,
		backtester,
		state
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

			simulation (Boolean): True if mode is simualtion and False if live.
			backtester (Backtester): Instance of class Backtester.
			state 				

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

		current_alert = None

		if simulation:
			current_alert = backtester.get_nth_last_alert(state, sym, tf, 1)
		else:
			current_alert = self.tvw_helpers.get_nth_last_alert(sym, tf, 1)

		if current_alert is None:
			return False

		_, current_fields = current_alert

		if not self.is_confirmation_signal(current_fields):
			if not simulation:
				logger.info(
					"lower-tf confirms skipped: current %r signal for %r is not confirmation: signal=%r signal_role=%r",
					tf,
					sym,
					current_fields.get("signal"),
					current_fields.get("signal_role"),
				)
			return False

		last_mid_signal = None

		if simulation:
			last_mid_signal = backtester.get_latest_confirmation_directional_signal(
				state,
				sym,
				mid_tf_norm,
				max_scan=500,
			)
		else:
			last_mid_signal = self.get_latest_confirmation_directional_signal(
				sym,
				mid_tf_norm,
				max_scan=500,
			)

		if last_mid_signal is None:
			return False

		last_higher_signal = None

		if simulation:
			last_higher_signal = backtester.get_latest_confirmation_directional_signal(
				state,
				sym,
				higher_tf_norm,
				max_scan=500,
			)
		else:
			last_higher_signal = self.get_latest_confirmation_directional_signal(
				sym,
				higher_tf_norm,
				max_scan=500,
			)		

		if last_higher_signal is None:
			return False

		last_mid_side = last_mid_signal["side"]
		last_higher_side = last_higher_signal["side"]

		is_true = (
			(current_side == last_mid_side) and
			(last_mid_side != last_higher_side)
		)		

		if not simulation:
			logger.info(
				"lower-tf confirms %r vs %r: ticker=%r tf=%r current=%r last_mid=%r last_higher=%r result=%r",
				mid_tf_norm,
				higher_tf_norm,
				sym,
				tf,
				current_side,
				last_mid_side,
				last_higher_side,
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
		simulation=False,
		backtester=None,
		state=None
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

			simulation (Boolean): True if mode is simualtion and False if live.
			backtester (Backtester): Instance of class Backtester.
			state 				

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
			if not simulation:
				logger.info(
					"Invalid signal passed to has_opposite_signal_since_last_valid_same_side_higher_tf: %r",
					signal,
				)
			return False

		if not opposite_tf_norm or not anchor_tf_norm:
			if not simulation:
				logger.info(
					"Invalid timeframe configuration in has_opposite_signal_since_last_valid_same_side_higher_tf: opposite_tf=%r anchor_tf=%r",
					opposite_tf,
					anchor_tf,
				)
			return False

		latest_anchor_signal = None

		if simulation:
			latest_anchor_signal = backtester.get_latest_confirmation_directional_signal(
				state,
				sym,
				anchor_tf_norm,
				max_scan=max_scan_anchor_tf,
			)
		else:
			latest_anchor_signal = self.get_latest_confirmation_directional_signal(
				sym,
				anchor_tf_norm,
				max_scan=max_scan_anchor_tf,
			)		

		if not latest_anchor_signal:
			if not simulation:
				logger.info("No confirmation %r anchor found for %r", anchor_tf_norm, sym)
			return False

		if latest_anchor_signal["side"] != target_side:
			if not simulation:
				logger.info(
					"Latest confirmation %r anchor for %r is not same-side: target_side=%r anchor_side=%r",
					anchor_tf_norm,
					sym,
					target_side,
					latest_anchor_signal["side"],
				)
			return False

		anchor_time_str = latest_anchor_signal["fields"].get(
			"bar_close_time_eastern"
		)		

		try:
			anchor_time = datetime.fromisoformat(anchor_time_str)
		except Exception:
			if not simulation:
				logger.exception(
					"Failed parsing anchor %r time for %r: %r",
					anchor_tf_norm,
					sym,
					anchor_time_str,
				)
			return False

		opposite_side = "sell" if target_side == "buy" else "buy"

		if simulation:
			opposite_events = state.all_events_by_ticker_tf.get((sym, opposite_tf_norm), [])

			scanned = 0

			for event in reversed(opposite_events):
				if scanned >= max_scan_opposite_tf:
					break

				scanned += 1

				if event.get("signal_role") != "confirmation":
					continue

				entry_side = event.get("side")
				if entry_side != opposite_side:
					continue

				entry_time = event.get("dt")
				if entry_time is None:
					continue

				if entry_time > anchor_time:
					return True

			return False


		else:

			stream_key = self.tvw_helpers.stream_key(opposite_tf_norm, sym)

			try:
				entries = self.r.xrevrange(stream_key, count=max_scan_opposite_tf)
			except Exception:
				if not simulation:
					logger.exception("Failed reading %r stream for %r", opposite_tf_norm, sym)
				return False

			if not entries:
				if not simulation:
					logger.info("No %r entries found for %r", opposite_tf_norm, sym)
				return False

			for entry_id, fields in entries:

				if not self.is_confirmation_signal(fields):
					continue

				entry_side = self.tvw_helpers.normalize_signal(fields.get("signal"))
				if entry_side != opposite_side:
					continue

				entry_time_str = fields.get("bar_close_time_eastern")
				if not entry_time_str:
					continue

				try:
					entry_time = datetime.fromisoformat(entry_time_str)
				except Exception:
					if not simulation:
						logger.exception(
							"Failed parsing %r signal time for %r entry_id=%r value=%r",
							opposite_tf_norm,
							sym,
							entry_id,
							entry_time_str,
						)
					continue

				if entry_time > anchor_time:
					if not simulation:
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
		simulation=False,
		backtester=None,
		state=None
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

				simulation (Boolean): True if mode is simualtion and False if live.
				backtester (Backtester): Instance of class Backtester.
				state 					

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

		anchor_signal = None

		if simulation:
			anchor_signal = backtester.get_latest_confirmation_directional_signal(
				state,
				sym,
				anchor_tf_norm,
				max_scan=max_scan_anchor_tf,
			)
		else:
			anchor_signal = self.get_latest_confirmation_directional_signal(
				sym,
				anchor_tf_norm,
				max_scan=max_scan_anchor_tf,
			)

		if not anchor_signal:
			return 0.0

		if anchor_signal["side"] != target_side:
			return 0.0

		anchor_time_str = anchor_signal["fields"].get("bar_close_time_eastern")
		if not anchor_time_str:
			return 0.0

		try:
			anchor_time = datetime.fromisoformat(anchor_time_str)
		except Exception:
			if not simulation:
				logger.exception("Failed parsing anchor time for signal-based sizing: %r", anchor_time_str)
			return 0.0


		if simulation:
		    entries = reversed(
		        state.all_events_by_ticker_tf.get((sym, entry_tf_norm), [])
		    )

		    qualifying_count = 0
		    scanned = 0

		    for event in entries:
		        if scanned >= max_scan_entry_tf:
		            break
		        scanned += 1

		        if event.get("signal_role") != "confirmation":
		            continue

		        if event.get("side") != target_side:
		            continue

		        entry_time = event.get("dt")
		        if entry_time is None:
		            continue

		        if entry_time < anchor_time:
		            break

		        qualifying_count += 1

		else:

			stream_key = self.tvw_helpers.stream_key(entry_tf_norm, sym)

			try:
				entries = self.r.xrevrange(stream_key, count=max_scan_entry_tf)
			except Exception:
				if not simulation:
					logger.exception("Failed reading %r stream for signal-based sizing: %r", entry_tf_norm, sym)
				return 0.0

			qualifying_count = 0

			for entry_id, fields in entries:

				if not self.is_confirmation_signal(fields):
					continue

				entry_side = self.tvw_helpers.normalize_signal(fields.get("signal"))

				if entry_side not in {"buy", "sell"}:
					continue

				entry_time_str = fields.get("bar_close_time_eastern")
				if not entry_time_str:
					continue

				try:
					entry_time = datetime.fromisoformat(entry_time_str)
				except Exception:
					if not simulation:
						logger.exception(
							"Failed parsing entry signal time for %r entry_id=%r value=%r",
							sym,
							entry_id,
							entry_time_str,
						)
					continue

				if entry_time < anchor_time:
					break		

				if entry_side != target_side: # Skip over opposite side entry tf signals. We only want same sides.
					continue					

				# At this point, signal is qualifying because it is same side to anchor and occurred later than anchor
				qualifying_count += 1

		if qualifying_count < 1:
			return 0.0

		execution_qty = base_num_shares / (2 ** (qualifying_count - 1))

		if target_side == "sell":
			execution_qty = math.floor(execution_qty)

		if execution_qty < smallest_share_size:
			return 0.0

		if not simulation:
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


	def is_latest_anchor_opposite_of_open_position(self, ticker, anchor_tf, alpaca_position_qty, simulation, backtester, state):
		"""
		Return True if the latest anchor timeframe signal is opposite
		of the currently open Alpaca position side.
		
		Parameters:
			ticker (str):Ticker symbol, e.g. "AAPL".
			anchor_tf (str): The higher timeframe anchor used to establish the current directional regime.
			alpaca_position_qty (float):
			simulation (Boolean): True if mode is simualtion and False if live.
			backtester (Backtester): Instance of class Backtester.
			state 					
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

		latest_anchor_signal = None

		if simulation:
			latest_anchor_signal = backtester.get_latest_confirmation_directional_signal(
				state,
				ticker,
				anchor_tf,
				max_scan=500,
			)
		else:
			latest_anchor_signal = self.get_latest_confirmation_directional_signal(
				ticker,
				anchor_tf,
				max_scan=500,
			)

		if latest_anchor_signal is None:
			return False

		last_anchor_signal = latest_anchor_signal["side"]

		is_true = last_anchor_signal == opposite_anchor_side

		if not simulation:
			logger.info(
				"anchor-position exit check: ticker=%r anchor_tf=%r position_side=%r latest_anchor_signal=%r result=%r",
				ticker,
				anchor_tf,
				position_side,
				last_anchor_signal,
				is_true,
			)

		return is_true			


	def _monitor_alpaca_order_fill(
		self,
		strategy_name,
		ticker,
		date,
		alpaca_api,
		order_id,
		order_type,
		execution_qty,
		fallback_price,
		do_redis_bookkeeping,
		pending_exit_key=None,
		timeout_seconds=90,
		poll_interval=1.0,
	):
		"""
		Monitors a submitted Alpaca order asynchronously until it either fills,
		reaches a terminal failure state, or times out.

		This function is intended to run in a background thread so that webhook
		and strategy-processing execution paths remain non-blocking. It repeatedly
		polls Alpaca for order status updates, optionally synchronizes confirmed
		fills back into Redis bookkeeping, and cancels stale unfilled orders after
		the timeout window expires.

		Execution flow:
			1. Poll Alpaca order status repeatedly.
			2. If order fills:
				- retrieve updated Alpaca position state
				- optionally synchronize fill into Redis
				- stop monitoring
			3. If order reaches a terminal failure state:
				- stop monitoring
			4. If timeout expires before fill:
				- cancel the order
				- stop monitoring
			5. Clear any pending-exit guard keys.

		Parameters:
			strategy_name (str):
				Strategy associated with the order.

			ticker (str):
				Ticker symbol (e.g. "AMD").

			date (str):
				Order submission timestamp.

			alpaca_api (REST):
				Authenticated Alpaca REST client.

			order_id (str):
				Alpaca order identifier.

			order_type (str):
				One of:
					- "long"
					- "short"
					- "sell"
					- "cover"

			execution_qty (float):
				Expected execution quantity.

			fallback_price (float):
				Price used if Alpaca does not provide a filled average price.

			do_redis_bookkeeping (bool):
				If True, synchronize confirmed fills into Redis using
				record_live_alpaca_fill().

			pending_exit_key (Optional[str]):
				Redis key used to prevent duplicate exit submissions while
				an exit order is still pending.

			timeout_seconds (int):
				Maximum number of seconds to wait for a fill before canceling
				the order.

			poll_interval (float):
				Seconds between Alpaca order-status checks.

		Returns:
			None

		Behavior:
			- Designed for asynchronous execution via ThreadPoolExecutor.
			- Does not block the webhook request path.
			- Intended to improve after-hours execution handling where fills
			  may take significantly longer than during regular market hours.
		"""	
		try:
			accepted_at = None
			hard_deadline = time.time() + max(timeout_seconds * 4, 90)
			terminal_bad_statuses = {
				"canceled",
				"expired",
				"rejected",
				"suspended",
				"stopped",
			}

			while time.time() < hard_deadline:
				try:
					order = alpaca_api.get_order(order_id)
					status = str(getattr(order, "status", "") or "").lower()

					logger.info(
						"Background Alpaca order status check: ticker=%r order_id=%r status=%r filled_qty=%r avg_fill_price=%r",
						ticker,
						order_id,
						status,
						getattr(order, "filled_qty", None),
						getattr(order, "filled_avg_price", None),
					)

					accepted_statuses = {
						"accepted",
						"new",
						"pending_new",
						"partially_filled",
						"pending_replace",
						"pending_cancel",
					}

					if accepted_at is None and status in accepted_statuses:
						accepted_at = time.time()
						logger.info(
							"Background Alpaca order accepted/submitted; starting fill timeout: strategy=%r ticker=%r order_type=%r order_id=%r status=%r timeout_seconds=%r",
							strategy_name,
							ticker,
							order_type,
							order_id,
							status,
							timeout_seconds,
						)						

					if status == "filled":
						fill_price = getattr(order, "filled_avg_price", None) or fallback_price

						time.sleep(0.5)

						alpaca_position_qty_after_fill = 0.0
						alpaca_avg_entry_price_after_fill = None

						try:
							position = alpaca_api.get_position(ticker)
							alpaca_position_qty_after_fill = float(getattr(position, "qty", 0.0) or 0.0)
							alpaca_avg_entry_price_after_fill = getattr(position, "avg_entry_price", None)

							logger.info(
								"Background post-fill Alpaca position check: ticker=%r qty=%r side=%r avg_entry_price=%r",
								ticker,
								getattr(position, "qty", None),
								getattr(position, "side", None),
								alpaca_avg_entry_price_after_fill,
							)
						except Exception:
							logger.info(
								"No Alpaca position found after background fill for ticker=%r order_type=%r; treating position as flat",
								ticker,
								order_type,
							)

						actual_filled_qty = float(
						    getattr(order, "filled_qty", execution_qty) or execution_qty
						)							

						return

					if status in terminal_bad_statuses:
						logger.info(
							"Background Alpaca order reached terminal failure: strategy=%r ticker=%r order_type=%r order_id=%r status=%r",
							strategy_name,
							ticker,
							order_type,
							order_id,
							status,
						)
						return

					if accepted_at is not None and (time.time() - accepted_at) >= timeout_seconds:
						logger.warning(
							"Background Alpaca order not filled after accepted/submitted timeout: strategy=%r ticker=%r order_type=%r order_id=%r status=%r timeout_seconds=%r",
							strategy_name,
							ticker,
							order_type,
							order_id,
							status,
							timeout_seconds,
						)

						try:
							alpaca_api.cancel_order(order_id)
							logger.info(
								"Background canceled unfilled Alpaca order after accepted/submitted timeout: ticker=%r order_id=%r",
								ticker,
								order_id,
							)
						except Exception:
							logger.exception(
								"Background failed canceling unfilled Alpaca order after accepted/submitted timeout: ticker=%r order_id=%r",
								ticker,
								order_id,
							)

						return						

				except Exception:
					logger.exception("Background failed polling Alpaca order status: ticker=%r order_id=%r", ticker, order_id)

				time.sleep(poll_interval)

			logger.warning(
				"Background Alpaca order monitor reached hard deadline before order became accepted/submitted: strategy=%r ticker=%r order_type=%r order_id=%r timeout_seconds=%r",
				strategy_name,
				ticker,
				order_type,
				order_id,
				timeout_seconds,
			)

			try:
				alpaca_api.cancel_order(order_id)
				logger.info(
					"Background canceled unfilled Alpaca order after timeout: ticker=%r order_id=%r",
					ticker,
					order_id,
				)
			except Exception:
				logger.exception(
					"Background failed canceling unfilled Alpaca order after timeout: ticker=%r order_id=%r",
					ticker,
					order_id,
				)

		finally:
			if pending_exit_key:
				try:
					self.r.delete(pending_exit_key)
				except Exception:
					logger.exception("Failed clearing pending exit guard in background: key=%r", pending_exit_key)


	def get_latest_directional_signal(self, ticker, timeframe, signal_role, max_scan=100):
		"""
		Retrieve the most recent directional trading signal from a Redis stream, filtered by signal role.

		Scans the stream in reverse chronological order and returns the latest
		directional signal entry whose normalized signal is either "buy" or "sell".

		Exit signals such as "bullish_exit" / "bearish_exit", unknown values,
		and non-directional events are ignored.

		This helper is primarily used by exit logic to determine whether an
		intermediary timeframe exit signal should liquidate a long or short
		position.

		Parameters:
			ticker (str):
				Ticker symbol to inspect.

			timeframe (str):
				Timeframe stream to inspect (ex: "5m", "15m", "1h").

			signal_role (str):
				confirmation or contrarian

			max_scan (int):
				Maximum number of recent stream entries to scan backwards.
				Default is 100.

		Returns:
			dict | None:
				Returns a dictionary containing:
					{
						"id": <redis stream entry id>,
						"side": "buy" | "sell",
						"fields": <raw redis stream fields>,
					}

				Returns None if no directional signal is found.
		"""
		sym = str(ticker or "").upper().strip()
		tf = self.tvw_helpers.normalize_tf(timeframe)
		expected_signal_role = str(signal_role or "").strip().lower()
		stream_key = self.tvw_helpers.stream_key(tf, sym)

		try:
			entries = self.r.xrevrange(stream_key, count=max_scan)
		except Exception:
			logger.exception(
				"Failed reading latest directional signal: ticker=%r tf=%r signal_role=%r",
				sym,
				tf,
				expected_signal_role,
			)
			return None

		for entry_id, fields in entries:
			entry_signal_role = str(fields.get("signal_role") or "").strip().lower()

			if expected_signal_role and entry_signal_role != expected_signal_role:
				continue

			side = self.tvw_helpers.normalize_signal(fields.get("signal"))

			if side in {"buy", "sell"}:
				return {
					"id": entry_id,
					"side": side,
					"signal_role": entry_signal_role,
					"fields": fields,
				}

		return None		


	def is_confirmation_signal(self, fields):
		"""
		Determine whether a Redis stream/state entry represents
		a confirmation signal.

		Checks the "signal_role" field and returns True only when
		the normalized role equals "confirmation".

		Parameters:
			fields (dict):
				Redis stream/state fields associated with a TradingView alert.

		Returns:
			bool:
				True if the signal_role is "confirmation",
				otherwise False.
		"""		
		return str(fields.get("signal_role") or "").strip().lower() == "confirmation"


	def latest_tf_signal_is_confirmation(self, ticker, timeframe, simulation, backtester, state):
		"""
		Check whether the latest alert for a timeframe is a
		confirmation signal.

		Retrieves the most recent Redis stream entry for the specified
		ticker/timeframe and evaluates whether its signal_role is
		"confirmation".

		This helper is primarily used by strategy validation logic to
		quickly determine whether the latest timeframe context is based
		on confirmation signals rather than contrarian or unknown signals.

		Parameters:
			ticker (str):
				Ticker symbol to inspect.

			timeframe (str):
				Timeframe stream to inspect (ex: "1m", "5m", "15m", "1h").

			simulation (Boolean): True if mode is simualtion and False if live.
			backtester (Backtester): Instance of class Backtester.
			state

		Returns:
			bool:
				True if the latest alert exists and is a confirmation signal.
				False otherwise.
		"""		
		last_alert = None

		if simulation:
			last_alert = backtester.get_nth_last_alert(state, ticker, timeframe, 1)
		else:
			last_alert = self.tvw_helpers.get_nth_last_alert(ticker, timeframe, 1)

		if last_alert is None:
			return False

		_, fields = last_alert
		return self.is_confirmation_signal(fields)
			

	def get_latest_confirmation_directional_signal(self, ticker, timeframe, max_scan=500):
		"""
		Retrieve the most recent confirmation-based directional signal
		from a Redis stream.

		Scans the specified timeframe stream in reverse chronological order
		and returns the latest directional signal whose:

			1. signal_role == "confirmation"
			2. normalized signal is either "buy" or "sell"

		Non-confirmation signals such as:
			- contrarian
			- unknown
			- exit signals
			- malformed signals

		are ignored.

		This helper is primarily used by strategy logic to ensure that
		entry/exit decisions are based only on confirmation regime signals,
		even if newer contrarian or unknown signals exist in the stream.

		Parameters:
			ticker (str):
				Ticker symbol to inspect.

			timeframe (str):
				Timeframe stream to inspect (ex: "1m", "5m", "15m", "1h").

			max_scan (int):
				Maximum number of recent Redis stream entries to scan
				backwards before giving up.
				Default is 500.

		Returns:
			dict | None:
				Returns a dictionary containing:

					{
						"id": <redis stream entry id>,
						"side": "buy" | "sell",
						"fields": <raw redis stream fields>,
					}

				Returns None if no qualifying confirmation directional
				signal is found.
		"""		
		sym = str(ticker or "").upper().strip()
		tf = self.tvw_helpers.normalize_tf(timeframe)
		stream_key = self.tvw_helpers.stream_key(tf, sym)

		try:
			entries = self.r.xrevrange(stream_key, count=max_scan)
		except Exception:
			#logger.exception(
				#"Failed reading latest confirmation directional signal: ticker=%r tf=%r",
				#sym,
				#tf,
			#)
			return None

		for entry_id, fields in entries:
			signal_role = str(fields.get("signal_role") or "").strip().lower()
			if signal_role != "confirmation":
				continue

			side = self.tvw_helpers.normalize_signal(fields.get("signal"))
			if side in {"buy", "sell"}:
				return {
					"id": entry_id,
					"side": side,
					"fields": fields,
				}

		return None			


	def _tf_rank(self, timeframe: str) -> int:
		tf = self.tvw_helpers.normalize_tf(timeframe)
		order = {
			"1m": 1,
			"3m": 2,
			"5m": 3,
			"15m": 4,
			"30m": 5,
			"45m": 6,
			"1h": 7,
			"2h": 8,
			"4h": 9,
			"1d": 10,
		}
		return order.get(tf, -1)


	def entry_strategy1(self, strategy_name, entry_tf, intermediary_tf, anchor_tf, simulation, date, signal, prices, ticker, timeframe, NUM_SHARES, alpaca_api, state, config, event, price, backtester):
		"""
		Strategy relies on latest signals of three different timeframes; an anchor timeframe (highest timeframe), an entry timeframe (lowerst timeframe)
		and an intermediary timeframe. A trade is taken upon the entry timeframe, if the latest anchor timeframe is the same side as the
		entry timeframe, and if there is no intermediary timeframe signal of the opposite side between the anchor and the entry. 

		Parameters:
			strategy_name (str): Strategy name.
			entry_tf (str): Entry timeframe (lowest timeframe). 
			intermediary_tf (str): intermediary timeframe.
			anchor_tf (str): Anchor timeframe (hihgest timeframe)
			simulation (bool): True for simulation and False for live mode.
			date (str): Eastern time.
			signal (str): "buy", "sell", "buy+" or "sell+".
			prices (dict): Market, ask, and bid prices for ticker symbol.
			ticker (str): Ticker symbol.
			timeframe (str): Timeframe of signal.
			NUM_SHARES (float): Number of shares to be traded.
			alpaca_api
			The following params only apply to simulation mode. For live, they will have values of None.
			state
			config
			event
			price (float): Current market price of ticker
			backtester (Backtester): Instance of backtester class
		Returns:
			place_long_order() or place_short_order() or None
		"""
		tf = self.tvw_helpers.normalize_tf(timeframe)
		if tf != entry_tf:
			return None

		current_entry_tf_alert = None
		if simulation:
			current_entry_tf_alert = backtester.get_nth_last_alert(state, ticker, tf, 1)
		else:
			current_entry_tf_alert = self.tvw_helpers.get_nth_last_alert(ticker, tf, 1)		

		if current_entry_tf_alert is None:
			if not simulation:
				logger.info("Entry skipped: missing current %r alert for %r", tf, ticker)
			return None

		_, current_entry_tf_fields = current_entry_tf_alert

		current_entry_tf_signal_role = str(
			current_entry_tf_fields.get("signal_role") or ""
		).strip().lower()

		if current_entry_tf_signal_role != "confirmation":
			if not simulation:
				logger.info(
					"Entry skipped: current entry_tf alert is not confirmation for %r tf=%r signal=%r signal_role=%r",
					ticker,
					tf,
					current_entry_tf_fields.get("signal"),
					current_entry_tf_signal_role,
				)
			return None

		last_entry_tf_alert = current_entry_tf_alert

		last_anchor_tf_alert = None

		if simulation:
			last_anchor_tf_alert = backtester.get_latest_confirmation_directional_signal(
				state,
				ticker,
				anchor_tf,
				max_scan=500,
			)
		else:
			last_anchor_tf_alert = self.get_latest_confirmation_directional_signal(
				ticker,
				anchor_tf,
				max_scan=500,
			)		

		if last_entry_tf_alert is None or last_anchor_tf_alert is None:
			if not simulation:
				logger.info("Strategy skipped: missing alert context for %r", ticker)
			return None

		_, last_entry_tf_fields = last_entry_tf_alert
		last_anchor_tf_fields = last_anchor_tf_alert["fields"]

		last_entry_tf_signal = self.tvw_helpers.normalize_signal(last_entry_tf_fields.get("signal"))
		last_anchor_tf_signal = self.tvw_helpers.normalize_signal(last_anchor_tf_fields.get("signal"))

		if last_entry_tf_signal not in {"buy", "sell"} or last_anchor_tf_signal not in {"buy", "sell"}:
			if not simulation:
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
				ticker, "buy", intermediary_tf, anchor_tf, 1000, 500, simulation, backtester, state
			):
				if not simulation:
					logger.info("Blocked Strategy %r long entry for %r due to opposite %r after anchor %r", strategy_name, ticker, intermediary_tf, anchor_tf)
				return None

			num_shares = self.get_signal_based_progressive_entry_size(
				ticker=ticker,
				side=signal,
				entry_tf=entry_tf,
				anchor_tf=anchor_tf,
				base_num_shares=NUM_SHARES,
				smallest_share_size=self.SMALLEST_SHARE_SIZE,
				simulation=simulation,
				backtester=backtester,
				state=state
			)

			if num_shares <= 0:
				if not simulation:
					logger.info(
						"Strategy skipped: signal-based progressive size is zero for %r strategy=%r signal=%r entry_tf=%r anchor_tf=%r",
						ticker,
						strategy_name,
						signal,
						entry_tf,
						anchor_tf,
					)
				return None		
							
			if simulation:
				return backtester._open_or_add_position(state, event, "short", num_shares) #ACTUALLY LONG
			else:
				return self.place_long_order(strategy_name, timeframe, ticker, date, prices, num_shares, alpaca_api)

		if last_anchor_tf_signal == "sell" and last_entry_tf_signal == "sell":
			if self.has_opposite_signal_since_last_valid_same_side_higher_tf(
				ticker, "sell", intermediary_tf, anchor_tf, 1000, 500, simulation, backtester, state
			):
				if not simulation:
					logger.info("Blocked Strategy %r short entry for %r due to opposite %r after anchor %r", strategy_name, ticker, intermediary_tf, anchor_tf)
				return None

			num_shares = self.get_signal_based_progressive_entry_size(
				ticker=ticker,
				side=signal,
				entry_tf=entry_tf,
				anchor_tf=anchor_tf,
				base_num_shares=NUM_SHARES,
				smallest_share_size=self.SMALLEST_SHARE_SIZE,
				simulation=simulation,
				backtester=backtester,
				state=state
			)

			if num_shares <= 0:
				if not simulation:
					logger.info(
						"Strategy skipped: signal-based progressive size is zero for %r strategy=%r signal=%r entry_tf=%r anchor_tf=%r",
						ticker,
						strategy_name,
						signal,
						entry_tf,
						anchor_tf,
					)
				return None	

			if simulation:
				return backtester._open_or_add_position(state, event, "long", num_shares) #ACTUALLY SHORT
			else:
				return self.place_short_order(strategy_name, timeframe, ticker, date, prices, num_shares, alpaca_api)

		if not simulation:
			logger.info(
				"No trade condition met for %r | entry_tf=%r entry_signal=%r anchor_tf=%r anchor_signal=%r",
				ticker,
				entry_tf,
				last_entry_tf_signal,
				anchor_tf,
				last_anchor_tf_signal,
			)		

		return None	
	
	def exit_strategy1(self, strategy_name, lower_timeframes, intermediary_tf, anchor_tf, simulation, date, signal, prices, ticker, timeframe, alpaca_api, state, config, event, price, backtester):
		"""
		Exit if the current intermediary timeframe signal is opposite of the latest
		anchor timeframe signal, if a lower timeframe confirms the intermediary
		timeframe against the anchor, if the anchor opposes the open position, or if
		an intermediary exit signal matches the actual open position.

		The live Alpaca position and simulated in-memory position are normalized into
		the same local fields so diagnostics are directly comparable:
			- position_side: "long" or "short"
			- position_qty: absolute share quantity
			- position_avg_price: current average entry price
		"""
		mode = "sim" if simulation else "live"

		exit_timeframes = lower_timeframes | {intermediary_tf}
		tf = self.tvw_helpers.normalize_tf(timeframe)
		if tf not in exit_timeframes:
			return None

		alpaca_position = None
		alpaca_position_qty = 0.0

		position_side = "flat"
		position_qty = 0.0
		position_avg_price = None

		if simulation:
			sim_position = state.positions.get(ticker)

			# No simulated position exists, so there is nothing to exit.
			if sim_position is None or sim_position.num_shares <= 0:
				return None

			position_side = sim_position.side
			position_qty = float(sim_position.num_shares)
			position_avg_price = float(sim_position.avg_price_per_share)

		else:
			try:
				alpaca_position = alpaca_api.get_position(ticker)
				alpaca_position_qty = float(
					getattr(alpaca_position, "qty", 0.0) or 0.0
				)
			except Exception:
				return None

			if alpaca_position_qty == 0:
				return None

			position_side = "long" if alpaca_position_qty > 0 else "short"
			position_qty = abs(alpaca_position_qty)

			raw_avg_price = getattr(alpaca_position, "avg_entry_price", None)
			try:
				position_avg_price = (
					float(raw_avg_price)
					if raw_avg_price is not None
					else None
				)
			except (TypeError, ValueError):
				position_avg_price = None

		# Preserve sign because is_latest_anchor_opposite_of_open_position()
		# derives long/short from whether the quantity is positive or negative.
		position_qty_for_anchor_check = (
			position_qty
			if position_side == "long"
			else -position_qty
		)

		logger.info(
			"%r: exit check: "
			"date=%r strategy=%r intermediary_tf=%r anchor_tf=%r "
			"ticker=%r timeframe=%r raw_signal=%r normalized_signal=%r "
			"position_side=%r position_qty=%r position_avg_price=%r",
			mode,
			date,
			strategy_name,
			intermediary_tf,
			anchor_tf,
			ticker,
			timeframe,
			signal,
			self.tvw_helpers.normalize_signal(signal),
			position_side,
			position_qty,
			position_avg_price,
		)

		# EXIT CONDITIONS
		is_intermediary_tf_opposite_of_last_anchor_tf = (
			self.is_tf_relative_to_last_higher_tf(
				ticker,
				signal,
				timeframe,
				intermediary_tf,
				anchor_tf,
				"opposite",
				simulation,
				backtester,
				state,
			)
		)

		lower_tf_confirms_intermediary_opposite_of_anchor = (
			self.lower_tf_confirms_mid_tf_opposite_of_higher_tf(
				ticker,
				signal,
				timeframe,
				lower_timeframes,
				intermediary_tf,
				anchor_tf,
				simulation,
				backtester,
				state,
			)
		)

		anchor_opposite_open_position = (
			self.is_latest_anchor_opposite_of_open_position(
				ticker,
				anchor_tf,
				position_qty_for_anchor_check,
				simulation,
				backtester,
				state,
			)
		)

		# Exit signal roles are unknown. Determine whether an exit signal qualifies
		# from the latest confirmation direction on the intermediary timeframe.
		is_intermediary_tf_exit_signal = (
			tf == self.tvw_helpers.normalize_tf(intermediary_tf)
			and signal in {"bullish_exit", "bearish_exit"}
		)

		exit_signal_matches_open_position = False
		latest_intermediary_direction = None

		if is_intermediary_tf_exit_signal:
			if simulation:
				latest_directional_alert = backtester.get_latest_directional_signal(
					state,
					ticker,
					intermediary_tf,
					"confirmation",
					max_scan=100,
				)
			else:
				latest_directional_alert = self.get_latest_directional_signal(
					ticker,
					intermediary_tf,
					"confirmation",
					max_scan=100,
				)

			if latest_directional_alert:
				latest_intermediary_direction = latest_directional_alert["side"]

				exit_signal_matches_open_position = (
					(
						position_side == "long"
						and latest_intermediary_direction == "buy"
						and signal == "bullish_exit"
					)
					or
					(
						position_side == "short"
						and latest_intermediary_direction == "sell"
						and signal == "bearish_exit"
					)
				)

		should_exit = (
			is_intermediary_tf_opposite_of_last_anchor_tf
			or lower_tf_confirms_intermediary_opposite_of_anchor
			or anchor_opposite_open_position
			or exit_signal_matches_open_position
		)

		logger.info(
			"%r: exit checks: "
			"date=%r strategy=%r ticker=%r timeframe=%r "
			"position_side=%r position_qty=%r position_avg_price=%r "
			"intermediary_opp_anchor=%r lower_confirms=%r "
			"anchor_opp_position=%r intermediary_exit_signal=%r "
			"latest_intermediary_direction=%r "
			"exit_matches_position=%r should_exit=%r",
			mode,
			date,
			strategy_name,
			ticker,
			timeframe,
			position_side,
			position_qty,
			position_avg_price,
			is_intermediary_tf_opposite_of_last_anchor_tf,
			lower_tf_confirms_intermediary_opposite_of_anchor,
			anchor_opposite_open_position,
			is_intermediary_tf_exit_signal,
			latest_intermediary_direction,
			exit_signal_matches_open_position,
			should_exit,
		)

		if not should_exit:
			return None

		if simulation:
			latest_intermediary_tf_signal = backtester.get_latest_directional_signal(
				state,
				ticker,
				intermediary_tf,
				"confirmation",
				max_scan=100,
			)
		else:
			latest_intermediary_tf_signal = self.get_latest_directional_signal(
				ticker,
				intermediary_tf,
				"confirmation",
				max_scan=100,
			)

		if latest_intermediary_tf_signal is None:
			logger.info(
				"%r: date=%r No confirmation %r directional signal found for %r "
				"position_side=%r position_qty=%r",
				mode,
				date,
				intermediary_tf,
				ticker,
				position_side,
				position_qty,
			)
			return None

		signal_intermediary_tf = latest_intermediary_tf_signal["side"]

		logger.info(
			"%r: date=%r exit_strategy1 signal context: "
			"ticker=%r intermediary_tf_signal=%r intermediary_signal_role=%r "
			"position_side=%r position_qty=%r position_avg_price=%r",
			mode,
			date,
			ticker,
			signal_intermediary_tf,
			latest_intermediary_tf_signal.get("signal_role"),
			position_side,
			position_qty,
			position_avg_price,
		)

		if signal_intermediary_tf not in {"buy", "sell"}:
			logger.info(
				"%r: date=%r Latest confirmation intermediary_tf signal "
				"is invalid/unknown for %r: %r "
				"position_side=%r position_qty=%r",
				mode,
				date,
				ticker,
				signal_intermediary_tf,
				position_side,
				position_qty,
			)
			return None

		# At this point, should_exit is already True. Liquidation is based on the
		# actual open position side, not the latest intermediary signal side.
		if simulation:
			return backtester._close_position(state, event)

		alpaca_num_shares = position_qty

		if position_side == "short" and alpaca_num_shares > 0:
			logger.info(
				"exit %r Alpaca cover for %r using alpaca_num_shares=%r "
				"position_side=%r position_avg_price=%r",
				strategy_name,
				ticker,
				alpaca_num_shares,
				position_side,
				position_avg_price,
			)
			return self.cover_short_order(
				strategy_name,
				timeframe,
				ticker,
				date,
				prices,
				alpaca_num_shares,
				alpaca_api,
				None,
				do_redis_bookkeeping=False,
			)

		if position_side == "long" and alpaca_num_shares > 0:
			logger.info(
				"exit %r Alpaca sell for %r using alpaca_num_shares=%r "
				"position_side=%r position_avg_price=%r",
				strategy_name,
				ticker,
				alpaca_num_shares,
				position_side,
				position_avg_price,
			)
			return self.sell_long_order(
				strategy_name,
				timeframe,
				ticker,
				date,
				prices,
				alpaca_num_shares,
				alpaca_api,
				None,
				do_redis_bookkeeping=False,
			)

		logger.info(
			"exit %r no Alpaca position to liquidate for %r; "
			"position_side=%r position_qty=%r raw_alpaca_qty=%r",
			strategy_name,
			ticker,
			position_side,
			position_qty,
			alpaca_position_qty,
		)
		return None


	def entry_strategy2(self, strategy_name, entry_tf, intermediary_tf, simulation, date, signal, prices, ticker, timeframe, NUM_SHARES, alpaca_api, state, config, event, price, backtester):
		tf = self.tvw_helpers.normalize_tf(timeframe)
		entry_tf = self.tvw_helpers.normalize_tf(entry_tf)

		if tf != entry_tf:
			return None

		side = self.tvw_helpers.normalize_signal(signal)
		if side not in {"buy", "sell"}:
			return None

		current_alert = backtester.get_nth_last_alert(state, ticker, tf, 1) if simulation else self.tvw_helpers.get_nth_last_alert(ticker, tf, 1)
		if current_alert is None:
			return None

		_, fields = current_alert
		if not self.is_confirmation_signal(fields):
			return None

		last_intermediary = (
			backtester.get_latest_confirmation_directional_signal(state, ticker, intermediary_tf, max_scan=500)
			if simulation
			else self.get_latest_confirmation_directional_signal(ticker, intermediary_tf, max_scan=500)
		)

		if not last_intermediary or last_intermediary["side"] != side:
			return None

		position_side = "long" if side == "buy" else "short"
		if simulation:#	CONTRARIAN EXPERIMENT. DELETE LATER
			position_side = "long" if side == "sell" else "short"

		if simulation:
			return backtester._open_or_add_position(state, event, position_side, NUM_SHARES)

		if side == "buy":
			return self.place_long_order(strategy_name, timeframe, ticker, date, prices, NUM_SHARES, alpaca_api)

		return self.place_short_order(strategy_name, timeframe, ticker, date, prices, NUM_SHARES, alpaca_api)


	def exit_strategy2(self, strategy_name, entry_tf, simulation, date, signal, prices, ticker, timeframe, alpaca_api, state, config, event, price, backtester):
		tf = self.tvw_helpers.normalize_tf(timeframe)
		entry_tf = self.tvw_helpers.normalize_tf(entry_tf)

		if self._tf_rank(tf) < self._tf_rank(entry_tf):
			return None

		side = self.tvw_helpers.normalize_signal(signal)
		is_exit_signal = str(signal or "").strip().lower() in {"bullish_exit", "bearish_exit"}
		is_confirmation_opposite = False

		if simulation:
			position = state.positions.get(ticker)
			if not position or position.num_shares <= 0:
				return None

			position_side = position.side
			open_qty = position.num_shares
		else:
			try:
				alpaca_position = alpaca_api.get_position(ticker)
				alpaca_qty = float(getattr(alpaca_position, "qty", 0.0) or 0.0)
			except Exception:
				return None

			if alpaca_qty == 0:
				return None

			position_side = "long" if alpaca_qty > 0 else "short"
			open_qty = abs(alpaca_qty)

		if side in {"buy", "sell"}:
			current_alert = backtester.get_nth_last_alert(state, ticker, tf, 1) if simulation else self.tvw_helpers.get_nth_last_alert(ticker, tf, 1)
			if current_alert is not None:
				_, fields = current_alert
				if self.is_confirmation_signal(fields):
					is_confirmation_opposite = (
						(position_side == "long" and side == "sell") or
						(position_side == "short" and side == "buy")
					)

		should_exit = is_exit_signal or is_confirmation_opposite
		if not should_exit:
			return None

		close_qty = open_qty if open_qty < 1 else open_qty / 2.0

		if simulation:
			return backtester._close_partial_position(state, event, close_qty)

		if position_side == "long":
			return self.sell_long_order(
				strategy_name, timeframe, ticker, date, prices, close_qty, alpaca_api, None, do_redis_bookkeeping=False
			)

		return self.cover_short_order(
			strategy_name, timeframe, ticker, date, prices, close_qty, alpaca_api, None, do_redis_bookkeeping=False
		)


	def exit_strategy3(self, strategy_name, anchor_tf, simulation, date, prices, ticker, timeframe, alpaca_api, state, config, event, price, backtester):
		"""
		Manage an entry-time anchor-ATR trailing-stop exit.

		Simulation:
			The BackTester owns minute-by-minute price tracking, high/low-water
			updates, and stop execution. This method does not inspect signals or
			close simulated positions.

		Live:
			During regular trading hours, ensure an Alpaca trailing-stop order
			exists for the current position. Outside regular trading hours, no
			trailing-stop order is submitted because extended-hours trailing-stop
			orders are not used. Signal-triggered exits fall back to the existing
			limit-order execution path.
		"""
		if simulation:
			return None

		sym = str(ticker or "").upper().strip()

		try:
			alpaca_position = alpaca_api.get_position(
				sym
			)

			raw_position_qty = float(
				getattr(
					alpaca_position,
					"qty",
					0.0,
				)
				or 0.0
			)

		except Exception:
			return None

		if raw_position_qty == 0:
			return None

		position_side = (
			"long"
			if raw_position_qty > 0
			else "short"
		)

		position_qty = abs(
			raw_position_qty
		)

		if not self.tvw_helpers._is_regular_hours_et(date):
			return None

		anchor_atr = self.trade_records._get_live_anchor_atr_placeholder(
			strategy_name=strategy_name,
			ticker=sym,
			anchor_tf=anchor_tf,
			price=price,
		)

		if anchor_atr is None or anchor_atr <= 0:
			logger.info(
				"Unable to create ATR trailing stop: "
				"strategy=%r ticker=%r anchor_tf=%r "
				"anchor_atr=%r",
				strategy_name,
				sym,
				anchor_tf,
				anchor_atr,
			)
			return None

		if position_side == "long":
			return self.sell_long_order(
				strategy_name, timeframe, ticker, date, prices, position_qty, alpaca_api, anchor_atr, do_redis_bookkeeping=False
			)

		return self.cover_short_order(
			strategy_name, timeframe, ticker, date, prices, position_qty, alpaca_api, anchor_atr, do_redis_bookkeeping=False
		)



	def place_order(
		self,
		strategy_name,
		tf,
		ticker,
		date,
		prices,
		num_shares,
		alpaca_api,
		order_type,
		anchor_atr,
		do_redis_bookkeeping=False,
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
			ask = prices.get(ticker, {}).get("ask")
			bid = prices.get(ticker, {}).get("bid")

			if ask is None or bid is None:
				logger.info(
					"Missing bid/ask for off-hours %r order_type=%r ask=%r bid=%r",
					ticker,
					order_type,
					ask,
					bid,
				)
				return None

			ask = float(ask)
			bid = float(bid)
			spread = max(ask - bid, 0.01)

			is_exit_order = order_type in {"sell", "cover"}

			if is_exit_order: # Dynamically compute buffer to be half spread or 5c whichever is more.
				buffer = max(0.05, spread * 0.50) # Exit attempts should be more aggressive.
			else:
				buffer = max(0.02, spread * 0.25) # Entry attempts more conservative.

			if order_type in {"long", "cover"}:
				price = ask + buffer
			elif order_type in {"short", "sell"}:
				price = bid - buffer

			price = float(
				Decimal(str(price)).quantize(
					Decimal("0.01"),
					rounding=ROUND_HALF_UP,
				)
			)

			logger.info(
				"Rounded off-hours dynamic limit price: ticker=%r order_type=%r ask=%r bid=%r spread=%r buffer=%r limit_price=%r",
				ticker,
				order_type,
				ask,
				bid,
				spread,
				buffer,
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

		def submit_to_alpaca_only():
			#order_type_name = "market" if is_regular_hours else "limit"
			if (
				is_regular_hours
				and anchor_atr is not None
				and order_type in {"sell", "cover"}
			):
				order_type_name = "trailing-stop"
			elif is_regular_hours:
				order_type_name = "market"
			else:
				order_type_name = "limit"			
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

					active_statuses = {
					    "pending_new",
					    "new",
					    "accepted",
					    "partially_filled",
					    "pending_replace",
					    "pending_cancel",
					}

					if existing_status not in active_statuses:
						continue



					is_trailing_request = (
						anchor_atr is not None
						and order_type in {"sell", "cover"}
					)

					if (
						is_trailing_request
						and existing_type == "trailing_stop"
					):
						logger.info(
							"Existing trailing-stop order already protects position; "
							"skipping duplicate: ticker=%r side=%r order_id=%r",
							ticker,
							broker_side,
							existing_order_id,
						)
						return None



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
						if anchor_atr is None:
							submitted_order = alpaca_api.submit_order(
								symbol=ticker,
								qty=execution_qty,
								side=broker_side,
								type="market",
								time_in_force="day",
							)
						else:
							trail_price = float(
								Decimal(str(anchor_atr)).quantize(
									Decimal("0.01"),
									rounding=ROUND_HALF_UP,
								)
							)							
							submitted_order = alpaca_api.submit_order(
								symbol=ticker,
								qty=execution_qty,
								side=broker_side,
								type="trailing_stop",
								time_in_force="day",
								trail_price=trail_price,
								extended_hours=False
							)							
					else:
						submitted_order = alpaca_api.submit_order(
							symbol=ticker,
							qty=execution_qty,
							side=broker_side,
							type="limit",
							time_in_force="day",
							limit_price=price,
							extended_hours=True,
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


			self.trade_records.log_trade_diagnostic(
				source="live",
				strategy_name=strategy_name,
				ticker=ticker,
				event_type="entry" if order_type in {"long", "short"} else "exit",
				timeframe=tf,
				side=order_type,
				requested_qty=num_shares,
				market_price=prices.get(ticker, {}).get("market"),
				order_id=order_id,
				decision_time=str(date),
			)
			return submitted_order

			"""
			qty=filled_qty,
			fill_price=fill_price,
			market_price=prices.get(ticker, {}).get("market"),
			order_id=order_id,
			order_status=order_status,
			"""
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
		if order_type in {"sell", "cover"}:
		    timeout_seconds = 60 if is_regular_hours else 120
		else:
		    timeout_seconds = 20 if is_regular_hours else 90

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
						ex=max(timeout_seconds * 5, 600),
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


		submitted_order = submit_to_alpaca_only()

		if submitted_order is None:
			if pending_exit_key:
				try:
					self.r.delete(pending_exit_key)
				except Exception:
					logger.exception("Failed clearing pending exit guard after submit failure: key=%r", pending_exit_key)

			logger.info(
				"Skipping Redis bookkeeping because Alpaca order was not submitted: strategy=%r ticker=%r order_type=%r qty=%r",
				strategy_name,
				ticker,
				order_type,
				execution_qty,
			)
			return None

		order_id = getattr(submitted_order, "id", None)
		if not order_id:
			if pending_exit_key:
				try:
					self.r.delete(pending_exit_key)
				except Exception:
					logger.exception(
						"Failed clearing pending exit guard after missing order id: key=%r",
						pending_exit_key,
					)

			logger.info("Submitted Alpaca order missing order id for %r", ticker)
			return None

		is_trailing_order = (
			is_regular_hours
			and anchor_atr is not None
			and order_type in {"sell", "cover"}
		)

		if is_trailing_order:
			if pending_exit_key:
				try:
					self.r.delete(
						pending_exit_key
					)
				except Exception:
					logger.exception(
						"Failed clearing pending exit guard after "
						"trailing-stop submission: key=%r",
						pending_exit_key,
					)

			logger.info(
				"Trailing-stop order submitted and left broker-managed: "
				"strategy=%r ticker=%r order_type=%r qty=%r "
				"order_id=%r trail_price=%r",
				strategy_name,
				ticker,
				order_type,
				execution_qty,
				order_id,
				anchor_atr,
			)

			return {
				"submitted": True,
				"order_id": order_id,
				"background_monitoring": False,
				"broker_managed_trailing_stop": True,
			}		

		self.order_monitor_executor.submit(
			self._monitor_alpaca_order_fill,
			strategy_name,
			ticker,
			date,
			alpaca_api,
			order_id,
			order_type,
			execution_qty,
			price,
			do_redis_bookkeeping,
			pending_exit_key,
			timeout_seconds,
			1.0,
		)

		logger.info(
			"Alpaca order monitoring scheduled in background: strategy=%r ticker=%r order_type=%r qty=%r order_id=%r timeout_seconds=%r",
			strategy_name,
			ticker,
			order_type,
			execution_qty,
			order_id,
			timeout_seconds,
		)

		return {
			"submitted": True,
			"order_id": order_id,
			"background_monitoring": True,
		}		


	def place_long_order(self, strategy_name, tf, ticker, date, prices, num_shares, alpaca_api):
		return self.place_order(
			strategy_name,
			tf,
			ticker,
			date,
			prices,
			num_shares,
			alpaca_api,
			"long",
			None,
			False,
		)

	def place_short_order(self, strategy_name, tf, ticker, date, prices, num_shares, alpaca_api):
		return self.place_order(
			strategy_name,
			tf,
			ticker,
			date,
			prices,
			num_shares,
			alpaca_api,
			"short",
			None,
			False,
		)

	def sell_long_order(self, strategy_name, tf, ticker, date, prices, num_shares, alpaca_api, anchor_atr, do_redis_bookkeeping=False):
		return self.place_order(
			strategy_name,
			tf,
			ticker,
			date,
			prices,
			num_shares,
			alpaca_api,
			"sell",
			anchor_atr,
			do_redis_bookkeeping,
		)

	def cover_short_order(self, strategy_name, tf, ticker, date, prices, num_shares, alpaca_api, anchor_atr, do_redis_bookkeeping=False):
		return self.place_order(
			strategy_name,
			tf,
			ticker,
			date,
			prices,
			num_shares,
			alpaca_api,
			"cover",
			anchor_atr,
			do_redis_bookkeeping,
		)	
