import logging
from golden_cross_dip_recovery import evaluate_entry, bearish_entry_placeholder, bearish_exit_placeholder
import time
from datetime import datetime
import math
from decimal import Decimal, ROUND_HALF_UP
from concurrent.futures import ThreadPoolExecutor
import pandas as pd

from pattern_analyzer import PatternAnalyzer
from signal_history import RedisSignalHistory, SimSignalHistory

logger = logging.getLogger("tv-webhook")

class Strategies:
	@staticmethod
	def evaluate_golden_cross_dip_recovery(price, base_amount, snapshot, current_exposure, pending_buy_exposure, settings):
		return evaluate_entry(price, base_amount, snapshot, current_exposure, pending_buy_exposure, settings)

	bearish_dip_recovery_entry = staticmethod(bearish_entry_placeholder)
	bearish_dip_recovery_exit = staticmethod(bearish_exit_placeholder)

	def __init__(self, trading_view_webhook_helpers, trade_records):
		self.tvw_helpers = trading_view_webhook_helpers
		self.r = trading_view_webhook_helpers.require_redis()
		self.trade_records = trade_records
		self.SMALLEST_SHARE_SIZE = 0.25
		self.order_monitor_executor = ThreadPoolExecutor(max_workers=20)
		self.redis_signal_history = RedisSignalHistory(trading_view_webhook_helpers)
		self.pattern_analyzer = PatternAnalyzer(trading_view_webhook_helpers)
		

	def _signal_history(self, simulation=False, state=None):
		if simulation:
			if state is None:
				raise ValueError("Simulation signal history requires SimState")
			return SimSignalHistory(self.tvw_helpers, state)

		return self.redis_signal_history

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
		state=None,
	):
		return self.pattern_analyzer.is_tf_relative_to_last_higher_tf(
			history=self._signal_history(simulation, state),
			ticker=ticker,
			signal=signal,
			timeframe=timeframe,
			current_tf=current_tf,
			higher_tf=higher_tf,
			relation=relation,
		)


	def get_latest_valid_same_side_signal(
		self,
		ticker,
		side,
		higher_tf,
		max_scan=500,
	):
		return self.pattern_analyzer.get_latest_valid_same_side_signal(
			history=self.redis_signal_history,
			ticker=ticker,
			side=side,
			higher_tf=higher_tf,
			max_scan=max_scan,
		)


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
		state,
	):
		return self.pattern_analyzer.lower_tf_confirms_mid_tf_opposite_of_higher_tf(
			history=self._signal_history(simulation, state),
			ticker=ticker,
			signal=signal,
			timeframe=timeframe,
			valid_lower_tfs=valid_lower_tfs,
			mid_tf=mid_tf,
			higher_tf=higher_tf,
		)


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
		state=None,
	):
		return self.pattern_analyzer.has_opposite_signal_since_last_valid_same_side_higher_tf(
			history=self._signal_history(simulation, state),
			ticker=ticker,
			signal=signal,
			opposite_tf=opposite_tf,
			anchor_tf=anchor_tf,
			max_scan_opposite_tf=max_scan_opposite_tf,
			max_scan_anchor_tf=max_scan_anchor_tf,
		)


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
		state=None,
	):
		return self.pattern_analyzer.get_signal_based_progressive_entry_size(
			history=self._signal_history(simulation, state),
			ticker=ticker,
			side=side,
			entry_tf=entry_tf,
			anchor_tf=anchor_tf,
			base_num_shares=base_num_shares,
			smallest_share_size=smallest_share_size,
			max_scan_entry_tf=max_scan_entry_tf,
			max_scan_anchor_tf=max_scan_anchor_tf,
		)


	def is_latest_anchor_opposite_of_open_position(
		self,
		ticker,
		anchor_tf,
		alpaca_position_qty,
		simulation,
		backtester,
		state,
	):
		return self.pattern_analyzer.is_latest_anchor_opposite_of_open_position(
			history=self._signal_history(simulation, state),
			ticker=ticker,
			anchor_tf=anchor_tf,
			alpaca_position_qty=alpaca_position_qty,
		)


	def is_latest_anchor_same_as_open_position(self, ticker, anchor_tf, alpaca_position_qty, simulation, backtester, state):
		"""
		Return True if the latest anchor timeframe signal is same
		as the currently open Alpaca position side.
		
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
		expected_anchor_side = "sell" if position_side == "long" else "sell"
		opposite_anchor_side = "buy" if expected_anchor_side == "buy" else "buy"

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


	def get_latest_directional_signal(
		self,
		ticker,
		timeframe,
		signal_role,
		max_scan=100,
	):
		return self.redis_signal_history.get_latest_directional_signal(
			ticker=ticker,
			timeframe=timeframe,
			signal_role=signal_role,
			max_scan=max_scan,
		)


	def is_confirmation_signal(self, fields):
		return self.pattern_analyzer.is_confirmation_signal(fields)


	def latest_tf_signal_is_confirmation(
		self,
		ticker,
		timeframe,
		simulation,
		backtester,
		state,
	):
		return self.pattern_analyzer.latest_tf_signal_is_confirmation(
			history=self._signal_history(simulation, state),
			ticker=ticker,
			timeframe=timeframe,
		)
			

	def get_latest_confirmation_directional_signal(
		self,
		ticker,
		timeframe,
		max_scan=500,
	):
		return self.redis_signal_history.get_latest_confirmation_directional_signal(
			ticker=ticker,
			timeframe=timeframe,
			max_scan=max_scan,
		)


	def _tf_rank(self, timeframe):
		return self.pattern_analyzer.tf_rank(timeframe)



	def _get_strategy4_anchor_alerts(
		self,
		ticker,
		anchor_tf,
		simulation,
		backtester,
		state,
		max_scan=500,
	):
		return self.pattern_analyzer.get_strategy4_anchor_alerts(
			history=self._signal_history(simulation, state),
			ticker=ticker,
			anchor_tf=anchor_tf,
			max_scan=max_scan,
		)


	def _get_strategy4_anchor_alerts_DISCARD_THIS_VERSION_AFTER_STABILITY_IS_ACHIEVED(
		self,
		ticker,
		anchor_tf,
		simulation,
		backtester,
		state,
		max_scan=500,
	):
		"""
		Return recent anchor-timeframe alerts in reverse chronological order.

		The newest alert is returned first. In live mode, alerts are read from the
		ticker's Redis stream. In simulation mode, alerts are read only from events
		that have already been processed by the current BackTester run.

		Parameters:
			ticker (str):
				Ticker symbol whose anchor alerts should be retrieved.

			anchor_tf (str):
				Anchor timeframe used by Strategy 4.

			simulation (bool):
				True when running through BackTester and False during live execution.

			backtester (BackTester | None):
				BackTester instance used to access simulated alert history.

			state (SimState | None):
				Current in-memory simulation state.

			max_scan (int):
				Maximum number of recent anchor alerts to return.

		Returns:
			list[tuple]:
				A newest-to-oldest list of tuples containing:
					(stream_id, alert_fields)
		"""
		sym = str(
			ticker
			or ""
		).upper().strip()

		tf = self.tvw_helpers.normalize_tf(
			anchor_tf
		)

		if not sym or not tf:
			return []

		if simulation:
			events = state.all_events_by_ticker_tf.get(
				(
					sym,
					tf,
				),
				[],
			)

			return [
				(
					item["stream_id"],
					item["raw_fields"],
				)
				for item in reversed(
					events[-max_scan:]
				)
			]

		stream_key = self.tvw_helpers.stream_key(
			tf,
			sym,
		)

		try:
			return self.r.xrevrange(
				stream_key,
				count=max_scan,
			)
		except Exception:
			logger.exception(
				"Strategy 4 failed reading anchor alerts: "
				"ticker=%r anchor_tf=%r",
				sym,
				tf,
			)
			return []


	def _is_strategy4_confirmation(self, fields, expected_side):
		return self.pattern_analyzer.is_strategy4_confirmation(
			fields,
			expected_side,
		)


	def _strategy4_trend_entry_is_valid(self, alerts, entry_side):
		return self.pattern_analyzer.strategy4_trend_entry_is_valid(
			alerts,
			entry_side,
		)


	def _strategy4_range_entry_is_valid(self, alerts, entry_side):
		return self.pattern_analyzer.strategy4_range_entry_is_valid(
			alerts,
			entry_side,
		)



	def _get_strategy4_entry_side(
		self,
		ticker,
		anchor_tf,
		signal,
		simulation,
		backtester,
		state,
	):
		return self.pattern_analyzer.get_strategy4_entry_side(
			history=self._signal_history(simulation, state),
			ticker=ticker,
			anchor_tf=anchor_tf,
			signal=signal,
		)

											
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
				return backtester._open_or_add_position(state, event, "long", num_shares) 
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
				return backtester._open_or_add_position(state, event, "short", num_shares) 
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


	def reverse_entry_strategy1(self, strategy_name, entry_tf, intermediary_tf, anchor_tf, simulation, date, signal, prices, ticker, timeframe, NUM_SHARES, alpaca_api, state, config, event, price, backtester):
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
				return backtester._open_or_add_position(state, event, "short", num_shares) 
			else:
				return self.place_short_order(strategy_name, timeframe, ticker, date, prices, num_shares, alpaca_api)

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
				return backtester._open_or_add_position(state, event, "long", num_shares)
			else:
				return self.place_long_order(strategy_name, timeframe, ticker, date, prices, num_shares, alpaca_api)

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


	def reverse_exit_strategy1(self, strategy_name, lower_timeframes, intermediary_tf, anchor_tf, simulation, date, signal, prices, ticker, timeframe, alpaca_api, state, config, event, price, backtester):
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
		is_intermediary_tf_opposite_of_last_anchor_tf = ( #** opp signal
			self.is_tf_relative_to_last_higher_tf(
				ticker,
				self.tvw_helpers.opp_signal(signal),
				timeframe,
				intermediary_tf,
				anchor_tf,
				"opposite",
				simulation,
				backtester,
				state,
			)
		)

		lower_tf_confirms_intermediary_opposite_of_anchor = ( #** opp signal
			self.lower_tf_confirms_mid_tf_opposite_of_higher_tf(
				ticker,
				self.tvw_helpers.opp_signal(signal),
				timeframe,
				lower_timeframes,
				intermediary_tf,
				anchor_tf,
				simulation,
				backtester,
				state,
			)
		)

		anchor_same_as_open_position = ( #**
			self.is_latest_anchor_same_as_open_position(
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

				exit_signal_does_not_matches_open_position = ( #** Maybe use the matches for this one 
					(
						position_side == "short" 
						and latest_intermediary_direction == "buy"
						and signal == "bullish_exit"
					)
					or
					(
						position_side == "long" 
						and latest_intermediary_direction == "sell"
						and signal == "bearish_exit"
					)
				)

		should_exit = (
			is_intermediary_tf_opposite_of_last_anchor_tf
			or lower_tf_confirms_intermediary_opposite_of_anchor 
			or anchor_same_as_open_position 
			or exit_signal_does_not_matches_open_position
			#or exit_signal_matches_open_position
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
		#if simulation:#	CONTRARIAN EXPERIMENT. DELETE LATER
			#position_side = "long" if side == "sell" else "short"

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


	"""
	def exit_strategy3(self, strategy_name, anchor_tf, simulation, date, prices, ticker, timeframe, alpaca_api, state, config, event, price, backtester):
		""
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
		""
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
	"""


	def exit_strategy3(
		self,
		strategy_name,
		anchor_tf,
		simulation,
		date,
		prices,
		ticker,
		timeframe,
		alpaca_api,
		state,
		config,
		event,
		price,
		backtester,
	):
		"""
		Compatibility wrapper.

		Simulation Exit Strategy 3 remains owned by BackTester.
		Live trailing-stop management is handled continuously by the generic
		live trailing-stop monitor.
		"""
		if simulation:
			return None

		self.register_live_trailing_stop_entry(
			owner_name=strategy_name,
			ticker=ticker,
			anchor_tf=anchor_tf,
		)

		return None


	def entry_strategy4(
		self,
		strategy_name,
		anchor_tf,
		simulation,
		date,
		signal,
		prices,
		ticker,
		timeframe,
		NUM_SHARES,
		alpaca_api,
		state,
		config,
		event,
		price,
		backtester,
	):
		"""
		Execute a Strategy 4 anchor-timeframe entry.

		Strategy 4 has two entry modes:

			Trend:
				A confirmation buy may enter long and a confirmation sell may enter
				short when the corresponding trend rules are satisfied.

			Range:
				A bullish_exit may enter short and a bearish_exit may enter long when
				exactly one matching opposite exit occurred after the relevant most
				recent confirmation signal.

		Only alerts from the configured anchor timeframe are eligible.

		Parameters:
			strategy_name (str):
				Strategy configuration name.

			anchor_tf (str):
				Required anchor timeframe.

			simulation (bool):
				True during a backtest and False during live execution.

			date (datetime):
				Strategy decision time.

			signal (str):
				Current TradingView signal.

			prices (dict | None):
				Current live market-price dictionary.

			ticker (str):
				Ticker being evaluated.

			timeframe (str):
				Timeframe of the current signal.

			NUM_SHARES (float):
				Requested number of shares.

			alpaca_api:
				Alpaca client used during live execution.

			state:
				BackTester simulation state.

			config (dict | None):
				Backtest strategy configuration.

			event (dict | None):
				Current simulated event.

			price (float | None):
				Current simulated market price.

			backtester:
				BackTester instance used during simulation.

		Returns:
			Any:
				Result returned by the simulated position function or live order
				function. Returns None when no entry qualifies.
		"""
		tf = self.tvw_helpers.normalize_tf(
			timeframe
		)

		anchor_tf_norm = self.tvw_helpers.normalize_tf(
			anchor_tf
		)

		if tf != anchor_tf_norm:
			return None

		entry_side = self._get_strategy4_entry_side(
			ticker=ticker,
			anchor_tf=anchor_tf_norm,
			signal=signal,
			simulation=simulation,
			backtester=backtester,
			state=state,
		)

		if entry_side is None:
			return None

		logger.info(
			"Strategy 4 entry qualified: "
			"strategy=%r ticker=%r timeframe=%r signal=%r "
			"entry_side=%r simulation=%r",
			strategy_name,
			ticker,
			tf,
			signal,
			entry_side,
			simulation,
		)

		if simulation:
			return backtester._open_or_add_position(
				state,
				event,
				entry_side,
				NUM_SHARES,
			)

		if entry_side == "long":
			return self.place_long_order(
				strategy_name,
				timeframe,
				ticker,
				date,
				prices,
				NUM_SHARES,
				alpaca_api,
			)

		return self.place_short_order(
			strategy_name,
			timeframe,
			ticker,
			date,
			prices,
			NUM_SHARES,
			alpaca_api,
		)


	def exit_strategy4(
		self,
		strategy_name,
		anchor_tf,
		loss_liquidation_atr_factor,
		simulation,
		date,
		signal,
		prices,
		ticker,
		timeframe,
		alpaca_api,
		state,
		config,
		event,
		price,
		backtester,
	):
		"""
		Exit Strategy 4 using either:

			1. An opposite normalized anchor-timeframe signal.
			2. An ATR-based defensive liquidation.

		Long position:
			Exit on normalized sell.

		Short position:
			Exit on normalized buy.

		During simulation, BackTester evaluates the ATR defensive
		liquidation threshold against one-minute market bars.

		During live execution, the generic live trailing-stop manager
		handles the ATR defensive liquidation. This function handles
		the opposite anchor-signal exit.
		"""
		if loss_liquidation_atr_factor is not None:
			try:
				loss_liquidation_atr_factor = float(
					loss_liquidation_atr_factor
				)
			except (TypeError, ValueError) as exc:
				raise ValueError(
					"loss_liquidation_atr_factor must be a number"
				) from exc

			if loss_liquidation_atr_factor <= 0:
				raise ValueError(
					"loss_liquidation_atr_factor must be > 0"
				)

		timeframe_normalized = self.tvw_helpers.normalize_tf(
			timeframe
		)

		anchor_tf_normalized = self.tvw_helpers.normalize_tf(
			anchor_tf
		)

		if timeframe_normalized != anchor_tf_normalized:
			return None

		normalized_signal = self.tvw_helpers.normalize_signal(
			signal
		)

		if normalized_signal not in {
			"buy",
			"sell",
		}:
			return None

		symbol = str(
			ticker or ""
		).strip().upper()

		if not symbol:
			return None

		if simulation:
			if (
				state is None
				or event is None
				or backtester is None
			):
				return None

			position = state.positions.get(
				symbol
			)

			if position is None:
				return None

			if float(
				position.num_shares or 0.0
			) <= 0:
				return None

			position_side = str(
				position.side or ""
			).strip().lower()

			should_exit = (
				position_side == "long"
				and normalized_signal == "sell"
			) or (
				position_side == "short"
				and normalized_signal == "buy"
			)

			if not should_exit:
				return None

			logger.info(
				"Strategy 4 opposite-signal exit: "
				"strategy=%r ticker=%r timeframe=%r "
				"position_side=%r signal=%r "
				"loss_liquidation_atr_factor=%r "
				"simulation=%r date=%r",
				strategy_name,
				symbol,
				timeframe_normalized,
				position_side,
				normalized_signal,
				loss_liquidation_atr_factor,
				simulation,
				date,
			)

			return backtester._close_position(
				state,
				event,
			)

		if alpaca_api is None:
			logger.error(
				"Strategy 4 live exit skipped because Alpaca API "
				"is unavailable: strategy=%r ticker=%r",
				strategy_name,
				symbol,
			)
			return None

		try:
			alpaca_position = alpaca_api.get_position(
				symbol
			)
		except Exception as exc:
			error_text = str(
				exc
			).lower()

			if (
				"position does not exist" in error_text
				or "no position" in error_text
				or "404" in error_text
			):
				logger.info(
					"Strategy 4 live exit skipped because no "
					"Alpaca position exists: "
					"strategy=%r ticker=%r signal=%r",
					strategy_name,
					symbol,
					normalized_signal,
				)
				return None

			logger.exception(
				"Strategy 4 failed retrieving live position: "
				"strategy=%r ticker=%r signal=%r",
				strategy_name,
				symbol,
				normalized_signal,
			)
			return None

		position_side = str(
			getattr(
				alpaca_position,
				"side",
				"",
			)
			or ""
		).strip().lower()

		try:
			position_qty = abs(
				float(
					getattr(
						alpaca_position,
						"qty",
						0.0,
					)
					or 0.0
				)
			)
		except (TypeError, ValueError):
			logger.exception(
				"Strategy 4 live position has invalid quantity: "
				"strategy=%r ticker=%r qty=%r",
				strategy_name,
				symbol,
				getattr(
					alpaca_position,
					"qty",
					None,
				),
			)
			return None

		if position_qty <= 0:
			return None

		should_exit = (
			position_side == "long"
			and normalized_signal == "sell"
		) or (
			position_side == "short"
			and normalized_signal == "buy"
		)

		if not should_exit:
			return None

		logger.info(
			"Strategy 4 live opposite-signal exit qualified: "
			"strategy=%r ticker=%r timeframe=%r "
			"position_side=%r position_qty=%r signal=%r "
			"loss_liquidation_atr_factor=%r date=%r",
			strategy_name,
			symbol,
			timeframe_normalized,
			position_side,
			position_qty,
			normalized_signal,
			loss_liquidation_atr_factor,
			date,
		)

		if position_side == "long":
			return self.sell_long_order(
				strategy_name,
				timeframe_normalized,
				symbol,
				date,
				prices,
				position_qty,
				alpaca_api,
				None,
				do_redis_bookkeeping=False,
			)

		if position_side == "short":
			return self.cover_short_order(
				strategy_name,
				timeframe_normalized,
				symbol,
				date,
				prices,
				position_qty,
				alpaca_api,
				None,
				do_redis_bookkeeping=False,
			)

		logger.warning(
			"Strategy 4 live exit skipped because Alpaca returned "
			"an unknown position side: "
			"strategy=%r ticker=%r position_side=%r",
			strategy_name,
			symbol,
			position_side,
		)

		return None


	def exit_strategy4_placeholder(
		self,
		strategy_name,
		anchor_tf,
		simulation,
		date,
		signal,
		prices,
		ticker,
		timeframe,
		alpaca_api,
		state,
		config,
		event,
		price,
		backtester,
	):
		"""
		Placeholder for Strategy 4 exit logic.

		Strategy 4 does not currently have a defined exit methodology. This function
		intentionally performs no position inspection, order submission, position
		modification, or simulation bookkeeping.

		The function exists so Strategy 4 follows the same dispatcher and execution
		structure as Strategies 1 and 2. Its implementation can later be replaced
		without changing app.py or BackTester dispatch.

		Returns:
			None
		"""
		return None



	def exit_strategy4_no_loss_liquidation(
		self,
		strategy_name,
		anchor_tf,
		simulation,
		date,
		signal,
		prices,
		ticker,
		timeframe,
		alpaca_api,
		state,
		config,
		event,
		price,
		backtester,
	):
		"""
		Exit Strategy 4 when the opposite normalized anchor signal arrives.

		Long position:
			Exit on normalized sell.

		Short position:
			Exit on normalized buy.
		"""
		if not simulation:
			return None

		if state is None or event is None or backtester is None:
			return None

		timeframe_normalized = self.tvw_helpers.normalize_tf(
			timeframe
		)

		anchor_tf_normalized = self.tvw_helpers.normalize_tf(
			anchor_tf
		)

		if timeframe_normalized != anchor_tf_normalized:
			return None

		normalized_signal = self.tvw_helpers.normalize_signal(
			signal
		)

		if normalized_signal not in {
			"buy",
			"sell",
		}:
			return None

		symbol = str(
			ticker or ""
		).strip().upper()

		position = state.positions.get(
			symbol
		)

		if position is None:
			return None

		if float(position.num_shares or 0.0) <= 0:
			return None

		position_side = str(
			position.side or ""
		).strip().lower()

		should_exit = (
			position_side == "long"
			and normalized_signal == "sell"
		) or (
			position_side == "short"
			and normalized_signal == "buy"
		)

		if not should_exit:
			return None

		logger.info(
			"Strategy 4 opposite-signal exit: "
			"strategy=%r ticker=%r timeframe=%r "
			"position_side=%r signal=%r date=%r",
			strategy_name,
			symbol,
			timeframe_normalized,
			position_side,
			normalized_signal,
			date,
		)

		return backtester._close_position(
			state,
			event,
		)


	def exit_strategy4_sim_only(
		self,
		strategy_name,
		anchor_tf,
		loss_liquidation_atr_factor,
		simulation,
		date,
		signal,
		prices,
		ticker,
		timeframe,
		alpaca_api,
		state,
		config,
		event,
		price,
		backtester,
	):
		"""
		Exit Strategy 4 using either:

			1. An opposite normalized anchor-timeframe signal.
			2. An ATR-based defensive liquidation managed continuously
			   by BackTester during simulation.

		Long position:
			Exit on normalized sell, or when price falls to:

				cost_basis
				- (
					entry_ATR
					* loss_liquidation_atr_factor
				)

		Short position:
			Exit on normalized buy, or when price rises to:

				cost_basis
				+ (
					entry_ATR
					* loss_liquidation_atr_factor
				)

		During simulation, BackTester evaluates the ATR threshold against
		one-minute market bars. This function handles the signal-driven exit.
		"""
		if loss_liquidation_atr_factor is not None:
			try:
				loss_liquidation_atr_factor = float(
					loss_liquidation_atr_factor
				)
			except (TypeError, ValueError) as exc:
				raise ValueError(
					"loss_liquidation_atr_factor must be a number"
				) from exc

			if loss_liquidation_atr_factor <= 0:
				raise ValueError(
					"loss_liquidation_atr_factor must be > 0"
				)

		if not simulation:
			return None

		if state is None or event is None or backtester is None:
			return None

		timeframe_normalized = self.tvw_helpers.normalize_tf(
			timeframe
		)

		anchor_tf_normalized = self.tvw_helpers.normalize_tf(
			anchor_tf
		)

		if timeframe_normalized != anchor_tf_normalized:
			return None

		normalized_signal = self.tvw_helpers.normalize_signal(
			signal
		)

		if normalized_signal not in {
			"buy",
			"sell",
		}:
			return None

		symbol = str(
			ticker or ""
		).strip().upper()

		position = state.positions.get(
			symbol
		)

		if position is None:
			return None

		if float(position.num_shares or 0.0) <= 0:
			return None

		position_side = str(
			position.side or ""
		).strip().lower()

		should_exit = (
			position_side == "long"
			and normalized_signal == "sell"
		) or (
			position_side == "short"
			and normalized_signal == "buy"
		)

		if not should_exit:
			return None

		logger.info(
			"Strategy 4 opposite-signal exit: "
			"strategy=%r ticker=%r timeframe=%r "
			"position_side=%r signal=%r "
			"loss_liquidation_atr_factor=%r date=%r",
			strategy_name,
			symbol,
			timeframe_normalized,
			position_side,
			normalized_signal,
			loss_liquidation_atr_factor,
			date,
		)

		return backtester._close_position(
			state,
			event,
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


	def _live_trailing_stop_state_key(
		self,
		owner_name: str,
		ticker: str,
	) -> str:
		return (
			"tv:live_trailing_stop:state:"
			f"{str(owner_name or '').strip()}:"
			f"{str(ticker or '').upper().strip()}"
		)


	def _live_trailing_stop_managed_key(
		self,
		owner_name: str,
		ticker: str,
	) -> str:
		return (
			"tv:live_trailing_stop:managed:"
			f"{str(owner_name or '').strip()}:"
			f"{str(ticker or '').upper().strip()}"
		)


	def _live_trailing_stop_lock_key(
		self,
		owner_name: str,
	) -> str:
		return (
			"tv:live_trailing_stop:lock:"
			f"{str(owner_name or '').strip()}"
		)

	"""def register_live_trailing_stop_entry(
		self,
		*,
		owner_name: str,
		ticker: str,
		anchor_tf: str,
		entry_order_id: str = None,
	) -> None:
		""
		Register a submitted live entry for trailing-stop management.

		The position may not have filled yet. The continuous monitor will wait
		until Alpaca reports an open position before initializing ATR state.
		""
		symbol = str(
			ticker or ""
		).upper().strip()

		key = self._live_trailing_stop_managed_key(
			owner_name,
			symbol,
		)

		self.r.hset(
			key,
			mapping={
				"owner_name": owner_name,
				"ticker": symbol,
				"anchor_tf": self.tvw_helpers.normalize_tf(
					anchor_tf
				),
				"entry_order_id": str(
					entry_order_id or ""
				),
				"registered_at": (
					self.tvw_helpers._now_et().isoformat()
				),
			},
		)

		self.r.expire(
			key,
			60 * 60 * 24 * 14,
		)"""


	def register_live_position(
		self,
		*,
		owner_name: str,
		ticker: str,
		anchor_tf: str,
		entry_order_id: str = None,
		entry_decision_time: datetime = None,
	) -> None:
		"""
		Register a submitted live entry for trailing-stop management.

		The position may not have filled yet. The continuous monitor will wait
		until Alpaca reports an open position before initializing ATR state.

		entry_decision_time is the time at which the entry strategy made its
		decision. It is later used to select the same completed anchor ATR that
		the backtester would have used.
		"""
		symbol = str(
			ticker or ""
		).upper().strip()

		entry_order_id = str(
			entry_order_id or ""
		).strip()

		if not entry_order_id:
			logger.error(
				"Refusing live trailing-stop registration without "
				"an entry order ID: owner=%r ticker=%r anchor_tf=%r",
				owner_name,
				symbol,
				anchor_tf,
			)
			return		

		registered_at = (
			self.tvw_helpers._now_et()
		)

		if entry_decision_time is None:
			entry_decision_time = registered_at
		elif entry_decision_time.tzinfo is None:
			entry_decision_time = (
				entry_decision_time.replace(
					tzinfo=self.tvw_helpers.eastern_tz
				)
			)
		else:
			entry_decision_time = (
				entry_decision_time.astimezone(
					self.tvw_helpers.eastern_tz
				)
			)

		key = self._live_trailing_stop_managed_key(
			owner_name,
			symbol,
		)

		self.r.hset(
			key,
			mapping={
				"owner_name": owner_name,
				"ticker": symbol,
				"anchor_tf": (
					self.tvw_helpers.normalize_tf(
						anchor_tf
					)
				),
				"entry_order_id": entry_order_id,
				"entry_decision_time": (
					entry_decision_time.isoformat()
				),
				"registered_at": (
					registered_at.isoformat()
				),			
			},
		)

		self.r.expire(
			key,
			60 * 60 * 24 * 14,
		)


	def _get_open_orders_for_symbol(
		self,
		alpaca_api,
		ticker: str,
	) -> list:
		symbol = str(
			ticker or ""
		).upper().strip()

		return [
			order
			for order in alpaca_api.list_orders(
				status="open",
				limit=500,
			)
			if str(
				getattr(
					order,
					"symbol",
					"",
				)
				or ""
			).upper()
			== symbol
		]


	def _get_live_closing_orders(
		self,
		alpaca_api,
		ticker: str,
		position_side: str,
	) -> list:
		closing_side = (
			"sell"
			if position_side == "long"
			else "buy"
		)

		return [
			order
			for order in self._get_open_orders_for_symbol(
				alpaca_api,
				ticker,
			)
			if str(
				getattr(
					order,
					"side",
					"",
				)
				or ""
			).lower()
			== closing_side
		]


	def _cancel_orders_and_wait(
		self,
		alpaca_api,
		orders: list,
		timeout_seconds: float = 10.0,
	) -> bool:
		if not orders:
			return True

		order_ids = {
			str(
				getattr(
					order,
					"id",
					"",
				)
			)
			for order in orders
			if getattr(
				order,
				"id",
				None,
			)
		}

		for order_id in order_ids:
			try:
				alpaca_api.cancel_order(
					order_id
				)
			except Exception:
				logger.exception(
					"Unable to request order cancellation: "
					"order_id=%r",
					order_id,
				)

		terminal_statuses = {
			"canceled",
			"filled",
			"expired",
			"rejected",
			"replaced",
		}

		deadline = (
			time.monotonic()
			+ timeout_seconds
		)

		while time.monotonic() < deadline:
			all_terminal = True

			for order_id in order_ids:
				try:
					order = alpaca_api.get_order(
						order_id
					)

					status = str(
						getattr(
							order,
							"status",
							"",
						)
						or ""
					).lower()

					if status not in terminal_statuses:
						all_terminal = False

				except Exception:
					continue

			if all_terminal:
				return True

			time.sleep(
				0.25
			)

		logger.error(
			"Timed out waiting for order cancellation: "
			"order_ids=%r",
			sorted(
				order_ids
			),
		)

		return False


	def _submit_live_trailing_stop_order(
		self,
		*,
		owner_name: str,
		ticker: str,
		position_side: str,
		position_qty: float,
		trailing_amount: float,
		alpaca_api,
	):
		if position_qty <= 0:
			return None

		if trailing_amount <= 0:
			return None

		order_side = (
			"sell"
			if position_side == "long"
			else "buy"
		)

		trail_price = float(
			Decimal(
				str(
					trailing_amount
				)
			).quantize(
				Decimal("0.01"),
				rounding=ROUND_HALF_UP,
			)
		)

		order = alpaca_api.submit_order(
			symbol=ticker,
			qty=position_qty,
			side=order_side,
			type="trailing_stop",
			time_in_force="day",
			trail_price=trail_price,
			extended_hours=False,
		)

		logger.info(
			"Live trailing stop submitted: "
			"owner=%r ticker=%r position_side=%r "
			"qty=%r trail_price=%r order_id=%r",
			owner_name,
			ticker,
			position_side,
			position_qty,
			trail_price,
			getattr(
				order,
				"id",
				None,
			),
		)

		return order


	def _submit_live_market_liquidation(
		self,
		*,
		owner_name: str,
		ticker: str,
		position_side: str,
		alpaca_api,
		exit_reason: str,
	):
		closing_orders = self._get_live_closing_orders(
			alpaca_api,
			ticker,
			position_side,
		)

		if not self._cancel_orders_and_wait(
			alpaca_api,
			closing_orders,
		):
			logger.error(
				"Liquidation aborted because existing closing "
				"orders were not confirmed canceled: "
				"owner=%r ticker=%r reason=%r",
				owner_name,
				ticker,
				exit_reason,
			)
			return None

		try:
			position = alpaca_api.get_position(
				ticker
			)
		except Exception:
			return None

		raw_qty = float(
			getattr(
				position,
				"qty",
				0.0,
			)
			or 0.0
		)

		if raw_qty == 0:
			return None

		current_side = (
			"long"
			if raw_qty > 0
			else "short"
		)

		current_qty = abs(
			raw_qty
		)

		order_side = (
			"sell"
			if current_side == "long"
			else "buy"
		)

		order = alpaca_api.submit_order(
			symbol=ticker,
			qty=current_qty,
			side=order_side,
			type="market",
			time_in_force="day",
		)

		logger.warning(
			"Live market liquidation submitted: "
			"owner=%r ticker=%r side=%r qty=%r "
			"reason=%r order_id=%r",
			owner_name,
			ticker,
			current_side,
			current_qty,
			exit_reason,
			getattr(
				order,
				"id",
				None,
			),
		)

		return order


	def _initialize_live_trailing_stop_state(
		self,
		*,
		owner_name: str,
		ticker: str,
		anchor_tf: str,
		alpaca_api,
		atr_period: int,
		atr_multiplier: float,
		entry_decision_time: datetime,
		now_et: datetime,
	) -> dict:
		entry_atr, atr_source_time = (
			self.trade_records.get_latest_completed_atr(
				alpaca_api=alpaca_api,
				ticker=ticker,
				timeframe=anchor_tf,
				period=atr_period,
				as_of=entry_decision_time,
			)
		)
		state_key = self._live_trailing_stop_state_key(
			owner_name,
			ticker,
		)

		state_mapping = {
			"owner_name": owner_name,
			"ticker": ticker,
			"anchor_tf": anchor_tf,
			"entry_decision_time": (entry_decision_time.isoformat()),			
			"entry_atr": entry_atr,
			"atr_source_time": (atr_source_time.isoformat()),
			"original_atr_multiplier": atr_multiplier,
			"active_atr_multiplier": atr_multiplier,
			"trailing_stop_expanded": "0",
			"exit_pending": "0",
			"created_at": now_et.isoformat(),
		}

		self.r.hset(
			state_key,
			mapping=state_mapping,
		)

		return {
			key: str(value)
			for key, value in state_mapping.items()
		}


	def _reconcile_live_trailing_stop_exit_pending(
		self,
		*,
		state_key: str,
		live_state: dict,
		ticker: str,
		alpaca_api,
	) -> bool:
		"""
		Return True while a valid liquidation remains pending.

		Reset exit_pending when an order failed and the position still exists.
		Delete tracking when the position has closed.
		"""
		if live_state.get(
			"exit_pending",
			"0",
		) != "1":
			return False

		exit_order_id = str(
			live_state.get(
				"exit_order_id",
				"",
			)
			or ""
		).strip()

		try:
			position = alpaca_api.get_position(
				ticker
			)

			position_qty = abs(
				float(
					getattr(
						position,
						"qty",
						0.0,
					)
					or 0.0
				)
			)

		except Exception:
			position_qty = 0.0

		if position_qty <= 0:
			self.r.delete(
				state_key
			)
			return True

		if not exit_order_id:
			self.r.hset(
				state_key,
				mapping={
					"exit_pending": "0",
					"exit_order_id": "",
				},
			)
			return False

		try:
			order = alpaca_api.get_order(
				exit_order_id
			)

			order_status = str(
				getattr(
					order,
					"status",
					"",
				)
				or ""
			).lower()

		except Exception:
			order_status = ""

		pending_statuses = {
			"pending_new",
			"accepted",
			"new",
			"partially_filled",
			"pending_cancel",
			"pending_replace",
		}

		if order_status in pending_statuses:
			return True

		#
		# The order is no longer active, but the position remains.
		# Permit another management attempt.
		#
		market_close_daily_key = str(
			live_state.get(
				"market_close_daily_key",
				"",
			)
			or ""
		).strip()

		if market_close_daily_key:
			self.r.delete(
				market_close_daily_key
			)

		self.r.hset(
			state_key,
			mapping={
				"exit_pending": "0",
				"exit_order_id": "",
				"exit_reason": "",
				"market_close_daily_key": "",
			},
		)

		return False


	def manage_live_trailing_stop_position(
		self,
		*,
		owner_name: str,
		ticker: str,
		anchor_tf: str,
		alpaca_api,
		current_price: float,
		atr_period: int,
		atr_multiplier: float,
		loss_liquidation_atr_factor: float,
		profit_expansion_atr_factor: float,
		trailing_stop_multiplier_factor: float,
		use_trailing_stop: bool,
		use_profit_expansion: bool,		
		entry_decision_time: datetime,
		now_et: datetime,
	) -> None:
		symbol = str(
			ticker or ""
		).upper().strip()

		try:
			position = alpaca_api.get_position(
				symbol
			)
		except Exception:
			# The entry may still be pending or not yet filled.
			return

		raw_qty = float(
			getattr(
				position,
				"qty",
				0.0,
			)
			or 0.0
		)

		if raw_qty == 0:
			return

		position_side = (
			"long"
			if raw_qty > 0
			else "short"
		)
		position_qty = abs(
			raw_qty
		)
		cost_basis = float(
			getattr(
				position,
				"avg_entry_price",
				0.0,
			)
			or 0.0
		)

		if cost_basis <= 0:
			return

		state_key = self._live_trailing_stop_state_key(
			owner_name,
			symbol,
		)

		live_state = self.r.hgetall(
			state_key
		)

		if not live_state:
			live_state = (
				self._initialize_live_trailing_stop_state(
					owner_name=owner_name,
					ticker=symbol,
					anchor_tf=anchor_tf,
					alpaca_api=alpaca_api,
					atr_period=atr_period,
					atr_multiplier=atr_multiplier,
					entry_decision_time=(
						entry_decision_time
					),
					now_et=now_et,
				)
			)

		if self._reconcile_live_trailing_stop_exit_pending(
			state_key=state_key,
			live_state=live_state,
			ticker=symbol,
			alpaca_api=alpaca_api,
		):
			return

		live_state = self.r.hgetall(
			state_key
		)			

		entry_atr = float(
			live_state[
				"entry_atr"
			]
		)
		original_multiplier = float(
			live_state.get(
				"original_atr_multiplier",
				atr_multiplier,
			)
		)
		active_multiplier = float(
			live_state.get(
				"active_atr_multiplier",
				original_multiplier,
			)
		)
		stop_expanded = (
			live_state.get(
				"trailing_stop_expanded",
				"0",
			)
			== "1"
		)

		#
		# 1. Defensive liquidation.
		#
		loss_distance = (
			entry_atr
			* loss_liquidation_atr_factor
		)

		if position_side == "long":
			defensive_price = (
				cost_basis
				- loss_distance
			)
			defensive_triggered = (
				current_price
				<= defensive_price
			)
		else:
			defensive_price = (
				cost_basis
				+ loss_distance
			)
			defensive_triggered = (
				current_price
				>= defensive_price
			)

		if defensive_triggered:
			order = self._submit_live_market_liquidation(
				owner_name=owner_name,
				ticker=symbol,
				position_side=position_side,
				alpaca_api=alpaca_api,
				exit_reason=(
					"atr_cost_basis_liquidation"
				),
			)

			if order is not None:
				self.r.hset(
					state_key,
					mapping={
						"exit_pending": "1",
						"exit_reason": (
							"atr_cost_basis_liquidation"
						),
						"exit_order_id": str(
							getattr(
								order,
								"id",
								"",
							)
						),
						"exit_requested_at": (
							now_et.isoformat()
						),
					},
				)

			return

		if not use_trailing_stop:
			return			

		#
		# 2. Optional trailing-stop management.
		#    (Profit expansion + trailing stop)
		#
		profit_distance = (
			entry_atr
			* profit_expansion_atr_factor
		)

		if position_side == "long":
			expansion_price = (
				cost_basis
				+ profit_distance
			)
			expansion_triggered = (
				current_price
				>= expansion_price
			)
		else:
			expansion_price = (
				cost_basis
				- profit_distance
			)
			expansion_triggered = (
				current_price
				<= expansion_price
			)

		if (
			use_profit_expansion
			and not stop_expanded
			and trailing_stop_multiplier_factor > 1
			and expansion_triggered
		):
			closing_orders = (
				self._get_live_closing_orders(
					alpaca_api,
					symbol,
					position_side,
				)
			)

			if not self._cancel_orders_and_wait(
				alpaca_api,
				closing_orders,
			):
				return

			try:
				refreshed_position = (
					alpaca_api.get_position(
						symbol
					)
				)
			except Exception:
				return

			refreshed_qty = abs(
				float(
					getattr(
						refreshed_position,
						"qty",
						0.0,
					)
					or 0.0
				)
			)

			if refreshed_qty <= 0:
				return

			expanded_multiplier = (
				original_multiplier
				* trailing_stop_multiplier_factor
			)
			expanded_amount = (
				entry_atr
				* expanded_multiplier
			)

			order = self._submit_live_trailing_stop_order(
				owner_name=owner_name,
				ticker=symbol,
				position_side=position_side,
				position_qty=refreshed_qty,
				trailing_amount=expanded_amount,
				alpaca_api=alpaca_api,
			)

			if order is not None:
				self.r.hset(
					state_key,
					mapping={
						"active_atr_multiplier": (
							expanded_multiplier
						),
						"trailing_stop_expanded": "1",
						"trailing_stop_expanded_at": (
							now_et.isoformat()
						),
						"trailing_order_id": str(
							getattr(
								order,
								"id",
								"",
							)
						),
					},
				)

			return

		#
		# 3. Ensure the active trailing order exists.
		#
		closing_orders = self._get_live_closing_orders(
			alpaca_api,
			symbol,
			position_side,
		)

		trailing_orders = [
			order
			for order in closing_orders
			if str(
				getattr(
					order,
					"type",
					"",
				)
				or ""
			).lower()
			== "trailing_stop"
		]

		#
		# Add up the quantity protected by all active
		# trailing-stop orders.
		#
		trailing_qty = 0.0

		for trailing_order in trailing_orders:
			try:
				order_qty = abs(
					float(
						getattr(
							trailing_order,
							"qty",
							0.0,
						)
						or 0.0
					)
				)

			except Exception:
				logger.exception(
					"Unable to parse trailing-stop order quantity: "
					"owner=%r ticker=%r order_id=%r qty=%r",
					owner_name,
					symbol,
					getattr(
						trailing_order,
						"id",
						None,
					),
					getattr(
						trailing_order,
						"qty",
						None,
					),
				)
				return

			trailing_qty += order_qty

		#
		# Use a small tolerance to avoid replacing an order
		# because of insignificant floating-point differences.
		#
		qty_tolerance = 0.000000001

		trailing_qty_matches_position = (
			bool(
				trailing_orders
			)
			and math.isclose(
				trailing_qty,
				position_qty,
				rel_tol=0.0,
				abs_tol=qty_tolerance,
			)
		)

		#
		# Existing trailing protection already covers the full
		# position. No replacement is needed.
		#
		if trailing_qty_matches_position:
			self.r.hset(
				state_key,
				mapping={
					"trailing_order_id": str(
						getattr(
							trailing_orders[0],
							"id",
							"",
						)
					),
					"last_checked_at": (
						now_et.isoformat()
					),
					"protected_position_qty": str(
						position_qty
					),
				},
			)

			return

		#
		# A trailing stop exists, but its total quantity does not
		# match the current Alpaca position. Cancel all existing
		# closing orders before replacing the trailing stop.
		#
		if trailing_orders:
			logger.warning(
				"Live trailing-stop quantity mismatch; "
				"canceling and replacing: "
				"owner=%r ticker=%r position_qty=%r "
				"trailing_qty=%r trailing_order_count=%r",
				owner_name,
				symbol,
				position_qty,
				trailing_qty,
				len(
					trailing_orders
				),
			)

			if not self._cancel_orders_and_wait(
				alpaca_api,
				closing_orders,
			):
				logger.warning(
					"Live trailing-stop replacement deferred "
					"because order cancellation was not confirmed: "
					"owner=%r ticker=%r",
					owner_name,
					symbol,
				)
				return

			#
			# Refresh the live position after cancellation. Its
			# quantity could have changed while cancellation was
			# being processed.
			#
			try:
				refreshed_position = (
					alpaca_api.get_position(
						symbol
					)
				)

			except Exception as exc:
				if (
					"position does not exist"
					in str(
						exc
					).lower()
				):
					logger.info(
						"Trailing-stop replacement skipped "
						"because the position is now flat: "
						"owner=%r ticker=%r",
						owner_name,
						symbol,
					)

					self.r.delete(
						state_key
					)

				else:
					logger.exception(
						"Unable to refresh Alpaca position "
						"after trailing-stop cancellation: "
						"owner=%r ticker=%r",
						owner_name,
						symbol,
					)

				return

			refreshed_raw_qty = float(
				getattr(
					refreshed_position,
					"qty",
					0.0,
				)
				or 0.0
			)

			if refreshed_raw_qty == 0:
				logger.info(
					"Trailing-stop replacement skipped because "
					"the refreshed position quantity is zero: "
					"owner=%r ticker=%r",
					owner_name,
					symbol,
				)

				self.r.delete(
					state_key
				)
				return

			position_qty = abs(
				refreshed_raw_qty
			)

			position_side = (
				"long"
				if refreshed_raw_qty > 0
				else "short"
			)

		#
		# No matching trailing stop exists. This handles both:
		#
		# 1. No trailing stop existed.
		# 2. A mismatched trailing stop was canceled and the
		#    position was refreshed.
		#
		trailing_amount = (
			entry_atr
			* active_multiplier
		)

		order = self._submit_live_trailing_stop_order(
			owner_name=owner_name,
			ticker=symbol,
			position_side=position_side,
			position_qty=position_qty,
			trailing_amount=trailing_amount,
			alpaca_api=alpaca_api,
		)

		if order is not None:
			self.r.hset(
				state_key,
				mapping={
					"trailing_order_id": str(
						getattr(
							order,
							"id",
							"",
						)
					),
					"last_checked_at": (
						now_et.isoformat()
					),
					"protected_position_qty": str(
						position_qty
					),
				},
			)


	def _entry_order_is_still_pending(
		self,
		alpaca_api,
		entry_order_id: str,
	) -> bool:
		order_id = str(
			entry_order_id or ""
		).strip()

		if not order_id:
			return False

		try:
			order = alpaca_api.get_order(
				order_id
			)
		except Exception:
			logger.exception(
				"Unable to verify entry-order status; "
				"deferring trailing-stop management: "
				"entry_order_id=%r",
				order_id,
			)
			return True

		status = str(
			getattr(
				order,
				"status",
				"",
			)
			or ""
		).lower()

		return status in {
			"pending_new",
			"accepted",
			"new",
			"partially_filled",
			"pending_replace",
			"pending_cancel",
		}



	"""def manage_live_positions(
		self,
		*,
		owner_name: str,
		alpaca_api,
		atr_period: int,
		atr_multiplier: float,
		loss_liquidation_atr_factor: float,
		profit_expansion_atr_factor: float,
		trailing_stop_multiplier_factor: float,
		liquidate_before_market_close: bool,
		market_close_buffer_seconds: int,
		manager_lock_seconds: int,
	) -> None:"""
	def manage_live_positions(
		self,
		*,
		owner_name: str,
		alpaca_api,
		atr_period: int,
		atr_multiplier: float,
		loss_liquidation_atr_factor: float,
		profit_expansion_atr_factor: float,
		trailing_stop_multiplier_factor: float,
		use_trailing_stop: bool,
		use_profit_expansion: bool,
		liquidate_before_market_close: bool,
		market_close_buffer_seconds: int,
		manager_lock_seconds: int,
	) -> None:	
		manager_lock = self.r.lock(
			self._live_trailing_stop_lock_key(
				owner_name
			),
			timeout=max(
				manager_lock_seconds,
				5,
			),
			blocking=False,
		)

		if not manager_lock.acquire(
			blocking=False
		):
			return

		try:
			now_et = self.tvw_helpers._now_et()

			#
			# Read the Alpaca market clock once for this manager
			# iteration.
			#
			try:
				clock = alpaca_api.get_clock()

			except Exception:
				logger.exception(
					"Unable to read Alpaca clock: owner=%r",
					owner_name,
				)
				return

			try:
				seconds_to_close = (
					pd.Timestamp(
						clock.next_close
					)
					- pd.Timestamp(
						clock.timestamp
					)
				).total_seconds()

			except Exception:
				logger.exception(
					"Unable to calculate time to market close: "
					"owner=%r",
					owner_name,
				)
				seconds_to_close = None

			market_is_open = bool(
				getattr(
					clock,
					"is_open",
					False,
				)
			)

			in_market_close_window = (
				liquidate_before_market_close
				and market_is_open
				and seconds_to_close is not None
				and 0
				< seconds_to_close
				<= market_close_buffer_seconds
			)

			#
			# Market-close liquidation takes priority over normal
			# trailing-stop management.
			#
			if in_market_close_window:
				entries_closed_key = (
					"tv:live_trailing_stop:entries_closed:"
					f"{owner_name}:"
					f"{now_et.date().isoformat()}"
				)

				self.r.set(
					entries_closed_key,
					"1",
					ex=60 * 60 * 24 * 3,
				)

				account_is_flat = (
					self.liquidate_live_account_positions(
						owner_name=owner_name,
						alpaca_api=alpaca_api,
						exit_reason=(
							"market_close_liquidation"
						),
					)
				)

				if account_is_flat:
					logger.warning(
						"Market-close liquidation complete: "
						"owner=%r",
						owner_name,
					)

				else:
					logger.warning(
						"Market-close liquidation still in progress; "
						"will retry next monitor iteration: "
						"owner=%r seconds_to_close=%r",
						owner_name,
						seconds_to_close,
					)

				return

			#
			# Normal trailing-stop management should run only while
			# the market is open.
			#
			if not market_is_open:
				return

			pattern = (
				"tv:live_trailing_stop:managed:"
				f"{owner_name}:*"
			)

			for managed_key in self.r.scan_iter(
				pattern
			):
				try:
					managed_state = self.r.hgetall(
						managed_key
					)

					symbol = str(
						managed_state.get(
							"ticker",
							"",
						)
						or ""
					).upper().strip()

					anchor_tf = (
						self.tvw_helpers.normalize_tf(
							managed_state.get(
								"anchor_tf",
								"",
							)
						)
					)

					entry_order_id = str(
						managed_state.get(
							"entry_order_id",
							"",
						)
						or ""
					).strip()

					if not symbol or not anchor_tf:
						logger.warning(
							"Invalid trailing-stop managed state: "
							"owner=%r key=%r state=%r",
							owner_name,
							managed_key,
							managed_state,
						)
						continue

					#
					# Do not initialize or modify trailing protection
					# while the entry order is still being filled.
					#
					if (
						entry_order_id
						and self._entry_order_is_still_pending(
							alpaca_api,
							entry_order_id,
						)
					):
						logger.info(
							"Live trailing-stop management deferred "
							"because entry order is still pending: "
							"owner=%r ticker=%r "
							"entry_order_id=%r",
							owner_name,
							symbol,
							entry_order_id,
						)
						continue

					entry_decision_time_raw = str(
						managed_state.get(
							"entry_decision_time",
							"",
						)
						or ""
					).strip()

					try:
						entry_decision_time = (
							datetime.fromisoformat(
								entry_decision_time_raw
							)
						)

					except Exception:
						logger.warning(
							"Invalid entry decision time; using "
							"registration time instead: "
							"owner=%r ticker=%r value=%r",
							owner_name,
							symbol,
							entry_decision_time_raw,
						)

						registered_at_raw = str(
							managed_state.get(
								"registered_at",
								"",
							)
							or ""
						).strip()

						try:
							entry_decision_time = (
								datetime.fromisoformat(
									registered_at_raw
								)
							)

						except Exception:
							entry_decision_time = now_et

					if entry_decision_time.tzinfo is None:
						entry_decision_time = (
							entry_decision_time.replace(
								tzinfo=(
									self.tvw_helpers.eastern_tz
								)
							)
						)

					else:
						entry_decision_time = (
							entry_decision_time.astimezone(
								self.tvw_helpers.eastern_tz
							)
						)

					#
					# Confirm that the position exists before fetching
					# prices or invoking the position manager.
					#
					try:
						position = alpaca_api.get_position(
							symbol
						)

					except Exception as exc:
						exception_message = str(
							exc
						).lower()

						if (
							"position does not exist"
							not in exception_message
						):
							logger.exception(
								"Unable to read Alpaca position: "
								"owner=%r ticker=%r",
								owner_name,
								symbol,
							)
							continue

						#
						# A newly submitted entry can exist before Alpaca exposes
						# the resulting position. Recheck the registered entry order
						# before treating the registration as stale.
						#
						entry_order_status = ""

						if entry_order_id:
							try:
								entry_order = alpaca_api.get_order(
									entry_order_id
								)

								entry_order_status = str(
									getattr(
										entry_order,
										"status",
										"",
									)
									or ""
								).lower()

							except Exception:
								logger.exception(
									"Unable to verify entry order before removing "
									"trailing-stop registration: "
									"owner=%r ticker=%r entry_order_id=%r",
									owner_name,
									symbol,
									entry_order_id,
								)

								#
								# Fail safe: retain the registration if Alpaca order
								# state cannot be verified.
								#
								continue

						if entry_order_status in {
							"accepted",
							"new",
							"pending_new",
							"partially_filled",
							"pending_replace",
							"pending_cancel",
						}:
							logger.info(
								"Keeping trailing-stop registration because entry "
								"order is still pending: owner=%r ticker=%r "
								"entry_order_id=%r status=%r",
								owner_name,
								symbol,
								entry_order_id,
								entry_order_status,
							)
							continue

						registered_at_raw = str(
							managed_state.get(
								"registered_at",
								"",
							)
							or ""
						).strip()

						registration_age_seconds = float(
							"inf"
						)

						if registered_at_raw:
							try:
								registered_at = datetime.fromisoformat(
									registered_at_raw
								)

								if registered_at.tzinfo is None:
									registered_at = registered_at.replace(
										tzinfo=self.tvw_helpers.eastern_tz
									)

								else:
									registered_at = registered_at.astimezone(
										self.tvw_helpers.eastern_tz
									)

								registration_age_seconds = (
									now_et
									- registered_at
								).total_seconds()

							except Exception:
								logger.warning(
									"Unable to parse trailing-stop registration "
									"time: owner=%r ticker=%r registered_at=%r",
									owner_name,
									symbol,
									registered_at_raw,
								)

						if (
							entry_order_status == "filled"
							and registration_age_seconds < 60
						):
							logger.info(
								"Keeping trailing-stop registration during post-fill "
								"position visibility grace period: "
								"owner=%r ticker=%r entry_order_id=%r "
								"registration_age_seconds=%r",
								owner_name,
								symbol,
								entry_order_id,
								registration_age_seconds,
							)						
							continue
						logger.info(
							"Removing trailing-stop registration because Alpaca "
							"confirms position is flat and the registered entry "
							"is no longer pending: owner=%r ticker=%r "
							"entry_order_id=%r entry_order_status=%r",
							owner_name,
							symbol,
							entry_order_id,
							entry_order_status,
						)

						self.r.delete(
							managed_key
						)

						self.r.delete(
							self._live_trailing_stop_state_key(
								owner_name,
								symbol,
							)
						)

						continue

					raw_qty = float(
						getattr(
							position,
							"qty",
							0.0,
						)
						or 0.0
					)

					if raw_qty == 0:
						logger.info(
							"Removing trailing-stop registration "
							"because Alpaca position quantity is "
							"zero: owner=%r ticker=%r",
							owner_name,
							symbol,
						)

						self.r.delete(
							managed_key
						)

						self.r.delete(
							self._live_trailing_stop_state_key(
								owner_name,
								symbol,
							)
						)

						continue

					prices = (
						self.trade_records.get_market_prices(
							[symbol],
							alpaca_api,
						)
					)

					current_price = (
						prices.get(
							symbol,
							{},
						).get(
							"market"
						)
					)

					if (
						current_price is None
						or float(
							current_price
						) <= 0
					):
						logger.warning(
							"Trailing-stop management skipped due "
							"to invalid price: owner=%r ticker=%r "
							"price=%r",
							owner_name,
							symbol,
							current_price,
						)
						continue

					#
					# Keep each symbol isolated. An Alpaca rejection or
					# unexpected failure for one position must not stop
					# management of the remaining positions.
					#
					try:
						self.manage_live_trailing_stop_position(
							owner_name=owner_name,
							ticker=symbol,
							anchor_tf=anchor_tf,
							alpaca_api=alpaca_api,
							current_price=float(
								current_price
							),
							atr_period=atr_period,
							atr_multiplier=atr_multiplier,
							loss_liquidation_atr_factor=(
								loss_liquidation_atr_factor
							),
							profit_expansion_atr_factor=(
								profit_expansion_atr_factor
							),
							trailing_stop_multiplier_factor=(
								trailing_stop_multiplier_factor
							),
							use_trailing_stop=use_trailing_stop,
							use_profit_expansion=use_profit_expansion,
							now_et=now_et,
							entry_decision_time=(
								entry_decision_time
							),
						)						
						"""self.manage_live_trailing_stop_position(
							owner_name=owner_name,
							ticker=symbol,
							anchor_tf=anchor_tf,
							alpaca_api=alpaca_api,
							current_price=float(
								current_price
							),
							atr_period=atr_period,
							atr_multiplier=atr_multiplier,
							loss_liquidation_atr_factor=(
								loss_liquidation_atr_factor
							),
							profit_expansion_atr_factor=(
								profit_expansion_atr_factor
							),
							trailing_stop_multiplier_factor=(
								trailing_stop_multiplier_factor
							),
							now_et=now_et,
							entry_decision_time=(
								entry_decision_time
							),
						)"""

					except Exception:
						logger.exception(
							"Live trailing-stop position management "
							"failed: owner=%r ticker=%r",
							owner_name,
							symbol,
						)
						continue

				except Exception:
					logger.exception(
						"Unexpected failure processing trailing-stop "
						"registration: owner=%r key=%r",
						owner_name,
						managed_key,
					)
					continue

		finally:
			try:
				manager_lock.release()

			except Exception:
				logger.exception(
					"Unable to release trailing-stop manager lock: "
					"owner=%r",
					owner_name,
				)


	def live_trailing_stop_entry_is_blocked(
		self,
		*,
		owner_name: str,
		ticker: str,
		alpaca_api,
	) -> bool:
		"""
		Match Exit Strategy 3 backtest behavior by allowing at most one
		active or pending position per ticker.
		"""
		symbol = str(
			ticker or ""
		).upper().strip()

		try:
			position = alpaca_api.get_position(
				symbol
			)

			position_qty = abs(
				float(
					getattr(
						position,
						"qty",
						0.0,
					)
					or 0.0
				)
			)

			if position_qty > 0:
				return True

		except Exception:
			pass

		managed_key = (
			self._live_trailing_stop_managed_key(
				owner_name,
				symbol,
			)
		)

		managed_state = self.r.hgetall(
			managed_key
		)

		if not managed_state:
			return False

		entry_order_id = managed_state.get(
			"entry_order_id",
			"",
		)

		return self._entry_order_is_still_pending(
			alpaca_api,
			entry_order_id,
		)


	def live_trailing_stop_entries_closed_for_day(
		self,
		*,
		owner_name: str,
		now_et: datetime,
	) -> bool:
		key = (
			"tv:live_trailing_stop:entries_closed:"
			f"{owner_name}:"
			f"{now_et.date().isoformat()}"
		)

		return bool(
			self.r.exists(
				key
			)
		)


	def liquidate_live_account_positions(
		self,
		*,
		owner_name: str,
		alpaca_api,
		exit_reason: str,
	) -> bool:
		"""
		Request liquidation of every position currently reported by Alpaca.

		Returns True only when Alpaca reports that the account is already flat.
		Otherwise, submits or maintains closing requests and returns False so the
		next manager iteration can reconcile again.
		"""
		try:
			positions = list(
				alpaca_api.list_positions()
			)

		except Exception:
			logger.exception(
				"Unable to list Alpaca positions during "
				"account liquidation: owner=%r",
				owner_name,
			)
			return False

		if not positions:
			logger.warning(
				"Live account liquidation confirmed flat: "
				"owner=%r reason=%r",
				owner_name,
				exit_reason,
			)
			return True

		try:
			open_orders = list(
				alpaca_api.list_orders(
					status="open",
					limit=500,
				)
			)

		except Exception:
			logger.exception(
				"Unable to list open orders during "
				"account liquidation: owner=%r",
				owner_name,
			)
			return False

		orders_by_symbol = {}

		for order in open_orders:
			symbol = str(
				getattr(
					order,
					"symbol",
					"",
				)
				or ""
			).upper().strip()

			if not symbol:
				continue

			orders_by_symbol.setdefault(
				symbol,
				[],
			).append(
				order
			)

		for position in positions:
			symbol = str(
				getattr(
					position,
					"symbol",
					"",
				)
				or ""
			).upper().strip()

			raw_qty = float(
				getattr(
					position,
					"qty",
					0.0,
				)
				or 0.0
			)

			if (
				not symbol
				or raw_qty == 0
			):
				continue

			symbol_orders = orders_by_symbol.get(
				symbol,
				[],
			)

			# Cancel trailing stops and any other existing orders that could
			# conflict with a full-position market close.
			for order in symbol_orders:
				order_id = str(
					getattr(
						order,
						"id",
						"",
					)
					or ""
				).strip()

				if not order_id:
					continue

				try:
					alpaca_api.cancel_order(
						order_id
					)

					logger.warning(
						"Canceled open order before account "
						"liquidation: owner=%r ticker=%r "
						"order_id=%r type=%r",
						owner_name,
						symbol,
						order_id,
						getattr(
							order,
							"type",
							"",
						),
					)

				except Exception:
					logger.exception(
						"Unable to cancel order before account "
						"liquidation: owner=%r ticker=%r "
						"order_id=%r",
						owner_name,
						symbol,
						order_id,
					)

			# Do not submit a second closing order in the same iteration.
			# The next monitor iteration will inspect the remaining position.
			if symbol_orders:
				continue

			try:
				order = alpaca_api.close_position(
					symbol
				)

				logger.warning(
					"Account liquidation submitted: "
					"owner=%r ticker=%r qty=%r "
					"order_id=%r reason=%r",
					owner_name,
					symbol,
					raw_qty,
					getattr(
						order,
						"id",
						"",
					),
					exit_reason,
				)

			except Exception:
				logger.exception(
					"Account liquidation submission failed: "
					"owner=%r ticker=%r qty=%r "
					"reason=%r",
					owner_name,
					symbol,
					raw_qty,
					exit_reason,
				)

		return False
