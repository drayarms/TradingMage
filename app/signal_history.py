import logging


logger = logging.getLogger("tv-webhook")


class RedisSignalHistory:
	"""Read-only signal history backed by TradingView Redis streams."""

	def __init__(self, trading_view_webhook_helpers):
		self.tvw_helpers = trading_view_webhook_helpers
		self.r = trading_view_webhook_helpers.require_redis()

	def get_recent_alerts(self, ticker, timeframe, max_scan=500):
		sym = str(ticker or "").upper().strip()
		tf = self.tvw_helpers.normalize_tf(timeframe)

		if not sym or not tf:
			return []

		stream_key = self.tvw_helpers.stream_key(tf, sym)

		try:
			return self.r.xrevrange(stream_key, count=max_scan)
		except Exception:
			logger.exception(
				"Failed reading signal history: ticker=%r tf=%r",
				sym,
				tf,
			)
			return []

	def get_recent_alerts_through_current_event(
		self,
		ticker,
		timeframe,
		max_scan=500,
	):
		return self.get_recent_alerts(
			ticker=ticker,
			timeframe=timeframe,
			max_scan=max_scan,
		)

	def get_nth_last_alert(self, ticker, timeframe, n=1):
		if n <= 0:
			return None

		entries = self.get_recent_alerts(
			ticker=ticker,
			timeframe=timeframe,
			max_scan=n,
		)

		if len(entries) < n:
			return None

		return entries[n - 1]

	def get_latest_directional_signal(
		self,
		ticker,
		timeframe,
		signal_role,
		max_scan=100,
	):
		expected_signal_role = str(signal_role or "").strip().lower()

		for entry_id, fields in self.get_recent_alerts(
			ticker=ticker,
			timeframe=timeframe,
			max_scan=max_scan,
		):
			entry_signal_role = str(
				fields.get("signal_role")
				or ""
			).strip().lower()

			if (
				expected_signal_role
				and entry_signal_role != expected_signal_role
			):
				continue

			side = self.tvw_helpers.normalize_signal(
				fields.get("signal")
			)

			if side in {"buy", "sell"}:
				return {
					"id": entry_id,
					"side": side,
					"signal_role": entry_signal_role,
					"fields": fields,
				}

		return None

	def get_latest_confirmation_directional_signal(
		self,
		ticker,
		timeframe,
		max_scan=500,
	):
		for entry_id, fields in self.get_recent_alerts(
			ticker=ticker,
			timeframe=timeframe,
			max_scan=max_scan,
		):
			signal_role = str(
				fields.get("signal_role")
				or ""
			).strip().lower()

			if signal_role != "confirmation":
				continue

			side = self.tvw_helpers.normalize_signal(
				fields.get("signal")
			)

			if side in {"buy", "sell"}:
				return {
					"id": entry_id,
					"side": side,
					"fields": fields,
				}

		return None


class SimSignalHistory:
	"""Read-only signal history backed by one BackTester SimState."""

	def __init__(self, trading_view_webhook_helpers, state):
		self.tvw_helpers = trading_view_webhook_helpers
		self.state = state

	def _events(self, ticker, timeframe):
		sym = str(ticker or "").upper().strip()
		tf = self.tvw_helpers.normalize_tf(timeframe)

		return self.state.all_events_by_ticker_tf.get(
			(sym, tf),
			[],
		)

	def get_recent_alerts(self, ticker, timeframe, max_scan=500):
		events = self._events(
			ticker,
			timeframe,
		)

		return [
			(
				event["stream_id"],
				event["raw_fields"],
			)
			for event in reversed(events[-max_scan:])
		]

	def get_recent_alerts_through_current_event(
		self,
		ticker,
		timeframe,
		max_scan=500,
	):
		"""
		Return the most recent simulated alerts through the current
		chronological simulation event.

		SimState.all_events_by_ticker_tf contains only events that have
		already been registered by the backtest. Therefore no Redis query
		is required here, and future events cannot be observed.
		"""
		return self.get_recent_alerts(
			ticker=ticker,
			timeframe=timeframe,
			max_scan=max_scan,
		)

	def get_nth_last_alert(self, ticker, timeframe, n=1):
		if n <= 0:
			return None

		events = self._events(
			ticker,
			timeframe,
		)

		if len(events) < n:
			return None

		event = events[-n]

		return (
			event["stream_id"],
			event["raw_fields"],
		)

	def get_latest_directional_signal(
		self,
		ticker,
		timeframe,
		signal_role,
		max_scan=100,
	):
		expected_signal_role = str(
			signal_role
			or ""
		).strip().lower()

		scanned = 0

		for event in reversed(
			self._events(
				ticker,
				timeframe,
			)
		):
			if scanned >= max_scan:
				break

			scanned += 1

			entry_signal_role = str(
				event.get("signal_role")
				or ""
			).strip().lower()

			if (
				expected_signal_role
				and entry_signal_role != expected_signal_role
			):
				continue

			side = event.get("side")

			if side in {"buy", "sell"}:
				return {
					"id": event["stream_id"],
					"side": side,
					"signal_role": entry_signal_role,
					"fields": event["raw_fields"],
					"event": event,
				}

		return None

	def get_latest_confirmation_directional_signal(
		self,
		ticker,
		timeframe,
		max_scan=500,
	):
		scanned = 0

		for event in reversed(
			self._events(
				ticker,
				timeframe,
			)
		):
			if scanned >= max_scan:
				break

			scanned += 1

			if event.get("signal_role") != "confirmation":
				continue

			side = event.get("side")

			if side in {"buy", "sell"}:
				return {
					"id": event["stream_id"],
					"side": side,
					"fields": event["raw_fields"],
					"signal_role": event["signal_role"],
					"event": event,
				}

		return None
