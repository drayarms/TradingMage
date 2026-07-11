import io
import logging
import math
import statistics
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from typing import Any, Optional

import matplotlib.pyplot as plt

logger = logging.getLogger("tv-webhook")


@dataclass
class SimPosition:
	"""In-memory position state for one ticker during a single backtest run."""
	ticker: str
	side: str
	avg_price_per_share: float
	num_shares: float
	realized_pnl: float = 0.0
	entry_sequence_count: int = 1


@dataclass
class SimState:
	"""Container for all mutable in-memory state created during one backtest run."""
	positions: dict[str, SimPosition] = field(default_factory=dict)
	realized_by_ticker: dict[str, float] = field(default_factory=dict)
	last_price_by_ticker: dict[str, float] = field(default_factory=dict)
	latest_directional: dict[tuple[str, str], dict[str, Any]] = field(default_factory=dict)
	latest_by_tf: dict[tuple[str, str], dict[str, Any]] = field(default_factory=dict)
	all_events_by_ticker_tf: dict[tuple[str, str], list[dict[str, Any]]] = field(default_factory=dict)
	overall_pnl_history: list[dict[str, Any]] = field(default_factory=list)
	ticker_pnl_history: dict[str, list[dict[str, Any]]] = field(default_factory=dict)
	daily_max_exposure: dict[str, float] = field(default_factory=dict)
	trade_events: list[dict[str, Any]] = field(default_factory=list)


class BackTester:
	"""Run isolated in-memory backtests from TradingView signals already stored in Redis."""

	STRATEGY_CONFIGS = {
		"strategy1_15m_anchor": {
			"entry_tf": "1m",
			"intermediary_tf": "5m",
			"anchor_tf": "15m",
			"lower_timeframes": {"1m"},
			"default_position_size": 2000.0,
		},
		"strategy1_1h_anchor": {
			"entry_tf": "5m",
			"intermediary_tf": "15m",
			"anchor_tf": "1h",
			"lower_timeframes": {"1m", "5m"},
			"default_position_size": 6000.0,
		},
		"strategy1_4h_anchor": {
			"entry_tf": "15m",
			"intermediary_tf": "1h",
			"anchor_tf": "4h",
			"lower_timeframes": {"1m", "5m", "15m"},
			"default_position_size": 20000.0,
		},

		"strategy2_15m_anchor": {
			"entry_tf": "1m",
			"intermediary_tf": "5m",
			"anchor_tf": "15m",
			"lower_timeframes": {"1m"},
			"default_position_size": 2000.0,
		},
		"strategy2_1h_anchor": {
			"entry_tf": "5m",
			"intermediary_tf": "15m",
			"anchor_tf": "1h",
			"lower_timeframes": {"1m", "5m"},
			"default_position_size": 6000.0,
		},
		"strategy2_4h_anchor": {
			"entry_tf": "15m",
			"intermediary_tf": "1h",
			"anchor_tf": "4h",
			"lower_timeframes": {"1m", "5m", "15m"},
			"default_position_size": 20000.0,
		},		
	}

	TIMEFRAME_ORDER = {
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

	BACKTEST_DIAGNOSTIC_MAX_DAYS = 3

	def __init__(self, trading_view_webhook_helpers, strategies_instance, trade_records_instance):
		"""
		Create a BackTester.

		Parameters:
			trading_view_webhook_helpers: Existing helper instance used for Redis access,
			timeframe normalization, timestamp parsing, and safe float conversion.
		"""
		self.tvw_helpers = trading_view_webhook_helpers
		self.strategies_instance = strategies_instance
		self.trade_records_instance = trade_records_instance
		self.r = trading_view_webhook_helpers.require_redis()
		self.smallest_share_size = 0.25
		self.diagnostic_logging_enabled = False
		self.recording_enabled = True

	def run(
		self,
		strategy_name: str,
		start: str,
		end: str,
		tickers: Optional[list[str]] = None,
		position_size: Optional[float] = None,
		warmup_days: int = 2,
	) -> dict[str, Any]:
		"""
		Run a chronological simulation with an optional unreported warm-up period.

		Warm-up events are fully processed so signal context and any positions that
		remain open at ``start`` carry into the requested backtest window. Warm-up
		trades, realized PnL, snapshots, exposure rows, and diagnostic trade records
		are excluded from the returned result.

		Parameters:
			strategy_name: Strategy config name, such as "strategy1_15m_anchor".
			start: Inclusive reporting start datetime. Naive datetimes are Eastern Time.
			end: Inclusive reporting end datetime. Naive datetimes are Eastern Time.
			tickers: Optional list of ticker symbols. If omitted, discover from Redis.
			position_size: Optional dollar notional used to size the first entry signal.
			warmup_days: Calendar days to simulate before ``start``. Defaults to 2.

		Returns:
			Dictionary containing only the requested reporting window, while preserving
			open positions and signal context created during warm-up.
		"""
		config = self._get_strategy_config(strategy_name)
		start_dt = self._parse_input_dt(start)
		end_dt = self._parse_input_dt(end)

		if start_dt > end_dt:
			raise ValueError("start must be <= end")

		try:
			warmup_days = int(warmup_days)
		except Exception as exc:
			raise ValueError("warmup_days must be an integer") from exc

		if warmup_days < 0:
			raise ValueError("warmup_days must be >= 0")

		self.diagnostic_logging_enabled = (
			(end_dt - start_dt) <= timedelta(days=self.BACKTEST_DIAGNOSTIC_MAX_DAYS)
		)

		if position_size is None:
			position_size = float(config["default_position_size"])
		else:
			position_size = float(position_size)

		if position_size <= 0:
			raise ValueError("position_size must be > 0")

		warmup_start_dt = start_dt - timedelta(days=warmup_days)
		state = SimState()
		timeframes = self._strategy_timeframes(config)
		symbols = self._normalize_tickers(tickers) or self._discover_tickers(timeframes)
		all_events = self._load_signal_events(
			strategy_name,
			symbols,
			timeframes,
			warmup_start_dt,
			end_dt,
		)

		warmup_events = [event for event in all_events if event["received_dt"] < start_dt]
		report_events = [event for event in all_events if event["received_dt"] >= start_dt]

		# Warm-up phase: update context and simulate positions, but do not register
		# trades, trade diagnostics, PnL history, or exposure.
		self.recording_enabled = False
		for event in warmup_events:
			self._register_event_context(state, event)
			self._process_event(strategy_name, state, config, event, position_size)

		# Preserve open positions and signal history, but start reported accounting
		# from zero at the user's requested start time.
		self._reset_reporting_state(state)

		# Reporting phase.
		self.recording_enabled = True
		for event in report_events:
			self._register_event_context(state, event)
			self._process_event(strategy_name, state, config, event, position_size)
			self._record_snapshots(state, event["received_dt"])

		self._print_daily_max_open_exposure_table(strategy_name, state.daily_max_exposure)

		return {
			"strategy_name": strategy_name,
			"start": start_dt.isoformat(),
			"end": end_dt.isoformat(),
			"warmup_start": warmup_start_dt.isoformat(),
			"warmup_days": warmup_days,
			"tickers": symbols,
			"warmup_signal_count": len(warmup_events),
			"signal_count": len(report_events),
			"trade_count": len(state.trade_events),
			"overall_pnl_history": state.overall_pnl_history,
			"ticker_pnl_history": state.ticker_pnl_history,
			"daily_max_open_exposure": self._daily_exposure_rows(state.daily_max_exposure),
			"daily_max_open_exposure_summary": self._daily_exposure_summary(state.daily_max_exposure),
			"trade_events": state.trade_events,
		}

	def _process_event(
		self,
		strategy_name: str,
		state: SimState,
		config: dict[str, Any],
		event: dict[str, Any],
		position_size: float,
	) -> None:
		"""Dispatch one event to the configured strategy family."""
		if strategy_name.startswith("strategy1_"):
			self._process_strategy1_event(strategy_name, state, config, event, position_size)
		elif strategy_name.startswith("strategy2_"):
			self._process_strategy2_event(strategy_name, state, config, event, position_size)
		else:
			raise ValueError(f"Unsupported strategy family: {strategy_name}")

	def _reset_reporting_state(self, state: SimState) -> None:
		"""
		Clear warm-up accounting while preserving signal context and open positions.

		An open position created during warm-up remains open with its original average
		entry price. Any PnL realized before the requested start is discarded so the
		returned PnL begins at zero.
		"""
		state.realized_by_ticker.clear()
		state.overall_pnl_history.clear()
		state.ticker_pnl_history.clear()
		state.daily_max_exposure.clear()
		state.trade_events.clear()

		for position in state.positions.values():
			position.realized_pnl = 0.0

	def plot_overall_pnl(self, result: dict[str, Any], title: Optional[str] = None) -> io.BytesIO:
		"""
		Render the aggregate running PnL history from a backtest result as a PNG image.

		Parameters:
			result: Dictionary returned by run().
			title: Optional chart title.

		Returns:
			BytesIO buffer positioned at the beginning of the PNG image.
		"""
		history = result.get("overall_pnl_history") or []
		if not history:
			raise ValueError("No PnL history available to plot")

		x_values = [datetime.fromisoformat(row["time"]) for row in history]
		y_values = [float(row["overall_total_pnl"]) for row in history]

		fig, ax = plt.subplots(figsize=(12, 6))
		ax.plot(x_values, y_values)
		ax.set_title(title or f"Backtest Overall PnL - {result.get('strategy_name')}")
		ax.set_xlabel("Time")
		ax.set_ylabel("Overall PnL ($)")
		ax.grid(True, alpha=0.3)
		fig.autofmt_xdate()

		buf = io.BytesIO()
		fig.tight_layout()
		fig.savefig(buf, format="png")
		plt.close(fig)
		buf.seek(0)
		return buf

	def _get_strategy_config(self, strategy_name: str) -> dict[str, Any]:
		"""Return a normalized strategy configuration or raise ValueError if unknown."""
		name = str(strategy_name or "").strip()
		if name not in self.STRATEGY_CONFIGS: # Iterate through the keys in STRATEGY_CONFIGS dict
			raise ValueError(f"Unsupported backtest strategy: {name}")
		config = dict(self.STRATEGY_CONFIGS[name])
		config["entry_tf"] = self.tvw_helpers.normalize_tf(config["entry_tf"])
		config["intermediary_tf"] = self.tvw_helpers.normalize_tf(config["intermediary_tf"])
		config["anchor_tf"] = self.tvw_helpers.normalize_tf(config["anchor_tf"])
		config["lower_timeframes"] = {self.tvw_helpers.normalize_tf(tf) for tf in config["lower_timeframes"]}
		return config

	def _strategy_timeframes(self, config: dict[str, Any]) -> set[str]:
		"""Return every timeframe whose Redis stream can influence the strategy."""
		return set(config["lower_timeframes"]) | {config["entry_tf"], config["intermediary_tf"], config["anchor_tf"]}

	def _parse_input_dt(self, value: str) -> datetime:
		"""Parse an ISO datetime and normalize it to timezone-aware Eastern Time."""
		if not value:
			raise ValueError("start and end are required")
		dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
		if dt.tzinfo is None:
			dt = dt.replace(tzinfo=self.tvw_helpers.eastern_tz)
		return dt.astimezone(self.tvw_helpers.eastern_tz)

	def _normalize_tickers(self, tickers: Optional[list[str]]) -> list[str]:
		"""Normalize optional ticker input into sorted uppercase symbols."""
		if not tickers:
			return []
		return sorted({str(t).upper().strip() for t in tickers if str(t or "").strip()})

	def _discover_tickers(self, timeframes: set[str]) -> list[str]:
		"""Discover symbols by scanning Redis signal stream keys for the required timeframes."""
		symbols = set()
		for tf in timeframes:
			pattern = f"tv:stream:{tf}:*"
			for key in self.r.scan_iter(pattern):
				parts = str(key).split(":")
				if len(parts) >= 4:
					symbols.add(parts[-1].upper().strip())
		return sorted(symbols)

	def _load_signal_events(self, strategy_name: str, symbols: list[str], timeframes: set[str], start_dt: datetime, end_dt: datetime) -> list[dict[str, Any]]:
		"""Load Redis stream alerts and order them by when they were received."""
		events = []
		for symbol in symbols:
			for tf in timeframes:
				stream_key = self.tvw_helpers.stream_key(tf, symbol)
				for stream_id, fields in self.r.xrange(stream_key, min="-", max="+"):
					event = self._build_event(strategy_name, stream_id, fields, symbol, tf)
					if event is None:
						continue
					if start_dt <= event["received_dt"] <= end_dt:
						events.append(event)

		# received_dt reproduces live arrival order. Redis stream_id is used only
		# as a deterministic tie-breaker when two events have the same timestamp.
		return sorted(events, key=lambda e: (e["received_dt"], e["stream_id"]))

	def _build_event(self, strategy_name: str, stream_id: str, fields: dict[str, Any], fallback_symbol: str, fallback_tf: str) -> Optional[dict[str, Any]]:
		"""Convert a raw Redis stream entry into a normalized signal event dictionary."""
		bar_close_str = fields.get("bar_close_time_eastern")
		received_str = fields.get("received_at")

		def parse_timestamp(value: Any) -> Optional[datetime]:
			if not value:
				return None
			dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
			if dt.tzinfo is None:
				dt = dt.replace(tzinfo=self.tvw_helpers.eastern_tz)
			return dt.astimezone(self.tvw_helpers.eastern_tz)

		try:
			bar_close_dt = parse_timestamp(bar_close_str)
			received_dt = parse_timestamp(received_str)
		except Exception:
			logger.exception(
				"Skipping backtest signal with invalid timestamp "
				"(bar_close_time_eastern=%r, received_at=%r)",
				bar_close_str,
				received_str,
			)
			return None

		if bar_close_dt is None and received_dt is None:
			return None

		strategy_dt = bar_close_dt or received_dt
		ordering_dt = received_dt or bar_close_dt

		ticker = str(fields.get("symbol") or fallback_symbol).upper().strip()
		tf = self.tvw_helpers.normalize_tf(fields.get("timeframe") or fallback_tf)
		close_price = self.tvw_helpers.safe_float(fields.get("close"))
		market_price = close_price or self.tvw_helpers.safe_float(fields.get("price"))
		if market_price is None or market_price <= 0:
			return None

		return {
			"strategy_name": strategy_name,
			"stream_id": stream_id,
			"ticker": ticker,
			"timeframe": tf,
			"dt": strategy_dt,
			"received_dt": ordering_dt,
			"sort_dt": ordering_dt,
			"time": strategy_dt.isoformat(),
			"received_time": ordering_dt.isoformat(),
			"signal": str(fields.get("signal") or "").strip().lower(),
			"side": self.tvw_helpers.normalize_signal(fields.get("signal")),
			"signal_role": str(fields.get("signal_role") or "").strip().lower(),
			"open": self.tvw_helpers.safe_float(fields.get("open")),
			"high": self.tvw_helpers.safe_float(fields.get("high")),
			"low": self.tvw_helpers.safe_float(fields.get("low")),
			"close": close_price,
			"price": market_price,
			"raw_fields": fields,
		}

	def _register_event_context(self, state: SimState, event: dict[str, Any]) -> None:
		"""Update in-memory signal context before evaluating the current event."""
		key = (event["ticker"], event["timeframe"])
		state.latest_by_tf[key] = event
		state.last_price_by_ticker[event["ticker"]] = event["price"]
		state.all_events_by_ticker_tf.setdefault(key, []).append(event)
		if event["signal_role"] == "confirmation" and event["side"] in {"buy", "sell"}:
			state.latest_directional[key] = event


	def _process_strategy1_event(self, strategy_name, state: SimState, config: dict[str, Any], event: dict[str, Any], position_size: float) -> None:
		"""Apply Strategy 1 entry and exit rules to one chronological signal event."""		
		now_et = event["dt"] 
		signal = event["side"] 
		symbol = event["ticker"]
		tf = event["timeframe"]
		market_price = event["price"]
		NUM_SHARES = position_size / market_price

		self.strategies_instance.exit_strategy1(
			strategy_name,
			config["lower_timeframes"],
			config["intermediary_tf"],
			config["anchor_tf"],			
			True,
			now_et,
			signal,
			None,
			symbol,
			tf,
			None,
			state,
			config,
			event,
			market_price,
			self			
		)

		self.strategies_instance.entry_strategy1(
			strategy_name,
			config["entry_tf"],
			config["intermediary_tf"],
			config["anchor_tf"],
			True,
			now_et,
			signal,
			None,
			symbol,
			tf,
			NUM_SHARES,
			None,
			state,
			config,
			event,
			market_price,
			self
		)

	def _process_strategy2_event(self, strategy_name, state: SimState, config: dict[str, Any], event: dict[str, Any], position_size: float) -> None:
		now_et = event["dt"]
		signal = event["signal"]
		symbol = event["ticker"]
		tf = event["timeframe"]
		market_price = event["price"]
		NUM_SHARES = position_size / market_price

		self.strategies_instance.exit_strategy2(
			strategy_name,
			config["entry_tf"],
			True,
			now_et,
			signal,
			None,
			symbol,
			tf,
			None,
			state,
			config,
			event,
			market_price,
			self,
		)

		self.strategies_instance.entry_strategy2(
			strategy_name,
			config["entry_tf"],
			config["intermediary_tf"],
			True,
			now_et,
			signal,
			None,
			symbol,
			tf,
			NUM_SHARES,
			None,
			state,
			config,
			event,
			market_price,
			self,
		)

		"""self.strategies_instance.entry_strategy1(
			strategy_name,
			config["entry_tf"],
			config["intermediary_tf"],
			config["anchor_tf"],
			True,
			now_et,
			signal,
			None,
			symbol,
			tf,
			NUM_SHARES,
			None,
			state,
			config,
			event,
			market_price,
			self
		)"""		








	def _process_experimental_strategy(self, strategy_name, state: SimState, config: dict[str, Any], event: dict[str, Any], position_size: float) -> None:
		now_et = event["dt"]
		signal = event["signal"]
		symbol = event["ticker"]
		tf = event["timeframe"]
		market_price = event["price"]
		NUM_SHARES = position_size / market_price

		self.strategies_instance.exit_strategy2(
			strategy_name,
			config["entry_tf"],
			True,
			now_et,
			signal,
			None,
			symbol,
			tf,
			None,
			state,
			config,
			event,
			market_price,
			self,
		)

		self.strategies_instance.entry_strategy1(
			strategy_name,
			config["entry_tf"],
			config["intermediary_tf"],
			True,
			now_et,
			signal,
			None,
			symbol,
			tf,
			NUM_SHARES,
			None,
			state,
			config,
			event,
			market_price,
			self,
		)



	def get_nth_last_alert(self, state: SimState, ticker: str, timeframe: str, n: int = 1):
		"""
		Return the nth most recent simulated alert for ticker/timeframe using only
		events already processed in the current backtest.

		Returns:
			Tuple of (stream_id, raw_fields) to match TradingViewWebhookHelpers.get_nth_last_alert().
		"""
		if n <= 0:
			return None

		sym = str(ticker or "").upper().strip()
		tf = self.tvw_helpers.normalize_tf(timeframe)

		events = state.all_events_by_ticker_tf.get((sym, tf), [])
		if len(events) < n:
			return None

		event = events[-n]
		return event["stream_id"], event["raw_fields"]


	def get_latest_directional_signal(
		self,
		state: SimState,
		ticker: str,
		timeframe: str,
		signal_role: str,
		max_scan: int = 100,
	):
		"""
		Return the latest simulated directional signal for ticker/timeframe,
		filtered by signal_role, using only already-processed backtest events.

		This mirrors Strategies.get_latest_directional_signal().
		"""
		sym = str(ticker or "").upper().strip()
		tf = self.tvw_helpers.normalize_tf(timeframe)
		expected_signal_role = str(signal_role or "").strip().lower()

		events = state.all_events_by_ticker_tf.get((sym, tf), [])
		scanned = 0

		for event in reversed(events):
			if scanned >= max_scan:
				break

			scanned += 1

			entry_signal_role = str(event.get("signal_role") or "").strip().lower()

			if expected_signal_role and entry_signal_role != expected_signal_role:
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
		state: SimState,
		ticker: str,
		timeframe: str,
		max_scan: int = 500,
	):
		"""
		Return the latest simulated confirmation directional signal for ticker/timeframe
		using only events already processed in the current backtest.

		Returns:
			Dict shaped like Strategies.get_latest_confirmation_directional_signal().
		"""
		sym = str(ticker or "").upper().strip()
		tf = self.tvw_helpers.normalize_tf(timeframe)

		events = state.all_events_by_ticker_tf.get((sym, tf), [])
		scanned = 0

		for event in reversed(events):
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


	def _open_or_add_position(self, state: SimState, event: dict[str, Any], position_side: str, qty: float) -> None:

		now_et = event["dt"]
		if self.tvw_helpers.is_between_8pm_sun_and_8pm_fri_et(now_et):
			"""Open a new position or add to an existing same-side position in memory."""
			ticker = event["ticker"]
			price = event["price"]

			if self.diagnostic_logging_enabled and self.recording_enabled:
				self.trade_records_instance.log_trade_diagnostic(
					source="backtest",
					strategy_name=event.get("strategy_name"),
					ticker=ticker,
					event_type="entry",
					timeframe=event["timeframe"],
					side=position_side,
					requested_qty=qty,
					market_price=price,
					order_id=None,
					decision_time=event.get("dt").isoformat() if event.get("dt") else None,
				)

			existing = state.positions.get(ticker)
			if existing and existing.side != position_side and existing.num_shares > 0:
				self._close_position(state, event)
				existing = None

			if existing and existing.num_shares > 0:
				old_qty = existing.num_shares
				new_qty = old_qty + qty
				existing.avg_price_per_share = ((existing.avg_price_per_share * old_qty) + (price * qty)) / new_qty
				existing.num_shares = new_qty
				existing.entry_sequence_count += 1
				event_type = "add"
			else:
				state.positions[ticker] = SimPosition(ticker=ticker, side=position_side, avg_price_per_share=price, num_shares=qty)
				event_type = "open"

			if self.recording_enabled:
				state.trade_events.append({"time": event["time"], "ticker": ticker, "event_type": event_type, "side": position_side, "price": price, "num_shares": qty, "realized_delta": 0.0})

	def _close_position(self, state: SimState, event: dict[str, Any]) -> None:

		now_et = event["dt"]
		if self.tvw_helpers.is_between_8pm_sun_and_8pm_fri_et(now_et):
			"""Close the current in-memory position and accumulate realized PnL."""
			ticker = event["ticker"]
			position = state.positions.get(ticker)
			if not position or position.num_shares <= 0:
				return
			price = event["price"]

			if self.diagnostic_logging_enabled and self.recording_enabled:
				self.trade_records_instance.log_trade_diagnostic(
					source="backtest",
					strategy_name=event.get("strategy_name"),
					ticker=ticker,
					event_type="exit",
					timeframe=event["timeframe"],
					side=position.side,
					requested_qty=position.num_shares,
					market_price=price,
					order_id=None,
					decision_time=event.get("dt").isoformat() if event.get("dt") else None,
				)

			if position.side == "long":
				realized_delta = (price - position.avg_price_per_share) * position.num_shares
				exit_side = "sell"
			else:
				realized_delta = (position.avg_price_per_share - price) * position.num_shares
				exit_side = "cover"
			state.realized_by_ticker[ticker] = state.realized_by_ticker.get(ticker, 0.0) + realized_delta
			if self.recording_enabled:
				state.trade_events.append({"time": event["time"], "ticker": ticker, "event_type": "close", "side": exit_side, "price": price, "num_shares": position.num_shares, "realized_delta": realized_delta})
			position.num_shares = 0.0
			state.positions.pop(ticker, None)


	def _close_partial_position(self, state: SimState, event: dict[str, Any], qty: float) -> None:

		now_et = event["dt"]
		if self.tvw_helpers.is_between_8pm_sun_and_8pm_fri_et(now_et):
			ticker = event["ticker"]
			position = state.positions.get(ticker)

			if not position or position.num_shares <= 0:
				return

			close_qty = min(float(qty), position.num_shares)
			if close_qty <= 0:
				return

			price = event["price"]

			if self.diagnostic_logging_enabled and self.recording_enabled:
				self.trade_records_instance.log_trade_diagnostic(
					source="backtest",
					strategy_name=event.get("strategy_name"),
					ticker=ticker,
					event_type="exit",
					timeframe=event["timeframe"],
					side=position.side,
					requested_qty=close_qty,
					market_price=price,
					order_id=None,
					decision_time=event.get("dt").isoformat() if event.get("dt") else None,
				)			

			if position.side == "long":
				realized_delta = (price - position.avg_price_per_share) * close_qty
				exit_side = "sell"
			else:
				realized_delta = (position.avg_price_per_share - price) * close_qty
				exit_side = "cover"

			state.realized_by_ticker[ticker] = state.realized_by_ticker.get(ticker, 0.0) + realized_delta

			if self.recording_enabled:
				state.trade_events.append({
					"time": event["time"],
					"ticker": ticker,
					"event_type": "partial_close" if close_qty < position.num_shares else "close",
					"side": exit_side,
					"price": price,
					"num_shares": close_qty,
					"realized_delta": realized_delta,
				})

			position.num_shares -= close_qty

			if position.num_shares < 1:
				state.positions.pop(ticker, None)


	def _record_snapshots(self, state: SimState, current_dt: datetime) -> None:
		"""Record per-ticker and aggregate running PnL plus daily max exposure at the current event time."""
		overall_total = 0.0
		gross_exposure = 0.0
		tickers = sorted(set(state.realized_by_ticker) | set(state.last_price_by_ticker) | set(state.positions))
		for ticker in tickers:
			price = state.last_price_by_ticker.get(ticker)
			realized = state.realized_by_ticker.get(ticker, 0.0)
			unrealized = 0.0
			position = state.positions.get(ticker)
			if position and price is not None:
				gross_exposure += abs(position.num_shares * price)
				if position.side == "long":
					unrealized = (price - position.avg_price_per_share) * position.num_shares
				else:
					unrealized = (position.avg_price_per_share - price) * position.num_shares
			total = realized + unrealized
			overall_total += total
			state.ticker_pnl_history.setdefault(ticker, []).append({"time": current_dt.isoformat(), "ticker": ticker, "realized_pnl": realized, "unrealized_pnl": unrealized, "total_pnl": total})
		state.overall_pnl_history.append({"time": current_dt.isoformat(), "overall_total_pnl": overall_total, "gross_open_exposure": gross_exposure})
		day = current_dt.date().isoformat()
		state.daily_max_exposure[day] = max(state.daily_max_exposure.get(day, 0.0), gross_exposure)

	def _daily_exposure_rows(self, daily_max_exposure: dict[str, float]) -> list[dict[str, Any]]:
		"""Convert daily max exposure mapping to sorted API rows."""
		return [{"date": day, "daily_max_gross_open_exposure": value} for day, value in sorted(daily_max_exposure.items())]

	def _daily_exposure_summary(self, daily_max_exposure: dict[str, float]) -> dict[str, float]:
		"""Compute summary stats for days with non-zero simulated exposure."""
		values = [value for value in daily_max_exposure.values() if value > 0]
		return {"days_with_exposure": len(values), "mean": statistics.mean(values) if values else 0.0, "standard_deviation": statistics.stdev(values) if len(values) > 1 else 0.0, "max": max(values) if values else 0.0, "min": min(values) if values else 0.0}

	def _print_daily_max_open_exposure_table(self, strategy_name: str, daily_max_exposure: dict[str, float]) -> None:
		"""Print a terminal-friendly daily max open exposure table for the completed simulation."""
		print(f"\nBacktest risk/daily-max-open-exposure-tabulated: {strategy_name}")
		print("Date                  Daily Max Gross Open Exposure")
		print("--------------- -----------------------------------")
		for day, value in sorted(daily_max_exposure.items()):
			print(f"{day:<15} $ {value:>33,.2f}")
