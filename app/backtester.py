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

	def run(self, strategy_name: str, start: str, end: str, tickers: Optional[list[str]] = None, position_size: Optional[float] = None) -> dict[str, Any]:
		"""
		Run a full chronological simulation for a strategy over a date range.

		Parameters:
			strategy_name: Strategy config name, such as "strategy1_15m_anchor".
			start: Inclusive ISO start datetime. Naive datetimes are interpreted as Eastern Time.
			end: Inclusive ISO end datetime. Naive datetimes are interpreted as Eastern Time.
			tickers: Optional list of ticker symbols. If omitted, tickers are discovered from Redis streams.
			position_size: Optional dollar notional used to size the first entry signal.

		Returns:
			Dictionary containing trade events, per-ticker PnL history, aggregate PnL history,
			and daily maximum gross open exposure. All simulation state is in memory only.
		"""
		config = self._get_strategy_config(strategy_name)
		start_dt = self._parse_input_dt(start)
		end_dt = self._parse_input_dt(end)

		self.diagnostic_logging_enabled = (
			(end_dt - start_dt) <= timedelta(days=self.BACKTEST_DIAGNOSTIC_MAX_DAYS)
		)		

		if start_dt > end_dt:
			raise ValueError("start must be <= end")

		if position_size is None:
			position_size = float(config["default_position_size"])
		else:
			position_size = float(position_size)

		if position_size <= 0:
			raise ValueError("position_size must be > 0")

		state = SimState()
		timeframes = self._strategy_timeframes(config)
		symbols = self._normalize_tickers(tickers) or self._discover_tickers(timeframes)
		events = self._load_signal_events(strategy_name, symbols, timeframes, start_dt, end_dt)

		#PRINT ALL EVENTS HERE TO ENSURE THEY ARE IN THE RIGHT CHRONOLOGICAL ORDER
		print(f"All Events in chronological order\n{events}")

		for event in events:
			self._register_event_context(state, event)
			#self._process_experimental_strategy(strategy_name, state, config, event, position_size)
			#self._process_strategy1_event(strategy_name, state, config, event, position_size)
			if strategy_name.startswith("strategy1_"):
				self._process_strategy1_event(strategy_name, state, config, event, position_size)
			elif strategy_name.startswith("strategy2_"):
				self._process_strategy2_event(strategy_name, state, config, event, position_size)
			else:
				raise ValueError(f"Unsupported strategy family: {strategy_name}")			
			self._record_snapshots(state, event["dt"])

		self._print_daily_max_open_exposure_table(strategy_name, state.daily_max_exposure)

		return {
			"strategy_name": strategy_name,
			"start": start_dt.isoformat(),
			"end": end_dt.isoformat(),
			"tickers": symbols,
			"signal_count": len(events),
			"trade_count": len(state.trade_events),
			"overall_pnl_history": state.overall_pnl_history,
			"ticker_pnl_history": state.ticker_pnl_history,
			"daily_max_open_exposure": self._daily_exposure_rows(state.daily_max_exposure),
			"daily_max_open_exposure_summary": self._daily_exposure_summary(state.daily_max_exposure),
			"trade_events": state.trade_events,
		}

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
		"""Load Redis stream alerts for all relevant ticker/timeframe pairs and sort them chronologically."""
		events = []
		for symbol in symbols:
			for tf in timeframes:
				stream_key = self.tvw_helpers.stream_key(tf, symbol)
				for stream_id, fields in self.r.xrange(stream_key, min="-", max="+"):
					event = self._build_event(strategy_name, stream_id, fields, symbol, tf)
					if event is None:
						continue
					if start_dt <= event["dt"] <= end_dt:
						events.append(event)
		return sorted(events, key=lambda e: (e["dt"], self.TIMEFRAME_ORDER.get(e["timeframe"], 999), e["ticker"], e["stream_id"]))

	def _build_event(self, strategy_name: str, stream_id: str, fields: dict[str, Any], fallback_symbol: str, fallback_tf: str) -> Optional[dict[str, Any]]:
		"""Convert a raw Redis stream entry into a normalized signal event dictionary."""
		time_str = fields.get("bar_close_time_eastern") or fields.get("received_at")
		if not time_str:
			return None
		try:
			dt = datetime.fromisoformat(str(time_str).replace("Z", "+00:00"))
			if dt.tzinfo is None:
				dt = dt.replace(tzinfo=self.tvw_helpers.eastern_tz)
			dt = dt.astimezone(self.tvw_helpers.eastern_tz)
		except Exception:
			logger.exception("Skipping backtest signal with invalid timestamp: %r", time_str)
			return None

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
			"dt": dt,
			"time": dt.isoformat(),
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

		"""self.strategies_instance.entry_strategy2(
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
		)"""

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

			if self.diagnostic_logging_enabled:
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

			if self.diagnostic_logging_enabled:
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

			if self.diagnostic_logging_enabled:
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

