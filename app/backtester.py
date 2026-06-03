import io
import logging
import math
import statistics
from dataclasses import dataclass, field
from datetime import datetime, timezone
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
			"default_position_size": 1000.0,
		},
		"strategy1_1h_anchor": {
			"entry_tf": "5m",
			"intermediary_tf": "15m",
			"anchor_tf": "1h",
			"lower_timeframes": {"1m", "5m"},
			"default_position_size": 1000.0,
		},
		"strategy1_4h_anchor": {
			"entry_tf": "15m",
			"intermediary_tf": "1h",
			"anchor_tf": "4h",
			"lower_timeframes": {"1m", "5m", "15m"},
			"default_position_size": 1000.0,
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

	def __init__(self, trading_view_webhook_helpers):
		"""
		Create a BackTester.

		Parameters:
			trading_view_webhook_helpers: Existing helper instance used for Redis access,
			timeframe normalization, timestamp parsing, and safe float conversion.
		"""
		self.tvw_helpers = trading_view_webhook_helpers
		self.r = trading_view_webhook_helpers.require_redis()
		self.smallest_share_size = 0.25

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
		events = self._load_signal_events(symbols, timeframes, start_dt, end_dt)

		for event in events:
			self._register_event_context(state, event)
			self._process_strategy1_event(state, config, event, position_size)
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
		if name not in self.STRATEGY_CONFIGS:
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

	def _load_signal_events(self, symbols: list[str], timeframes: set[str], start_dt: datetime, end_dt: datetime) -> list[dict[str, Any]]:
		"""Load Redis stream alerts for all relevant ticker/timeframe pairs and sort them chronologically."""
		events = []
		for symbol in symbols:
			for tf in timeframes:
				stream_key = self.tvw_helpers.stream_key(tf, symbol)
				for stream_id, fields in self.r.xrange(stream_key, min="-", max="+"):
					event = self._build_event(stream_id, fields, symbol, tf)
					if event is None:
						continue
					if start_dt <= event["dt"] <= end_dt:
						events.append(event)
		return sorted(events, key=lambda e: (e["dt"], self.TIMEFRAME_ORDER.get(e["timeframe"], 999), e["ticker"], e["stream_id"]))

	def _build_event(self, stream_id: str, fields: dict[str, Any], fallback_symbol: str, fallback_tf: str) -> Optional[dict[str, Any]]:
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

	def _process_strategy1_event(self, state: SimState, config: dict[str, Any], event: dict[str, Any], position_size: float) -> None:
		"""Apply Strategy 1 entry and exit rules to one chronological signal event."""
		self._try_strategy1_entry(state, config, event, position_size)
		self._try_strategy1_exit(state, config, event)

	def _try_strategy1_entry(self, state: SimState, config: dict[str, Any], event: dict[str, Any], position_size: float) -> None:
		"""Open or add to an in-memory position when the current event qualifies as an entry."""
		if event["timeframe"] != config["entry_tf"]:
			return
		if event["signal_role"] != "confirmation" or event["side"] not in {"buy", "sell"}:
			return

		anchor = state.latest_directional.get((event["ticker"], config["anchor_tf"]))
		if not anchor or anchor["side"] != event["side"]:
			return
		if self._has_opposite_signal_after_anchor(state, event["ticker"], event["side"], config["intermediary_tf"], anchor["dt"]):
			return

		base_qty = position_size / event["price"]
		qty = self._progressive_entry_size(state, event["ticker"], event["side"], config["entry_tf"], anchor["dt"], base_qty)
		if qty <= 0:
			return

		if event["side"] == "buy":
			self._open_or_add_position(state, event, "long", qty)
		else:
			qty = math.floor(qty)
			if qty >= self.smallest_share_size:
				self._open_or_add_position(state, event, "short", qty)

	def _try_strategy1_exit(self, state: SimState, config: dict[str, Any], event: dict[str, Any]) -> None:
		"""Close an in-memory position when the current event qualifies as an exit."""
		exit_tfs = set(config["lower_timeframes"]) | {config["intermediary_tf"]}
		if event["timeframe"] not in exit_tfs:
			return

		position = state.positions.get(event["ticker"])
		if not position or position.num_shares <= 0:
			return

		anchor = state.latest_directional.get((event["ticker"], config["anchor_tf"]))
		intermediary = state.latest_directional.get((event["ticker"], config["intermediary_tf"]))
		if not anchor:
			return

		position_anchor_opposite = (
			(position.side == "long" and anchor["side"] == "sell")
			or (position.side == "short" and anchor["side"] == "buy")
		)
		intermediary_opposite_anchor = (
			event["timeframe"] == config["intermediary_tf"]
			and event["signal_role"] == "confirmation"
			and event["side"] in {"buy", "sell"}
			and event["side"] != anchor["side"]
		)
		lower_confirms_intermediary_opposite_anchor = (
			event["timeframe"] in config["lower_timeframes"]
			and intermediary is not None
			and event["signal_role"] == "confirmation"
			and event["side"] == intermediary["side"]
			and intermediary["side"] != anchor["side"]
		)
		exit_signal_matches_position = self._exit_signal_matches_position(event, intermediary, position)

		if position_anchor_opposite or intermediary_opposite_anchor or lower_confirms_intermediary_opposite_anchor or exit_signal_matches_position:
			self._close_position(state, event)

	def _exit_signal_matches_position(self, event: dict[str, Any], intermediary: Optional[dict[str, Any]], position: SimPosition) -> bool:
		"""Return True when an intermediary timeframe exit signal matches the current open position side."""
		if event["signal"] not in {"bullish_exit", "bearish_exit"}:
			return False
		if intermediary is None:
			return False
		return (
			(position.side == "long" and intermediary["side"] == "buy" and event["signal"] == "bullish_exit")
			or (position.side == "short" and intermediary["side"] == "sell" and event["signal"] == "bearish_exit")
		)

	def _has_opposite_signal_after_anchor(self, state: SimState, ticker: str, side: str, opposite_tf: str, anchor_dt: datetime) -> bool:
		"""Check whether an opposite-side intermediary signal occurred after the anchor signal."""
		opposite_side = "sell" if side == "buy" else "buy"
		for candidate in state.all_events_by_ticker_tf.get((ticker, opposite_tf), []):
			if candidate["signal_role"] == "confirmation" and candidate["side"] == opposite_side and candidate["dt"] > anchor_dt:
				return True
		return False

	def _progressive_entry_size(self, state: SimState, ticker: str, side: str, entry_tf: str, anchor_dt: datetime, base_qty: float) -> float:
		"""Compute progressive halving size from qualifying same-side entry signals since anchor time."""
		count = 0
		for candidate in state.all_events_by_ticker_tf.get((ticker, entry_tf), []):
			if candidate["signal_role"] == "confirmation" and candidate["side"] == side and candidate["dt"] > anchor_dt:
				count += 1
		if count <= 0:
			return 0.0
		qty = base_qty / (2 ** (count - 1))
		if qty < self.smallest_share_size:
			return 0.0
		return qty

	def _open_or_add_position(self, state: SimState, event: dict[str, Any], position_side: str, qty: float) -> None:
		"""Open a new position or add to an existing same-side position in memory."""
		ticker = event["ticker"]
		price = event["price"]
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
		"""Close the current in-memory position and accumulate realized PnL."""
		ticker = event["ticker"]
		position = state.positions.get(ticker)
		if not position or position.num_shares <= 0:
			return
		price = event["price"]
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
