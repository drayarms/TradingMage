import io
import logging
import math
import statistics
from dataclasses import dataclass, field
from datetime import datetime, time, timezone, timedelta
import pandas as pd
from typing import Any, Optional

import matplotlib.pyplot as plt
from matplotlib.patches import Rectangle
import zipfile

logger = logging.getLogger("tv-webhook")


@dataclass
class SimPosition:
	"""In-memory position state for one ticker during a single backtest run."""
	ticker: str
	side: str
	avg_price_per_share: float
	num_shares: float
	realized_pnl: float = 0.0
	reporting_baseline_price: Optional[float] = None
	entry_sequence_count: int = 1
	high_water_price: Optional[float] = None
	low_water_price: Optional[float] = None
	trailing_stop_amount: Optional[float] = None
	trailing_stop_price: Optional[float] = None
	trailing_stop_source_time: Optional[datetime] = None
	last_trailing_bar_time: Optional[datetime] = None


@dataclass
class SimState:
	"""Container for all mutable in-memory state created during one backtest run."""
	positions: dict[str, SimPosition] = field(default_factory=dict)
	realized_by_ticker: dict[str, float] = field(default_factory=dict)
	last_price_by_ticker: dict[str, float] = field(default_factory=dict)
	last_exit_time_by_ticker: dict[str, datetime] = field(default_factory=dict)
	latest_directional: dict[tuple[str, str], dict[str, Any]] = field(default_factory=dict)
	latest_by_tf: dict[tuple[str, str], dict[str, Any]] = field(default_factory=dict)
	all_events_by_ticker_tf: dict[tuple[str, str], list[dict[str, Any]]] = field(default_factory=dict)
	overall_pnl_history: list[dict[str, Any]] = field(default_factory=list)
	ticker_pnl_history: dict[str, list[dict[str, Any]]] = field(default_factory=dict)
	daily_max_exposure: dict[str, float] = field(default_factory=dict)
	trade_events: list[dict[str, Any]] = field(default_factory=list)
	market_data: dict[str, Any] = field(default_factory=dict)
	market_close_liquidation_dates: set[str] = field(default_factory=set)
	reporting_baselines: dict[str, dict[str, Any]] = field(default_factory=dict)


class SimulatedOrderPriceUnavailable(ValueError):
	"""Raised when a simulated order has no sufficiently recent execution price."""
	pass

class BackTester:
	"""Run isolated in-memory backtests from TradingView signals already stored in Redis."""

	STRATEGY_CONFIGS = {
		"strategy1_15m_anchor": {
			"entry_tf": "1m",
			"intermediary_tf": "5m",
			"anchor_tf": "15m",
			"lower_timeframes": {"1m"},
			"default_position_size": 2000.0,
			"exit_strategy": 1,
			"warmup_sessions": 2,
		},
		"strategy1_1h_anchor": {
			"entry_tf": "5m",
			"intermediary_tf": "15m",
			"anchor_tf": "1h",
			"lower_timeframes": {"1m", "5m"},
			"default_position_size": 6000.0,
			"exit_strategy": 1,
			"warmup_sessions": 5,
		},
		"strategy1_4h_anchor": {
			"entry_tf": "15m",
			"intermediary_tf": "1h",
			"anchor_tf": "4h",
			"lower_timeframes": {"1m", "5m", "15m"},
			"default_position_size": 20000.0,
			"exit_strategy": 1,
			"warmup_sessions": 10,
		},

		"strategy2_15m_anchor": {
			"entry_tf": "1m",
			"intermediary_tf": "5m",
			"anchor_tf": "15m",
			"lower_timeframes": {"1m"},
			"default_position_size": 2000.0,
			"exit_strategy": 2,
			"warmup_sessions": 2,
		},
		"strategy2_1h_anchor": {
			"entry_tf": "5m",
			"intermediary_tf": "15m",
			"anchor_tf": "1h",
			"lower_timeframes": {"1m", "5m"},
			"default_position_size": 6000.0,
			"exit_strategy": 2,
			"warmup_sessions": 5,
		},
		"strategy2_4h_anchor": {
			"entry_tf": "15m",
			"intermediary_tf": "1h",
			"anchor_tf": "4h",
			"lower_timeframes": {"1m", "5m", "15m"},
			"default_position_size": 20000.0,
			"exit_strategy": 2,
			"warmup_sessions": 10,
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
	PNL_SNAPSHOT_INTERVAL_MINUTES = 5

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
		alpaca_api,
		strategy_name: str,
		start: str,
		end: str,
		tickers: Optional[list[str]] = None,
		position_size: Optional[float] = None,
		ATR_period: int = 14,
		ATR_multiplier: float = 1.0,
		#warmup_days: int = 2,
		warmup_sessions: Optional[int] = None,
		exit_strategy: Optional[int] = None,
		liquidate_before_market_close: bool = False,
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
			warmup_sessions: Number of prior market sessions to simulate before start. 
							 If omitted, the strategy-specific configured value is used.
			exit_strategy: Optional override for the strategy config. Must be 1, 2, or 3.

		Returns:
			Dictionary containing only the requested reporting window, while preserving
			open positions and signal context created during warm-up.
		"""
		if not isinstance(
			liquidate_before_market_close,
			bool,
		):
			raise ValueError(
				"liquidate_before_market_close must be a boolean"
			)		
		try:
			ATR_multiplier = float(
				ATR_multiplier
			)
		except (TypeError, ValueError) as exc:
			raise ValueError(
				"ATR_multiplier must be a number"
			) from exc

		if ATR_multiplier <= 0:
			raise ValueError(
				"ATR_multiplier must be > 0"
			)		
		try:
			ATR_period = int(ATR_period)
		except (TypeError, ValueError) as exc:
			raise ValueError("ATR_period must be an integer") from exc

		if ATR_period < 1:
			raise ValueError("ATR_period must be >= 1")
	
		config = self._get_strategy_config(strategy_name)

		default_exit_strategy = config["exit_strategy"]

		if exit_strategy is not None:
			try:
				selected_exit_strategy = int(exit_strategy)
			except (TypeError, ValueError) as exc:
				raise ValueError(
					"exit_strategy must be 1, 2, or 3"
				) from exc

			if selected_exit_strategy not in {1, 2, 3}:
				raise ValueError(
					"exit_strategy must be 1, 2, or 3"
				)
		else:
			selected_exit_strategy = default_exit_strategy

		config["selected_exit_strategy"] = selected_exit_strategy
		config["ATR_multiplier"] = ATR_multiplier
		config["liquidate_before_market_close"] = (liquidate_before_market_close)

		start_dt = self._parse_input_dt(start)
		end_dt = self._parse_input_dt(end)

		if start_dt > end_dt:
			raise ValueError("start must be <= end")

		if warmup_sessions is None:
			warmup_sessions = int(
				config["warmup_sessions"]
			)
		else:
			try:
				warmup_sessions = int(
					warmup_sessions
				)
			except (TypeError, ValueError) as exc:
				raise ValueError(
					"warmup_sessions must be an integer"
				) from exc

		if warmup_sessions < 0:
			raise ValueError(
				"warmup_sessions must be >= 0"
			)	

		if warmup_sessions == 0:
			warmup_start_dt = start_dt
		else:
			warmup_start_dt = self._get_warmup_start_dt(alpaca_api=alpaca_api, start_dt=start_dt, warmup_sessions=warmup_sessions)	


		timeframes = self._strategy_timeframes(config)
		
		discovered_symbols = (
			self._normalize_tickers(tickers)
			or self._discover_tickers(timeframes)
		)

		if not discovered_symbols:
			raise ValueError(
				f"No Redis signal streams found for strategy {strategy_name}"
			)

		all_events = self._load_signal_events(
			strategy_name,
			discovered_symbols,
			timeframes,
			warmup_start_dt,
			end_dt,
		)

		# Include only symbols actually represented by signals in this run's
		# warm-up/reporting range.
		symbols = sorted({
			event["ticker"]
			for event in all_events
			if event.get("ticker")
		})

		if not symbols:
			raise ValueError(
				"No Redis signals were found in the requested period, "
				"including the warm-up period"
			)

		_1min_timeframe = self.trade_records_instance._1min_time_frame
		
		anchor_timeframes = {
			"15m": self.trade_records_instance._15min_time_frame,
			"1h": self.trade_records_instance._1hr_time_frame,
			"4h": self.trade_records_instance._4hr_time_frame,
		}

		anchor_tf = config["anchor_tf"]
		anchor_timeframe = anchor_timeframes[anchor_tf]
		
		one_minute_market_data_start = pd.Timestamp(
			warmup_start_dt - timedelta(minutes=10)
		)
		#Add extra bars to lookback period to account for holidays and weekends for large timeframes
		anchor_bars_per_trading_day = {
			"15m": 26,
			"1h": 7,
			"4h": 2,
		}

		required_anchor_bars = ATR_period + 10
		required_trading_days = math.ceil(
			required_anchor_bars / anchor_bars_per_trading_day[anchor_tf]
		)

		calendar_lookback_days = math.ceil(required_trading_days * 7 / 5) + 2

		anchor_market_data_start = pd.Timestamp(
			warmup_start_dt - timedelta(days=calendar_lookback_days)
		)

		market_data_end = pd.Timestamp(end_dt)

		_1m_df = self.trade_records_instance.get_df(
			alpaca_api, 
			symbols, 
			_1min_timeframe, 
			one_minute_market_data_start,
			market_data_end,
		)
		anchor_df = self.trade_records_instance.get_df(
			alpaca_api, 
			symbols, 
			anchor_timeframe, 
			anchor_market_data_start,
			market_data_end,
		)

		if _1m_df.empty:
			raise ValueError("Alpaca returned no 1-minute price data")

		if anchor_df.empty:
			raise ValueError(
				f"Alpaca returned no {config['anchor_tf']} price data"
			)

		_1min_close_prices = (
			self.trade_records_instance.dataframe_column_to_dict(
				_1m_df,
				"close"
			)
		)

		anchor_ATR = self.trade_records_instance.dataframe_to_atr_dict(
			anchor_df,
			period=ATR_period
		)

		anchor_ohlc = self._dataframe_to_ohlc_rows(
			anchor_df,
			start_dt,
			end_dt,
		)

		state = SimState()

		market_close_liquidation_times = []

		if liquidate_before_market_close:
			market_close_liquidation_times = (
				self._get_market_close_liquidation_times(
					alpaca_api=alpaca_api,
					start_dt=warmup_start_dt,
					end_dt=end_dt,
				)
			)

		state.market_data = {
			"close_1m": _1min_close_prices,
			"anchor_atr": anchor_ATR,
			"anchor_ohlc": anchor_ohlc,
			"market_close_liquidation_times": market_close_liquidation_times,
		}

		self.diagnostic_logging_enabled = (
			(end_dt - start_dt) <= timedelta(days=self.BACKTEST_DIAGNOSTIC_MAX_DAYS)
		)

		if position_size is None:
			position_size = float(config["default_position_size"])
		else:
			position_size = float(position_size)

		if position_size <= 0:
			raise ValueError("position_size must be > 0")

		warmup_events = [event for event in all_events if event["received_dt"] < start_dt]
		report_events = [event for event in all_events if event["received_dt"] >= start_dt]

		if config["selected_exit_strategy"] == 3:
			self._run_price_tracked_backtest(
				strategy_name,
				state,
				config,
				warmup_events,
				report_events,
				position_size,
				warmup_start_dt,
				start_dt,
				end_dt,
			)
		else:
			self._run_signal_backtest(
				strategy_name,
				state,
				config,
				warmup_events,
				report_events,
				position_size,
				warmup_start_dt,
				start_dt,
				end_dt,				
			)

		self._print_daily_max_open_exposure_table(strategy_name, state.daily_max_exposure)

		return {
			"strategy_name": strategy_name,
			"exit_strategy": config["selected_exit_strategy"],
			"ATR_period": ATR_period,
			"ATR_multiplier": ATR_multiplier,
			"liquidate_before_market_close": liquidate_before_market_close,
			"anchor_timeframe": config["anchor_tf"],
			"anchor_bars": anchor_ohlc,			
			"start": start_dt.isoformat(),
			"end": end_dt.isoformat(),
			"warmup_start": warmup_start_dt.isoformat(),
			#"warmup_days": warmup_days,
			"warmup_sessions": warmup_sessions,
			"tickers": symbols,
			"warmup_signal_count": len(warmup_events),
			"signal_count": len(report_events),
			"trade_count": sum(
				1
				for trade_event in state.trade_events
				if trade_event.get("event_type") != "order_rejected"
			),
			"trade_attempt_count": len(state.trade_events),
			"overall_pnl_history": state.overall_pnl_history,
			"ticker_pnl_history": state.ticker_pnl_history,
			"daily_max_open_exposure": self._daily_exposure_rows(state.daily_max_exposure),
			"daily_max_open_exposure_summary": self._daily_exposure_summary(state.daily_max_exposure),
			"trade_events": state.trade_events,
			"rejected_order_count": sum(
				1
				for trade_event in state.trade_events
				if trade_event.get("event_type") == "order_rejected"
			),	
			"rejected_orders": [
				trade_event
				for trade_event in state.trade_events
				if trade_event.get("event_type") == "order_rejected"
			],	
			"reporting_baselines": state.reporting_baselines,				
		}

	def _get_warmup_start_dt(
		self,
		alpaca_api,
		start_dt: datetime,
		warmup_sessions: int,
	) -> datetime:
		calendars = alpaca_api.get_calendar(
			start=(start_dt - timedelta(days=60)).date().isoformat(),
			end=start_dt.date().isoformat(),
		)

		previous_sessions = [
			session
			for session in calendars
			if pd.Timestamp(session.date).date() < start_dt.date()
		]

		if len(previous_sessions) < warmup_sessions:
			raise ValueError(
				"Insufficient market-calendar history for warm-up"
			)

		first_session = previous_sessions[-warmup_sessions]

		return datetime.combine(
			pd.Timestamp(first_session.date).date(),
			time(hour=4),
			tzinfo=self.tvw_helpers.eastern_tz,
		)

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


	def _run_signal_backtest(
		self,
		strategy_name: str,
		state: SimState,
		config: dict[str, Any],
		warmup_events: list[dict[str, Any]],
		report_events: list[dict[str, Any]],
		position_size: float,
		warmup_start_dt: datetime,
		start_dt: datetime,
		end_dt: datetime,
	) -> None:
		"""
		Run exit strategies 1 and 2 on a merged signal and sampled-price timeline.

		Signals retain their exact received times. Market prices are sampled every
		five minutes for smoother PnL and exposure graphs without processing every
		one-minute bar as a reporting snapshot.
		"""
		warmup_timeline = self._build_backtest_timeline(
			state=state,
			signal_events=warmup_events,
			start_dt=warmup_start_dt,
			end_dt=start_dt - timedelta(microseconds=1),
			market_bar_interval_minutes=self.PNL_SNAPSHOT_INTERVAL_MINUTES,
		)

		report_timeline = self._build_backtest_timeline(
			state=state,
			signal_events=report_events,
			start_dt=start_dt,
			end_dt=end_dt,
			market_bar_interval_minutes=self.PNL_SNAPSHOT_INTERVAL_MINUTES,
		)

		self.recording_enabled = False

		self._process_signal_timeline(
			strategy_name=strategy_name,
			state=state,
			config=config,
			timeline=warmup_timeline,
			position_size=position_size,
			record_snapshots=False,
		)

		self._set_reporting_baselines(state,start_dt)

		self._reset_reporting_state(state)		

		self.recording_enabled = True

		self._process_signal_timeline(
			strategy_name=strategy_name,
			state=state,
			config=config,
			timeline=report_timeline,
			position_size=position_size,
			record_snapshots=True,
		)


	def _process_signal_timeline(
		self,
		strategy_name: str,
		state: SimState,
		config: dict[str, Any],
		timeline: list[dict[str, Any]],
		position_size: float,
		record_snapshots: bool,
	) -> None:
		"""
		Process signal-driven strategies using exact signal times and sampled
		market-price updates.
		"""
		for timeline_event in timeline:
			event_dt = timeline_event["dt"]
			payload = timeline_event["payload"]

			if timeline_event["kind"] == "market_bar":
				state.last_price_by_ticker[payload["ticker"]] = float(
					payload["close"]
				)

				if record_snapshots:
					self._record_snapshots(
						state,
						event_dt,
					)

				continue

			if timeline_event["kind"] == "market_close_liquidation":
				self._liquidate_all_positions_before_market_close(state=state, liquidation_dt=event_dt)

				continue				

			self._register_event_context(
				state,
				payload,
			)

			self._process_event(
				strategy_name,
				state,
				config,
				payload,
				position_size,
			)


	def _run_price_tracked_backtest(
		self,
		strategy_name: str,
		state: SimState,
		config: dict[str, Any],
		warmup_events: list[dict[str, Any]],
		report_events: list[dict[str, Any]],
		position_size: float,
		warmup_start_dt: datetime,
		start_dt: datetime,
		end_dt: datetime,
	) -> None:
		"""Run exit strategy 3 on a merged signal and one-minute-price timeline."""
		warmup_timeline = self._build_backtest_timeline(
			state,
			warmup_events,
			warmup_start_dt,
			start_dt - timedelta(microseconds=1),
			market_bar_interval_minutes=1,
		)

		report_timeline = self._build_backtest_timeline(
			state,
			report_events,
			start_dt,
			end_dt,
			market_bar_interval_minutes=1,
		)

		self.recording_enabled = False
		self._process_price_tracked_timeline(
			strategy_name,
			state,
			config,
			warmup_timeline,
			position_size,
			record_snapshots=False,
		)

		self._set_reporting_baselines(state, start_dt)

		self._reset_reporting_state(state)

		self.recording_enabled = True
		self._process_price_tracked_timeline(
			strategy_name,
			state,
			config,
			report_timeline,
			position_size,
			record_snapshots=True,
		)


	def _build_backtest_timeline(
		self,
		state: SimState,
		signal_events: list[dict[str, Any]],
		start_dt: datetime,
		end_dt: datetime,
		market_bar_interval_minutes: int = 1,
	) -> list[dict[str, Any]]:
		"""
		Merge signal arrivals with completed Alpaca one-minute bars.

		market_bar_interval_minutes controls which market bars are included in
		the timeline. A value of 1 includes every completed one-minute bar. A
		value of 5 includes one market-price update every five minutes.
		"""
		if market_bar_interval_minutes < 1:
			raise ValueError(
				"market_bar_interval_minutes must be >= 1"
			)

		timeline = []

		for ticker, ticker_prices in state.market_data.get(
			"close_1m",
			{},
		).items():
			for timestamp, close_price in ticker_prices.items():
				source_bar_dt = pd.Timestamp(timestamp)

				if source_bar_dt.tzinfo is None:
					source_bar_dt = source_bar_dt.tz_localize(
						self.tvw_helpers.eastern_tz
					)
				else:
					source_bar_dt = source_bar_dt.tz_convert(
						self.tvw_helpers.eastern_tz
					)

				available_dt = (
					source_bar_dt
					+ pd.Timedelta(minutes=1)
				).to_pydatetime()

				if not start_dt <= available_dt <= end_dt:
					continue

				if (
					market_bar_interval_minutes > 1
					and available_dt.minute
					% market_bar_interval_minutes
					!= 0
				):
					continue

				timeline.append({
					"kind": "market_bar",
					"dt": available_dt,
					"payload": {
						"ticker": ticker,
						"dt": available_dt,
						"source_bar_time": source_bar_dt.to_pydatetime(),
						"close": float(close_price),
						"snapshot_due": (
							available_dt.minute
							% self.PNL_SNAPSHOT_INTERVAL_MINUTES
							== 0
						),
					},
				})

		for event in signal_events:
			timeline.append({
				"kind": "signal",
				"dt": event["received_dt"],
				"payload": event,
			})

		for liquidation_dt in state.market_data.get(
			"market_close_liquidation_times",
			[],
		):
			if not start_dt <= liquidation_dt <= end_dt:
				continue

			timeline.append({
				"kind": "market_close_liquidation",
				"dt": liquidation_dt,
				"payload": {
					"dt": liquidation_dt,
					"trading_date":
						liquidation_dt.date().isoformat(),
				},
			})			

		return sorted(
			timeline,
			key=lambda row: (
				row["dt"],
				{
					"market_bar": 0,
					"market_close_liquidation": 1,
					"signal": 2,
				}.get(
					row["kind"],
					3,
				),
				row["payload"].get("ticker", ""),
				row["payload"].get("stream_id", ""),
			),
		)


	def _process_price_tracked_timeline(
		self,
		strategy_name: str,
		state: SimState,
		config: dict[str, Any],
		timeline: list[dict[str, Any]],
		position_size: float,
		record_snapshots: bool,
	) -> None:
		"""
		Process exit strategy 3 using every completed one-minute bar, while
		recording PnL snapshots only at the configured reporting interval.
		"""
		for timeline_event in timeline:
			event_dt = timeline_event["dt"]
			payload = timeline_event["payload"]

			if timeline_event["kind"] == "market_bar":
				self._process_trailing_stop_market_bar(
					state,
					payload,
				)

				if (
					record_snapshots
					and payload.get("snapshot_due", False)
				):
					self._record_snapshots(
						state,
						event_dt,
					)

				continue

			if timeline_event["kind"] == "market_close_liquidation":
				self._liquidate_all_positions_before_market_close(
					state=state,
					liquidation_dt=event_dt,
				)

				continue				

			self._process_exit_strategy3_signal(
				strategy_name,
				state,
				config,
				payload,
				position_size,
			)


	def _process_exit_strategy3_signal(
		self,
		strategy_name: str,
		state: SimState,
		config: dict[str, Any],
		event: dict[str, Any],
		position_size: float,
	) -> None:
		"""Record every signal, but evaluate entries only while the ticker is flat."""
		self._register_event_context(state, event)

		ticker = event["ticker"]
		position = state.positions.get(ticker)

		if position is not None and position.num_shares > 0:
			return

		last_exit_time = state.last_exit_time_by_ticker.get(ticker)

		if last_exit_time is not None and event["received_dt"] <= last_exit_time:
			return

		self._process_entry_only(
			strategy_name,
			state,
			config,
			event,
			position_size,
		)

	def _process_entry_only(
		self,
		strategy_name: str,
		state: SimState,
		config: dict[str, Any],
		event: dict[str, Any],
		position_size: float,
	) -> None:
		"""Evaluate the configured strategy family's entry rules without signal exits."""
		event["exit_strategy"] = 3
		event["anchor_tf"] = config["anchor_tf"]
		event["ATR_multiplier"] = config["ATR_multiplier"]		

		if strategy_name.startswith("strategy1_"):
			self._process_strategy1_entry_only(
				strategy_name, state, config, event, position_size
			)
		elif strategy_name.startswith("strategy2_"):
			self._process_strategy2_entry_only(
				strategy_name, state, config, event, position_size
			)
		else:
			raise ValueError(f"Unsupported strategy family: {strategy_name}")

	def _process_strategy1_entry_only(
		self,
		strategy_name: str,
		state: SimState,
		config: dict[str, Any],
		event: dict[str, Any],
		position_size: float,
	) -> None:
		now_et = event["dt"]
		signal = event["side"]
		symbol = event["ticker"]
		tf = event["timeframe"]
		market_price = event["price"]
		num_shares = position_size / market_price

		self.strategies_instance.entry_strategy1(
			strategy_name, config["entry_tf"], config["intermediary_tf"],
			config["anchor_tf"], True, now_et, signal, None, symbol, tf,
			num_shares, None, state, config, event, market_price, self,
		)

	def _process_strategy2_entry_only(
		self,
		strategy_name: str,
		state: SimState,
		config: dict[str, Any],
		event: dict[str, Any],
		position_size: float,
	) -> None:
		now_et = event["dt"]
		signal = event["signal"]
		symbol = event["ticker"]
		tf = event["timeframe"]
		market_price = event["price"]
		num_shares = position_size / market_price

		self.strategies_instance.entry_strategy2(
			strategy_name, config["entry_tf"], config["intermediary_tf"],
			True, now_et, signal, None, symbol, tf, num_shares, None, state,
			config, event, market_price, self,
		)

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


	"""def plot_overall_pnl(
		self,
		result: dict[str, Any],
		title: Optional[str] = None,
	) -> io.BytesIO:
		
		Render overall PnL followed by anchor-timeframe candlesticks for every
		ticker that received at least one simulated entry.
		
		history = result.get(
			"overall_pnl_history"
		) or []

		if not history:
			raise ValueError(
				"No PnL history available to plot"
			)

		trade_events = result.get(
			"trade_events"
		) or []

		entry_events = [
			event
			for event in trade_events
			if event.get("event_type") in {
				"open",
				"add",
			}
		]

		traded_tickers = sorted({
			event["ticker"]
			for event in entry_events
			if event.get("ticker")
		})

		anchor_bars = result.get(
			"anchor_bars"
		) or {}

		anchor_timeframe = result.get(
			"anchor_timeframe"
		) or "anchor"

		chart_count = 1 + len(
			traded_tickers
		)

		figure_height = (
			5
			+ 4 * len(traded_tickers)
		)

		fig, axes = plt.subplots(
			nrows=chart_count,
			ncols=1,
			figsize=(
				14,
				figure_height,
			),
			squeeze=False,
		)

		axes = axes.flatten()

		pnl_ax = axes[0]

		x_values = [
			datetime.fromisoformat(
				row["time"]
			)
			for row in history
		]
		y_values = [
			float(
				row["overall_total_pnl"]
			)
			for row in history
		]

		pnl_ax.plot(
			x_values,
			y_values,
		)
		pnl_ax.axhline(
			0,
			linewidth=0.8,
			alpha=0.5,
		)
		pnl_ax.set_title(
			title
			or (
				"Backtest Overall PnL - "
				f"{result.get('strategy_name')}"
			)
		)
		pnl_ax.set_xlabel(
			"Time"
		)
		pnl_ax.set_ylabel(
			"Overall PnL ($)"
		)
		pnl_ax.grid(
			True,
			alpha=0.3,
		)

		for chart_index, ticker in enumerate(
			traded_tickers,
			start=1,
		):
			ticker_entries = [
				event
				for event in entry_events
				if event.get("ticker") == ticker
			]

			self._plot_ticker_candlesticks(
				ax=axes[chart_index],
				ticker=ticker,
				bars=anchor_bars.get(
					ticker,
					[],
				),
				entries=ticker_entries,
				anchor_timeframe=anchor_timeframe,
			)

		fig.autofmt_xdate()
		fig.tight_layout()

		buf = io.BytesIO()

		fig.savefig(
			buf,
			format="png",
			dpi=120,
			bbox_inches="tight",
		)

		plt.close(
			fig
		)

		buf.seek(0)

		return buf"""


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

		try:
			config["exit_strategy"] = int(config["exit_strategy"])
		except (KeyError, TypeError, ValueError) as exc:
			raise ValueError(
				f"Invalid exit_strategy for {name}; expected 1, 2, or 3"
			) from exc

		if config["exit_strategy"] not in {1, 2, 3}:
			raise ValueError(
				f"Invalid exit_strategy for {name}; expected 1, 2, or 3"
			)

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


	def _register_event_context(
		self,
		state: SimState,
		event: dict[str, Any],
	) -> None:
		"""Update simulated signal and market-price context."""
		key = (
			event["ticker"],
			event["timeframe"],
		)

		state.latest_by_tf[key] = event
		state.all_events_by_ticker_tf.setdefault(
			key,
			[],
		).append(event)

		if (
			event["signal_role"] == "confirmation"
			and event["side"] in {"buy", "sell"}
		):
			state.latest_directional[key] = event

		try:
			market_price, _ = (
				self._get_execution_market_price(
					state,
					event,
				)
			)

			state.last_price_by_ticker[
				event["ticker"]
			] = market_price

		except SimulatedOrderPriceUnavailable as exc:
			logger.warning(
				"Unable to update one-minute market price: "
				"ticker=%s received_dt=%s reason=%s",
				event["ticker"],
				event["received_dt"],
				exc,
			)


	def _process_strategy1_event(self, strategy_name, state: SimState, config: dict[str, Any], event: dict[str, Any], position_size: float) -> None:
		"""Apply Strategy 1 using the configured signal-driven exit strategy."""
		now_et = event["dt"]
		signal = event["side"]
		symbol = event["ticker"]
		tf = event["timeframe"]
		market_price = event["price"]
		num_shares = position_size / market_price
		event["exit_strategy"] = config["selected_exit_strategy"]
		event["anchor_tf"] = config["anchor_tf"]

		if config["selected_exit_strategy"] == 1:
			self.strategies_instance.exit_strategy1(
				strategy_name, config["lower_timeframes"], config["intermediary_tf"],
				config["anchor_tf"], True, now_et, signal, None, symbol, tf, None,
				state, config, event, market_price, self,
			)
		elif config["selected_exit_strategy"] == 2:
			self.strategies_instance.exit_strategy2(
				strategy_name, config["entry_tf"], True, now_et, signal, None,
				symbol, tf, None, state, config, event, market_price, self,
			)

		self.strategies_instance.entry_strategy1(
			strategy_name, config["entry_tf"], config["intermediary_tf"],
			config["anchor_tf"], True, now_et, signal, None, symbol, tf,
			num_shares, None, state, config, event, market_price, self,
		)

	def _process_strategy2_event(self, strategy_name, state: SimState, config: dict[str, Any], event: dict[str, Any], position_size: float) -> None:
		"""Apply Strategy 2 using the configured signal-driven exit strategy."""
		now_et = event["dt"]
		signal = event["signal"]
		symbol = event["ticker"]
		tf = event["timeframe"]
		market_price = event["price"]
		num_shares = position_size / market_price
		event["exit_strategy"] = config["selected_exit_strategy"]
		event["anchor_tf"] = config["anchor_tf"]

		if config["selected_exit_strategy"] == 1:
			self.strategies_instance.exit_strategy1(
				strategy_name, config["lower_timeframes"], config["intermediary_tf"],
				config["anchor_tf"], True, now_et, signal, None, symbol, tf, None,
				state, config, event, market_price, self,
			)
		elif config["selected_exit_strategy"] == 2:
			self.strategies_instance.exit_strategy2(
				strategy_name, config["entry_tf"], True, now_et, signal, None,
				symbol, tf, None, state, config, event, market_price, self,
			)

		self.strategies_instance.entry_strategy2(
			strategy_name, config["entry_tf"], config["intermediary_tf"],
			True, now_et, signal, None, symbol, tf, num_shares, None, state,
			config, event, market_price, self,
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


	def _timeframe_timedelta(self, timeframe: str) -> pd.Timedelta:
		"""Convert a normalized timeframe to its bar duration."""
		tf = self.tvw_helpers.normalize_tf(timeframe)
		durations = {
			"1m": pd.Timedelta(minutes=1),
			"3m": pd.Timedelta(minutes=3),
			"5m": pd.Timedelta(minutes=5),
			"15m": pd.Timedelta(minutes=15),
			"30m": pd.Timedelta(minutes=30),
			"45m": pd.Timedelta(minutes=45),
			"1h": pd.Timedelta(hours=1),
			"2h": pd.Timedelta(hours=2),
			"4h": pd.Timedelta(hours=4),
			"1d": pd.Timedelta(days=1),
		}

		if tf not in durations:
			raise ValueError(f"Unsupported timeframe duration: {timeframe}")

		return durations[tf]

	def _get_anchor_atr_at_entry(
		self,
		state: SimState,
		event: dict[str, Any],
	) -> tuple[float, datetime]:
		"""Return the latest fully completed anchor ATR available at entry time."""
		ticker = str(event["ticker"]).upper().strip()
		entry_dt = pd.Timestamp(event["received_dt"])
		anchor_tf = self.tvw_helpers.normalize_tf(
			event.get("anchor_tf")
			or event.get("config_anchor_tf")
		)

		if not anchor_tf:
			raise ValueError(f"Missing anchor timeframe for {ticker} entry")

		ticker_atr = state.market_data.get("anchor_atr", {}).get(ticker, {})

		if not ticker_atr:
			raise ValueError(f"No anchor ATR data available for {ticker}")

		anchor_duration = self._timeframe_timedelta(anchor_tf)
		closest_source_dt = None
		closest_available_dt = None
		closest_atr = None

		if entry_dt.tzinfo is None:
			entry_dt = entry_dt.tz_localize(
				self.tvw_helpers.eastern_tz
			)
		else:
			entry_dt = entry_dt.tz_convert(
				self.tvw_helpers.eastern_tz
			)

		for timestamp, atr_value in ticker_atr.items():
			source_dt = pd.Timestamp(timestamp)

			if source_dt.tzinfo is None:
				source_dt = source_dt.tz_localize(
					self.tvw_helpers.eastern_tz
				)
			else:
				source_dt = source_dt.tz_convert(
					self.tvw_helpers.eastern_tz
				)

			available_dt = source_dt + anchor_duration

			if available_dt > entry_dt:
				continue

			if closest_available_dt is None or available_dt > closest_available_dt:
				closest_source_dt = source_dt
				closest_available_dt = available_dt
				closest_atr = atr_value

		if closest_source_dt is None or closest_atr is None:
			raise ValueError(
				f"No completed {anchor_tf} ATR available for {ticker} at {entry_dt}"
			)

		closest_atr = float(closest_atr)

		if closest_atr <= 0:
			raise ValueError(
				f"Invalid anchor ATR for {ticker} at {closest_source_dt}: {closest_atr}"
			)

		return closest_atr, closest_source_dt.to_pydatetime()

	def _open_or_add_position(self, state: SimState, event: dict[str, Any], position_side: str, qty: float) -> bool:

		execution_dt = event["received_dt"]

		execution_date = (
			execution_dt.date().isoformat()
		)

		if (
			execution_date
			in state.market_close_liquidation_dates
		):
			if (
				self.diagnostic_logging_enabled
				and self.recording_enabled
			):
				logger.info(
					"Simulated entry skipped after "
					"market-close liquidation: "
					"ticker=%s received_dt=%s",
					event["ticker"],
					execution_dt,
				)

			return False

		if not self.tvw_helpers.is_between_8pm_sun_and_8pm_fri_et(execution_dt):
			return False

		"""Open a new position or add to an existing same-side position in memory."""
		ticker = event["ticker"]
		try:
			quote = self._get_simulated_quote(
				state,
				event,
			)
		except SimulatedOrderPriceUnavailable as exc:
			logger.warning(
				"Simulated entry rejected because no fresh execution price "
				"is available: ticker=%s side=%s qty=%s "
				"received_dt=%s reason=%s",
				ticker,
				position_side,
				qty,
				event["received_dt"],
				exc,
			)

			self._record_rejected_order(state=state, event=event, order_type="entry", side=position_side, qty=qty, reason=str(exc))
			return False			

		order_side = (
			"buy"
			if position_side == "long"
			else "short"
		)

		price = self._get_execution_price(
			quote,
			order_side,
		)


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
				decision_time=(event["received_dt"].isoformat() if event.get("received_dt") else None),
			)

		existing = state.positions.get(ticker)
		if existing and existing.side != position_side and existing.num_shares > 0:
			position_closed = self._close_position(state, event)
			if not position_closed:
				return False
			existing = None

		if existing and existing.num_shares > 0:
			old_qty = existing.num_shares
			new_qty = old_qty + qty
			existing.avg_price_per_share = ((existing.avg_price_per_share * old_qty) + (price * qty)) / new_qty
			existing.num_shares = new_qty
			existing.entry_sequence_count += 1
			existing.high_water_price = max(existing.high_water_price or price, price)
			existing.low_water_price = min(existing.low_water_price or price, price)
			event_type = "add"
		else:
			position_kwargs = {}

			if event.get("exit_strategy") == 3:
				#trailing_stop_amount, trailing_stop_source_time = (
					#self._get_anchor_atr_at_entry(state, event)
				#)
				anchor_atr, trailing_stop_source_time = (
					self._get_anchor_atr_at_entry(
						state,
						event,
					)
				)

				ATR_multiplier = float(
					event.get(
						"ATR_multiplier",
						1.0,
					)
				)

				trailing_stop_amount = (
					anchor_atr
					* ATR_multiplier
				)
				if position_side == "long":
					trailing_stop_price = price - trailing_stop_amount
				else:
					trailing_stop_price = price + trailing_stop_amount

				position_kwargs = {
					"trailing_stop_amount": trailing_stop_amount,
					"trailing_stop_price": trailing_stop_price,
					"trailing_stop_source_time": trailing_stop_source_time,
					"last_trailing_bar_time": event["received_dt"],
				}

			state.positions[ticker] = SimPosition(
				ticker=ticker,
				side=position_side,
				avg_price_per_share=price,
				num_shares=qty,
				high_water_price=price,
				low_water_price=price,
				**position_kwargs,
			)
			event_type = "open"

		if self.recording_enabled:
			state.trade_events.append({
				"time": event["received_dt"].isoformat(),
				"signal_time": event["time"],
				"ticker": ticker,
				"event_type": event_type,
				"side": position_side,
				"price": price,
				"num_shares": qty,
				"realized_delta": 0.0,
			})
		return True


	def _process_trailing_stop_market_bar(
		self,
		state: SimState,
		market_event: dict[str, Any],
	) -> bool:
		"""Advance one exit-strategy-3 position using a completed one-minute close."""
		ticker = market_event["ticker"]
		bar_dt = market_event["dt"]
		market_price = float(market_event["close"])
		position = state.positions.get(ticker)

		state.last_price_by_ticker[ticker] = market_price

		if position is None or position.num_shares <= 0:
			return False

		if position.trailing_stop_amount is None or position.trailing_stop_amount <= 0:
			return False

		if (
			position.last_trailing_bar_time is not None
			and bar_dt <= position.last_trailing_bar_time
		):
			return False

		position.last_trailing_bar_time = bar_dt

		if not self.tvw_helpers._is_regular_hours_et(bar_dt):
			return False

		trailing_amount = float(position.trailing_stop_amount)

		if position.side == "long":
			position.high_water_price = max(
				position.high_water_price or market_price,
				market_price,
			)
			position.trailing_stop_price = (
				position.high_water_price - trailing_amount
			)

			if market_price <= position.trailing_stop_price:
				self._close_position_at_market_bar(
					state, ticker, bar_dt, market_price, "trailing_stop"
				)
				return True

		elif position.side == "short":
			position.low_water_price = min(
				position.low_water_price or market_price,
				market_price,
			)
			position.trailing_stop_price = (
				position.low_water_price + trailing_amount
			)

			if market_price >= position.trailing_stop_price:
				self._close_position_at_market_bar(
					state, ticker, bar_dt, market_price, "trailing_stop"
				)
				return True

		return False

	def _close_position_at_market_bar(
		self,
		state: SimState,
		ticker: str,
		bar_dt: datetime,
		market_price: float,
		exit_reason: str,
	) -> None:
		"""Close a simulated position when a one-minute trailing stop is crossed."""
		position = state.positions.get(ticker)

		if position is None or position.num_shares <= 0:
			return

		pnl_cost_basis = (
			position.reporting_baseline_price
			if position.reporting_baseline_price is not None
			else position.avg_price_per_share
		)

		if position.side == "long":
			exit_side = "sell"
			fill_price = max(
				0.01,
				float(market_price) - 0.01,
			)
			realized_delta = (
				fill_price - pnl_cost_basis
			) * position.num_shares
		else:
			exit_side = "cover"
			fill_price = float(
				market_price
			) + 0.01
			realized_delta = (
				pnl_cost_basis - fill_price
			) * position.num_shares

		state.realized_by_ticker[ticker] = (
			state.realized_by_ticker.get(ticker, 0.0) + realized_delta
		)
		state.last_price_by_ticker[ticker] = market_price
		state.last_exit_time_by_ticker[ticker] = bar_dt

		if self.recording_enabled:
			state.trade_events.append({
				"time": bar_dt.isoformat(),
				"ticker": ticker,
				"event_type": "close",
				"exit_reason": exit_reason,
				"side": exit_side,
				"price": fill_price,
				"market_price": market_price,
				"num_shares": position.num_shares,
				"realized_delta": realized_delta,
				"trailing_stop_amount": position.trailing_stop_amount,
				"trailing_stop_price": position.trailing_stop_price,
				"high_water_price": position.high_water_price,
				"low_water_price": position.low_water_price,
			})		

		state.positions.pop(ticker, None)


	def _plot_ticker_candlesticks(
		self,
		ax,
		ticker: str,
		bars: list[dict[str, Any]],
		entries: list[dict[str, Any]],
		anchor_timeframe: str,
	) -> None:
		"""Plot anchor-timeframe candlesticks and simulated entry markers."""
		if not bars:
			ax.set_title(
				f"{ticker} — no {anchor_timeframe} candle data"
			)
			ax.set_axis_off()
			return

		bar_times = [
			datetime.fromisoformat(bar["time"])
			for bar in bars
		]

		x_positions = list(range(len(bars)))
		candle_width = 0.65

		for x_position, bar in zip(x_positions, bars):
			open_price = float(bar["open"])
			high_price = float(bar["high"])
			low_price = float(bar["low"])
			close_price = float(bar["close"])

			is_bullish = close_price >= open_price
			candle_color = "green" if is_bullish else "red"

			ax.vlines(
				x_position,
				low_price,
				high_price,
				color=candle_color,
				linewidth=1.0,
			)

			body_bottom = min(
				open_price,
				close_price,
			)
			body_height = abs(
				close_price - open_price
			)

			if body_height == 0:
				body_height = max(
					high_price - low_price,
					0.01,
				) * 0.02

			candle_body = Rectangle(
				(
					x_position - candle_width / 2,
					body_bottom,
				),
				candle_width,
				body_height,
				facecolor=candle_color,
				edgecolor=candle_color,
				linewidth=1.0,
				alpha=0.8,
			)

			ax.add_patch(candle_body)

		long_label_added = False
		short_label_added = False

		for entry in entries:
			entry_dt = datetime.fromisoformat(
				entry["time"]
			)
			entry_price = float(
				entry["price"]
			)

			nearest_bar_index = min(
				range(len(bar_times)),
				key=lambda index: abs(
					bar_times[index] - entry_dt
				),
			)

			if entry["side"] == "long":
				label = (
					"Long entry"
					if not long_label_added
					else None
				)

				ax.scatter(
					nearest_bar_index,
					entry_price,
					marker="^",
					s=90,
					color="blue",
					edgecolors="black",
					linewidths=0.5,
					zorder=5,
					label=label,
				)

				long_label_added = True

			else:
				label = (
					"Short entry"
					if not short_label_added
					else None
				)

				ax.scatter(
					nearest_bar_index,
					entry_price,
					marker="v",
					s=90,
					color="orange",
					edgecolors="black",
					linewidths=0.5,
					zorder=5,
					label=label,
				)

				short_label_added = True

			ax.annotate(
				f"${entry_price:,.2f}",
				(
					nearest_bar_index,
					entry_price,
				),
				xytext=(0, 10),
				textcoords="offset points",
				ha="center",
				fontsize=8,
			)

		tick_interval = max(
			1,
			len(bars) // 10,
		)

		tick_positions = list(
			range(
				0,
				len(bars),
				tick_interval,
			)
		)

		tick_labels = [
			bar_times[index].strftime(
				"%m-%d\n%H:%M"
			)
			for index in tick_positions
		]

		ax.set_xticks(
			tick_positions
		)
		ax.set_xticklabels(
			tick_labels
		)

		ax.set_xlim(
			-1,
			len(bars),
		)
		ax.set_title(
			f"{ticker} — {anchor_timeframe} candles"
		)
		ax.set_ylabel(
			"Price ($)"
		)
		ax.grid(
			True,
			alpha=0.25,
		)

		if entries:
			ax.legend(
				loc="best"
			)


	def _build_overall_pnl_image(
		self,
		result: dict[str, Any],
		title: Optional[str] = None,
	) -> io.BytesIO:
		"""Render the overall PnL chart as one PNG image."""
		history = result.get(
			"overall_pnl_history"
		) or []

		if not history:
			raise ValueError(
				"No PnL history available to plot"
			)

		x_values = [
			datetime.fromisoformat(
				row["time"]
			)
			for row in history
		]

		y_values = [
			float(
				row["overall_total_pnl"]
			)
			for row in history
		]

		fig, ax = plt.subplots(
			figsize=(14, 7)
		)

		ax.plot(
			x_values,
			y_values,
		)

		ax.axhline(
			0,
			linewidth=0.8,
			alpha=0.5,
		)

		ax.set_title(
			title
			or (
				"Backtest Overall PnL - "
				f"{result.get('strategy_name')}"
				f"Exit {result.get('exit_strategy')} - "
				f"ATR {result.get('ATR_period')} × "
				f"{result.get('ATR_multiplier')}"				
			)
		)

		ax.set_xlabel(
			"Time"
		)

		ax.set_ylabel(
			"Overall PnL ($)"
		)

		ax.grid(
			True,
			alpha=0.3,
		)

		fig.autofmt_xdate()
		fig.tight_layout()

		image_buffer = io.BytesIO()

		fig.savefig(
			image_buffer,
			format="png",
			dpi=120,
			bbox_inches="tight",
		)

		plt.close(
			fig
		)

		image_buffer.seek(0)

		return image_buffer			

		
	def _build_ticker_candlestick_image(
		self,
		ticker: str,
		bars: list[dict[str, Any]],
		entries: list[dict[str, Any]],
		anchor_timeframe: str,
	) -> io.BytesIO:
		"""Render one ticker's candlestick chart as one PNG image."""
		fig, ax = plt.subplots(
			figsize=(14, 7)
		)

		self._plot_ticker_candlesticks(
			ax=ax,
			ticker=ticker,
			bars=bars,
			entries=entries,
			anchor_timeframe=anchor_timeframe,
		)

		fig.tight_layout()

		image_buffer = io.BytesIO()

		fig.savefig(
			image_buffer,
			format="png",
			dpi=120,
			bbox_inches="tight",
		)

		plt.close(
			fig
		)

		image_buffer.seek(0)

		return image_buffer


	def build_backtest_chart_zip(
		self,
		result: dict[str, Any],
		title: Optional[str] = None,
	) -> io.BytesIO:
		"""
		Build a ZIP containing the overall PnL chart followed by one separate
		candlestick PNG for each ticker that received an entry.
		"""
		trade_events = result.get(
			"trade_events"
		) or []

		entry_events = [
			event
			for event in trade_events
			if event.get("event_type") in {
				"open",
				"add",
			}
		]

		traded_tickers = sorted({
			event["ticker"]
			for event in entry_events
			if event.get("ticker")
		})

		anchor_bars = result.get(
			"anchor_bars"
		) or {}

		anchor_timeframe = result.get(
			"anchor_timeframe"
		) or "anchor"

		zip_buffer = io.BytesIO()

		with zipfile.ZipFile(
			zip_buffer,
			mode="w",
			compression=zipfile.ZIP_DEFLATED,
		) as zip_file:
			pnl_image = self._build_overall_pnl_image(
				result=result,
				title=title,
			)

			zip_file.writestr(
				"00_overall_pnl.png",
				pnl_image.getvalue(),
			)

			for chart_number, ticker in enumerate(
				traded_tickers,
				start=1,
			):
				ticker_entries = [
					event
					for event in entry_events
					if event.get("ticker") == ticker
				]

				ticker_image = (
					self._build_ticker_candlestick_image(
						ticker=ticker,
						bars=anchor_bars.get(
							ticker,
							[],
						),
						entries=ticker_entries,
						anchor_timeframe=anchor_timeframe,
					)
				)

				filename = (
					f"{chart_number:02d}_"
					f"{ticker}_"
					f"{anchor_timeframe}_candles.png"
				)

				zip_file.writestr(
					filename,
					ticker_image.getvalue(),
				)

		zip_buffer.seek(0)

		return zip_buffer

											
	def _close_position(self, state: SimState, event: dict[str, Any]) -> bool:

		execution_dt = event["received_dt"]
		if not self.tvw_helpers.is_between_8pm_sun_and_8pm_fri_et(execution_dt):
			return False

		"""Close the current in-memory position and accumulate realized PnL."""
		ticker = event["ticker"]
		position = state.positions.get(ticker)

		if not position or position.num_shares <= 0:
			return False

		try:
			quote = self._get_simulated_quote(
				state,
				event,
			)
		except SimulatedOrderPriceUnavailable as exc:
			logger.warning(
				"Simulated exit rejected because no fresh execution price "
				"is available: ticker=%s side=%s qty=%s "
				"received_dt=%s reason=%s",
				ticker,
				position.side,
				position.num_shares,
				event["received_dt"],
				exc,
			)

			self._record_rejected_order(state=state, event=event, order_type="exit", side=position.side, qty=position.num_shares, reason=str(exc))
			return False

		if position.side == "long":
			exit_side = "sell"
		else:
			exit_side = "cover"

		price = self._get_execution_price(
			quote,
			exit_side,
		)

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
				decision_time=(event["received_dt"].isoformat() if event.get("received_dt") else None),
			)

		pnl_cost_basis = (
			position.reporting_baseline_price
			if position.reporting_baseline_price is not None
			else position.avg_price_per_share
		)

		if position.side == "long":
			realized_delta = (
				price - pnl_cost_basis
			) * position.num_shares
		else:
			realized_delta = (
				pnl_cost_basis - price
			) * position.num_shares

		state.realized_by_ticker[ticker] = state.realized_by_ticker.get(ticker, 0.0) + realized_delta
		if self.recording_enabled:
			state.trade_events.append({"time": event["time"], "ticker": ticker, "event_type": "close", "side": exit_side, "price": price, "num_shares": position.num_shares, "realized_delta": realized_delta})
		position.num_shares = 0.0
		state.positions.pop(ticker, None)
		return True

	def _close_partial_position(self, state: SimState, event: dict[str, Any], qty: float) -> bool:

		execution_dt = event["received_dt"]
		if not self.tvw_helpers.is_between_8pm_sun_and_8pm_fri_et(execution_dt):
			return False

		ticker = event["ticker"]
		position = state.positions.get(ticker)

		if not position or position.num_shares <= 0:
			return False

		close_qty = min(float(qty), position.num_shares)
		if close_qty <= 0:
			return False		

		try:
			quote = self._get_simulated_quote(
				state,
				event,
			)
		except SimulatedOrderPriceUnavailable as exc:
			logger.warning(
				"Simulated partial exit rejected because no fresh execution price "
				"is available: ticker=%s side=%s qty=%s "
				"received_dt=%s reason=%s",
				ticker,
				position.side,
				close_qty,
				event["received_dt"],
				exc,
			)

			self._record_rejected_order(state=state, event=event, order_type="partial_exit", side=position.side, qty=close_qty, reason=str(exc))
			return False

		if position.side == "long":
			exit_side = "sell"
		else:
			exit_side = "cover"

		price = self._get_execution_price(
			quote,
			exit_side,
		)

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
				decision_time=(event["received_dt"].isoformat() if event.get("received_dt") else None),
			)			

		pnl_cost_basis = (
			position.reporting_baseline_price
			if position.reporting_baseline_price is not None
			else position.avg_price_per_share
		)

		if position.side == "long":
			realized_delta = (price - pnl_cost_basis) * close_qty
		else:
			realized_delta = (pnl_cost_basis - price) * close_qty

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

		if position.num_shares <= self.smallest_share_size:
			state.positions.pop(ticker, None)
		return True


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

				baseline_price = (
					position.reporting_baseline_price
					if position.reporting_baseline_price is not None
					else position.avg_price_per_share
				)

				if position.side == "long":
					unrealized = (price - baseline_price) * position.num_shares
				else:
					unrealized = (baseline_price - price) * position.num_shares

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


	def _dataframe_to_ohlc_rows(
		self,
		df: pd.DataFrame,
		start_dt: datetime,
		end_dt: datetime,
	) -> dict[str, list[dict[str, Any]]]:
		"""
		Convert an Alpaca OHLC DataFrame into JSON-serializable rows grouped
		by ticker.

		Only bars whose source timestamps fall within the requested reporting
		window are included.
		"""
		open_prices = self.trade_records_instance.dataframe_column_to_dict(
			df,
			"open",
		)
		high_prices = self.trade_records_instance.dataframe_column_to_dict(
			df,
			"high",
		)
		low_prices = self.trade_records_instance.dataframe_column_to_dict(
			df,
			"low",
		)
		close_prices = self.trade_records_instance.dataframe_column_to_dict(
			df,
			"close",
		)

		ohlc_by_ticker: dict[str, list[dict[str, Any]]] = {}

		tickers = sorted(
			set(open_prices)
			| set(high_prices)
			| set(low_prices)
			| set(close_prices)
		)

		for ticker in tickers:
			ticker_open = open_prices.get(ticker, {})
			ticker_high = high_prices.get(ticker, {})
			ticker_low = low_prices.get(ticker, {})
			ticker_close = close_prices.get(ticker, {})

			common_timestamps = sorted(
				set(ticker_open)
				& set(ticker_high)
				& set(ticker_low)
				& set(ticker_close),
				key=pd.Timestamp,
			)

			rows = []

			for timestamp in common_timestamps:
				bar_dt = pd.Timestamp(timestamp)

				if bar_dt.tzinfo is None:
					bar_dt = bar_dt.tz_localize(
						self.tvw_helpers.eastern_tz
					)
				else:
					bar_dt = bar_dt.tz_convert(
						self.tvw_helpers.eastern_tz
					)

				bar_datetime = bar_dt.to_pydatetime()

				if not start_dt <= bar_datetime <= end_dt:
					continue

				open_price = float(ticker_open[timestamp])
				high_price = float(ticker_high[timestamp])
				low_price = float(ticker_low[timestamp])
				close_price = float(ticker_close[timestamp])

				if min(
					open_price,
					high_price,
					low_price,
					close_price,
				) <= 0:
					continue

				rows.append({
					"time": bar_datetime.isoformat(),
					"open": open_price,
					"high": high_price,
					"low": low_price,
					"close": close_price,
				})

			if rows:
				ohlc_by_ticker[ticker] = rows

		return ohlc_by_ticker


	def _get_execution_market_price(
		self,
		state: SimState,
		event: dict[str, Any],
		max_bar_age_minutes: int = 5,
	) -> tuple[float, datetime]:
		"""
		Return the close of the most recent real one-minute Alpaca bar that was
		fully completed when the TradingView signal was received.

		Raise SimulatedOrderPriceUnavailable when no sufficiently recent
		execution price exists. Never use a future bar.
		"""
		ticker = str(event["ticker"]).upper().strip()
		received_dt = pd.Timestamp(event["received_dt"])

		ticker_prices = (
			state.market_data
			.get("close_1m", {})
			.get(ticker, {})
		)

		if not ticker_prices:
			raise SimulatedOrderPriceUnavailable(
				f"No one-minute market prices available for {ticker}"
			)

		target_bar_dt = (
			received_dt.floor("min")
			- pd.Timedelta(minutes=1)
		)

		target_key = target_bar_dt.isoformat(sep=" ")
		exact_price = ticker_prices.get(target_key)

		if exact_price is not None:
			exact_price = float(exact_price)

			if exact_price <= 0:
				raise SimulatedOrderPriceUnavailable(
					f"Invalid one-minute price for "
					f"{ticker} at {target_key}: {exact_price}"
				)

			return exact_price, target_bar_dt.to_pydatetime()

		closest_bar_dt = None
		closest_price = None

		for timestamp, price in ticker_prices.items():
			bar_dt = pd.Timestamp(timestamp)

			if bar_dt > target_bar_dt:
				continue

			if (
				closest_bar_dt is None
				or bar_dt > closest_bar_dt
			):
				closest_bar_dt = bar_dt
				closest_price = price

		if closest_bar_dt is None or closest_price is None:
			raise SimulatedOrderPriceUnavailable(
				f"No prior real one-minute bar available for "
				f"{ticker} at or before {target_bar_dt}"
			)

		bar_age = target_bar_dt - closest_bar_dt

		if bar_age > pd.Timedelta(minutes=max_bar_age_minutes):
			raise SimulatedOrderPriceUnavailable(
				f"Closest one-minute bar for {ticker} is too old: "
				f"received_dt={received_dt}, "
				f"target_bar_dt={target_bar_dt}, "
				f"closest_bar_dt={closest_bar_dt}, "
				f"bar_age={bar_age}"
			)

		closest_price = float(closest_price)

		if closest_price <= 0:
			raise SimulatedOrderPriceUnavailable(
				f"Invalid one-minute price for "
				f"{ticker} at {closest_bar_dt}: {closest_price}"
			)

		return closest_price, closest_bar_dt.to_pydatetime()	


	def _get_simulated_quote(
		self,
		state: SimState,
		event: dict[str, Any],
	) -> dict[str, Any]:
		"""
		Construct a simulated bid and ask from the latest completed real
		one-minute Alpaca bar available when the signal was received.
		"""
		market_price, source_bar_dt = (
			self._get_execution_market_price(
				state,
				event,
			)
		)

		execution_dt = event["received_dt"]

		if self.tvw_helpers._is_regular_hours_et(
			execution_dt
		):
			price_offset = 0.01
		else:
			price_offset = 0.05

		return {
			"market": market_price,
			"bid": max(
				0.01,
				market_price - price_offset,
			),
			"ask": market_price + price_offset,
			"source_bar_time": source_bar_dt,
		}	


	def _get_execution_price(
		self,
		quote: dict[str, float],
		order_side: str,
	) -> float:
		"""
		Return the simulated executable quote side.

		Buying or covering executes at the ask.
		Selling or shorting executes at the bid.
		"""
		side = str(order_side or "").strip().lower()

		if side in {"buy", "cover"}:
			return float(quote["ask"])

		if side in {"sell", "short"}:
			return float(quote["bid"])

		raise ValueError(
			f"Unsupported simulated order side: {order_side}"
		)	

	def _record_rejected_order(
		self,
		state: SimState,
		event: dict[str, Any],
		order_type: str,
		side: str,
		qty: float,
		reason: str,
	) -> None:
		if not self.recording_enabled:
			return

		state.trade_events.append({
			"time": event["received_dt"].isoformat(),
			"ticker": event["ticker"],
			"timeframe": event["timeframe"],
			"signal": event["signal"],
			"event_type": "order_rejected",
			"requested_order_type": order_type,
			"side": side,
			"requested_qty": float(qty),
			"reason": reason,
			"stream_id": event.get("stream_id"),
		})


	def _get_market_close_liquidation_times(
		self,
		alpaca_api,
		start_dt: datetime,
		end_dt: datetime,
	) -> list[datetime]:
		"""
		Return one timestamp per trading day, exactly one minute before that
		day's official market close.

		This handles regular 4:00 PM closes and official early-close days.
		"""
		calendar_days = alpaca_api.get_calendar(
			start=start_dt.date().isoformat(),
			end=end_dt.date().isoformat(),
		)

		liquidation_times = []

		for calendar_day in calendar_days:
			trading_date = pd.Timestamp(
				calendar_day.date
			).date()

			close_value = getattr(
				calendar_day,
				"close",
				None,
			)

			if close_value is None:
				continue

			if isinstance(
				close_value,
				time,
			):
				close_time = close_value

			else:
				close_text = str(
					close_value
				).strip()

				close_time = None

				for fmt in (
					"%H:%M:%S",
					"%H:%M",
				):
					try:
						close_time = datetime.strptime(
							close_text,
							fmt,
						).time()

						break

					except ValueError:
						continue

				if close_time is None:
					raise ValueError(
						"Unsupported market close value: "
						f"{close_value!r}"
					)

			close_dt = datetime.combine(
				trading_date,
				close_time,
				tzinfo=self.tvw_helpers.eastern_tz,
			)

			liquidation_dt = (
				close_dt
				- timedelta(minutes=1)
			)

			if start_dt <= liquidation_dt <= end_dt:
				liquidation_times.append(
					liquidation_dt
				)

		return sorted(
			liquidation_times
		)


	def _liquidate_all_positions_before_market_close(
		self,
		state: SimState,
		liquidation_dt: datetime,
	) -> None:
		"""Close every open position using the latest available one-minute price."""
		trading_date = (
			liquidation_dt.date().isoformat()
		)

		state.market_close_liquidation_dates.add(
			trading_date
		)

		open_tickers = list(
			state.positions.keys()
		)

		for ticker in open_tickers:
			position = state.positions.get(
				ticker
			)

			if (
				position is None
				or position.num_shares <= 0
			):
				continue

			market_price = (
				state.last_price_by_ticker.get(
					ticker
				)
			)

			if (
				market_price is None
				or market_price <= 0
			):
				logger.warning(
					"Unable to liquidate position before "
					"market close because no price is available: "
					"ticker=%s liquidation_dt=%s",
					ticker,
					liquidation_dt,
				)
				continue

			self._close_position_at_market_bar(
				state=state,
				ticker=ticker,
				bar_dt=liquidation_dt,
				market_price=float(
					market_price
				),
				exit_reason=(
					"market_close_liquidation"
				),
			)

		self._record_snapshots(
			state,
			liquidation_dt,
		)


	def _set_reporting_baselines(
		self,
		state: SimState,
		start_dt: datetime,
	) -> None:
		"""
		Set the reporting-window baseline for positions inherited from warm-up.

		Use the latest one-minute market bar available at or before the reporting
		window begins rather than relying on potentially stale timeline state.
		"""
		target_dt = pd.Timestamp(
			start_dt
		).floor(
			"min"
		) - pd.Timedelta(
			minutes=1
		)

		for ticker, position in state.positions.items():
			ticker_prices = (
				state.market_data
				.get(
					"close_1m",
					{},
				)
				.get(
					ticker,
					{},
				)
			)

			if not ticker_prices:
				logger.warning(
					"Unable to establish reporting baseline: "
					"ticker=%s start_dt=%s reason=no one-minute prices",
					ticker,
					start_dt,
				)
				continue

			baseline_dt = None
			baseline_price = None

			for timestamp, price in ticker_prices.items():
				bar_dt = pd.Timestamp(
					timestamp
				)

				if bar_dt.tzinfo is None:
					bar_dt = bar_dt.tz_localize(
						self.tvw_helpers.eastern_tz
					)
				else:
					bar_dt = bar_dt.tz_convert(
						self.tvw_helpers.eastern_tz
					)

				if bar_dt > target_dt:
					continue

				if (
					baseline_dt is None
					or bar_dt > baseline_dt
				):
					baseline_dt = bar_dt
					baseline_price = float(
						price
					)

			if (
				baseline_dt is None
				or baseline_price is None
				or baseline_price <= 0
			):
				logger.warning(
					"Unable to establish reporting baseline: "
					"ticker=%s start_dt=%s target_dt=%s",
					ticker,
					start_dt,
					target_dt,
				)
				continue

			position.reporting_baseline_price = baseline_price

			state.reporting_baselines[ticker] = {
				"ticker": ticker,
				"side": position.side,
				"num_shares": position.num_shares,
				"original_avg_price": position.avg_price_per_share,
				"baseline_price": baseline_price,
				"baseline_time": baseline_dt.isoformat(),
			}			

			logger.info(
				"Reporting baseline established: "
					"ticker=%s side=%s qty=%s original_avg=%s "
					"baseline_price=%s baseline_dt=%s start_dt=%s",
				ticker,
				position.side,
				position.num_shares,
				position.avg_price_per_share,
				position.reporting_baseline_price,
				baseline_dt,
				start_dt,
			)
