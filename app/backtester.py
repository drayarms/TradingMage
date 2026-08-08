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

import csv
import hashlib
import json
import os
import zipfile
import threading
import numpy as np

logger = logging.getLogger("tv-webhook")

BACKTEST_FACTOR_RESEARCH_LOG_PATH = os.getenv(
	"TV_BACKTEST_FACTOR_RESEARCH_LOG",
	"/app/logs/backtest_factor_research.jsonl",
)

BACKTEST_FACTOR_RESEARCH_FILE_LOCK = threading.RLock()

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
	entry_atr: Optional[float] = None
	original_atr_multiplier: Optional[float] = None
	active_atr_multiplier: Optional[float] = None
	loss_liquidation_atr_factor: Optional[float] = None
	profit_expansion_atr_factor: Optional[float] = None
	trailing_stop_multiplier_factor: float = 1.0
	trailing_stop_expanded: bool = False
	trailing_stop_expanded_at: Optional[datetime] = None	
	entry_time: Optional[datetime] = None
	entry_signal_time: Optional[datetime] = None
	entry_price: Optional[float] = None
	entry_quantity: Optional[float] = None
	entry_features: dict[str, Optional[float]] = field(default_factory=dict)
	trade_id: Optional[str] = None	


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
	completed_trade_records: list[dict[str, Any]] = field(default_factory=list)
	research_group_id: Optional[str] = None
	record_factor_research: bool = False
	research_reporting_start: Optional[datetime] = None
	research_reporting_end: Optional[datetime] = None
	research_run_metadata: dict[str, Any] = field(
		default_factory=dict
	)	


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
			"default_position_size": 6600.0,
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
			"default_position_size": 6600.0,
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
		"strategy4_15m_anchor": {
			"entry_tf": "15m",
			"intermediary_tf": "15m",
			"anchor_tf": "15m",
			"lower_timeframes": set(),
			"default_position_size": 2000.0,
			"exit_strategy": 4,
			"warmup_sessions": 2,
		},
		"strategy4_1h_anchor": {
			"entry_tf": "1h",
			"intermediary_tf": "1h",
			"anchor_tf": "1h",
			"lower_timeframes": set(),
			"default_position_size": 6600.0,
			"exit_strategy": 4,
			"warmup_sessions": 5,
		},
		"strategy4_4h_anchor": {
			"entry_tf": "4h",
			"intermediary_tf": "4h",
			"anchor_tf": "4h",
			"lower_timeframes": set(),
			"default_position_size": 20000.0,
			"exit_strategy": 4,
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
		loss_liquidation_atr_factor: Optional[float] = None,
		profit_expansion_atr_factor: Optional[float] = None,
		trailing_stop_multiplier_factor: float = 1.0,		
		#warmup_days: int = 2,
		warmup_sessions: Optional[int] = None,
		exit_strategy: Optional[int] = None,
		liquidate_before_market_close: bool = False,
		run_exit_strategy: bool = True,
		record_factor_research: bool = False,
		research_group_id: Optional[str] = None,		
	) -> dict[str, Any]:
		"""
		Run a chronological simulation with an optional unreported warm-up period.

		Warm-up events are fully processed so signal context and any positions that
		remain open at ``start`` carry into the requested backtest window. Warm-up
		trades, realized PnL, snapshots, exposure rows, and diagnostic trade records
		are excluded from the returned result.

		Run ATR-sensitive exit strategies on a merged signal and
		one-minute-price timeline.
		
		Parameters:
			strategy_name: Strategy config name, such as "strategy1_15m_anchor".
			start: Inclusive reporting start datetime. Naive datetimes are Eastern Time.
			end: Inclusive reporting end datetime. Naive datetimes are Eastern Time.
			tickers: Optional list of ticker symbols. If omitted, discover from Redis.
			position_size: Optional dollar notional used to size the first entry signal.
			warmup_sessions: Number of prior market sessions to simulate before start. 
							 If omitted, the strategy-specific configured value is used.
			exit_strategy: Optional override for the strategy config. Must be 1, 2, 3, or 4.

		Returns:
			Dictionary containing only the requested reporting window, while preserving
			open positions and signal context created during warm-up.
		"""
		if not isinstance(
			record_factor_research,
			bool,
		):
			raise ValueError(
				"record_factor_research must be a boolean"
			)

		if record_factor_research:
			research_group_id = str(
				research_group_id or ""
			).strip()

			if not research_group_id:
				raise ValueError(
					"research_group_id is required when "
					"record_factor_research=true"
				)

			if loss_liquidation_atr_factor is None:
				raise ValueError(
					"loss_liquidation_atr_factor is required when "
					"record_factor_research=true"
				)		
		if not isinstance(
			run_exit_strategy,
			bool,
		):
			raise ValueError(
				"run_exit_strategy must be a boolean"
			)

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

		if profit_expansion_atr_factor is not None:
			try:
				profit_expansion_atr_factor = float(
					profit_expansion_atr_factor
				)
			except (TypeError, ValueError) as exc:
				raise ValueError(
					"profit_expansion_atr_factor must be a number"
				) from exc

			if profit_expansion_atr_factor <= 0:
				raise ValueError(
					"profit_expansion_atr_factor must be > 0"
				)

		try:
			trailing_stop_multiplier_factor = float(
				trailing_stop_multiplier_factor
			)
		except (TypeError, ValueError) as exc:
			raise ValueError(
				"trailing_stop_multiplier_factor must be a number"
			) from exc

		if trailing_stop_multiplier_factor < 1:
			raise ValueError(
				"trailing_stop_multiplier_factor must be >= 1"
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
					"exit_strategy must be 1, 2, 3, or 4"
				) from exc

			if selected_exit_strategy not in {1, 2, 3, 4}:
				raise ValueError(
					"exit_strategy must be 1, 2, or 3, or 4"
				)
		else:
			selected_exit_strategy = default_exit_strategy

		config["selected_exit_strategy"] = selected_exit_strategy
		config["ATR_multiplier"] = ATR_multiplier
		config["loss_liquidation_atr_factor"] = (loss_liquidation_atr_factor)
		config["profit_expansion_atr_factor"] = (profit_expansion_atr_factor)
		config["trailing_stop_multiplier_factor"] = (trailing_stop_multiplier_factor)		
		config["liquidate_before_market_close"] = (liquidate_before_market_close  and run_exit_strategy)
		config["run_exit_strategy"] = run_exit_strategy
		config["entry_validation_only"] = not run_exit_strategy		

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

		anchor_ATR = self.trade_records_instance.dataframe_to_atr_dict(anchor_df,period=ATR_period)

		#anchor_entry_features = (self._build_anchor_entry_features(anchor_df=anchor_df,anchor_tf=anchor_tf,atr_period=ATR_period,))	
		anchor_entry_features = {}

		if record_factor_research:
			#anchor_entry_features = self._build_anchor_entry_features(anchor_df=anchor_df,anchor_tf=anchor_tf,atr_period=ATR_period,)
			anchor_entry_features = self._build_anchor_entry_features(anchor_df=anchor_df,anchor_tf=anchor_tf,anchor_atr=anchor_ATR)

		anchor_ohlc = self._dataframe_to_ohlc_rows(anchor_df,start_dt,end_dt,)

		state = SimState()

		state.research_group_id = research_group_id
		state.record_factor_research = record_factor_research
		state.research_reporting_start = start_dt
		state.research_reporting_end = end_dt

		market_close_liquidation_times = []

		if config["liquidate_before_market_close"]:
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
			"anchor_entry_features":anchor_entry_features,
			"market_close_liquidation_times":market_close_liquidation_times,
		}	

		if position_size is None:
			position_size = float(config["default_position_size"])
		else:
			position_size = float(position_size)

		if position_size <= 0:
			raise ValueError("position_size must be > 0")
		state.research_run_metadata = {
			"research_group_id": research_group_id,
			"strategy_name": strategy_name,
			"start": start_dt.isoformat(),
			"end": end_dt.isoformat(),
			"position_size": position_size,
			"anchor_timeframe": config["anchor_tf"],
			"exit_strategy": selected_exit_strategy,
			"ATR_period": ATR_period,
			"ATR_multiplier": ATR_multiplier,
			"loss_liquidation_atr_factor": (loss_liquidation_atr_factor),
			"liquidate_before_market_close": (config["liquidate_before_market_close"]),
		}		

		close_1m_data = state.market_data.get(
			"close_1m",
			{},
		)

		target_windows = {
			"GS": (
				pd.Timestamp(
					"2026-08-04T04:15:00-04:00"
				),
				pd.Timestamp(
					"2026-08-04T05:05:00-04:00"
				),
			),
			"MS": (
				pd.Timestamp(
					"2026-08-04T08:20:00-04:00"
				),
				pd.Timestamp(
					"2026-08-04T09:05:00-04:00"
				),
			),
		}

		for diagnostic_ticker in {
			"GS",
			"MS",
		}:
			ticker_prices = close_1m_data.get(
				diagnostic_ticker,
				{},
			)

			if not ticker_prices:
				logger.warning(
					"[1M_DATA] no one-minute prices loaded: "
					"ticker=%r",
					diagnostic_ticker,
				)
				continue

			sorted_timestamps = []

			for timestamp in ticker_prices:
				normalized_timestamp = pd.Timestamp(
					timestamp
				)

				if normalized_timestamp.tzinfo is None:
					normalized_timestamp = (
						normalized_timestamp.tz_localize(
							self.tvw_helpers.eastern_tz
						)
					)
				else:
					normalized_timestamp = (
						normalized_timestamp.tz_convert(
							self.tvw_helpers.eastern_tz
						)
					)

				sorted_timestamps.append(
					normalized_timestamp
				)

			sorted_timestamps.sort()

			logger.info(
				"[1M_DATA] summary: "
				"ticker=%r bars=%d first=%s last=%s",
				diagnostic_ticker,
				len(sorted_timestamps),
				sorted_timestamps[0],
				sorted_timestamps[-1],
			)

			window_start, window_end = (
				target_windows[
					diagnostic_ticker
				]
			)

			window_timestamps = [
				timestamp
				for timestamp in sorted_timestamps
				if (
					window_start
					<= timestamp
					<= window_end
				)
			]

			logger.info(
				"[1M_DATA] target window: "
				"ticker=%r start=%s end=%s "
				"bars_found=%d timestamps=%r",
				diagnostic_ticker,
				window_start,
				window_end,
				len(window_timestamps),
				[
					str(timestamp)
					for timestamp in window_timestamps
				],
			)


		self.diagnostic_logging_enabled = (
			(end_dt - start_dt) <= timedelta(days=self.BACKTEST_DIAGNOSTIC_MAX_DAYS)
		)

		warmup_events = [event for event in all_events if event["received_dt"] < start_dt]
		report_events = [event for event in all_events if event["received_dt"] >= start_dt]

		#if (config["run_exit_strategy"] and config["selected_exit_strategy"] == 3):
		use_price_tracked_backtest = (
			config["run_exit_strategy"]
			and (
				config["selected_exit_strategy"] == 3
				or (
					config["selected_exit_strategy"] == 4
					and config[
						"loss_liquidation_atr_factor"
					] is not None
				)
			)
		)

		if use_price_tracked_backtest:				
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

		research_records_written = 0

		if record_factor_research:
			research_records_written = self._append_factor_research_records(state.completed_trade_records)	

		return {
			"strategy_name": strategy_name,
			"exit_strategy": config["selected_exit_strategy"],
			"ATR_period": ATR_period,
			"ATR_multiplier": ATR_multiplier,
			"loss_liquidation_atr_factor": (loss_liquidation_atr_factor),
			"profit_expansion_atr_factor": (profit_expansion_atr_factor),
			"trailing_stop_multiplier_factor": (trailing_stop_multiplier_factor),
			"defensive_liquidation_count": sum(
				1
				for trade_event in state.trade_events
				if trade_event.get("exit_reason")
				== "atr_cost_basis_liquidation"
			),
			"trailing_stop_expansion_count": sum(
				1
				for trade_event in state.trade_events
				if trade_event.get("event_type")
				== "trailing_stop_expanded"
			),			
			"liquidate_before_market_close": (
				config["liquidate_before_market_close"]
			),
			"run_exit_strategy": run_exit_strategy,
			"entry_validation_only": config["entry_validation_only"],
			"entry_condition_count": sum(
				1
				for trade_event in state.trade_events
				if trade_event.get("event_type") == "entry_condition"
			),	
			"trade_attempt_count": sum(
				1
				for trade_event in state.trade_events
				if trade_event.get("event_type")
				!= "entry_condition"
			),					
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
				if trade_event.get("event_type")
				not in {
					"order_rejected",
					"entry_condition",
				}
			),
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
			"record_factor_research":
				record_factor_research,
			"research_group_id":
				research_group_id,
			"research_records_written":
				research_records_written,
			"completed_trade_records":
				state.completed_trade_records,						
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
		"""
		Dispatch one signal event to the configured strategy family.

		Parameters:
			strategy_name (str):
				Configured strategy name.

			state (SimState):
				Current in-memory backtest state.

			config (dict):
				Resolved strategy configuration.

			event (dict):
				Current chronological signal event.

			position_size (float):
				Dollar notional used to calculate entry quantity.
		"""
		if strategy_name.startswith("strategy1_"):
			self._process_strategy1_event(strategy_name, state, config, event, position_size)
		elif strategy_name.startswith("strategy2_"):
			self._process_strategy2_event(strategy_name, state, config, event, position_size)
		elif strategy_name.startswith("strategy4_"):
			self._process_strategy4_event(strategy_name, state, config, event, position_size)
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
		Run signal-driven exit strategies on a merged signal and sampled-price timeline.

		Strategies 1, 2, and 4 use this path. Exit Strategy 4 currently performs no
		action. Exit Strategy 3 uses the separate minute-price-tracked path.

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

			#if timeline_event["kind"] == "market_bar":
				#self._process_trailing_stop_market_bar(
					#state,
					#config,
					#payload,
				#)


			if timeline_event["kind"] == "market_bar":
				if config["selected_exit_strategy"] == 3:
					self._process_trailing_stop_market_bar(
						state,
						config,
						payload,
					)

				elif config["selected_exit_strategy"] == 4:
					self._process_atr_liquidation_market_bar(
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

			#self._process_exit_strategy3_signal(
				#strategy_name,
				#state,
				#config,
				#payload,
				#position_size,
			#)

			if config["selected_exit_strategy"] == 3:
				self._process_exit_strategy3_signal(
					strategy_name,
					state,
					config,
					payload,
					position_size,
				)

			elif config["selected_exit_strategy"] == 4:
				self._register_event_context(
					state,
					payload,
				)

				self._process_strategy4_event(
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
		"""
		Evaluate entries without running a signal-driven exit.

		This path is used when Exit Strategy 3 manages positions through
		the one-minute market-price timeline.
		"""
		event["exit_strategy"] = 3
		event["anchor_tf"] = config["anchor_tf"]
		event["ATR_multiplier"] = config["ATR_multiplier"]
		event["loss_liquidation_atr_factor"] = config["loss_liquidation_atr_factor"]
		event["profit_expansion_atr_factor"] = config["profit_expansion_atr_factor"]
		event["trailing_stop_multiplier_factor"] = config["trailing_stop_multiplier_factor"]		
		event["entry_validation_only"] = config["entry_validation_only"]

		if strategy_name.startswith("strategy1_"):
			self._process_strategy1_entry_only(strategy_name, state, config, event, position_size)
		elif strategy_name.startswith("strategy2_"):
			self._process_strategy2_entry_only(strategy_name, state, config, event, position_size)
		elif strategy_name.startswith("strategy4_"):
			self._process_strategy4_entry_only(strategy_name, state, config, event, position_size)
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

	def _process_strategy4_entry_only(
		self,
		strategy_name: str,
		state: SimState,
		config: dict[str, Any],
		event: dict[str, Any],
		position_size: float,
	) -> None:
		"""
		Evaluate Strategy 4 entries without running a signal-driven exit.

		This method is used by Exit Strategy 3. It opens actual simulated
		positions so the trailing-stop execution path can manage them.
		"""
		now_et = event["dt"]
		signal = event["signal"]
		symbol = event["ticker"]
		tf = event["timeframe"]
		market_price = event["price"]

		if market_price is None or market_price <= 0:
			return

		num_shares = (
			position_size
			/ market_price
		)

		self.strategies_instance.entry_strategy4(
			strategy_name,
			config["anchor_tf"],
			True,
			now_et,
			signal,
			None,
			symbol,
			tf,
			num_shares,
			None,
			state,
			config,
			event,
			market_price,
			self,
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
				f"Invalid exit_strategy for {name}; expected 1, 2, 3, or 4"
			) from exc

		if config["exit_strategy"] not in {1, 2, 3, 4}:
			raise ValueError(
				f"Invalid exit_strategy for {name}; expected 1, 2, 3, or 4"
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
		event["entry_validation_only"] = config["entry_validation_only"]

		if config["run_exit_strategy"]:
			if config["selected_exit_strategy"] == 1:
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
					self,
				)

			elif config["selected_exit_strategy"] == 2:
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
			config["anchor_tf"],
			True,
			now_et,
			signal,
			None,
			symbol,
			tf,
			num_shares,
			None,
			state,
			config,
			event,
			market_price,
			self,
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
		event["entry_validation_only"] = config["entry_validation_only"]

		if config["run_exit_strategy"]:
			if config["selected_exit_strategy"] == 1:
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
					self,
				)

			elif config["selected_exit_strategy"] == 2:
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
			num_shares,
			None,
			state,
			config,
			event,
			market_price,
			self,
		)


	def _process_strategy4_event(
		self,
		strategy_name: str,
		state: SimState,
		config: dict[str, Any],
		event: dict[str, Any],
		position_size: float,
	) -> None:
		"""
		Apply Strategy 4 to one chronological anchor signal event.

		Strategy 4 uses the ordinary signal-driven execution path. The configured
		exit function is called before the entry function, matching the execution
		order used by Strategies 1 and 2. Exit Strategy 4 currently does nothing.

		Parameters:
			strategy_name (str):
				Configured Strategy 4 name.

			state (SimState):
				Current in-memory backtest state.

			config (dict):
				Resolved strategy configuration.

			event (dict):
				Current chronological event.

			position_size (float):
				Dollar notional used to calculate entry quantity.
		"""
		now_et = event["dt"]
		signal = event["signal"]
		symbol = event["ticker"]
		tf = event["timeframe"]
		market_price = event["price"]

		if market_price is None or market_price <= 0:
			return

		num_shares = position_size / market_price

		event["exit_strategy"] = config[
			"selected_exit_strategy"
		]
		event["anchor_tf"] = config[
			"anchor_tf"
		]
		event["ATR_multiplier"] = config[
			"ATR_multiplier"
		]
		event["loss_liquidation_atr_factor"] = config[
			"loss_liquidation_atr_factor"
		]		
		event["entry_validation_only"] = config[
			"entry_validation_only"
		]

		if config["run_exit_strategy"]:
			if config["selected_exit_strategy"] == 1:
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
					self,
				)

			elif config["selected_exit_strategy"] == 2:
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

			elif config["selected_exit_strategy"] == 4:
				self.strategies_instance.exit_strategy4(
					strategy_name,
					config["anchor_tf"],
					config["loss_liquidation_atr_factor"],
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

		self.strategies_instance.entry_strategy4(
			strategy_name,
			config["anchor_tf"],
			True,
			now_et,
			signal,
			None,
			symbol,
			tf,
			num_shares,
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


	def _record_valid_entry_condition(
		self,
		state: SimState,
		event: dict[str, Any],
		position_side: str,
		qty: float,
	) -> bool:
		"""
		Record a valid entry condition without creating or changing
		a simulated position.
		"""
		try:
			quote = self._get_simulated_quote(
				state,
				event,
			)
		except SimulatedOrderPriceUnavailable as exc:
			logger.warning(
				"Validated entry condition has no fresh execution price: "
				"ticker=%s side=%s qty=%s received_dt=%s reason=%s",
				event["ticker"],
				position_side,
				qty,
				event["received_dt"],
				exc,
			)

			if self.recording_enabled:
				self._record_rejected_order(
					state=state,
					event=event,
					order_type="entry_condition",
					side=position_side,
					qty=qty,
					reason=str(exc),
				)

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

		if self.recording_enabled:
			state.trade_events.append({
				"time": event["received_dt"].isoformat(),
				"signal_time": event["time"],
				"ticker": event["ticker"],
				"event_type": "entry_condition",
				"side": position_side,
				"price": price,
				"num_shares": qty,
				"realized_delta": 0.0,
			})

		return True


	def _open_or_add_position(self, state: SimState, event: dict[str, Any], position_side: str, qty: float) -> bool:

		execution_dt = event["received_dt"]

		if not self.tvw_helpers.is_between_8pm_sun_and_8pm_fri_et(
			execution_dt
		):
			return False

		if event.get("entry_validation_only"):
			return self._record_valid_entry_condition(
				state=state,
				event=event,
				position_side=position_side,
				qty=qty,
			)

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
				anchor_atr, trailing_stop_source_time = (
					self._get_anchor_atr_at_entry(
						state,
						event,
					)
				)
				if anchor_atr is None:
					return False

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
					trailing_stop_price = (
						price
						- trailing_stop_amount
					)
				else:
					trailing_stop_price = (
						price
						+ trailing_stop_amount
					)

				position_kwargs = {
					"trailing_stop_amount":
						trailing_stop_amount,
					"trailing_stop_price":
						trailing_stop_price,
					"trailing_stop_source_time":
						trailing_stop_source_time,
					"last_trailing_bar_time":
						event["received_dt"],
					"entry_atr":
						anchor_atr,
					"original_atr_multiplier":
						ATR_multiplier,
					"active_atr_multiplier":
						ATR_multiplier,
					"loss_liquidation_atr_factor":
						event.get(
							"loss_liquidation_atr_factor"
						),
					"profit_expansion_atr_factor":
						event.get(
							"profit_expansion_atr_factor"
						),
					"trailing_stop_multiplier_factor":
						float(
							event.get(
								"trailing_stop_multiplier_factor",
								1.0,
							)
						),
				}

			elif (
				event.get("exit_strategy") == 4
				and event.get(
					"loss_liquidation_atr_factor"
				) is not None
			):			
				anchor_atr, atr_source_time = (
					self._get_anchor_atr_at_entry(
						state,
						event,
					)
				)
				if anchor_atr is None:
					return False				

				position_kwargs = {
					"entry_atr":
						anchor_atr,
					"trailing_stop_source_time":
						atr_source_time,
					"last_trailing_bar_time":
						event["received_dt"],
					"loss_liquidation_atr_factor":
						event.get(
							"loss_liquidation_atr_factor"
						),
				}				

			#entry_signal_time = event.get(
				#"dt"
			#) or event.get(
				#"received_dt"
			#)

			entry_signal_time = None
			entry_features = {}
			trade_id = None

			if state.record_factor_research:
				entry_signal_time = (event.get("dt") or event.get("received_dt"))
				entry_features = self._get_entry_features(state,event)

				trade_id = self._build_research_trade_id(strategy_name=event.get("strategy_name",""),ticker=ticker,side=position_side,entry_signal_time=entry_signal_time)

			state.positions[ticker] = SimPosition(
				ticker=ticker,
				side=position_side,
				avg_price_per_share=price,
				num_shares=qty,
				high_water_price=price,
				low_water_price=price,
				entry_time=event["received_dt"],
				entry_signal_time=entry_signal_time,
				entry_price=price,
				entry_quantity=qty,
				entry_features=entry_features,
				trade_id=trade_id,

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
		config: dict[str, Any],
		market_event: dict[str, Any],
	) -> bool:
		"""
		Advance one Exit Strategy 3 position using a completed one-minute close.

		The processing priority is:

			1. Defensive cost-basis liquidation.
			2. One-time profitable-position trailing-stop expansion.
			3. Normal trailing-stop movement and execution.
		"""
		ticker = market_event["ticker"]
		bar_dt = market_event["dt"]
		market_price = float(
			market_event["close"]
		)
		position = state.positions.get(
			ticker
		)

		state.last_price_by_ticker[
			ticker
		] = market_price

		if (
			position is None
			or position.num_shares <= 0
		):
			return False

		if (
			position.trailing_stop_amount is None
			or position.trailing_stop_amount <= 0
			or position.entry_atr is None
			or position.entry_atr <= 0
		):
			return False

		if (
			position.last_trailing_bar_time is not None
			and bar_dt
			<= position.last_trailing_bar_time
		):
			return False

		position.last_trailing_bar_time = bar_dt

		if not self.tvw_helpers._is_regular_hours_et(
			bar_dt
		):
			return False

		entry_atr = float(
			position.entry_atr
		)
		cost_basis = float(
			position.avg_price_per_share
		)

		loss_distance = None
		defensive_exit_price = None
		defensive_exit_triggered = False

		profit_distance = None
		expansion_trigger_price = None
		expansion_triggered = False		

		#
		# 1. Defensive liquidation below/above cost basis.
		#
		loss_factor = (
			position.loss_liquidation_atr_factor
		)

		if loss_factor is not None:
			loss_distance = (
				entry_atr
				* float(loss_factor)
			)

			if position.side == "long":
				defensive_exit_price = (
					cost_basis
					- loss_distance
				)

				defensive_exit_triggered = (
					market_price
					<= defensive_exit_price
				)

			else:
				defensive_exit_price = (
					cost_basis
					+ loss_distance
				)

				defensive_exit_triggered = (
					market_price
					>= defensive_exit_price
				)

			if defensive_exit_triggered:
				if (
					self.diagnostic_logging_enabled
					and self.recording_enabled
					and ticker in {
						"CRM",
						"ADBE",
						"SNOW",
						"NFLX",
						"AMAT",
					}
				):
					logger.info(
						"[EXIT3_STATE] "
						"source=sim "
						"ticker=%r "
						"time=%s "
						"decision=%r "
						"side=%r "
						"current_price=%.6f "
						"entry_price=%.6f "
						"entry_atr=%.6f "
						"high_water_price=%r "
						"low_water_price=%r "
						"original_multiplier=%r "
						"active_multiplier=%r "
						"trailing_amount=%r "
						"trailing_stop_price=%r "
						"loss_distance=%r "
						"defensive_price=%r "
						"defensive_triggered=%r "
						"profit_distance=%r "
						"expansion_price=%r "
						"expansion_triggered=%r "
						"stop_expanded=%r",
						ticker,
						bar_dt.isoformat(),
						"atr_cost_basis_liquidation",
						position.side,
						market_price,
						cost_basis,
						entry_atr,
						position.high_water_price,
						position.low_water_price,
						position.original_atr_multiplier,
						position.active_atr_multiplier,
						position.trailing_stop_amount,
						position.trailing_stop_price,
						loss_distance,
						defensive_exit_price,
						defensive_exit_triggered,
						profit_distance,
						expansion_trigger_price,
						expansion_triggered,
						position.trailing_stop_expanded,
					)

				return self._close_position_at_market_bar(
					state=state,
					ticker=ticker,
					bar_dt=bar_dt,
					market_price=market_price,
					exit_reason=(
						"atr_cost_basis_liquidation"
					),
				)

				return True

		#
		# 2. One-time expansion after sufficient profit.
		#
		profit_factor = (
			position.profit_expansion_atr_factor
		)
		multiplier_factor = float(
			position.trailing_stop_multiplier_factor
		)

		can_expand = (
			not position.trailing_stop_expanded
			and profit_factor is not None
			and multiplier_factor > 1
			and position.original_atr_multiplier
			is not None
		)

		if can_expand:
			profit_distance = (
				entry_atr
				* float(profit_factor)
			)

			if position.side == "long":
				expansion_trigger_price = (
					cost_basis
					+ profit_distance
				)

				expansion_triggered = (
					market_price
					>= expansion_trigger_price
				)

			else:
				expansion_trigger_price = (
					cost_basis
					- profit_distance
				)

				expansion_triggered = (
					market_price
					<= expansion_trigger_price
				)

			if expansion_triggered:
				original_trailing_amount = float(
					position.trailing_stop_amount
				)
				original_atr_multiplier = float(
					position.original_atr_multiplier
				)
				expanded_atr_multiplier = (
					original_atr_multiplier
					* multiplier_factor
				)
				expanded_trailing_amount = (
					entry_atr
					* expanded_atr_multiplier
				)

				position.active_atr_multiplier = (
					expanded_atr_multiplier
				)
				position.trailing_stop_amount = (
					expanded_trailing_amount
				)
				position.trailing_stop_expanded = True
				position.trailing_stop_expanded_at = (
					bar_dt
				)

				#
				# Simulate canceling the old trailing stop and
				# submitting a new one at the current price.
				#
				if position.side == "long":
					position.high_water_price = (
						market_price
					)
					position.trailing_stop_price = (
						market_price
						- expanded_trailing_amount
					)
				else:
					position.low_water_price = (
						market_price
					)
					position.trailing_stop_price = (
						market_price
						+ expanded_trailing_amount
					)

				if self.recording_enabled:
					state.trade_events.append({
						"time": bar_dt.isoformat(),
						"ticker": ticker,
						"event_type": (
							"trailing_stop_expanded"
						),
						"side": position.side,
						"market_price": market_price,
						"cost_basis": cost_basis,
						"entry_atr": entry_atr,
						"profit_expansion_atr_factor": (
							float(profit_factor)
						),
						"expansion_trigger_price": (
							expansion_trigger_price
						),
						"original_atr_multiplier": (
							original_atr_multiplier
						),
						"active_atr_multiplier": (
							expanded_atr_multiplier
						),
						"trailing_stop_multiplier_factor": (
							multiplier_factor
						),
						"original_trailing_stop_amount": (
							original_trailing_amount
						),
						"trailing_stop_amount": (
							expanded_trailing_amount
						),
						"trailing_stop_price": (
							position.trailing_stop_price
						),
						"realized_delta": 0.0,
					})

		if (
			self.diagnostic_logging_enabled
			and self.recording_enabled
			and ticker in {
				"CRM",
				"ADBE",
				"SNOW",
				"NFLX",
				"AMAT",
			}
		):
			logger.info(
				"[EXIT3_STATE] "
				"source=sim "
				"ticker=%r "
				"time=%s "
				"decision=%r "
				"side=%r "
				"current_price=%.6f "
				"entry_price=%.6f "
				"entry_atr=%.6f "
				"high_water_price=%r "
				"low_water_price=%r "
				"original_multiplier=%r "
				"active_multiplier=%r "
				"trailing_amount=%r "
				"trailing_stop_price=%r "
				"loss_distance=%r "
				"defensive_price=%r "
				"defensive_triggered=%r "
				"profit_distance=%r "
				"expansion_price=%r "
				"expansion_triggered=%r "
				"stop_expanded=%r",
				ticker,
				bar_dt.isoformat(),
				"continue",
				position.side,
				market_price,
				cost_basis,
				entry_atr,
				position.high_water_price,
				position.low_water_price,
				position.original_atr_multiplier,
				position.active_atr_multiplier,
				position.trailing_stop_amount,
				position.trailing_stop_price,
				loss_distance,
				defensive_exit_price,
				defensive_exit_triggered,
				profit_distance,
				expansion_trigger_price,
				expansion_triggered,
				position.trailing_stop_expanded,
			)
		#
		# 3. Continue normal trailing-stop processing.
		#
		trailing_amount = float(
			position.trailing_stop_amount
		)

		if position.side == "long":
			position.high_water_price = max(
				position.high_water_price
				or market_price,
				market_price,
			)
			position.trailing_stop_price = (
				position.high_water_price
				- trailing_amount
			)

			if (
				market_price
				<= position.trailing_stop_price
			):
				if (
					self.diagnostic_logging_enabled
					and self.recording_enabled
					and ticker in {
						"CRM",
						"ADBE",
						"SNOW",
						"NFLX",
						"AMAT",
					}
				):
					logger.info(
						"[EXIT3_STATE] "
						"source=sim "
						"ticker=%r "
						"time=%s "
						"decision=%r "
						"side=%r "
						"current_price=%.6f "
						"entry_price=%.6f "
						"entry_atr=%.6f "
						"high_water_price=%r "
						"low_water_price=%r "
						"active_multiplier=%r "
						"trailing_amount=%r "
						"trailing_stop_price=%r "
						"stop_expanded=%r",
						ticker,
						bar_dt.isoformat(),
						"trailing_stop",
						position.side,
						market_price,
						cost_basis,
						entry_atr,
						position.high_water_price,
						position.low_water_price,
						position.active_atr_multiplier,
						position.trailing_stop_amount,
						position.trailing_stop_price,
						position.trailing_stop_expanded,
					)			
				return self._close_position_at_market_bar(
					state=state,
					ticker=ticker,
					bar_dt=bar_dt,
					market_price=market_price,
					exit_reason="trailing_stop",
				)

				return True

		elif position.side == "short":
			position.low_water_price = min(
				position.low_water_price
				or market_price,
				market_price,
			)
			position.trailing_stop_price = (
				position.low_water_price
				+ trailing_amount
			)

			if (
				market_price
				>= position.trailing_stop_price
			):
				if (
					self.diagnostic_logging_enabled
					and self.recording_enabled
					and ticker in {
						"CRM",
						"ADBE",
						"SNOW",
						"NFLX",
						"AMAT",
					}
				):
					logger.info(
						"[EXIT3_STATE] "
						"source=sim "
						"ticker=%r "
						"time=%s "
						"decision=%r "
						"side=%r "
						"current_price=%.6f "
						"entry_price=%.6f "
						"entry_atr=%.6f "
						"high_water_price=%r "
						"low_water_price=%r "
						"active_multiplier=%r "
						"trailing_amount=%r "
						"trailing_stop_price=%r "
						"stop_expanded=%r",
						ticker,
						bar_dt.isoformat(),
						"trailing_stop",
						position.side,
						market_price,
						cost_basis,
						entry_atr,
						position.high_water_price,
						position.low_water_price,
						position.active_atr_multiplier,
						position.trailing_stop_amount,
						position.trailing_stop_price,
						position.trailing_stop_expanded,
					)			
				return self._close_position_at_market_bar(
					state=state,
					ticker=ticker,
					bar_dt=bar_dt,
					market_price=market_price,
					exit_reason="trailing_stop",
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
				"entry_atr": position.entry_atr,
				"cost_basis": position.avg_price_per_share,
				"original_atr_multiplier": (position.original_atr_multiplier),
				"active_atr_multiplier": (position.active_atr_multiplier),
				"loss_liquidation_atr_factor": (position.loss_liquidation_atr_factor),
				"profit_expansion_atr_factor": (position.profit_expansion_atr_factor),
				"trailing_stop_multiplier_factor": (position.trailing_stop_multiplier_factor),
				"trailing_stop_expanded": (position.trailing_stop_expanded),
				"trailing_stop_expanded_at": (
					position.trailing_stop_expanded_at.isoformat()
					if position.trailing_stop_expanded_at
					is not None
					else None
				),
				"trailing_stop_amount": (position.trailing_stop_amount),
				"trailing_stop_price": (position.trailing_stop_price),
				"high_water_price": (position.high_water_price),
				"low_water_price": (position.low_water_price),
			})		

		self._record_completed_research_trade(
			state=state,
			position=position,
			exit_time=bar_dt,
			exit_price=fill_price,
			exit_reason=exit_reason,
			realized_pnl=realized_delta,
		)

		state.positions.pop(ticker, None)

		return True


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
		Build a ZIP containing an optional overall PnL chart followed by one
		candlestick PNG for each ticker with an entry or validated entry condition.
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
				"entry_condition",
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
			history = result.get(
				"overall_pnl_history"
			) or []

			if history:
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

		self._record_completed_research_trade(
			state=state,
			position=position,
			exit_time=execution_dt,
			exit_price=price,
			exit_reason=str(
				event.get(
					"exit_reason"
				)
				or "opposite_signal"
			),
			realized_pnl=realized_delta,
		)			

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

			return self._close_position_at_market_bar(
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


	def _record_valid_entry_condition(
		self,
		state: SimState,
		event: dict[str, Any],
		position_side: str,
		qty: float,
	) -> bool:
		"""
		Record a valid entry condition without opening, adding to,
		reversing, or closing a simulated position.
		"""
		if not self.recording_enabled:
			return True

		try:
			quote = self._get_simulated_quote(
				state,
				event,
			)

		except SimulatedOrderPriceUnavailable as exc:
			logger.warning(
				"Validated entry condition has no fresh marker price: "
				"ticker=%s side=%s received_dt=%s reason=%s",
				event["ticker"],
				position_side,
				event["received_dt"],
				exc,
			)

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

		state.trade_events.append({
			"time": event["received_dt"].isoformat(),
			"signal_time": event["time"],
			"ticker": event["ticker"],
			"event_type": "entry_condition",
			"side": position_side,
			"price": price,
			"num_shares": qty,
			"realized_delta": 0.0,
		})

		return True


	def _process_atr_liquidation_market_bar(
		self,
		state: SimState,
		market_event: dict[str, Any],
	) -> bool:
		ticker = market_event["ticker"]
		bar_dt = market_event["dt"]
		#market_price = float(
			#market_event["close"]
		#)
		try:
			market_price = float(
				market_event["close"]
			)
		except (KeyError, TypeError, ValueError):
			return False

		if market_price <= 0:
			return False

		state.last_price_by_ticker[
			ticker
		] = market_price

		return self._check_atr_cost_basis_liquidation(
			state=state,
			ticker=ticker,
			bar_dt=bar_dt,
			market_price=market_price,
		)


	def _check_atr_cost_basis_liquidation(
		self,
		state: SimState,
		ticker: str,
		bar_dt: datetime,
		market_price: float,
	) -> bool:
		position = state.positions.get(
			ticker
		)

		if position is None:
			return False

		if position.entry_atr is None:
			return False

		loss_factor = (
			position.loss_liquidation_atr_factor
		)

		if loss_factor is None:
			return False

		if not self.tvw_helpers._is_regular_hours_et(
			bar_dt
		):
			return False

		entry_atr = float(
			position.entry_atr
		)
		cost_basis = float(
			position.avg_price_per_share
		)
		loss_distance = (
			entry_atr
			* float(loss_factor)
		)

		if position.side == "long":
			defensive_exit_price = (
				cost_basis
				- loss_distance
			)
			defensive_exit_triggered = (
				market_price
				<= defensive_exit_price
			)
		else:
			defensive_exit_price = (
				cost_basis
				+ loss_distance
			)
			defensive_exit_triggered = (
				market_price
				>= defensive_exit_price
			)

		if not defensive_exit_triggered:
			return False

		logger.info(
			"ATR cost-basis liquidation: "
			"ticker=%r time=%s side=%r "
			"current_price=%r cost_basis=%r "
			"entry_atr=%r loss_factor=%r "
			"defensive_exit_price=%r",
			ticker,
			bar_dt.isoformat(),
			position.side,
			market_price,
			cost_basis,
			entry_atr,
			loss_factor,
			defensive_exit_price,
		)

		return self._close_position_at_market_bar(
			state=state,
			ticker=ticker,
			bar_dt=bar_dt,
			market_price=market_price,
			exit_reason=(
				"atr_cost_basis_liquidation"
			),
		)


	#def _build_anchor_entry_features(
		#self,
		#anchor_df: pd.DataFrame,
		#anchor_tf: str,
		#atr_period: int,
	#) -> dict[str, list[dict[str, Any]]]:
	def _build_anchor_entry_features(
		self,
		anchor_df: pd.DataFrame,
		anchor_tf: str,
		anchor_atr: dict[str, dict[Any, float]],
	) -> dict[str, list[dict[str, Any]]]:	
		"""
		Precompute OHLCV-derived entry features for every completed anchor bar.

		Each feature row includes the time at which the anchor bar became
		available. Entry lookups therefore use only information that was
		available at the simulated decision time.
		"""
		if anchor_df.empty:
			return {}

		required_columns = {
			"symbol",
			"open",
			"high",
			"low",
			"close",
		}

		missing_columns = (
			required_columns
			- set(anchor_df.columns)
		)

		if missing_columns:
			raise ValueError(
				"Anchor DataFrame is missing required columns: "
				f"{sorted(missing_columns)}"
			)

		anchor_duration = self._timeframe_timedelta(
			anchor_tf
		)

		working = anchor_df.copy()

		working.index = pd.to_datetime(
			working.index,
			utc=True,
		).tz_convert(
			self.tvw_helpers.eastern_tz
		)

		features_by_ticker: dict[
			str,
			list[dict[str, Any]],
		] = {}

		for ticker, ticker_df in working.groupby(
			"symbol",
			sort=False,
		):
			ticker = str(
				ticker
			).upper().strip()

			group = (
				ticker_df
				.sort_index()
				.loc[
					lambda frame:
						~frame.index.duplicated(
							keep="last"
						)
				]
				.copy()
			)

			high = group["high"].astype(float)
			low = group["low"].astype(float)
			close = group["close"].astype(float)

			#previous_close = close.shift(1)

			#true_range = pd.concat(
				#[
					#high - low,
					#(high - previous_close).abs(),
					#(low - previous_close).abs(),
				#],
				#axis=1,
			#).max(
				#axis=1
			#)

			#atr = true_range.rolling(
				#window=atr_period,
				#min_periods=atr_period,
			#).mean()

			#atr_mean_20 = atr.rolling(
				#window=20,
				#min_periods=10,
			#).mean()


			ticker_atr = (
				anchor_atr.get(
					ticker,
					{},
				)
			)

			atr = pd.Series(
				index=group.index,
				dtype=float,
			)

			for timestamp, atr_value in ticker_atr.items():
				atr_timestamp = pd.Timestamp(
					timestamp
				)

				if atr_timestamp.tzinfo is None:
					atr_timestamp = (
						atr_timestamp.tz_localize(
							self.tvw_helpers.eastern_tz
						)
					)
				else:
					atr_timestamp = (
						atr_timestamp.tz_convert(
							self.tvw_helpers.eastern_tz
						)
					)

				if atr_timestamp in atr.index:
					atr.loc[
						atr_timestamp
					] = float(
						atr_value
					)


			atr_mean_20 = (
				atr
				.rolling(
					window=20,
					min_periods=10,
				)
				.mean()
			)




			returns = close.pct_change()

			range_percent = (
				(high - low)
				/ close.replace(
					0.0,
					np.nan,
				)
				* 100.0
			)

			ema_10 = close.ewm(
				span=10,
				adjust=False,
			).mean()

			ema_20 = close.ewm(
				span=20,
				adjust=False,
			).mean()

			def efficiency_ratio(
				lookback: int,
			) -> pd.Series:
				net_change = (
					close
					- close.shift(
						lookback
					)
				).abs()

				total_movement = (
					close.diff()
					.abs()
					.rolling(
						window=lookback,
						min_periods=lookback,
					)
					.sum()
				)

				return (
					net_change
					/ total_movement.replace(
						0.0,
						np.nan,
					)
				)

			feature_frame = pd.DataFrame(
				index=group.index
			)

			feature_frame["vol_atr_percent"] = (
				atr
				/ close.replace(
					0.0,
					np.nan,
				)
				* 100.0
			)

			feature_frame["vol_atr_to_mean_ratio"] = (
				atr
				/ atr_mean_20.replace(
					0.0,
					np.nan,
				)
			)

			feature_frame["vol_atr_change_percent_5"] = (
				(
					atr
					/ atr.shift(
						5
					)
				)
				- 1.0
			) * 100.0

			feature_frame["vol_return_stddev_10"] = (
				returns
				.rolling(
					window=10,
					min_periods=10,
				)
				.std()
				* 100.0
			)

			feature_frame["vol_return_stddev_20"] = (
				returns
				.rolling(
					window=20,
					min_periods=20,
				)
				.std()
				* 100.0
			)

			feature_frame[
				"vol_average_range_percent_10"
			] = (
				range_percent
				.rolling(
					window=10,
					min_periods=10,
				)
				.mean()
			)

			feature_frame[
				"vol_average_range_percent_20"
			] = (
				range_percent
				.rolling(
					window=20,
					min_periods=20,
				)
				.mean()
			)

			feature_frame[
				"vol_efficiency_ratio_10"
			] = efficiency_ratio(
				10
			)

			feature_frame[
				"vol_efficiency_ratio_20"
			] = efficiency_ratio(
				20
			)

			feature_frame[
				"vol_ema_10_slope_percent_5"
			] = (
				(
					ema_10
					/ ema_10.shift(
						5
					)
				)
				- 1.0
			) * 100.0

			feature_frame[
				"vol_distance_from_ema_20_percent"
			] = (
				(
					close
					- ema_20
				)
				/ ema_20.replace(
					0.0,
					np.nan,
				)
				* 100.0
			)

			rows = []

			for source_time, feature_row in (
				feature_frame.iterrows()
			):
				available_time = (
					source_time
					+ anchor_duration
				)

				row = {
					"source_time":
						source_time.to_pydatetime(),
					"available_time":
						available_time.to_pydatetime(),
				}

				for column_name, value in (
					feature_row.items()
				):
					if pd.isna(
						value
					):
						row[column_name] = None
					else:
						row[column_name] = float(
							value
						)

				rows.append(
					row
				)

			features_by_ticker[
				ticker
			] = rows

		return features_by_ticker


	def _get_entry_features(
		self,
		state: SimState,
		event: dict[str, Any],
	) -> dict[str, Optional[float]]:
		"""
		Return the latest completed anchor feature row available at entry.
		"""
		ticker = str(
			event["ticker"]
		).upper().strip()

		entry_time = pd.Timestamp(
			event["received_dt"]
		)

		if entry_time.tzinfo is None:
			entry_time = entry_time.tz_localize(
				self.tvw_helpers.eastern_tz
			)
		else:
			entry_time = entry_time.tz_convert(
				self.tvw_helpers.eastern_tz
			)

		rows = (
			state.market_data
			.get(
				"anchor_entry_features",
				{},
			)
			.get(
				ticker,
				[],
			)
		)

		selected_row = None

		for row in rows:
			available_time = pd.Timestamp(
				row["available_time"]
			)

			if available_time.tzinfo is None:
				available_time = (
					available_time.tz_localize(
						self.tvw_helpers.eastern_tz
					)
				)
			else:
				available_time = (
					available_time.tz_convert(
						self.tvw_helpers.eastern_tz
					)
				)

			if available_time > entry_time:
				break

			selected_row = row

		if selected_row is None:
			return {}

		return {
			key: value
			for key, value in selected_row.items()
			if key.startswith(
				"vol_"
			)
		}


	def _build_research_trade_id(
		self,
		strategy_name: str,
		ticker: str,
		side: str,
		entry_signal_time: datetime,
	) -> str:
		raw_key = "|".join(
			[
				str(
					strategy_name
				).strip(),
				str(
					ticker
				).upper().strip(),
				str(
					side
				).lower().strip(),
				pd.Timestamp(
					entry_signal_time
				).isoformat(),
			]
		)

		return hashlib.sha256(
			raw_key.encode(
				"utf-8"
			)
		).hexdigest()


	def _record_completed_research_trade(
		self,
		state: SimState,
		position: SimPosition,
		exit_time: datetime,
		exit_price: float,
		exit_reason: str,
		realized_pnl: float,
	) -> None:
		if not state.record_factor_research:
			return

		if position.entry_time is None:
			return

		reporting_start = state.research_reporting_start
		reporting_end = state.research_reporting_end

		if (
			reporting_start is not None
			and position.entry_time
			< reporting_start
		):
			# Do not include a position inherited from warm-up.
			return

		if (
			reporting_end is not None
			and position.entry_time
			> reporting_end
		):
			return

		entry_notional = (
			float(
				position.avg_price_per_share
			)
			* float(
				position.num_shares
			)
		)

		pnl_percent = (
			float(
				realized_pnl
			)
			/ entry_notional
			* 100.0
			if entry_notional > 0
			else 0.0
		)

		record = {
			**state.research_run_metadata,

			"trade_id": position.trade_id,
			"ticker": position.ticker,
			"side": position.side,

			"entry_time": (
				position.entry_time.isoformat()
			),
			"entry_signal_time": (
				position.entry_signal_time.isoformat()
				if position.entry_signal_time
				is not None
				else None
			),
			"initial_entry_price": (
				float(
					position.entry_price
				)
				if position.entry_price is not None
				else None
			),
			"initial_entry_quantity": (
				float(
					position.entry_quantity
				)
				if position.entry_quantity is not None
				else None
			),

			"final_cost_basis": float(
				position.avg_price_per_share
			),
			"final_quantity": float(
				position.num_shares
			),
			"entry_notional": entry_notional,
			"entry_sequence_count": int(
				position.entry_sequence_count
			),

			"exit_time": pd.Timestamp(
				exit_time
			).isoformat(),
			"exit_price": float(
				exit_price
			),
			"exit_reason": str(
				exit_reason
			),

			"pnl": float(
				realized_pnl
			),
			"pnl_percent": float(
				pnl_percent
			),

			**position.entry_features,
		}

		state.completed_trade_records.append(
			record
		)


	def _append_factor_research_records(
		self,
		records: list[dict[str, Any]],
	) -> int:
		if not records:
			return 0

		directory = os.path.dirname(
			BACKTEST_FACTOR_RESEARCH_LOG_PATH
		)

		if directory:
			os.makedirs(
				directory,
				exist_ok=True,
			)

		with BACKTEST_FACTOR_RESEARCH_FILE_LOCK:
			with open(
				BACKTEST_FACTOR_RESEARCH_LOG_PATH,
				"a",
				encoding="utf-8",
			) as research_file:
				for record in records:
					research_file.write(
						json.dumps(
							record,
							sort_keys=True,
							allow_nan=False,
						)
					)

					research_file.write(
						"\n"
					)

		return len(
			records
		)


	def delete_factor_research_records(
		self,
		research_group_ids: Optional[list[str]] = None,
		delete_all: bool = False,
	) -> dict[str, Any]:
		"""
		Delete factor-research records from the JSONL file.

		When delete_all is false, only records whose research_group_id
		matches one of research_group_ids are removed.
		"""
		normalized_group_ids = {
			str(
				group_id
			).strip()
			for group_id in (
				research_group_ids or []
			)
			if str(
				group_id
			).strip()
		}

		if (
			not delete_all
			and not normalized_group_ids
		):
			raise ValueError(
				"Provide at least one research_group_id "
				"or set delete_all=true"
			)

		research_path = (
			BACKTEST_FACTOR_RESEARCH_LOG_PATH
		)

		if not os.path.exists(
			research_path
		):
			return {
				"ok": True,
				"delete_all": delete_all,
				"requested_research_group_ids": sorted(
					normalized_group_ids
				),
				"records_before": 0,
				"records_deleted": 0,
				"records_remaining": 0,
				"deleted_research_group_ids": [],
				"message": (
					"No factor-research file existed"
				),
			}

		directory = os.path.dirname(
			research_path
		)

		if directory:
			os.makedirs(
				directory,
				exist_ok=True,
			)

		temporary_path = (
			f"{research_path}.delete.tmp"
		)

		records_before = 0
		records_deleted = 0
		records_remaining = 0

		deleted_group_ids = set()
		found_group_ids = set()

		with BACKTEST_FACTOR_RESEARCH_FILE_LOCK:
			try:
				with open(
					research_path,
					"r",
					encoding="utf-8",
				) as source_file:
					with open(
						temporary_path,
						"w",
						encoding="utf-8",
					) as destination_file:
						for line_number, line in enumerate(
							source_file,
							start=1,
						):
							stripped_line = line.strip()

							if not stripped_line:
								continue

							try:
								record = json.loads(
									stripped_line
								)

							except json.JSONDecodeError:
								logger.warning(
									"Preserving invalid factor-research "
									"JSONL row during deletion: line=%s",
									line_number,
								)

								destination_file.write(
									line
								)

								continue

							records_before += 1

							record_group_id = str(
								record.get(
									"research_group_id",
									"",
								)
							).strip()

							if record_group_id:
								found_group_ids.add(
									record_group_id
								)

							should_delete = (
								delete_all
								or record_group_id
								in normalized_group_ids
							)

							if should_delete:
								records_deleted += 1

								if record_group_id:
									deleted_group_ids.add(
										record_group_id
									)

								continue

							destination_file.write(
								json.dumps(
									record,
									sort_keys=True,
									allow_nan=False,
								)
							)

							destination_file.write(
								"\n"
							)

							records_remaining += 1

						destination_file.flush()

						os.fsync(
							destination_file.fileno()
						)

				os.replace(
					temporary_path,
					research_path,
				)

			finally:
				if os.path.exists(
					temporary_path
				):
					os.remove(
						temporary_path
					)

		missing_group_ids = (
			normalized_group_ids
			- found_group_ids
		)

		return {
			"ok": True,
			"delete_all": delete_all,
			"requested_research_group_ids": sorted(
				normalized_group_ids
			),
			"deleted_research_group_ids": sorted(
				deleted_group_ids
			),
			"missing_research_group_ids": sorted(
				missing_group_ids
			),
			"records_before": records_before,
			"records_deleted": records_deleted,
			"records_remaining": records_remaining,
		}


	def build_factor_research_chart_zip(
		self,
		research_group_id: str,
		minimum_pnl_margin: float = 0.0,
		pnl_tie_tolerance: float = 0.0,
		require_all_factors: bool = True,
	) -> io.BytesIO:
		research_group_id = str(
			research_group_id or ""
		).strip()

		if not research_group_id:
			raise ValueError(
				"research_group_id is required"
			)

		if minimum_pnl_margin < 0:
			raise ValueError(
				"minimum_pnl_margin must be >= 0"
			)

		if pnl_tie_tolerance < 0:
			raise ValueError(
				"pnl_tie_tolerance must be >= 0"
			)

		if not os.path.exists(
			BACKTEST_FACTOR_RESEARCH_LOG_PATH
		):
			raise ValueError(
				"No factor-research file exists"
			)

		records = []

		with BACKTEST_FACTOR_RESEARCH_FILE_LOCK:
			with open(
				BACKTEST_FACTOR_RESEARCH_LOG_PATH,
				"r",
				encoding="utf-8",
			) as research_file:
				for line_number, line in enumerate(
					research_file,
					start=1,
				):
					line = line.strip()

					if not line:
						continue

					try:
						record = json.loads(
							line
						)

					except json.JSONDecodeError:
						logger.warning(
							"Skipping invalid factor-research "
							"JSONL row: line=%s",
							line_number,
						)

						continue

					if (
						record.get(
							"research_group_id"
						)
						!= research_group_id
					):
						continue

					records.append(
						record
					)

		if not records:
			raise ValueError(
				"No records found for research_group_id="
				f"{research_group_id!r}"
			)

		df = pd.DataFrame(
			records
		)

		required_columns = {
			"trade_id",
			"loss_liquidation_atr_factor",
			"pnl",
			"pnl_percent",
		}

		missing_columns = (
			required_columns
			- set(df.columns)
		)

		if missing_columns:
			raise ValueError(
				"Research data is missing required columns: "
				f"{sorted(missing_columns)}"
			)

		df = (
			df
			.sort_values(
				[
					"trade_id",
					"loss_liquidation_atr_factor",
					"exit_time",
				]
			)
			.drop_duplicates(
				subset=[
					"trade_id",
					"loss_liquidation_atr_factor",
				],
				keep="last",
			)
		)

		tested_factors = sorted(
			float(
				value
			)
			for value in (
				df[
					"loss_liquidation_atr_factor"
				]
				.dropna()
				.unique()
			)
		)

		if len(
			tested_factors
		) < 2:
			raise ValueError(
				"At least two liquidation factors are required"
			)

		factor_count_by_trade = (
			df.groupby(
				"trade_id"
			)[
				"loss_liquidation_atr_factor"
			]
			.nunique()
		)

		if require_all_factors:
			complete_trade_ids = (
				factor_count_by_trade[
					factor_count_by_trade
					== len(
						tested_factors
					)
				]
				.index
			)

			comparison_df = df[
				df["trade_id"].isin(
					complete_trade_ids
				)
			].copy()
		else:
			comparison_df = df.copy()

		if comparison_df.empty:
			raise ValueError(
				"No comparable trades remain after "
				"factor-completeness filtering"
			)

		best_rows = []

		for trade_id, trade_group in (
			comparison_df.groupby(
				"trade_id",
				sort=False,
			)
		):
			group = trade_group.sort_values(
				[
					"pnl_percent",
					"loss_liquidation_atr_factor",
				],
				ascending=[
					False,
					True,
				],
			)

			maximum_pnl = float(
				group["pnl_percent"].max()
			)

			acceptable = group[
				group["pnl_percent"]
				>= (
					maximum_pnl
					- pnl_tie_tolerance
				)
			]

			# Choose the tightest factor among effective ties.
			best_row = (
				acceptable
				.sort_values(
					"loss_liquidation_atr_factor"
				)
				.iloc[0]
				.copy()
			)

			ordered_pnl = (
				group["pnl_percent"]
				.sort_values(
					ascending=False
				)
				.tolist()
			)

			second_best_pnl = (
				float(
					ordered_pnl[1]
				)
				if len(
					ordered_pnl
				) > 1
				else float(
					ordered_pnl[0]
				)
			)

			best_row[
				"best_factor_pnl_margin"
			] = (
				maximum_pnl
				- second_best_pnl
			)

			best_rows.append(
				best_row
			)

		best_df = pd.DataFrame(
			best_rows
		)

		best_df = best_df[
			best_df[
				"best_factor_pnl_margin"
			]
			>= minimum_pnl_margin
		].copy()

		if best_df.empty:
			raise ValueError(
				"No trades remain after minimum_pnl_margin filtering"
			)

		feature_columns = sorted(
			column
			for column in best_df.columns
			if column.startswith(
				"vol_"
			)
		)

		if not feature_columns:
			raise ValueError(
				"No volatility-feature columns were found"
			)

		summary_rows = []
		zip_buffer = io.BytesIO()

		with zipfile.ZipFile(
			zip_buffer,
			"w",
			compression=zipfile.ZIP_DEFLATED,
		) as zip_file:
			for chart_number, feature_name in enumerate(
				feature_columns,
				start=1,
			):
				plot_df = (
					best_df[
						[
							feature_name,
							"loss_liquidation_atr_factor",
							"best_factor_pnl_margin",
							"trade_id",
							"ticker",
						]
					]
					.dropna(
						subset=[
							feature_name,
							"loss_liquidation_atr_factor",
						]
					)
					.copy()
				)

				if plot_df.empty:
					continue

				x_values = plot_df[
					feature_name
				].astype(
					float
				)

				y_values = plot_df[
					"loss_liquidation_atr_factor"
				].astype(
					float
				)

				pearson = (
					float(
						x_values.corr(
							y_values,
							method="pearson",
						)
					)
					if len(
						plot_df
					) >= 2
					else None
				)

				spearman = (
					float(
						x_values.corr(
							y_values,
							method="spearman",
						)
					)
					if len(
						plot_df
					) >= 2
					else None
				)

				slope = None
				intercept = None
				r_squared = None

				if (
					len(
						plot_df
					) >= 2
					and x_values.nunique() >= 2
				):
					slope, intercept = np.polyfit(
						x_values,
						y_values,
						1,
					)

					predicted = (
						slope
						* x_values
						+ intercept
					)

					residual_sum = float(
						(
							(
								y_values
								- predicted
							)
							** 2
						).sum()
					)

					total_sum = float(
						(
							(
								y_values
								- y_values.mean()
							)
							** 2
						).sum()
					)

					r_squared = (
						1.0
						- residual_sum
						/ total_sum
						if total_sum > 0
						else None
					)

				figure, axis = plt.subplots(
					figsize=(
						11,
						7,
					)
				)

				random_generator = (
					np.random.default_rng(
						42
					)
				)

				y_jittered = (
					y_values
					+ random_generator.normal(
						loc=0.0,
						scale=0.008,
						size=len(
							y_values
						),
					)
				)

				axis.scatter(
					x_values,
					y_jittered,
					alpha=0.45,
				)

				if (
					slope is not None
					and intercept is not None
				):
					line_x = np.linspace(
						float(
							x_values.min()
						),
						float(
							x_values.max()
						),
						100,
					)

					line_y = (
						slope
						* line_x
						+ intercept
					)

					axis.plot(
						line_x,
						line_y,
					)

				axis.set_title(
					f"{feature_name} vs ideal loss-liquidation factor"
				)

				axis.set_xlabel(
					feature_name
				)

				axis.set_ylabel(
					"Ideal loss-liquidation ATR factor"
				)

				axis.grid(
					True,
					alpha=0.25,
				)

				figure.tight_layout()

				image_buffer = io.BytesIO()

				figure.savefig(
					image_buffer,
					format="png",
					dpi=150,
				)

				plt.close(
					figure
				)

				image_buffer.seek(
					0
				)

				zip_file.writestr(
					(
						f"{chart_number:02d}_"
						f"{feature_name}_vs_ideal_factor.png"
					),
					image_buffer.getvalue(),
				)

				summary_rows.append({
					"feature": feature_name,
					"trade_count": len(
						plot_df
					),
					"pearson_correlation": pearson,
					"spearman_correlation": spearman,
					"linear_slope": (
						float(
							slope
						)
						if slope is not None
						else None
					),
					"linear_intercept": (
						float(
							intercept
						)
						if intercept is not None
						else None
					),
					"r_squared": r_squared,
				})

			best_csv = best_df.to_csv(
				index=False
			)

			summary_csv = pd.DataFrame(
				summary_rows
			).to_csv(
				index=False
			)

			zip_file.writestr(
				"best_factor_by_trade.csv",
				best_csv,
			)

			zip_file.writestr(
				"feature_relationship_summary.csv",
				summary_csv,
			)

			metadata = {
				"research_group_id":
					research_group_id,
				"tested_factors":
					tested_factors,
				"raw_record_count":
					len(
						df
					),
				"comparable_trade_count":
					int(
						comparison_df[
							"trade_id"
						].nunique()
					),
				"plotted_trade_count":
					int(
						best_df[
							"trade_id"
						].nunique()
					),
				"minimum_pnl_margin":
					minimum_pnl_margin,
				"pnl_tie_tolerance":
					pnl_tie_tolerance,
				"require_all_factors":
					require_all_factors,
			}

			zip_file.writestr(
				"analysis_metadata.json",
				json.dumps(
					metadata,
					indent=2,
					sort_keys=True,
				),
			)

		zip_buffer.seek(
			0
		)

		return zip_buffer


	def list_factor_research_groups(
		self,
	) -> dict[str, Any]:
		"""
		Return a summary of every research group stored in the
		factor-research JSONL file.
		"""
		research_path = (
			BACKTEST_FACTOR_RESEARCH_LOG_PATH
		)

		if not os.path.exists(
			research_path
		):
			return {
				"ok": True,
				"research_file_exists": False,
				"group_count": 0,
				"record_count": 0,
				"invalid_line_count": 0,
				"groups": [],
			}

		group_summaries: dict[
			str,
			dict[str, Any],
		] = {}

		record_count = 0
		invalid_line_count = 0

		with BACKTEST_FACTOR_RESEARCH_FILE_LOCK:
			with open(
				research_path,
				"r",
				encoding="utf-8",
			) as research_file:
				for line_number, line in enumerate(
					research_file,
					start=1,
				):
					line = line.strip()

					if not line:
						continue

					try:
						record = json.loads(
							line
						)

					except json.JSONDecodeError:
						invalid_line_count += 1

						logger.warning(
							"Skipping invalid factor-research "
							"JSONL row while listing groups: "
							"line=%s",
							line_number,
						)

						continue

					research_group_id = str(
						record.get(
							"research_group_id",
							"",
						)
					).strip()

					if not research_group_id:
						invalid_line_count += 1

						logger.warning(
							"Skipping factor-research row with "
							"no research_group_id: line=%s",
							line_number,
						)

						continue

					record_count += 1

					if (
						research_group_id
						not in group_summaries
					):
						group_summaries[
							research_group_id
						] = {
							"research_group_id":
								research_group_id,
							"record_count": 0,
							"trade_ids": set(),
							"strategies": set(),
							"anchor_timeframes": set(),
							"exit_strategies": set(),
							"liquidation_factors": set(),
							"atr_periods": set(),
							"atr_multipliers": set(),
							"position_sizes": set(),
							"backtest_starts": set(),
							"backtest_ends": set(),
							"liquidate_before_market_close":
								set(),
							"entry_times": [],
							"exit_times": [],
							"tickers": set(),
							"sides": set(),
							"exit_reasons": set(),
						}

					summary = group_summaries[
						research_group_id
					]

					summary["record_count"] += 1

					trade_id = record.get(
						"trade_id"
					)

					if trade_id:
						summary["trade_ids"].add(
							str(
								trade_id
							)
						)

					strategy_name = record.get(
						"strategy_name"
					)

					if strategy_name:
						summary["strategies"].add(
							str(
								strategy_name
							)
						)

					anchor_timeframe = record.get(
						"anchor_timeframe"
					)

					if anchor_timeframe:
						summary[
							"anchor_timeframes"
						].add(
							str(
								anchor_timeframe
							)
						)

					exit_strategy = record.get(
						"exit_strategy"
					)

					if exit_strategy is not None:
						summary[
							"exit_strategies"
						].add(
							int(
								exit_strategy
							)
						)

					liquidation_factor = record.get(
						"loss_liquidation_atr_factor"
					)

					if liquidation_factor is not None:
						try:
							summary[
								"liquidation_factors"
							].add(
								float(
									liquidation_factor
								)
							)

						except (
							TypeError,
							ValueError,
						):
							pass

					atr_period = record.get(
						"ATR_period"
					)

					if atr_period is not None:
						try:
							summary[
								"atr_periods"
							].add(
								int(
									atr_period
								)
							)

						except (
							TypeError,
							ValueError,
						):
							pass

					atr_multiplier = record.get(
						"ATR_multiplier"
					)

					if atr_multiplier is not None:
						try:
							summary[
								"atr_multipliers"
							].add(
								float(
									atr_multiplier
								)
							)

						except (
							TypeError,
							ValueError,
						):
							pass

					position_size = record.get(
						"position_size"
					)

					if position_size is not None:
						try:
							summary[
								"position_sizes"
							].add(
								float(
									position_size
								)
							)

						except (
							TypeError,
							ValueError,
						):
							pass

					backtest_start = record.get(
						"start"
					)

					if backtest_start:
						summary[
							"backtest_starts"
						].add(
							str(
								backtest_start
							)
						)

					backtest_end = record.get(
						"end"
					)

					if backtest_end:
						summary[
							"backtest_ends"
						].add(
							str(
								backtest_end
							)
						)

					market_close_liquidation = (
						record.get(
							"liquidate_before_market_close"
						)
					)

					if (
						market_close_liquidation
						is not None
					):
						summary[
							"liquidate_before_market_close"
						].add(
							bool(
								market_close_liquidation
							)
						)

					entry_time = record.get(
						"entry_time"
					)

					if entry_time:
						try:
							summary[
								"entry_times"
							].append(
								pd.Timestamp(
									entry_time
								)
							)

						except Exception:
							pass

					exit_time = record.get(
						"exit_time"
					)

					if exit_time:
						try:
							summary[
								"exit_times"
							].append(
								pd.Timestamp(
									exit_time
								)
							)

						except Exception:
							pass

					ticker = record.get(
						"ticker"
					)

					if ticker:
						summary["tickers"].add(
							str(
								ticker
							).upper().strip()
						)

					side = record.get(
						"side"
					)

					if side:
						summary["sides"].add(
							str(
								side
							).lower().strip()
						)

					exit_reason = record.get(
						"exit_reason"
					)

					if exit_reason:
						summary[
							"exit_reasons"
						].add(
							str(
								exit_reason
							)
						)

		groups = []

		for research_group_id, summary in (
			group_summaries.items()
		):
			entry_times = summary.pop(
				"entry_times"
			)

			exit_times = summary.pop(
				"exit_times"
			)

			trade_ids = summary.pop(
				"trade_ids"
			)

			summary["unique_trade_count"] = len(
				trade_ids
			)

			summary["earliest_entry_time"] = (
				min(
					entry_times
				).isoformat()
				if entry_times
				else None
			)

			summary["latest_entry_time"] = (
				max(
					entry_times
				).isoformat()
				if entry_times
				else None
			)

			summary["earliest_exit_time"] = (
				min(
					exit_times
				).isoformat()
				if exit_times
				else None
			)

			summary["latest_exit_time"] = (
				max(
					exit_times
				).isoformat()
				if exit_times
				else None
			)

			for set_field in [
				"strategies",
				"anchor_timeframes",
				"exit_strategies",
				"liquidation_factors",
				"atr_periods",
				"atr_multipliers",
				"position_sizes",
				"backtest_starts",
				"backtest_ends",
				"liquidate_before_market_close",
				"tickers",
				"sides",
				"exit_reasons",
			]:
				summary[set_field] = sorted(
					summary[set_field]
				)

			groups.append(
				summary
			)

		groups.sort(
			key=lambda group: (
				group.get(
					"latest_entry_time"
				)
				or "",
				group[
					"research_group_id"
				],
			),
			reverse=True,
		)

		return {
			"ok": True,
			"research_file_exists": True,
			"group_count": len(
				groups
			),
			"record_count": record_count,
			"invalid_line_count":
				invalid_line_count,
			"groups": groups,
		}

