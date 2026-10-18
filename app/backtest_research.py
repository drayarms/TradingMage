from __future__ import annotations

import csv
import hashlib
import io
import json
import logging
import math
import os
import statistics
import threading
import zipfile
from bisect import bisect_left, bisect_right
from datetime import datetime, timedelta
from itertools import combinations
from typing import Any, Optional, TYPE_CHECKING

import numpy as np
import pandas as pd


if TYPE_CHECKING:
	from backtester import BackTester, SimPosition, SimState


logger = logging.getLogger("tv-webhook")

BACKTEST_FACTOR_RESEARCH_LOG_PATH = os.getenv(
	"TV_BACKTEST_FACTOR_RESEARCH_LOG",
	os.path.join(
		os.getcwd(),
		"logs",
		"backtest_factor_research.jsonl",
	),
)

BACKTEST_FACTOR_RESEARCH_FILE_LOCK = threading.RLock()

BACKTEST_EXIT_RESEARCH_LOG_PATH = os.getenv(
	"TV_BACKTEST_EXIT_RESEARCH_LOG",
	os.path.join(
		os.getcwd(),
		"logs",
		"backtest_exit_research.jsonl",
	),
)

BACKTEST_EXIT_RESEARCH_FILE_LOCK = threading.RLock()

EXIT_RESEARCH_TIMEFRAMES = {
	"1m",
	"5m",
	"15m",
	"1h",
	"4h",
}


class BacktestResearch:
	"""Factor research and analysis for baseline backtest strategies."""
	def __init__(self, backtester: BackTester):
		self.backtester = backtester
		self.tvw_helpers = backtester.tvw_helpers
		self.r = backtester.r


	def __getattr__(self, name):
		return getattr(self.backtester, name)


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


	def _get_anchor_features_at_time(
		self,
		state: SimState,
		ticker: str,
		observation_time: datetime,
	) -> dict[str, Optional[float]]:
		ticker = str(
			ticker
		).upper().strip()

		target_time = pd.Timestamp(
			observation_time
		)

		if target_time.tzinfo is None:
			target_time = target_time.tz_localize(
				self.tvw_helpers.eastern_tz
			)
		else:
			target_time = target_time.tz_convert(
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
				row[
					"available_time"
				]
			)

			if available_time.tzinfo is None:
				available_time = (
					available_time
					.tz_localize(
						self.tvw_helpers.eastern_tz
					)
				)
			else:
				available_time = (
					available_time
					.tz_convert(
						self.tvw_helpers.eastern_tz
					)
				)

			if available_time > target_time:
				break

			selected_row = row

		if selected_row is None:
			return {}

		return {
			key:
				value
			for key, value in (
				selected_row.items()
			)
			if key.startswith(
				"vol_"
			)
		}


	def _get_threshold_recent_price_features(
		self,
		state: SimState,
		ticker: str,
		observation_time: datetime,
	) -> dict[str, Optional[float]]:
		ticker = str(
			ticker
		).upper().strip()

		close_by_time = (
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

		if not close_by_time:
			return {}

		target_time = pd.Timestamp(
			observation_time
		)

		if target_time.tzinfo is None:
			target_time = target_time.tz_localize(
				self.tvw_helpers.eastern_tz
			)
		else:
			target_time = target_time.tz_convert(
				self.tvw_helpers.eastern_tz
			)

		rows = []

		for timestamp, value in (
			close_by_time.items()
		):
			try:
				row_time = pd.Timestamp(
					timestamp
				)

				if row_time.tzinfo is None:
					row_time = row_time.tz_localize(
						self.tvw_helpers.eastern_tz
					)
				else:
					row_time = row_time.tz_convert(
						self.tvw_helpers.eastern_tz
					)

				price = float(
					value
				)

			except (
				TypeError,
				ValueError,
			):
				continue

			if (
				row_time <= target_time
				and price > 0
			):
				rows.append(
					(
						row_time,
						price,
					)
				)

		if not rows:
			return {}

		rows.sort(
			key=lambda item:
				item[
					0
				]
		)

		prices = [
			price
			for _, price in rows
		]

		current = prices[
			-1
		]

		def return_over(
			lookback: int,
		) -> Optional[float]:
			if len(
				prices
			) <= lookback:
				return None

			previous = prices[
				-(lookback + 1)
			]

			if previous == 0:
				return None

			return (
				(
					current
					/ previous
				)
				- 1.0
			) * 100.0

		return {
			"recent_return_1_percent":
				return_over(
					1
				),

			"recent_return_3_percent":
				return_over(
					3
				),

			"recent_return_5_percent":
				return_over(
					5
				),
		}


	def _capture_research_threshold_snapshot(
		self,
		state: SimState,
		position: SimPosition,
		bar_dt: datetime,
		market_price: float,
	) -> None:
		if not state.record_factor_research:
			return

		if (
			position.research_threshold_time
			is not None
		):
			return

		if position.entry_atr is None:
			return

		if position.entry_time is None:
			return

		entry_atr = float(
			position.entry_atr
		)

		if entry_atr <= 0:
			return

		cost_basis = float(
			position.avg_price_per_share
		)

		threshold_factor = float(
			position.research_threshold_factor
		)

		threshold_distance = (
			entry_atr
			* threshold_factor
		)

		if position.side == "long":
			threshold_price = (
				cost_basis
				- threshold_distance
			)

			threshold_crossed = (
				market_price
				<= threshold_price
			)

			adverse_price_distance = (
				cost_basis
				- market_price
			)

		else:
			threshold_price = (
				cost_basis
				+ threshold_distance
			)

			threshold_crossed = (
				market_price
				>= threshold_price
			)

			adverse_price_distance = (
				market_price
				- cost_basis
			)

		if not threshold_crossed:
			return

		elapsed_minutes = (
			pd.Timestamp(
				bar_dt
			)
			- pd.Timestamp(
				position.entry_time
			)
		).total_seconds() / 60.0

		adverse_atr_units = (
			adverse_price_distance
			/ entry_atr
		)

		anchor_features = (
			self._get_anchor_features_at_time(
				state=state,
				ticker=position.ticker,
				observation_time=bar_dt,
			)
		)

		recent_price_features = (
			self._get_threshold_recent_price_features(
				state=state,
				ticker=position.ticker,
				observation_time=bar_dt,
			)
		)

		position.research_threshold_time = (
			bar_dt
		)

		position.research_threshold_price = float(
			market_price
		)

		position.research_threshold_features = {
			"threshold_elapsed_minutes":
				float(
					elapsed_minutes
				),

			"threshold_entry_atr":
				entry_atr,

			"threshold_cost_basis":
				cost_basis,

			"threshold_trigger_price":
				float(
					threshold_price
				),

			"threshold_market_price":
				float(
					market_price
				),

			"threshold_adverse_atr_units":
				float(
					adverse_atr_units
				),

			**{
				f"threshold_{key}":
					value
				for key, value in (
					anchor_features.items()
				)
			},

			**{
				f"threshold_{key}":
					value
				for key, value in (
					recent_price_features.items()
				)
			},
		}


	def _get_entry_features(
		self,
		state: SimState,
		event: dict[str, Any],
	) -> dict[str, Optional[float]]:
		"""
		Return the latest completed anchor feature row
		available at entry.
		"""
		return self._get_anchor_features_at_time(
			state=state,
			ticker=event[
				"ticker"
			],
			observation_time=event[
				"received_dt"
			],
		)


	def _get_entry_features_DEPRECATED(
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
			#"pnl_percent": float(
				#pnl_percent
			#),

			#**position.entry_features,
			"pnl_percent": float(
				pnl_percent
			),

			"research_threshold_factor": float(
				position.research_threshold_factor
			),

			"research_threshold_time": (
				pd.Timestamp(
					position.research_threshold_time
				).isoformat()
				if (
					position.research_threshold_time
					is not None
				)
				else None
			),

			"research_threshold_price": (
				float(
					position.research_threshold_price
				)
				if (
					position.research_threshold_price
					is not None
				)
				else None
			),

			**position.entry_features,

			**position.research_threshold_features,

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


	def _find_best_regime_threshold(
		self,
		feature_values: pd.Series,
		wide_labels: pd.Series,
	) -> dict[str, Any]:
		"""
		Find the best single-feature threshold for separating
		tight-factor and wide-factor trades.

		Both threshold orientations are tested:

			x >= threshold -> wide
			x <= threshold -> wide

		Balanced accuracy is used so the result is not dominated
		by whichever regime contains more trades.
		"""
		x = pd.to_numeric(
			feature_values,
			errors="coerce",
		)

		y = wide_labels.astype(
			bool
		)

		valid_mask = (
			x.notna()
			& y.notna()
		)

		x = x[
			valid_mask
		].astype(
			float
		)

		y = y[
			valid_mask
		].astype(
			bool
		)

		if (
			len(
				x
			) < 2
			or x.nunique() < 2
			or y.nunique() < 2
		):
			return {
				"threshold": None,
				"orientation": None,
				"balanced_accuracy": None,
				"accuracy": None,
				"wide_recall": None,
				"tight_recall": None,
				"wide_precision": None,
			}

		unique_values = np.sort(
			x.unique()
		)

		thresholds = (
			unique_values[:-1]
			+ unique_values[1:]
		) / 2.0

		best_result = None

		for threshold in thresholds:
			for orientation in (
				"greater_equal_is_wide",
				"less_equal_is_wide",
			):
				if (
					orientation
					== "greater_equal_is_wide"
				):
					predicted_wide = (
						x >= threshold
					)
				else:
					predicted_wide = (
						x <= threshold
					)

				true_positive = int(
					(
						predicted_wide
						& y
					).sum()
				)

				true_negative = int(
					(
						~predicted_wide
						& ~y
					).sum()
				)

				false_positive = int(
					(
						predicted_wide
						& ~y
					).sum()
				)

				false_negative = int(
					(
						~predicted_wide
						& y
					).sum()
				)

				wide_count = (
					true_positive
					+ false_negative
				)

				tight_count = (
					true_negative
					+ false_positive
				)

				wide_recall = (
					true_positive
					/ wide_count
					if wide_count
					else 0.0
				)

				tight_recall = (
					true_negative
					/ tight_count
					if tight_count
					else 0.0
				)

				balanced_accuracy = (
					wide_recall
					+ tight_recall
				) / 2.0

				accuracy = (
					true_positive
					+ true_negative
				) / len(
					x
				)

				wide_precision = (
					true_positive
					/ (
						true_positive
						+ false_positive
					)
					if (
						true_positive
						+ false_positive
					)
					else 0.0
				)

				result = {
					"threshold":
						float(
							threshold
						),

					"orientation":
						orientation,

					"balanced_accuracy":
						float(
							balanced_accuracy
						),

					"accuracy":
						float(
							accuracy
						),

					"wide_recall":
						float(
							wide_recall
						),

					"tight_recall":
						float(
							tight_recall
						),

					"wide_precision":
						float(
							wide_precision
						),

					"true_positive":
						true_positive,

					"true_negative":
						true_negative,

					"false_positive":
						false_positive,

					"false_negative":
						false_negative,
				}

				if (
					best_result is None
					or result[
						"balanced_accuracy"
					]
					> best_result[
						"balanced_accuracy"
					]
				):
					best_result = result

		return best_result


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


	def _walk_forward_regime_validation(
		self,
		best_df: pd.DataFrame,
		factor_pnl_df: pd.DataFrame,
		feature_name: str,
		tested_factors: list[float],
		minimum_tested_factor: float,
		maximum_tested_factor: float,
		initial_train_size: int = 8,
		test_size: int = 2,
	) -> tuple[
		pd.DataFrame,
		pd.DataFrame,
		dict[str, Any],
	]:
		"""
		Perform expanding-window walk-forward validation.

		For each fold:

			1. Learn the feature threshold ONLY from prior
			   tight/wide trades.

			2. Freeze that threshold.

			3. Predict tight/wide for the next chronological trades.

			4. Use:
				   minimum_tested_factor for tight
				   maximum_tested_factor for wide

			5. Compare dynamic-rule PnL against every fixed factor.
		"""
		if initial_train_size < 2:
			raise ValueError(
				"initial_train_size must be >= 2"
			)

		if test_size < 1:
			raise ValueError(
				"test_size must be >= 1"
			)

		required_columns = {
			"trade_id",
			"ticker",
			"side",
			"entry_time",
			"factor_regime",
			feature_name,
		}

		missing_columns = (
			required_columns
			- set(
				best_df.columns
			)
		)

		if missing_columns:
			raise ValueError(
				"Walk-forward validation is missing columns: "
				f"{sorted(missing_columns)}"
			)

		walk_df = best_df[
			best_df[
				"factor_regime"
			].isin(
				[
					"tight",
					"wide",
				]
			)
		][
			[
				"trade_id",
				"ticker",
				"side",
				"entry_time",
				"factor_regime",
				feature_name,
			]
		].copy()

		walk_df[
			"entry_time_parsed"
		] = pd.to_datetime(
			walk_df[
				"entry_time"
			],
			utc=True,
			errors="coerce",
		)

		walk_df[
			feature_name
		] = pd.to_numeric(
			walk_df[
				feature_name
			],
			errors="coerce",
		)

		walk_df = (
			walk_df
			.dropna(
				subset=[
					"entry_time_parsed",
					feature_name,
				]
			)
			.sort_values(
				[
					"entry_time_parsed",
					"trade_id",
				]
			)
			.reset_index(
				drop=True
			)
		)

		total_trade_count = len(
			walk_df
		)

		if total_trade_count <= initial_train_size:
			raise ValueError(
				"Not enough tight/wide trades for "
				"walk-forward validation"
			)

		factor_pnl_columns = [
			column
			for column in factor_pnl_df.columns
			if (
				column == "trade_id"
				or column.startswith(
					"pnl_percent_factor_"
				)
			)
		]

		walk_df = walk_df.merge(
			factor_pnl_df[
				factor_pnl_columns
			],
			on="trade_id",
			how="left",
			validate="one_to_one",
		)

		prediction_rows = []
		fold_rows = []

		fold_number = 0

		test_start = initial_train_size

		while test_start < total_trade_count:
			fold_number += 1

			test_end = min(
				test_start
				+ test_size,
				total_trade_count,
			)

			train_df = walk_df.iloc[
				:test_start
			].copy()

			test_df = walk_df.iloc[
				test_start:test_end
			].copy()

			train_wide_labels = (
				train_df[
					"factor_regime"
				]
				== "wide"
			)

			if train_wide_labels.nunique() < 2:
				test_start = test_end

				continue

			threshold_result = (
				self._find_best_regime_threshold(
					feature_values=(
						train_df[
							feature_name
						]
					),
					wide_labels=(
						train_wide_labels
					),
				)
			)

			threshold = threshold_result.get(
				"threshold"
			)

			orientation = threshold_result.get(
				"orientation"
			)

			if (
				threshold is None
				or orientation is None
			):
				test_start = test_end

				continue

			if (
				orientation
				== "greater_equal_is_wide"
			):
				predicted_wide = (
					test_df[
						feature_name
					]
					>= threshold
				)

			else:
				predicted_wide = (
					test_df[
						feature_name
					]
					<= threshold
				)

			actual_wide = (
				test_df[
					"factor_regime"
				]
				== "wide"
			)

			true_positive = int(
				(
					predicted_wide
					& actual_wide
				).sum()
			)

			true_negative = int(
				(
					~predicted_wide
					& ~actual_wide
				).sum()
			)

			false_positive = int(
				(
					predicted_wide
					& ~actual_wide
				).sum()
			)

			false_negative = int(
				(
					~predicted_wide
					& actual_wide
				).sum()
			)

			actual_wide_count = (
				true_positive
				+ false_negative
			)

			actual_tight_count = (
				true_negative
				+ false_positive
			)

			wide_recall = (
				true_positive
				/ actual_wide_count
				if actual_wide_count
				else None
			)

			tight_recall = (
				true_negative
				/ actual_tight_count
				if actual_tight_count
				else None
			)

			balanced_accuracy = (
				(
					wide_recall
					+ tight_recall
				)
				/ 2.0
				if (
					wide_recall is not None
					and tight_recall is not None
				)
				else None
			)

			accuracy = (
				(
					true_positive
					+ true_negative
				)
				/ len(
					test_df
				)
			)

			dynamic_pnl_total = 0.0

			for row_index, (
				(_, row),
				is_predicted_wide,
			) in enumerate(
				zip(
					test_df.iterrows(),
					predicted_wide.tolist(),
				)
			):
				predicted_regime = (
					"wide"
					if is_predicted_wide
					else "tight"
				)

				selected_factor = (
					maximum_tested_factor
					if is_predicted_wide
					else minimum_tested_factor
				)

				pnl_column = (
					"pnl_percent_factor_"
					f"{float(selected_factor):g}"
				)

				dynamic_pnl = (
					float(
						row[
							pnl_column
						]
					)
					if (
						pnl_column
						in row.index
						and pd.notna(
							row[
								pnl_column
							]
						)
					)
					else None
				)

				if dynamic_pnl is not None:
					dynamic_pnl_total += (
						dynamic_pnl
					)

				prediction_rows.append({
					"fold":
						fold_number,

					"trade_id":
						row[
							"trade_id"
						],

					"ticker":
						row[
							"ticker"
						],

					"side":
						row.get(
							"side"
						),

					"entry_time":
						row[
							"entry_time"
						],

					feature_name:
						float(
							row[
								feature_name
							]
						),

					"training_threshold":
						float(
							threshold
						),

					"threshold_orientation":
						orientation,

					"actual_regime":
						row[
							"factor_regime"
						],

					"predicted_regime":
						predicted_regime,

					"prediction_correct":
						(
							predicted_regime
							== row[
								"factor_regime"
							]
						),

					"selected_factor":
						float(
							selected_factor
						),

					"dynamic_pnl_percent":
						dynamic_pnl,
				})

			fold_row = {
				"fold":
					fold_number,

				"train_trade_count":
					len(
						train_df
					),

				"test_trade_count":
					len(
						test_df
					),

				"training_threshold":
					float(
						threshold
					),

				"threshold_orientation":
					orientation,

				"training_balanced_accuracy":
					threshold_result.get(
						"balanced_accuracy"
					),

				"test_accuracy":
					float(
						accuracy
					),

				"test_balanced_accuracy":
					balanced_accuracy,

				"true_positive":
					true_positive,

				"true_negative":
					true_negative,

				"false_positive":
					false_positive,

				"false_negative":
					false_negative,

				"dynamic_pnl_percent":
					float(
						dynamic_pnl_total
					),
			}

			for factor in tested_factors:
				pnl_column = (
					"pnl_percent_factor_"
					f"{float(factor):g}"
				)

				if (
					pnl_column
					in test_df.columns
				):
					fold_row[
						(
							"fixed_factor_"
							f"{float(factor):g}"
							"_pnl_percent"
						)
					] = float(
						test_df[
							pnl_column
						]
						.dropna()
						.sum()
					)

			fold_rows.append(
				fold_row
			)

			test_start = test_end

		predictions_df = pd.DataFrame(
			prediction_rows
		)

		folds_df = pd.DataFrame(
			fold_rows
		)

		if predictions_df.empty:
			raise ValueError(
				"No walk-forward predictions were produced"
			)

		total_predictions = len(
			predictions_df
		)

		correct_predictions = int(
			predictions_df[
				"prediction_correct"
			].sum()
		)

		dynamic_total_pnl = float(
			predictions_df[
				"dynamic_pnl_percent"
			]
			.dropna()
			.sum()
		)

		fixed_factor_totals = {}

		for factor in tested_factors:
			pnl_column = (
				"pnl_percent_factor_"
				f"{float(factor):g}"
			)

			if pnl_column not in walk_df.columns:
				continue

			test_trade_ids = set(
				predictions_df[
					"trade_id"
				]
			)

			test_factor_rows = walk_df[
				walk_df[
					"trade_id"
				].isin(
					test_trade_ids
				)
			]

			fixed_factor_totals[
				f"{float(factor):g}"
			] = float(
				test_factor_rows[
					pnl_column
				]
				.dropna()
				.sum()
			)

		best_fixed_factor = None
		best_fixed_pnl = None

		if fixed_factor_totals:
			best_fixed_factor = max(
				fixed_factor_totals,
				key=(
					fixed_factor_totals.get
				),
			)

			best_fixed_pnl = (
				fixed_factor_totals[
					best_fixed_factor
				]
			)

		summary = {
			"feature":
				feature_name,

			"initial_train_size":
				initial_train_size,

			"test_size":
				test_size,

			"total_binary_regime_trades":
				total_trade_count,

			"walk_forward_test_trade_count":
				total_predictions,

			"correct_predictions":
				correct_predictions,

			"walk_forward_accuracy":
				float(
					correct_predictions
					/ total_predictions
				),

			"dynamic_rule_total_pnl_percent":
				dynamic_total_pnl,

			"dynamic_tight_factor":
				float(
					minimum_tested_factor
				),

			"dynamic_wide_factor":
				float(
					maximum_tested_factor
				),

			"best_fixed_factor":
				best_fixed_factor,

			"best_fixed_factor_total_pnl_percent":
				best_fixed_pnl,

			"fixed_factor_total_pnl_percent":
				fixed_factor_totals,
		}

		if best_fixed_pnl is not None:
			summary[
				"dynamic_minus_best_fixed_pnl_percent"
			] = float(
				dynamic_total_pnl
				- best_fixed_pnl
			)

		return (
			predictions_df,
			folds_df,
			summary,
		)


	def _walk_forward_all_trade_pnl_validation(
		self,
		comparison_df: pd.DataFrame,
		best_df: pd.DataFrame,
		factor_pnl_df: pd.DataFrame,
		feature_name: str,
		tested_factors: list[float],
		minimum_tested_factor: float,
		maximum_tested_factor: float,
		initial_binary_train_size: int = 8,
		test_size: int = 20,
	) -> tuple[
		pd.DataFrame,
		pd.DataFrame,
		dict[str, Any],
	]:
		"""
		Learn the tight-vs-wide threshold only from prior
		high-confidence binary-regime trades, but evaluate
		the resulting dynamic factor on ALL subsequent
		comparable trades.

		This avoids scoring only trades whose hindsight
		best factor was at one of the search boundaries.
		"""
		if initial_binary_train_size < 2:
			raise ValueError(
				"initial_binary_train_size must be >= 2"
			)

		if test_size < 1:
			raise ValueError(
				"test_size must be >= 1"
			)

		required_comparison_columns = {
			"trade_id",
			"ticker",
			"side",
			"entry_time",
			feature_name,
		}

		missing_comparison_columns = (
			required_comparison_columns
			- set(
				comparison_df.columns
			)
		)

		if missing_comparison_columns:
			raise ValueError(
				"All-trade validation is missing comparison columns: "
				f"{sorted(missing_comparison_columns)}"
			)

		required_binary_columns = {
			"trade_id",
			"entry_time",
			"factor_regime",
			feature_name,
		}

		missing_binary_columns = (
			required_binary_columns
			- set(
				best_df.columns
			)
		)

		if missing_binary_columns:
			raise ValueError(
				"All-trade validation is missing binary columns: "
				f"{sorted(missing_binary_columns)}"
			)

		# One entry-feature row per comparable trade.
		# Entry features should be identical across factor runs,
		# so the smallest-factor row is used deterministically.
		all_trade_df = (
			comparison_df
			.sort_values(
				[
					"trade_id",
					"loss_liquidation_atr_factor",
				]
			)
			.drop_duplicates(
				subset=[
					"trade_id"
				],
				keep="first",
			)
			[
				[
					"trade_id",
					"ticker",
					"side",
					"entry_time",
					feature_name,
				]
			]
			.copy()
		)

		all_trade_df[
			"entry_time_parsed"
		] = pd.to_datetime(
			all_trade_df[
				"entry_time"
			],
			utc=True,
			errors="coerce",
		)

		all_trade_df[
			feature_name
		] = pd.to_numeric(
			all_trade_df[
				feature_name
			],
			errors="coerce",
		)

		all_trade_df = (
			all_trade_df
			.dropna(
				subset=[
					"entry_time_parsed",
					feature_name,
				]
			)
			.sort_values(
				[
					"entry_time_parsed",
					"trade_id",
				]
			)
			.reset_index(
				drop=True
			)
		)

		factor_pnl_columns = [
			column
			for column in factor_pnl_df.columns
			if (
				column == "trade_id"
				or column.startswith(
					"pnl_percent_factor_"
				)
			)
		]

		all_trade_df = all_trade_df.merge(
			factor_pnl_df[
				factor_pnl_columns
			],
			on="trade_id",
			how="left",
			validate="one_to_one",
		)

		# Training labels still come ONLY from the high-confidence
		# tight/wide population.
		binary_df = best_df[
			best_df[
				"factor_regime"
			].isin(
				[
					"tight",
					"wide",
				]
			)
		][
			[
				"trade_id",
				"entry_time",
				"factor_regime",
				feature_name,
			]
		].copy()

		binary_df[
			"entry_time_parsed"
		] = pd.to_datetime(
			binary_df[
				"entry_time"
			],
			utc=True,
			errors="coerce",
		)

		binary_df[
			feature_name
		] = pd.to_numeric(
			binary_df[
				feature_name
			],
			errors="coerce",
		)

		binary_df = (
			binary_df
			.dropna(
				subset=[
					"entry_time_parsed",
					feature_name,
				]
			)
			.sort_values(
				[
					"entry_time_parsed",
					"trade_id",
				]
			)
			.reset_index(
				drop=True
			)
		)

		if len(
			binary_df
		) <= initial_binary_train_size:
			raise ValueError(
				"Not enough binary-regime trades for "
				"all-trade walk-forward validation"
			)

		initial_training_df = binary_df.iloc[
			:initial_binary_train_size
		]

		if (
			initial_training_df[
				"factor_regime"
			].nunique()
			< 2
		):
			raise ValueError(
				"Initial binary training set does not contain "
				"both tight and wide regimes"
			)

		# Start scoring immediately AFTER the last trade used
		# for the initial binary training set.
		initial_cutoff = (
			initial_training_df[
				"entry_time_parsed"
			]
			.max()
		)

		scoring_df = (
			all_trade_df[
				all_trade_df[
					"entry_time_parsed"
				]
				> initial_cutoff
			]
			.copy()
			.reset_index(
				drop=True
			)
		)

		if scoring_df.empty:
			raise ValueError(
				"No future comparable trades remain for "
				"all-trade validation"
			)

		prediction_rows = []
		fold_rows = []

		fold_number = 0
		test_start = 0

		while test_start < len(
			scoring_df
		):
			fold_number += 1

			test_end = min(
				test_start
				+ test_size,
				len(
					scoring_df
				),
			)

			test_df = scoring_df.iloc[
				test_start:test_end
			].copy()

			fold_start_time = (
				test_df[
					"entry_time_parsed"
				]
				.min()
			)

			# CRITICAL:
			# only labeled binary trades that occurred before
			# this test block can influence the threshold.
			train_df = binary_df[
				binary_df[
					"entry_time_parsed"
				]
				< fold_start_time
			].copy()

			train_wide_labels = (
				train_df[
					"factor_regime"
				]
				== "wide"
			)

			if (
				len(
					train_df
				) < initial_binary_train_size
				or train_wide_labels.nunique() < 2
			):
				test_start = test_end

				continue

			threshold_result = (
				self._find_best_regime_threshold(
					feature_values=(
						train_df[
							feature_name
						]
					),
					wide_labels=(
						train_wide_labels
					),
				)
			)

			threshold = threshold_result.get(
				"threshold"
			)

			orientation = threshold_result.get(
				"orientation"
			)

			if (
				threshold is None
				or orientation is None
			):
				test_start = test_end

				continue

			if (
				orientation
				== "greater_equal_is_wide"
			):
				predicted_wide = (
					test_df[
						feature_name
					]
					>= threshold
				)
			else:
				predicted_wide = (
					test_df[
						feature_name
					]
					<= threshold
				)

			dynamic_pnl_total = 0.0

			for (
				(_, row),
				is_predicted_wide,
			) in zip(
				test_df.iterrows(),
				predicted_wide.tolist(),
			):
				predicted_regime = (
					"wide"
					if is_predicted_wide
					else "tight"
				)

				selected_factor = (
					maximum_tested_factor
					if is_predicted_wide
					else minimum_tested_factor
				)

				pnl_column = (
					"pnl_percent_factor_"
					f"{float(selected_factor):g}"
				)

				dynamic_pnl = (
					float(
						row[
							pnl_column
						]
					)
					if (
						pnl_column
						in row.index
						and pd.notna(
							row[
								pnl_column
							]
						)
					)
					else None
				)

				if dynamic_pnl is not None:
					dynamic_pnl_total += (
						dynamic_pnl
					)

				prediction_rows.append({
					"fold":
						fold_number,

					"trade_id":
						row[
							"trade_id"
						],

					"ticker":
						row[
							"ticker"
						],

					"side":
						row[
							"side"
						],

					"entry_time":
						row[
							"entry_time"
						],

					feature_name:
						float(
							row[
								feature_name
							]
						),

					"training_binary_trade_count":
						len(
							train_df
						),

					"training_threshold":
						float(
							threshold
						),

					"threshold_orientation":
						orientation,

					"predicted_regime":
						predicted_regime,

					"selected_factor":
						float(
							selected_factor
						),

					"dynamic_pnl_percent":
						dynamic_pnl,
				})

			fold_row = {
				"fold":
					fold_number,

				"training_binary_trade_count":
					len(
						train_df
					),

				"test_trade_count":
					len(
						test_df
					),

				"test_start_time":
					test_df[
						"entry_time_parsed"
					].min().isoformat(),

				"test_end_time":
					test_df[
						"entry_time_parsed"
					].max().isoformat(),

				"training_threshold":
					float(
						threshold
					),

				"threshold_orientation":
					orientation,

				"training_balanced_accuracy":
					threshold_result.get(
						"balanced_accuracy"
					),

				"dynamic_pnl_percent":
					float(
						dynamic_pnl_total
					),
			}

			for factor in tested_factors:
				pnl_column = (
					"pnl_percent_factor_"
					f"{float(factor):g}"
				)

				if pnl_column not in test_df.columns:
					continue

				fold_row[
					(
						"fixed_factor_"
						f"{float(factor):g}"
						"_pnl_percent"
					)
				] = float(
					test_df[
						pnl_column
					]
					.dropna()
					.sum()
				)

			fold_rows.append(
				fold_row
			)

			test_start = test_end

		predictions_df = pd.DataFrame(
			prediction_rows
		)

		folds_df = pd.DataFrame(
			fold_rows
		)

		if predictions_df.empty:
			raise ValueError(
				"No all-trade walk-forward predictions were produced"
			)

		test_trade_ids = set(
			predictions_df[
				"trade_id"
			]
		)

		test_trade_df = all_trade_df[
			all_trade_df[
				"trade_id"
			].isin(
				test_trade_ids
			)
		].copy()

		dynamic_total_pnl = float(
			predictions_df[
				"dynamic_pnl_percent"
			]
			.dropna()
			.sum()
		)

		dynamic_average_pnl = float(
			predictions_df[
				"dynamic_pnl_percent"
			]
			.dropna()
			.mean()
		)

		dynamic_win_rate = float(
			(
				predictions_df[
					"dynamic_pnl_percent"
				]
				> 0
			).mean()
		)

		dynamic_worst_trade = float(
			predictions_df[
				"dynamic_pnl_percent"
			]
			.dropna()
			.min()
		)

		fixed_factor_totals = {}

		for factor in tested_factors:
			pnl_column = (
				"pnl_percent_factor_"
				f"{float(factor):g}"
			)

			if pnl_column not in test_trade_df.columns:
				continue

			fixed_factor_totals[
				f"{float(factor):g}"
			] = float(
				test_trade_df[
					pnl_column
				]
				.dropna()
				.sum()
			)

		best_fixed_factor = None
		best_fixed_pnl = None

		if fixed_factor_totals:
			best_fixed_factor = max(
				fixed_factor_totals,
				key=(
					fixed_factor_totals.get
				),
			)

			best_fixed_pnl = (
				fixed_factor_totals[
					best_fixed_factor
				]
			)

		summary = {
			"feature":
				feature_name,

			"initial_binary_train_size":
				initial_binary_train_size,

			"test_size":
				test_size,

			"total_comparable_trade_count":
				int(
					all_trade_df[
						"trade_id"
					].nunique()
				),

			"walk_forward_test_trade_count":
				int(
					predictions_df[
						"trade_id"
					].nunique()
				),

			"dynamic_tight_factor":
				float(
					minimum_tested_factor
				),

			"dynamic_wide_factor":
				float(
					maximum_tested_factor
				),

			"dynamic_rule_total_pnl_percent":
				dynamic_total_pnl,

			"dynamic_rule_average_pnl_percent":
				dynamic_average_pnl,

			"dynamic_rule_win_rate":
				dynamic_win_rate,

			"dynamic_rule_worst_trade_pnl_percent":
				dynamic_worst_trade,

			"best_fixed_factor":
				best_fixed_factor,

			"best_fixed_factor_total_pnl_percent":
				best_fixed_pnl,

			"fixed_factor_total_pnl_percent":
				fixed_factor_totals,
		}

		if best_fixed_pnl is not None:
			summary[
				"dynamic_minus_best_fixed_pnl_percent"
			] = float(
				dynamic_total_pnl
				- best_fixed_pnl
			)

		return (
			predictions_df,
			folds_df,
			summary,
		)


	def _validate_regime_threshold_chronologically(
		self,
		best_df: pd.DataFrame,
		feature_name: str,
		train_fraction: float = 0.70,
	) -> dict[str, Any]:
		"""
		Chronologically train and test a single-feature
		tight-vs-wide regime classifier.

		The threshold is learned ONLY from the training trades
		and then frozen before evaluating the test trades.
		"""
		if not 0 < train_fraction < 1:
			raise ValueError(
				"train_fraction must be between 0 and 1"
			)

		required_columns = {
			"trade_id",
			"entry_time",
			"factor_regime",
			feature_name,
		}

		missing_columns = (
			required_columns
			- set(
				best_df.columns
			)
		)

		if missing_columns:
			return {
				"feature":
					feature_name,

				"error":
					(
						"Missing columns: "
						f"{sorted(missing_columns)}"
					),
			}

		validation_df = best_df[
			best_df[
				"factor_regime"
			].isin(
				[
					"tight",
					"wide",
				]
			)
		][
			[
				"trade_id",
				"ticker",
				"entry_time",
				"factor_regime",
				feature_name,
			]
		].copy()

		validation_df[
			"entry_time_parsed"
		] = pd.to_datetime(
			validation_df[
				"entry_time"
			],
			utc=True,
			errors="coerce",
		)

		validation_df[
			feature_name
		] = pd.to_numeric(
			validation_df[
				feature_name
			],
			errors="coerce",
		)

		validation_df = (
			validation_df
			.dropna(
				subset=[
					"entry_time_parsed",
					feature_name,
				]
			)
			.sort_values(
				[
					"entry_time_parsed",
					"trade_id",
				]
			)
			.reset_index(
				drop=True
			)
		)

		total_trade_count = len(
			validation_df
		)

		if total_trade_count < 4:
			return {
				"feature":
					feature_name,

				"error":
					"Not enough trades for chronological validation",

				"total_trade_count":
					total_trade_count,
			}

		split_index = int(
			total_trade_count
			* train_fraction
		)

		split_index = max(
			1,
			min(
				split_index,
				total_trade_count - 1,
			),
		)

		train_df = validation_df.iloc[
			:split_index
		].copy()

		test_df = validation_df.iloc[
			split_index:
		].copy()

		train_wide_labels = (
			train_df[
				"factor_regime"
			]
			== "wide"
		)

		if train_wide_labels.nunique() < 2:
			return {
				"feature":
					feature_name,

				"error":
					(
						"Training set does not contain "
						"both tight and wide regimes"
					),

				"train_trade_count":
					len(
						train_df
					),

				"test_trade_count":
					len(
						test_df
					),
			}

		threshold_result = (
			self._find_best_regime_threshold(
				feature_values=(
					train_df[
						feature_name
					]
				),
				wide_labels=(
					train_wide_labels
				),
			)
		)

		threshold = threshold_result.get(
			"threshold"
		)

		orientation = threshold_result.get(
			"orientation"
		)

		if (
			threshold is None
			or orientation is None
		):
			return {
				"feature":
					feature_name,

				"error":
					"Could not determine training threshold",
			}

		if orientation == "greater_equal_is_wide":
			test_predicted_wide = (
				test_df[
					feature_name
				]
				>= threshold
			)
		else:
			test_predicted_wide = (
				test_df[
					feature_name
				]
				<= threshold
			)

		test_actual_wide = (
			test_df[
				"factor_regime"
			]
			== "wide"
		)

		true_positive = int(
			(
				test_predicted_wide
				& test_actual_wide
			).sum()
		)

		true_negative = int(
			(
				~test_predicted_wide
				& ~test_actual_wide
			).sum()
		)

		false_positive = int(
			(
				test_predicted_wide
				& ~test_actual_wide
			).sum()
		)

		false_negative = int(
			(
				~test_predicted_wide
				& test_actual_wide
			).sum()
		)

		actual_wide_count = (
			true_positive
			+ false_negative
		)

		actual_tight_count = (
			true_negative
			+ false_positive
		)

		wide_recall = (
			true_positive
			/ actual_wide_count
			if actual_wide_count
			else None
		)

		tight_recall = (
			true_negative
			/ actual_tight_count
			if actual_tight_count
			else None
		)

		balanced_accuracy = (
			(
				wide_recall
				+ tight_recall
			)
			/ 2.0
			if (
				wide_recall is not None
				and tight_recall is not None
			)
			else None
		)

		accuracy = (
			(
				true_positive
				+ true_negative
			)
			/ len(
				test_df
			)
			if len(
				test_df
			)
			else None
		)

		test_details_df = test_df[
			[
				"trade_id",
				"ticker",
				"entry_time",
				feature_name,
				"factor_regime",
			]
		].copy()

		test_details_df[
			"predicted_regime"
		] = np.where(
			test_predicted_wide,
			"wide",
			"tight",
		)

		test_details_df[
			"prediction_correct"
		] = (
			test_details_df[
				"predicted_regime"
			]
			== test_details_df[
				"factor_regime"
			]
		)

		return {
			"feature":
				feature_name,

			"train_fraction":
				float(
					train_fraction
				),

			"total_trade_count":
				total_trade_count,

			"train_trade_count":
				len(
					train_df
				),

			"test_trade_count":
				len(
					test_df
				),

			"train_tight_count":
				int(
					(
						train_df[
							"factor_regime"
						]
						== "tight"
					).sum()
				),

			"train_wide_count":
				int(
					(
						train_df[
							"factor_regime"
						]
						== "wide"
					).sum()
				),

			"test_tight_count":
				int(
					(
						test_df[
							"factor_regime"
						]
						== "tight"
					).sum()
				),

			"test_wide_count":
				int(
					(
						test_df[
							"factor_regime"
						]
						== "wide"
					).sum()
				),

			"training_threshold":
				float(
					threshold
				),

			"threshold_orientation":
				orientation,

			"training_balanced_accuracy":
				threshold_result.get(
					"balanced_accuracy"
				),

			"test_balanced_accuracy":
				balanced_accuracy,

			"test_accuracy":
				accuracy,

			"test_wide_recall":
				wide_recall,

			"test_tight_recall":
				tight_recall,

			"true_positive":
				true_positive,

			"true_negative":
				true_negative,

			"false_positive":
				false_positive,

			"false_negative":
				false_negative,

			"test_details_df":
				test_details_df,
		}


	def _walk_forward_fixed_factor_validation(
		self,
		comparison_df: pd.DataFrame,
		factor_pnl_df: pd.DataFrame,
		tested_factors: list[float],
		initial_train_size: int = 60,
		test_size: int = 20,
		window_mode: str = "expanding",
		training_window_size: Optional[int] = None,
	) -> tuple[
		pd.DataFrame,
		pd.DataFrame,
		dict[str, Any],
	]:
		"""
		Walk-forward selection of one liquidation factor.

		For every fold:

			1. Use only trades that occurred before the test block.

			2. Sum historical PnL for every tested factor.

			3. Select the factor with the highest historical
			   cumulative PnL.

			4. Freeze that factor.

			5. Apply it to the next chronological block.

		No volatility feature or hindsight best-factor label is used.
		"""
		if initial_train_size < 2:
			raise ValueError(
				"initial_train_size must be >= 2"
			)

		if test_size < 1:
			raise ValueError(
				"test_size must be >= 1"
			)

		window_mode = str(
			window_mode or ""
		).strip().lower()

		if window_mode not in {
			"expanding",
			"rolling",
		}:
			raise ValueError(
				"window_mode must be "
				"'expanding' or 'rolling'"
			)

		if training_window_size is None:
			training_window_size = (
				initial_train_size
			)

		try:
			training_window_size = int(
				training_window_size
			)

		except (TypeError, ValueError) as exc:
			raise ValueError(
				"training_window_size must be an integer"
			) from exc

		if training_window_size < 2:
			raise ValueError(
				"training_window_size must be >= 2"
			)

		if (
			window_mode == "rolling"
			and training_window_size
			< initial_train_size
		):
			raise ValueError(
				"training_window_size must be >= "
				"initial_train_size when "
				"window_mode='rolling'"
			)			

		required_columns = {
			"trade_id",
			"ticker",
			"side",
			"entry_time",
		}

		missing_columns = (
			required_columns
			- set(
				comparison_df.columns
			)
		)

		if missing_columns:
			raise ValueError(
				"Fixed-factor walk-forward validation is "
				"missing columns: "
				f"{sorted(missing_columns)}"
			)

		#
		# One row per trade.
		#
		trade_df = (
			comparison_df
			.sort_values(
				[
					"trade_id",
					"loss_liquidation_atr_factor",
				]
			)
			.drop_duplicates(
				subset=[
					"trade_id",
				],
				keep="first",
			)
			[
				[
					"trade_id",
					"ticker",
					"side",
					"entry_time",
				]
			]
			.copy()
		)

		trade_df[
			"entry_time_parsed"
		] = pd.to_datetime(
			trade_df[
				"entry_time"
			],
			utc=True,
			errors="coerce",
		)

		trade_df = (
			trade_df
			.dropna(
				subset=[
					"entry_time_parsed",
				]
			)
			.sort_values(
				[
					"entry_time_parsed",
					"trade_id",
				]
			)
			.reset_index(
				drop=True
			)
		)

		factor_pnl_columns = [
			column
			for column in factor_pnl_df.columns
			if (
				column == "trade_id"
				or column.startswith(
					"pnl_percent_factor_"
				)
			)
		]

		trade_df = trade_df.merge(
			factor_pnl_df[
				factor_pnl_columns
			],
			on="trade_id",
			how="left",
			validate="one_to_one",
		)

		total_trade_count = len(
			trade_df
		)

		if total_trade_count <= initial_train_size:
			raise ValueError(
				"Not enough comparable trades for "
				"fixed-factor walk-forward validation"
			)

		factor_columns = {}

		for factor in tested_factors:
			pnl_column = (
				"pnl_percent_factor_"
				f"{float(factor):g}"
			)

			if pnl_column in trade_df.columns:
				factor_columns[
					float(
						factor
					)
				] = pnl_column

		if len(
			factor_columns
		) < 2:
			raise ValueError(
				"At least two factor PnL columns are required"
			)

		prediction_rows = []
		fold_rows = []

		test_start = initial_train_size
		fold_number = 0

		while test_start < total_trade_count:
			test_end = min(
				test_start
				+ test_size,
				total_trade_count,
			)

			candidate_test_df = trade_df.iloc[
				test_start:test_end
			].copy()

			if candidate_test_df.empty:
				break

			#
			# Everything strictly before the first test entry.
			# This also prevents trades with the exact same
			# entry timestamp from leaking into training.
			#
			test_start_time = (
				candidate_test_df[
					"entry_time_parsed"
				]
				.min()
			)

			eligible_train_df = trade_df[
				trade_df[
					"entry_time_parsed"
				]
				< test_start_time
			].copy()

			if window_mode == "rolling":
				train_df = (
					eligible_train_df
					.tail(
						training_window_size
					)
					.copy()
				)

			else:
				train_df = (
					eligible_train_df
					.copy()
				)

			test_df = candidate_test_df.copy()

			if len(
				train_df
			) < initial_train_size:
				test_start = test_end
				continue

			#
			# Determine historical cumulative PnL
			# for every factor.
			#
			training_factor_pnl = {}

			for factor, pnl_column in (
				factor_columns.items()
			):
				training_factor_pnl[
					factor
				] = float(
					train_df[
						pnl_column
					]
					.dropna()
					.sum()
				)

			#
			# Highest historical PnL wins.
			#
			# For an exact tie, use the smaller factor.
			#
			selected_factor = min(
				training_factor_pnl,
				key=lambda factor: (
					-training_factor_pnl[
						factor
					],
					factor,
				),
			)

			selected_pnl_column = (
				factor_columns[
					selected_factor
				]
			)

			fold_dynamic_pnl = float(
				test_df[
					selected_pnl_column
				]
				.dropna()
				.sum()
			)

			fold_number += 1

			fold_row = {
				"fold":
					fold_number,

				"train_trade_count":
					int(
						len(
							train_df
						)
					),

				"window_mode":
					window_mode,

				"training_window_size":
					(
						training_window_size
						if window_mode == "rolling"
						else None
					),

				"eligible_historical_trade_count":
					int(
						len(
							eligible_train_df
						)
					),

				"test_trade_count":
					int(
						len(
							test_df
						)
					),

				"test_start_time":
					test_df[
						"entry_time_parsed"
					]
					.min()
					.isoformat(),

				"test_end_time":
					test_df[
						"entry_time_parsed"
					]
					.max()
					.isoformat(),

				"selected_factor":
					float(
						selected_factor
					),

				"selected_factor_training_pnl_percent":
					float(
						training_factor_pnl[
							selected_factor
						]
					),

				"selected_factor_test_pnl_percent":
					fold_dynamic_pnl,

			}


			#
			# Record historical training performance
			# and future test performance of every factor.
			#
			for factor, pnl_column in (
				factor_columns.items()
			):
				factor_label = (
					f"{float(factor):g}"
				)

				fold_row[
					(
						"training_factor_"
						f"{factor_label}"
						"_pnl_percent"
					)
				] = float(
					training_factor_pnl[
						factor
					]
				)

				fold_row[
					(
						"test_factor_"
						f"{factor_label}"
						"_pnl_percent"
					)
				] = float(
					test_df[
						pnl_column
					]
					.dropna()
					.sum()
				)

			fold_rows.append(
				fold_row
			)

			for _, row in test_df.iterrows():
				selected_trade_pnl = (
					float(
						row[
							selected_pnl_column
						]
					)
					if pd.notna(
						row[
							selected_pnl_column
						]
					)
					else None
				)

				prediction_rows.append({
					"fold":
						fold_number,

					"trade_id":
						row[
							"trade_id"
						],

					"ticker":
						row[
							"ticker"
						],

					"side":
						row[
							"side"
						],

					"entry_time":
						row[
							"entry_time"
						],

					"selected_factor":
						float(
							selected_factor
						),

					"selected_factor_training_pnl_percent":
						float(
							training_factor_pnl[
								selected_factor
							]
						),

					"walk_forward_pnl_percent":
						selected_trade_pnl,
				})

			test_start = test_end

		folds_df = pd.DataFrame(
			fold_rows
		)

		predictions_df = pd.DataFrame(
			prediction_rows
		)

		if predictions_df.empty:
			raise ValueError(
				"No fixed-factor walk-forward predictions "
				"were produced"
			)

		#
		# Every benchmark below uses exactly the same
		# out-of-sample trades.
		#
		test_trade_ids = set(
			predictions_df[
				"trade_id"
			]
		)

		test_trade_df = trade_df[
			trade_df[
				"trade_id"
			].isin(
				test_trade_ids
			)
		].copy()

		walk_forward_total_pnl = float(
			predictions_df[
				"walk_forward_pnl_percent"
			]
			.dropna()
			.sum()
		)

		walk_forward_average_pnl = float(
			predictions_df[
				"walk_forward_pnl_percent"
			]
			.dropna()
			.mean()
		)

		walk_forward_win_rate = float(
			(
				predictions_df[
					"walk_forward_pnl_percent"
				]
				> 0
			)
			.mean()
		)

		walk_forward_worst_trade = float(
			predictions_df[
				"walk_forward_pnl_percent"
			]
			.dropna()
			.min()
		)

		fixed_factor_totals = {}

		for factor, pnl_column in (
			factor_columns.items()
		):
			fixed_factor_totals[
				f"{float(factor):g}"
			] = float(
				test_trade_df[
					pnl_column
				]
				.dropna()
				.sum()
			)

		#
		# This benchmark is hindsight-selected across
		# the complete OOS population, so it is a very
		# demanding benchmark rather than a tradable rule.
		#
		hindsight_best_fixed_factor = max(
			fixed_factor_totals,
			key=(
				fixed_factor_totals.get
			),
		)

		hindsight_best_fixed_pnl = (
			fixed_factor_totals[
				hindsight_best_fixed_factor
			]
		)

		selected_factor_counts = (
			predictions_df[
				"selected_factor"
			]
			.value_counts()
			.sort_index()
		)

		summary = {
			"initial_train_size":
				initial_train_size,

			"test_size":
				test_size,

			"total_comparable_trade_count":
				total_trade_count,

			"walk_forward_test_trade_count":
				int(
					len(
						predictions_df
					)
				),

			"fold_count":
				int(
					len(
						folds_df
					)
				),

			"walk_forward_total_pnl_percent":
				walk_forward_total_pnl,

			"walk_forward_average_pnl_percent":
				walk_forward_average_pnl,

			"walk_forward_win_rate":
				walk_forward_win_rate,

			"walk_forward_worst_trade_pnl_percent":
				walk_forward_worst_trade,

			"selected_factor_counts": {
				f"{float(factor):g}":
					int(
						count
					)
				for factor, count in (
					selected_factor_counts.items()
				)
			},

			"hindsight_best_fixed_factor":
				hindsight_best_fixed_factor,

			"hindsight_best_fixed_factor_total_pnl_percent":
				float(
					hindsight_best_fixed_pnl
				),

			"fixed_factor_total_pnl_percent":
				fixed_factor_totals,

			"walk_forward_minus_hindsight_best_fixed_pnl_percent":
				float(
					walk_forward_total_pnl
					- hindsight_best_fixed_pnl
				),

			"window_mode":
				window_mode,

			"training_window_size":
				(
					training_window_size
					if window_mode == "rolling"
					else None
				),				
		}

		return (
			predictions_df,
			folds_df,
			summary,
		)

