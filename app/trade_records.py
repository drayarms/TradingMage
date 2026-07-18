import os
from concurrent.futures import ThreadPoolExecutor
import logging
from datetime import datetime, timezone, date, timedelta
import statistics
import csv
import pandas as pd
import numpy as np

logger = logging.getLogger("tv-webhook")

DIAGNOSTIC_TICKERS = {
    "AAPL", "MSFT", "NVDA", "TSLA", "AMZN",
    "META", "GOOGL", "JPM", "XOM", "SPY",
}

LIVE_DIAGNOSTIC_LOG_PATH = os.getenv(
	"TV_LIVE_DIAGNOSTIC_LOG",
	"/app/logs/live_trade_diagnostics.csv",
)

BACKTEST_DIAGNOSTIC_LOG_PATH = os.getenv(
	"TV_BACKTEST_DIAGNOSTIC_LOG",
	"/app/logs/backtest_trade_diagnostics.csv",
)

class TradeRecords:
	def __init__(self, trading_view_webhook_helpers, TimeFrame, TimeFrameUnit):

		self.tvw_helpers = trading_view_webhook_helpers
		self.r = trading_view_webhook_helpers.require_redis()
		self.stream_maxlen = int(os.getenv("TV_MAXLEN", "500"))
		self.trade_event_maxlen = int(os.getenv("TV_TRADE_EVENT_MAXLEN", str(self.stream_maxlen)))
		self.pnl_stream_maxlen = int(os.getenv("TV_PNL_MAXLEN", str(self.stream_maxlen)))	

		self.TZ = self.tvw_helpers.eastern_tz
		self.MIN1_CANDLESTICK_PERIODS = {'time delta':'1 minutes', 'time frame':TimeFrame(1, TimeFrameUnit.Minute)}
		self.MIN5_CANDLESTICK_PERIODS = {'time delta':'5 minutes', 'time frame':TimeFrame(5, TimeFrameUnit.Minute)}
		self.MIN15_CANDLESTICK_PERIODS = {'time delta':'15 minutes', 'time frame':TimeFrame(15, TimeFrameUnit.Minute)}
		self.HOUR1_CANDLESTICK_PERIODS = {'time delta':'1 hours', 'time frame':TimeFrame(1, TimeFrameUnit.Hour)}
		self.HOUR4_CANDLESTICK_PERIODS = {'time delta':'4 hours', 'time frame':TimeFrame(4, TimeFrameUnit.Hour)}
		self.DAY_CANDLESTICK_PERIODS = {'time delta':'1 days', 'time frame':'1Day'}	

		self._1min_time_delta = self.MIN1_CANDLESTICK_PERIODS.get('time delta')	
		self._1min_time_frame = self.MIN1_CANDLESTICK_PERIODS.get('time frame')
		self._5min_time_delta = self.MIN5_CANDLESTICK_PERIODS.get('time delta')	
		self._5min_time_frame = self.MIN5_CANDLESTICK_PERIODS.get('time frame')
		self._15min_time_delta = self.MIN15_CANDLESTICK_PERIODS.get('time delta')	
		self._15min_time_frame = self.MIN15_CANDLESTICK_PERIODS.get('time frame')	
		self._1hr_time_delta = self.HOUR1_CANDLESTICK_PERIODS.get('time delta')	
		self._1hr_time_frame = self.HOUR1_CANDLESTICK_PERIODS.get('time frame')	
		self._4hr_time_delta = self.HOUR4_CANDLESTICK_PERIODS.get('time delta')	
		self._4hr_time_frame = self.HOUR4_CANDLESTICK_PERIODS.get('time frame')		
		self.day_time_delta = self.DAY_CANDLESTICK_PERIODS.get('time delta')
		self.day_time_frame = self.DAY_CANDLESTICK_PERIODS.get('time frame')				

	def _normalize_strategy(self, strategy_name: str) -> str:
		return str(strategy_name or "").strip()

	def _normalize_ticker(self, ticker: str) -> str:
		return str(ticker or "").upper().strip()

	def _parse_float(self, value, default=0.0) -> float:
		try:
			if value in (None, ""):
				return float(default)
			return float(value)
		except (TypeError, ValueError):
			return float(default)

	def _parse_int(self, value, default=0) -> int:
		try:
			if value in (None, ""):
				return int(default)
			return int(value)
		except (TypeError, ValueError):
			return int(default)

	def _to_timestamp(self, iso_str: str) -> float:
		return datetime.fromisoformat(str(iso_str).replace("Z", "+00:00")).timestamp()

	def _iso_now(self) -> str:
		return datetime.now(timezone.utc).isoformat()

	def _to_eastern_iso(self, iso_str: str) -> str:
		dt = datetime.fromisoformat(str(iso_str).replace("Z", "+00:00"))
		if dt.tzinfo is None:
			dt = dt.replace(tzinfo=timezone.utc)
		return dt.astimezone(self.tvw_helpers.eastern_tz).isoformat()		

	def _compute_unrealized_pnl(self, side: str, avg_price_per_share: float, market_price: float, num_shares: float) -> float:
		if num_shares <= 0:
			return 0.0

		if side == "long":
			return (market_price - avg_price_per_share) * num_shares

		if side == "short":
			return (avg_price_per_share - market_price) * num_shares

		raise ValueError(f"Invalid side: {side}")


	def log_trade_diagnostic(
		self,
		*,
		source,              # "live" or "backtest"
		strategy_name,
		ticker,
		event_type,          # "entry", "exit", "partial_exit", "add", "reject", "cancel"
		#signal=None,
		timeframe=None,
		side=None,           # "long", "short", "buy", "sell", "cover"
		requested_qty=None,
		#price=None,
		#avg_price=None,
		market_price=None,
		order_id=None,
		#realized_pnl=None,
		#unrealized_pnl=None,
		#position_qty_before=None,
		#position_qty_after=None,
		#bar_close_time=None,
		decision_time=None,
	):
		ticker = str(ticker or "").upper().strip()

		if ticker not in DIAGNOSTIC_TICKERS:
			return

		if source == "live":
			log_path = LIVE_DIAGNOSTIC_LOG_PATH
		elif source == "backtest":
			log_path = BACKTEST_DIAGNOSTIC_LOG_PATH
		else:
			raise ValueError(f"Unknown diagnostic source: {source}")			

		os.makedirs(os.path.dirname(log_path), exist_ok=True)

		file_exists = os.path.exists(log_path)

		row = {
			"logged_at_utc": datetime.now(timezone.utc).isoformat(),
			"source": source,
			"strategy_name": strategy_name,
			"ticker": ticker,
			"event_type": event_type,
			#"signal": signal,
			"timeframe": timeframe,
			"side": side,
			#"qty": qty,
			"requested_qty": requested_qty,
			#"price": price,
			#"avg_price": avg_price,
			#"fill_price": fill_price,
			"market_price": market_price,
			"order_id": order_id,
			#"order_status": order_status,
			#"realized_pnl": realized_pnl,
			#"unrealized_pnl": unrealized_pnl,
			#"position_qty_before": position_qty_before,
			#"position_qty_after": position_qty_after,
			#"bar_close_time": bar_close_time,
			"decision_time": decision_time,
			#"reason": reason,
		}

		with open(log_path, "a", newline="") as f:
			writer = csv.DictWriter(f, fieldnames=row.keys())

			if not file_exists:
				writer.writeheader()

			writer.writerow(row)


	def _compute_realized_delta(self, side: str, avg_price_per_share: float, fill_price: float, close_qty: float) -> float:
		if close_qty <= 0:
			return 0.0

		if side == "long":
			return (fill_price - avg_price_per_share) * close_qty

		if side == "short":
			return (avg_price_per_share - fill_price) * close_qty

		raise ValueError(f"Invalid side: {side}")


	def get_current_trading_day(self) -> str:
		"""
		Returns the current trading day in Eastern Time as an ISO-formatted date string.

		The trading system uses U.S. market time (America/New_York) as the canonical
		reference for daily aggregations, exposure tracking, and risk reporting.
		This helper ensures that all daily calculations remain aligned to the market
		calendar regardless of the server's local timezone (e.g., UTC).

		Parameters:
			None

		Returns:
			str: The current trading day in YYYY-MM-DD format based on Eastern Time.

		Example:
			If the server time is:
				2026-06-02 00:30:00 UTC

			and Eastern Time is:
				2026-06-01 20:30:00 EDT

			then this function returns:
				"2026-06-01"
		"""
		return datetime.now(self.tvw_helpers.eastern_tz).date().isoformat()


	def get_market_prices(self, tickers, alpaca_api):
		"""
		Retrieves current market prices (bid, ask, and midpoint) for a list of tickers
		using the provided API client. Executes concurrent requests via threading,
		retries failed attempts up to a fixed limit, and aggregates results into a
		dictionary keyed by ticker.

		Parameters:
			tickers (list[str]): A list of ticker symbols (e.g., ["AAPL", "TSLA"]).
			alpaca_api (REST): Alpaca API client used to fetch latest quote data.

		Returns:
			dict: A dictionary mapping each ticker to its corresponding market data,
			including bid, ask, and calculated midpoint price ("market").
		"""		
		market_prices = {
			ticker: {
				"ticker": ticker,
				"ask": None,
				"bid": None,
				"market": None,
			}
			for ticker in tickers
		}

		def get_prices(ticker):
			trial = 0

			while trial < 3:
				try:
					last_quote = alpaca_api.get_latest_quote(ticker)
					ask = last_quote.ap
					bid = last_quote.bp

					if ask is None or bid is None:
						raise ValueError(f"Missing ask/bid for {ticker}: ask={ask}, bid={bid}")

					market = (ask + bid) / 2.0

					market_prices[ticker]["ask"] = ask
					market_prices[ticker]["bid"] = bid
					market_prices[ticker]["market"] = market
					return

				except Exception as e:
					trial += 1
					logger.info(
						"Unable to obtain market price for %s on attempt %d: %s",
						ticker,
						trial,
						e,
					)


		from concurrent.futures import ThreadPoolExecutor
		max_workers = min(5, len(tickers))
		with ThreadPoolExecutor(max_workers=max_workers) as executor:
			executor.map(get_prices, tickers)

		return market_prices


	def _entry_in_range(self, snapshot_time: str, start: str = None, end: str = None) -> bool:
		"""
		Determines whether a given snapshot timestamp falls within an optional
		start and end time range. Converts all timestamps to comparable numeric
		values before performing boundary checks.

		Parameters:
			snapshot_time (str): The ISO timestamp of the snapshot entry.
			start (str, optional): Inclusive start time boundary in ISO format.
			end (str, optional): Inclusive end time boundary in ISO format.

		Returns:
			bool: True if the snapshot_time falls within the specified range,
			or if no bounds exclude it; otherwise False.
		"""		
		entry_ts = self._to_timestamp(snapshot_time)

		if start:
			start_ts = self._to_timestamp(start)
			if entry_ts < start_ts:
				return False

		if end:
			end_ts = self._to_timestamp(end)
			if entry_ts > end_ts:
				return False

		return True


	def reset_tv_data(self):
		"""
		Deletes all Redis keys under the "tv:*" namespace in batches to efficiently
		clear application data. Iterates through matching keys, removes them in chunks,
		and logs the total number of keys deleted.

		Parameters:
			None

		Returns:
			dict: A summary containing the total number of deleted Redis keys.
		"""		
		deleted = 0
		batch = []

		for key in self.r.scan_iter("tv:*"):
			batch.append(key)

			if len(batch) >= 500:
				deleted += self.r.delete(*batch)
				batch = []

		if batch:
			deleted += self.r.delete(*batch)

		logger.warning("Deleted %d Redis keys under namespace tv:*", deleted)
		return {"deleted_keys": deleted}


	def _get_df(self, api, securities, time_frame, start_dt, end_dt, max_attempts=3):
		"""
		Returns a pandas dataframe for securities specified within the time range specified by start date and end date, at intervals specified by the timeframe
		Parameters:
			api: 
			securities ([String]): A list of the securities in play sorted alphabetically by ticker symbol.
			time_frame (TimeFrame): An object specifying the timeframe
			start_dt (pandas.Timestamp): Specifies the begining of the time range for which the dataframe is requested. 
			end_dt (pandas.Timestamp): Specifies the end of the time range for which the dataframe is requested. 
		Returns:
			barset.df (pandas.DataFrame): Dataframe for securities specified within the time range specified by start date and end date, at intervals specified by the timeframe
		"""			
		for attempt in range(1, max_attempts + 1):
			try:
				barset = api.get_bars(securities, time_frame, start_dt, end_dt, adjustment="raw")

				return barset.df

			except Exception:
				logger.exception(
					"Unable to obtain Alpaca bars: "
					"attempt=%r/%r securities=%r timeframe=%r "
					"start=%r end=%r",
					attempt,
					max_attempts,
					securities,
					time_frame,
					start_dt,
					end_dt,
				)

				if attempt >= max_attempts:
					raise

				time.sleep(
					3
				)


	def get_df(self, api, securities, time_frame, start_dt, end_dt):
		"""
		Returns a pandas dataframe for securities specified within the time range specified by start date and end date, at intervals specified by the timeframe
		Parameters:
			api:
			securities ([String]): A list of the securities in play sorted alphabetically by ticker symbol.
			time_frame (TimeFrame): An object specifying the timeframe
			start_dt (pandas.Timestamp): Specifies the begining of the time range for which the dataframe is requested. 
			end_dt (pandas.Timestamp): Specifies the end of the time range for which the dataframe is requested. 
		Returns:
			(pandas.DataFrame): Dataframe for securities specified within the time range specified by start date and end date, at intervals specified by the timeframe
		"""		
		return self._get_df(api, securities, time_frame, start_dt.isoformat(), end_dt.isoformat())


	def dataframe_column_to_dict(
		self,
		df: pd.DataFrame,
		column: str,
		symbol_column: str = "symbol",
	) -> dict[str, dict[str, float]]:
		"""
		Convert actual market-data rows to a nested dictionary without creating
		or interpolating missing timestamps.
		Parameters:
			df (pandas.DataFrame):
			column (String): Name of the column to be returned
		Returns:

		{
			"AAPL": {
				"2026-06-15 05:30:00-04:00": 293.50,
				...
			},
			"TSLA": {
				...
			}
		}		
		"""
		if df.empty:
			return {}

		if column not in df.columns:
			raise ValueError(
				f"Column {column!r} is not present in DataFrame"
			)

		if symbol_column not in df.columns:
			raise ValueError(
				f"Column {symbol_column!r} is not present in DataFrame"
			)

		result: dict[str, dict[str, float]] = {}

		for ticker, ticker_df in df.groupby(
			symbol_column,
			sort=False,
		):
			series = ticker_df[column].dropna()

			result[str(ticker).upper().strip()] = {
				pd.Timestamp(timestamp).isoformat(sep=" "): float(value)
				for timestamp, value in series.items()
				if float(value) > 0
			}

		return result


	def dataframe_column_to_dict_with_interpolation(
		self,
		df: pd.DataFrame,
		column: str,
		timeframe: TimeFrame = None,
		timedelta=None,
		symbol_column: str = "symbol"
	):
		"""
		Parameters:
			df (pandas.DataFrame):
			column (String): Name of the column to be returned
			timeframe (TimeFrame): An object specifying the timeframe
			timedelta
			symbol_column (String): 
		Returns:

		{
			"AAPL": {
				"2026-06-15 05:30:00-04:00": 293.50,
				...
			},
			"TSLA": {
				...
			}
		}
		"""

		if column not in df.columns:
			raise ValueError(f"Column '{column}' not found.")

		if symbol_column not in df.columns:
			raise ValueError(f"Column '{symbol_column}' not found.")

		# Determine bar spacing
		if timeframe is not None:
			unit_map = {
				TimeFrameUnit.Minute: "min",
				TimeFrameUnit.Hour: "h",
				TimeFrameUnit.Day: "D",
				TimeFrameUnit.Week: "W"
			}

			freq = pd.Timedelta(
				timeframe.amount,
				unit=unit_map[timeframe.unit]
			)
		elif timedelta is not None:
			freq = pd.Timedelta(timedelta)
		else:
			raise ValueError("Either timeframe or timedelta must be supplied.")

		working = df.copy()

		working.index = pd.to_datetime(
			working.index,
			utc=True
		).tz_convert(self.TZ)

		result = {}

		for symbol, group in working.groupby(symbol_column):

			series = (
				group[column]
				.sort_index()
				.loc[lambda s: ~s.index.duplicated(keep="last")]
				.astype(float)
			)

			full_index = pd.date_range(
				start=series.index.min(),
				end=series.index.max(),
				freq=freq,
				tz=self.TZ
			)

			series = (
				series
				.reindex(full_index)
				.interpolate(method="time", limit_direction="both")
			)

			result[symbol] = {
				timestamp.isoformat(sep=" "): float(value)
				for timestamp, value in series.items()
			}

		return result


	def dataframe_to_atr_dict(
		self,
		df: pd.DataFrame,
		period: int = 14
	):
		"""
		Calculate Wilder's ATR.

		The first ATR is the simple average of the first `period`
		valid True Range values.

		Each later ATR is:

			((previous_atr * (period - 1)) + current_true_range) / period

		Parameters:
			df (pandas.DataFrame):
			period (Int): Number of periods
		Returns:	
			ATR (Float): ATR		
		"""	
		working = df.copy()

		working.index = pd.to_datetime(
			working.index,
			utc=True
		)

		ATR = {}

		for symbol, ticker_df in working.groupby(
			"symbol",
			sort=False
		):
			ticker_df = (
				ticker_df
				.sort_index()
				.loc[
					lambda rows:
						~rows.index.duplicated(keep="last")
				]
			)

			previous_close = ticker_df["close"].shift(1)

			true_range = pd.concat(
				[
					ticker_df["high"] - ticker_df["low"],
					(ticker_df["high"] - previous_close).abs(),
					(ticker_df["low"] - previous_close).abs()
				],
				axis=1
			).max(
				axis=1
			)

			# The first row has no previous close.
			true_range.iloc[0] = np.nan

			atr = pd.Series(
				np.nan,
				index=true_range.index
			)

			valid_true_range = true_range.dropna()

			if len(valid_true_range) < period:
				ATR[str(symbol)] = {}
				continue

			first_atr_timestamp = valid_true_range.index[
				period - 1
			]

			atr.loc[first_atr_timestamp] = (
				valid_true_range.iloc[:period].mean()
			)

			start_position = true_range.index.get_loc(
				first_atr_timestamp
			)

			for position in range(
				start_position + 1,
				len(true_range)
			):
				atr.iloc[position] = (
					(
						atr.iloc[position - 1]
						* (period - 1)
					)
					+ true_range.iloc[position]
				) / period

			atr = atr.dropna()

			atr.index = atr.index.tz_convert(self.TZ)

			ATR[str(symbol)] = {
				timestamp.isoformat(sep=" "): float(value)
				for timestamp, value in atr.items()
			}

		return ATR		


	def _get_live_anchor_atr_placeholder(
		self,
		*,
		strategy_name,
		ticker,
		anchor_tf,
		price,
	) -> float:
		"""
		Return a temporary live trailing-stop amount until live anchor ATR
		retrieval is implemented.
		"""
		placeholder_atr = 1.00

		#logger.info(
			#"Using live anchor ATR placeholder: "
			#"strategy=%r ticker=%r anchor_tf=%r "
			#"market_price=%r placeholder_atr=%r",
			#strategy_name,
			#ticker,
			#anchor_tf,
			#price,
			#placeholder_atr,
		#)

		return placeholder_atr		