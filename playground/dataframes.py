"""
Author: Matthew Akofu
Date Created: Feb 13, 2026
"""
import pandas as pd
import numpy as np
import alpaca_trade_api as tradeapi
#from werkzeug.exceptions import HTTPException
#import time
#import math
#import copy
#from datetime import datetime, timezone
from pytz import timezone
#from alpaca_trade_api.rest import REST, TimeFrame, TimeFrameUnit
from alpaca_trade_api.rest import TimeFrame, TimeFrameUnit


APCA_API_BASE_URL= "https://paper-api.alpaca.markets"
APCA_API_KEY_ID= "PKCLU6E4QGQT6OSESNYKSPKN4G"
APCA_API_SECRET_KEY= "4c4icyLMhBdADUGtR8JNpJcRkZ4jVTpbZ9BicVwiwzho"

api = tradeapi.REST(
    base_url=APCA_API_BASE_URL,
    key_id=APCA_API_KEY_ID,
    secret_key=APCA_API_SECRET_KEY
)

class Dataframes:

	def __init__(self, TZ):
		self.TZ = TZ

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

	def _get_df1(self, api, securities, time_frame, start_dt, end_dt):
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
		def get_barset(securities):

			barset_got = False
			while(not barset_got):
				try:
					return api.get_bars(securities, time_frame, start_dt, end_dt, adjustment='raw')

				#except HTTPError:
				except HTTPException:
					print("Waiting before retrying...")
					time.sleep(3)#Suspends thread for specified num seconds					
					barset_got = False

		barset = get_barset(securities)

		return barset.df


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

	"""
	def dataframe_column_to_dict_fill_missing_rows(
		self,
		df: pd.DataFrame,
		column: str,
		timeframe: TimeFrame = None,
		timedelta=None,
		symbol_column: str = "symbol"
	):
		/
		For each ticker and each UTC calendar date, expected bars are generated
		between 08:00:00 UTC and 23:59:00 UTC.

		Missing bars are inserted and their values are interpolated.
		Output timestamps are converted to US/Eastern.		
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
		/

		if column not in df.columns:
			raise ValueError(
				f"Column '{column}' not found."
			)

		if symbol_column not in df.columns:
			raise ValueError(
				f"Column '{symbol_column}' not found."
			)

		freq = self._resolve_dataframe_frequency(
			timeframe=timeframe,
			timedelta=timedelta
		)

		working = df.copy()

		# Keep the timestamps in UTC while determining UTC trading dates
		# and constructing each day's expected timestamp range.
		working.index = pd.to_datetime(
			working.index,
			utc=True,
			errors="raise"
		)

		working.index.name = "timestamp"

		working = working.sort_index()

		result = {}

		for symbol, symbol_group in working.groupby(
			symbol_column,
			sort=False
		):
			completed_days = []

			# Group by UTC calendar date, not Eastern calendar date.
			for utc_date, day_group in symbol_group.groupby(
				symbol_group.index.date
			):
				series = (
					day_group[column]
					.sort_index()
					.loc[
						lambda values:
							~values.index.duplicated(keep="last")
					]
					.astype(float)
				)

				if series.empty:
					continue

				day_start = pd.Timestamp(
					year=utc_date.year,
					month=utc_date.month,
					day=utc_date.day,
					hour=8,
					minute=0,
					second=0,
					tz="UTC"
				)

				day_limit = pd.Timestamp(
					year=utc_date.year,
					month=utc_date.month,
					day=utc_date.day,
					hour=23,
					minute=59,
					second=0,
					tz="UTC"
				)

				# date_range automatically stops at the final timestamp
				# aligned with the selected frequency before 23:59.
				#
				# For a five-minute timeframe, this ends at 23:55 UTC.
				full_index = pd.date_range(
					start=day_start,
					end=day_limit,
					freq=freq,
					tz="UTC",
					name="timestamp"
				)

				# Keep only rows within the valid UTC session.
				series = series.loc[
					(series.index >= day_start)
					& (series.index <= day_limit)
				]

				# Insert rows for every missing expected timestamp.
				series = series.reindex(full_index)

				# Interpolate gaps between known observations.
				#
				# limit_direction="both" also fills missing bars at the
				# beginning or end of the day's valid range using the nearest
				# available value.
				series = series.interpolate(
					method="time",
					limit_direction="both"
				)

				completed_days.append(series)

			if not completed_days:
				result[str(symbol)] = {}
				continue

			complete_series = pd.concat(
				completed_days
			).sort_index()

			# Convert to Eastern only after all UTC date ranges are completed.
			complete_series.index = complete_series.index.tz_convert(self.TZ)

			result[str(symbol)] = {
				timestamp.isoformat(sep=" "): float(value)
				for timestamp, value in complete_series.items()
				if pd.notna(value)
			}

		return result


	def _resolve_dataframe_frequency(
		self,
		timeframe: TimeFrame = None,
		timedelta=None
	):
		if timeframe is not None:
			unit_map = {
				TimeFrameUnit.Minute: "min",
				TimeFrameUnit.Hour: "h",
				TimeFrameUnit.Day: "D",
				TimeFrameUnit.Week: "W"
			}

			if timeframe.unit not in unit_map:
				raise ValueError(
					f"Unsupported timeframe unit: {timeframe.unit}"
				)

			freq = pd.Timedelta(
				timeframe.amount,
				unit=unit_map[timeframe.unit]
			)

		elif timedelta is not None:
			freq = pd.Timedelta(timedelta)

		else:
			raise ValueError(
				"Either timeframe or timedelta must be supplied."
			)

		if freq <= pd.Timedelta(0):
			raise ValueError(
				"The timeframe must be greater than zero."
			)

		return freq
	"""

if __name__ == "__main__":

	dataframes_instance = Dataframes(timezone('US/Eastern'))
	start_dt = pd.Timestamp('2026-07-15 09:30',tz=dataframes_instance.TZ)
	end_dt = pd.Timestamp('2026-07-15 10:00',tz=dataframes_instance.TZ)
	securities = ["CVX"]#["TSLA", "AAPL", "MSFT", "NVDA", "AMZN", "GOOGL", "META", "AMD", "AMAT", "INTC", "CRM", "ADBE", "SNOW", "JPM", "GS", "MS", "XOM", "CVX", "NFLX", "DIS"]
	timeframe = dataframes_instance._1min_time_frame
	#timeframe = dataframes_instance._1hr_time_frame
		
	df = dataframes_instance.get_df(api, securities, timeframe, start_dt, end_dt)

	pd.set_option('display.max_rows', None)
	pd.set_option('display.max_columns', None)
	pd.set_option('display.width', None)
	pd.set_option('display.max_colwidth', None)

	print(df)	

	close_prices = dataframes_instance.dataframe_column_to_dict(
		df,
		"close"
	)

	open_prices = dataframes_instance.dataframe_column_to_dict(
		df,
		"open"
	)

	high_prices = dataframes_instance.dataframe_column_to_dict(
		df,
		"high"
	)

	low_prices = dataframes_instance.dataframe_column_to_dict_with_interpolation(
		df,
		"low",
		timeframe=timeframe
	)	

	ATR = dataframes_instance.dataframe_to_atr_dict(
		df,
		period=14
	)		

	print(f"\n###################\nClose Prices:\n {close_prices}")
	#print(f"\n###################\nLow Prices:\n {low_prices}")
	#print(f"\n###################\nATR:\n {ATR}")
