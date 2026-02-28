"""
Author: Matthew Akofu
Date Created: Feb 13, 2026
"""
#import pandas as pd
#import numpy as np
import config
from werkzeug.exceptions import HTTPException
#import time
#import math
#import copy
#from datetime import datetime, timezone
#import pytz


class Dataframes:


	def __init__(self, MY_TZ):
		self.MY_TZ = MY_TZ
		#self.security_ohlcv = []
		#self.yesterdays_ohlcv = []	

		self.TimeFrame = config.TimeFrame 
		self.TimeFrameUnit = config.TimeFrameUnit

		self.MIN1_CANDLESTICK_PERIODS = {'time delta':'1 minutes', 'time frame':self.TimeFrame(1, self.TimeFrameUnit.Minute)}
		self.MIN5_CANDLESTICK_PERIODS = {'time delta':'5 minutes', 'time frame':self.TimeFrame(5, self.TimeFrameUnit.Minute)}
		self.MIN15_CANDLESTICK_PERIODS = {'time delta':'15 minutes', 'time frame':self.TimeFrame(15, self.TimeFrameUnit.Minute)}
		self.HOUR1_CANDLESTICK_PERIODS = {'time delta':'1 hours', 'time frame':self.TimeFrame(1, self.TimeFrameUnit.Hour)}
		self.HOUR4_CANDLESTICK_PERIODS = {'time delta':'4 hours', 'time frame':self.TimeFrame(4, self.TimeFrameUnit.Hour)}
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


	def _get_df(self, config, securities, time_frame, start_dt, end_dt):
		"""
		Returns a pandas dataframe for securities specified within the time range specified by start date and end date, at intervals specified by the timeframe
		Parameters:
			config: Reference to config.py
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
					return config.api.get_bars(securities, time_frame, start_dt, end_dt, adjustment='raw')

				#except HTTPError:
				except HTTPException:
					print("Waiting before retrying...")
					time.sleep(3)#Suspends thread for specified num seconds					
					barset_got = False

		barset = get_barset(securities)

		return barset.df


	def get_df(self, config, securities, time_frame, start_dt, end_dt):
		"""
		Returns a pandas dataframe for securities specified within the time range specified by start date and end date, at intervals specified by the timeframe
		Parameters:
			config: Reference to config.py
			securities ([String]): A list of the securities in play sorted alphabetically by ticker symbol.
			time_frame (TimeFrame): An object specifying the timeframe
			start_dt (pandas.Timestamp): Specifies the begining of the time range for which the dataframe is requested. 
			end_dt (pandas.Timestamp): Specifies the end of the time range for which the dataframe is requested. 
		Returns:
			(pandas.DataFrame): Dataframe for securities specified within the time range specified by start date and end date, at intervals specified by the timeframe
		"""		
		return self._get_df(config, securities, time_frame, start_dt.isoformat(), end_dt.isoformat())


