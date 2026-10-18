"""Progressive backward search, with completed candles and no forward signal lookup."""
import threading
import pandas as pd
from golden_cross_dip_recovery import build_indicator_frame, snapshot_at

class GoldenCrossHistory:
	def __init__(self, api, settings, timeframe, earliest='1970-01-01', initial_days=365):
		self.api, self.settings, self.timeframe = api, settings, timeframe
		self.earliest = pd.Timestamp(earliest, tz='UTC')
		self.initial_days = initial_days
		self.cache = {}
		self.lock = threading.RLock()

	def _fetch(self, ticker, start, end):
		# alpaca_trade_api.get_bars follows next_page_token internally when limit=None.
		# No regular-hours filter: use every native bar the configured feed provides.
		frame = self.api.get_bars(ticker, self.timeframe, start=start.isoformat(), end=end.isoformat(), adjustment='raw').df.copy()
		if frame.empty:
			return frame
		if isinstance(frame.index, pd.MultiIndex):
			if 'symbol' in frame.index.names:
				frame = frame.xs(ticker, level='symbol')
			else:
				frame.index = frame.index.get_level_values('timestamp')
		if 'symbol' in frame.columns:
			frame = frame[frame.symbol == ticker]
		frame.index = pd.to_datetime(frame.index, utc=True)
		# Exclude partially formed final bars, even if the provider returns one.
		return frame[(frame.index >= start) & (frame.index+pd.Timedelta(hours=4) <= end)]

	def get_frame(self, ticker, start, end):
		ticker = ticker.upper().strip()
		start, end = pd.Timestamp(start), pd.Timestamp(end)
		if start.tzinfo is None or end.tzinfo is None or start > end:
			raise ValueError('Historical bounds must be ordered, timezone-aware dates')
		start, end = start.tz_convert('UTC'), end.tz_convert('UTC')
		with self.lock:
			cached = self.cache.get(ticker)
			if cached and cached['requested_start'] <= start and cached['end'] >= end:
				return cached['frame'].loc[cached['frame'].completed_at <= end].copy()
			# Retain completed history; refresh the trailing interval, never its unfinished candle.
			span = pd.Timedelta(days=self.initial_days)
			left = max(self.earliest, start-span)
			if cached and cached['requested_start'] <= start and cached['end'] < end:
				left = cached['left']
				raw = pd.concat([cached['raw'], self._fetch(ticker, max(left,cached['end']-pd.Timedelta(hours=8)),end)])
			else:
				raw = self._fetch(ticker,left,end)
			while True:
				if not raw.empty:
					raw = raw[~raw.index.duplicated(keep='last')].sort_index()
				frame = build_indicator_frame(raw,self.settings)
				prior = frame.loc[frame.completed_at <= start] if not frame.empty else frame
				# A sizable prehistory keeps EMA startup bias away from the latest cross.
				ready = not prior.empty and pd.notna(prior.iloc[-1].golden_cross_price)
				if ready:
					diff = prior.fast_moving_average-prior.slow_moving_average
					crosses = prior.index[(diff.shift(1)<=0)&(diff>0)]
					minimum = max(self.settings.slow_period, self.settings.fast_period)*5
					ready = len(crosses)>0 and prior.index.get_loc(crosses[-1]) >= minimum
				if ready or left <= self.earliest:
					break
				span *= 2
				new_left = max(self.earliest,start-span)
				older = self._fetch(ticker,new_left,left+pd.Timedelta(hours=4))
				raw = pd.concat([older,raw])
				left = new_left
			self.cache[ticker] = {'requested_start': start, 'end':end, 'left':left, 'raw':raw, 'frame':frame}
			return frame.copy()

	def snapshot(self, ticker, as_of):
		return snapshot_at(self.get_frame(ticker,as_of,as_of),as_of)
