"""Position-lifetime simulation exit experiments; no broker operations."""
import math
import pandas as pd

class ExitExperiments:
	def __init__(self, settings):
		self.settings = settings
		self.stop = None
		self.armed_bar = None
		self.last_completed_at = None
		self.entry_time = None
		self.entry_price = None
		self.entry_atr = None
		self.highest_close = None

	def open(self, price, snapshot, time):
		self.stop = None
		self.armed_bar = None
		self.entry_time = pd.Timestamp(time)
		self.entry_price = self.highest_close = price
		self.entry_atr = snapshot.get('atr')
		self.last_completed_at = snapshot.get('completed_at')

	def arm(self, snapshot):
		# Repeated signals do not indefinitely extend the original window.
		if self.armed_bar is None:
			self.armed_bar = snapshot.get('completed_bar_number')

	def check(self, price, snapshot):
		fast, atr = snapshot.get('fast_moving_average'), snapshot.get('atr')
		if any(v is None or not math.isfinite(v) or v <= 0 for v in (price, fast, atr)):
			return None
		if self.settings.profit_protection_enabled and self.entry_time is not None:
			completed = snapshot.get('completed_at')
			close = snapshot.get('close')
			if completed and completed != self.last_completed_at and pd.Timestamp(completed) > self.entry_time and close is not None and math.isfinite(close) and close > 0:
				self.last_completed_at = completed
				self.highest_close = max(self.highest_close, close)
				if self.entry_atr is not None and math.isfinite(self.entry_atr) and self.entry_atr > 0 and self.highest_close >= self.entry_price + self.settings.profit_activation_atr_multiple*self.entry_atr:
					candidate = self.highest_close - self.settings.profit_trailing_atr_multiple*atr
					self.stop = max(self.stop if self.stop is not None else candidate, candidate)
			if self.stop is not None and price < self.stop:
				return 'profit_protection_trailing_exit'
		if self.armed_bar is not None:
			bar = snapshot.get('completed_bar_number')
			if bar is None:
				return None
			if bar-self.armed_bar > self.settings.bearish_exit_confirmation_window_candles or price > fast:
				self.armed_bar = None
			elif price < fast - self.settings.confirmatory_fast_average_exit_atr_multiple*atr:
				return 'bearish_exit_window_confirmed'
		return None
