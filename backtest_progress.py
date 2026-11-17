"""Opt-in, flushed progress markers consumed by the local endpoint runner."""
import os
from zoneinfo import ZoneInfo

class BacktestProgress:
	def __init__(self):
		self.enabled=os.getenv('TV_BACKTEST_PROGRESS','false').lower() in {'true','1','yes'}
		self.last_bucket=None

	def stage(self,message):
		if self.enabled: print('__BACKTEST_PROGRESS__'+message,flush=True)

	def update(self,when):
		if not self.enabled: return
		bucket=int(when.timestamp())//900
		if self.last_bucket is not None and bucket<=self.last_bucket: return
		self.last_bucket=bucket
		self.stage('Simulating: '+when.astimezone(ZoneInfo('America/New_York')).strftime('%Y-%m-%d %H:%M:%S %Z'))
