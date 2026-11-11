"""Shared elapsed-session-time coordinates for every chart layer."""
import bisect
import pandas as pd
from matplotlib import dates as md
from matplotlib.ticker import MaxNLocator,FuncFormatter

class ChartTimeAxis:
	def __init__(self,sessions):
		self.sessions=sorted((pd.Timestamp(r['start']),pd.Timestamp(r['end'])) for r in sessions)
		self.starts=[s.value for s,e in self.sessions]
		self.offsets=[];elapsed=0.
		for start,end in self.sessions:
			self.offsets.append(elapsed);elapsed+=(end-start).total_seconds()/86400
		self.total=elapsed

	def __call__(self,value):
		t=pd.Timestamp(value)
		if not self.sessions:return md.date2num(t.to_pydatetime())
		i=bisect.bisect_right(self.starts,t.value)-1
		if i<0:return 0.
		start,end=self.sessions[i]
		return self.offsets[i]+max(0.,(min(t,end)-start).total_seconds()/86400)

	def timestamp(self,value):
		i=max(0,min(len(self.sessions)-1,bisect.bisect_right(self.offsets,value)-1))
		start,end=self.sessions[i]
		return min(end,start+pd.Timedelta(days=max(0.,value-self.offsets[i])))

	def configure(self,ax):
		if self.sessions:
			ax.xaxis.set_major_locator(MaxNLocator(nbins=9))
			ax.xaxis.set_major_formatter(FuncFormatter(lambda v,pos:self.timestamp(v).round('min').tz_convert('America/New_York').strftime('%b %d\n%H:%M')))
		else:
			from zoneinfo import ZoneInfo
			locator=md.AutoDateLocator(minticks=4,maxticks=10,tz=ZoneInfo('America/New_York'))
			ax.xaxis.set_major_locator(locator);ax.xaxis.set_major_formatter(md.ConciseDateFormatter(locator,tz=ZoneInfo('America/New_York')))
