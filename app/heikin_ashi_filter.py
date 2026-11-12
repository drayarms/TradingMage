"""Completed native-hour HA runs, independent of chart display settings."""
import numpy as np
import pandas as pd
from chart_data import heikin_ashi

EXIT_REASON='opposing_1h_heikin_ashi_run'

class HeikinAshiRuns:
	def __init__(self,frames,n,anchors=None):
		self.index={}
		self.color_clears={}
		anchors=anchors or {}
		for ticker,raw in frames.items():
			frame=heikin_ashi(raw.sort_index().loc[lambda f:~f.index.duplicated(keep='last')])
			red=green=0;values=[];red_near=green_near=False
			anchor=anchors.get(ticker,pd.DataFrame())
			if not anchor.empty:
				anchor=anchor.sort_values('completed_at')
				anchor_times=pd.DatetimeIndex(anchor.completed_at).asi8
				anchor_rows=anchor.to_dict('records')
			else:anchor_times=np.array([],dtype=np.int64);anchor_rows=[]
			def near(row,side):
				when=pd.Timestamp(row.Index)+pd.Timedelta(hours=1)
				i=int(np.searchsorted(anchor_times,when.value,side='right'))-1
				if i<0:return False
				a=anchor_rows[i];band=a.get('bollinger_lower' if side=='long' else 'bollinger_upper');atr=a.get('atr')
				if band is None or atr is None or not np.isfinite(band) or not np.isfinite(atr) or atr<=0:return False
				return row.low<band+.2*atr if side=='long' else row.high>band-.2*atr
			for row in frame.itertuples():
				tol=max(abs(row.open),abs(row.close),1.)*1e-10
				red=red+1 if row.close<row.open and abs(row.high-max(row.open,row.close))<=tol else 0
				green=green+1 if row.close>row.open and abs(row.low-min(row.open,row.close))<=tol else 0
				if red==1:red_near=near(row,'long')
				if green==1:green_near=near(row,'short')
				values.append((red>=n and red_near,green>=n and green_near))
			self.index[ticker]=(pd.DatetimeIndex(frame.index+pd.Timedelta(hours=1)).asi8,values)
			times=self.index[ticker][0]
			valid=np.isfinite(frame.open.to_numpy()) & np.isfinite(frame.close.to_numpy())
			indices=np.arange(len(frame))
			long_clear=np.maximum.accumulate(np.where(valid & (frame.close.to_numpy()>=frame.open.to_numpy()),indices,-1))
			short_clear=np.maximum.accumulate(np.where(valid & (frame.close.to_numpy()<=frame.open.to_numpy()),indices,-1))
			self.color_clears[ticker]=(times,long_clear,short_clear)

	def only_opposing_since(self,ticker,side,after,when):
		"""A green/doji clears longs; a red/doji clears shorts, strictly after fill."""
		if ticker not in self.color_clears:return True
		times,long_clear,short_clear=self.color_clears[ticker]
		first=int(np.searchsorted(times,pd.Timestamp(after).value,side='right'))
		last=int(np.searchsorted(times,pd.Timestamp(when).value,side='right'))-1
		if last<first:return True
		return (long_clear if side=='long' else short_clear)[last]<first


	def blocks(self,ticker,side,when):
		if ticker not in self.index:return False
		times,values=self.index[ticker];i=int(np.searchsorted(times,pd.Timestamp(when).value,side='right'))-1
		return i>=0 and values[i][0 if side=='long' else 1]


def load_hourly_runs(api,settings,symbols,start,end,anchors):
	from golden_cross_history import GoldenCrossHistory
	from alpaca_trade_api.rest import TimeFrame,TimeFrameUnit
	loader=GoldenCrossHistory(api,settings,TimeFrame(1,TimeFrameUnit.Hour),bar_duration=pd.Timedelta(hours=1))
	frames={t:loader._fetch(t,pd.Timestamp(start)-pd.Timedelta(days=max(365,settings.heikin_ashi_filter_candles)),pd.Timestamp(end)) for t in symbols}
	for ticker,frame in frames.items():
		if frame.empty:raise ValueError('No completed native 1h Heikin-Ashi filter data for '+ticker)
	return HeikinAshiRuns(frames,settings.heikin_ashi_filter_candles,anchors)
