"""Plot-only native-timeframe calculations; never consulted by trade decisions."""
from dataclasses import dataclass,asdict
import math
import pandas as pd

SELECTORS=('plotted_range_zone','plotted_stretch_zones','plotted_fast_ma','plotted_slow_ma','plotted_bollingers','plotted_candlesticks','plotted_ichimoku_cloud')

@dataclass(frozen=True)
class PlotOptions:
	plotted_range_zone:str='4h'
	plotted_stretch_zones:str='4h'
	plotted_fast_ma:str='4h'
	plotted_slow_ma:str='4h'
	plotted_bollingers:str='4h'
	plotted_candlesticks:str='4h'
	plotted_ichimoku_cloud:str='4h'
	candlestick_type:str='regular'
	def __post_init__(self):
		for name in SELECTORS:
			if getattr(self,name) not in {'1h','4h'}:raise ValueError(name+' must be 1h or 4h')
		if self.candlestick_type not in {'regular','heikin_ashi'}:raise ValueError('candlestick_type must be regular or heikin_ashi')
	def to_dict(self):return asdict(self)


def heikin_ashi(frame):
	out=frame.copy()
	if out.empty:return out
	close=frame[['open','high','low','close']].mean(axis=1)
	opens=[(float(frame.open.iloc[0])+float(frame.close.iloc[0]))/2]
	for i in range(1,len(frame)):opens.append((opens[-1]+float(close.iloc[i-1]))/2)
	out['open']=opens;out['close']=close
	out['high']=pd.concat([frame.high,out.open,out.close],axis=1).max(axis=1)
	out['low']=pd.concat([frame.low,out.open,out.close],axis=1).min(axis=1)
	return out


def ichimoku_cloud(frame,completed_slots):
	"""Each cloud value is sourced at its own completed candle, displaced +26 slots."""
	def midpoint(n):return (frame.high.rolling(n,min_periods=n).max()+frame.low.rolling(n,min_periods=n).min())/2
	a=(midpoint(9)+midpoint(26))/2;b=midpoint(52);records=[]
	for i in range(len(frame)):
		if i+26>=len(completed_slots):break
		if pd.notna(a.iloc[i]) and pd.notna(b.iloc[i]):records.append(dict(time=pd.Timestamp(completed_slots[i+26]),source_time=pd.Timestamp(frame.completed_at.iloc[i]),span_a=float(a.iloc[i]),span_b=float(b.iloc[i])))
	return pd.DataFrame(records,columns=['time','source_time','span_a','span_b'])


def _records(frame,names):
	records=[]
	for when,row in frame.iterrows():
		r=dict(time=pd.Timestamp(when).isoformat(),completed_at=pd.Timestamp(row.completed_at).isoformat())
		for name in names:
			value=row.get(name)
			if value is None or pd.isna(value):r[name]=None
			elif isinstance(value,(bool,)) or str(getattr(value,'dtype',''))=='bool':r[name]=bool(value)
			else:r[name]=float(value) if math.isfinite(value) else None
		records.append(r)
	return records


def future_slots(api,frame,tf,cache):
	from backtest_sessions import BacktestSessions
	last=pd.Timestamp(frame.completed_at.iloc[-1]);key=(tf,last.isoformat())
	if key in cache:return cache[key]
	duration=pd.Timedelta(tf);calendar=BacktestSessions.load(api,last,last+pd.Timedelta(days=45));future=[]
	for _,bounds in sorted(calendar.days.items()):
		when=bounds[0]+duration
		while when<=bounds[3]:
			if when>last:future.append(when.tz_convert('UTC'))
			when+=duration
		if len(future)>=26:break
	if len(future)<26:raise ValueError('Insufficient calendar sessions for projected '+tf+' Ichimoku cloud')
	cache[key]=future[:26];return cache[key]


def prepare_chart_data(api,settings,frames,start,end,options):
	from golden_cross_history import GoldenCrossHistory
	from golden_cross_dip_recovery import build_indicator_frame
	from cross_strategy_indicators import fast_moving_average_reversed
	from cross_entry_policy import attach_cross_turning_context
	from range_transition_zones import transition_zones
	from alpaca_trade_api.rest import TimeFrame,TimeFrameUnit
	start,end=pd.Timestamp(start),pd.Timestamp(end);result={};cache={}
	wanted={getattr(options,k) for k in SELECTORS}
	if settings.cross_exit_method=='dema5':wanted.add('4h')
	for ticker,original in frames.items():
		native={'4h':original.copy()}
		if '1h' in wanted:
			# Bounded prehistory: enough for mature averages/ATR/HA without a cross search.
			days=max(365,math.ceil(max(settings.slow_period,settings.fast_period,settings.bollinger_period,52)*5/8*7/5)+30)
			history=GoldenCrossHistory(api,settings,TimeFrame(1,TimeFrameUnit.Hour),bar_duration=pd.Timedelta(hours=1))
			raw=history._fetch(ticker,start-pd.Timedelta(days=days),end)
			if raw.empty:raise ValueError('No completed native 1h chart data for '+ticker)
			native['1h']=build_indicator_frame(raw,settings,pd.Timedelta(hours=1))
		layers={}
		for tf in wanted:
			frame=native[tf]
			if frame.empty:continue
			points=fast_moving_average_reversed(frame,tf)
			frame=attach_cross_turning_context(frame,points)
			visible=frame.loc[(frame.index>=start)&(frame.completed_at<=end)]
			transformed=heikin_ashi(frame) if options.candlestick_type=='heikin_ashi' and tf==options.plotted_candlesticks else frame
			candles=transformed.loc[visible.index]
			candle_rows=_records(candles,('open','high','low','close'))
			for r,(_,raw_row) in zip(candle_rows,visible.iterrows()):r['raw_close']=float(raw_row.close)
			# Seed overlays with the last completed value at the left edge.
			prior=frame.loc[frame.completed_at<=start].tail(1)
			indicator_frame=pd.concat([prior,frame.loc[(frame.completed_at>start)&(frame.completed_at<=end)]])
			names=('close','fast_moving_average','slow_moving_average','bollinger_upper','bollinger_lower','dema5','atr','high','low','ranging_market','extreme_bullish_price_stretch','extreme_bearish_price_stretch')
			rows=_records(indicator_frame,names)
			for r,(_,row) in zip(rows,indicator_frame.iterrows()):
				bull,bear=transition_zones(float(row.close),row,settings);r.update(bullish_transition=bool(bull),bearish_transition=bool(bear))
			cloud=[]
			if tf==options.plotted_ichimoku_cloud:
				slots=list(frame.completed_at)+future_slots(api,frame,tf,cache)
				computed=ichimoku_cloud(frame,slots)
				selected=pd.concat([computed.loc[computed.time<start].tail(1),computed.loc[computed.time>=start]])
				for r in selected.to_dict('records'):
					cloud.append(dict(time=r['time'].isoformat(),source_time=r['source_time'].isoformat(),span_a=r['span_a'],span_b=r['span_b']))
			layers[tf]=dict(candles=candle_rows,indicators=rows,ichimoku=cloud)
		from backtest_sessions import BacktestSessions
		axis_end=max([end]+[pd.Timestamp(r['time']) for layer in layers.values() for r in layer['ichimoku']])
		axis_key=('axis',start.isoformat(),axis_end.isoformat())
		if axis_key not in cache:
			calendar=BacktestSessions.load(api,start,axis_end)
			cache[axis_key]=[dict(start=b[0].isoformat(),end=b[3].isoformat()) for _,b in sorted(calendar.days.items())]
		result[ticker]=dict(options=options.to_dict(),layers=layers,start=start.isoformat(),end=end.isoformat(),sessions=cache[axis_key])
	return result
