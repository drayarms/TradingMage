"""Causal native 4h trend classifications used exclusively for chart overlays."""
import math
import pandas as pd
from chart_data import ichimoku_spans,heikin_ashi

TREND_FIELDS=('strong_long_trend','strong_short_trend')

def finite(v):return v is not None and math.isfinite(v)

def _cloud_trend_step(state,i,rows,ha,gap,turn_reference,direction,fraction):
	"""Advance one direction's cloud-only state before rendering priority is applied."""
	previous=state['previous_gap'];was_active=state['active'];row=rows[i]
	if gap is not None and previous is not None:
		if previous>=0 and gap<0:
			state['cross']=i;state['armed']=True
		elif previous<0 and gap>=0:
			state['continuing']=was_active
			if not was_active:state['armed']=False
	state['previous_gap']=gap
	if state['continuing']:
		candle=ha.iloc[i]
		values=(candle.open,candle.close,candle.low,candle.high)
		wick=candle.open-candle.low if direction==1 else candle.high-candle.open
		active=all(finite(v) for v in values) and direction*(candle.close-candle.open)>0 and wick<=1e-9*max(1.,abs(candle.open))
		if not active:state['continuing']=False;state['armed']=False
	else:
		active=False;cross=state['cross'];fast=row.fast_moving_average
		if state['armed'] and gap is not None and gap<0 and cross is not None and finite(fast):
			anchor=rows[cross]
			band=row.bollinger_lower if direction==1 else row.bollinger_upper
			anchor_band=anchor.bollinger_lower if direction==1 else anchor.bollinger_upper
			active=finite(anchor.fast_moving_average) and direction*(fast-anchor.fast_moving_average)>0 and direction*(band-anchor_band)>0
			if active and not was_active:
				# These extra checks govern startup only, not an already active cloud trend.
				upper,lower=row.bollinger_upper,row.bollinger_lower
				threshold=upper-fraction*(upper-lower) if direction==1 else lower+fraction*(upper-lower)
				opposite=upper if direction==1 else lower
				old_opposite=None if turn_reference is None else (rows[turn_reference].bollinger_upper if direction==1 else rows[turn_reference].bollinger_lower)
				active=direction*(fast-threshold)>=0 and finite(old_opposite) and direction*(opposite-old_opposite)>0
	state['active']=bool(active)
	return state['active']

def annotate_trend_zones(frame,points,trend_cloud_condition_enabled=True,trend_heikin_ashi_streak_condition_enabled=True,trend_cloud_fast_ma_band_gap_fraction=.4,trend_heikin_ashi_streak_min_candles=2,trend_heikin_ashi_streak_max_ma_gap_atr_multiple=1.2):
	if isinstance(trend_cloud_fast_ma_band_gap_fraction,bool) or not math.isfinite(trend_cloud_fast_ma_band_gap_fraction) or trend_cloud_fast_ma_band_gap_fraction<0:raise ValueError('trend_cloud_fast_ma_band_gap_fraction must be nonnegative and finite')
	if type(trend_heikin_ashi_streak_min_candles) is not int or trend_heikin_ashi_streak_min_candles<1:raise ValueError('trend_heikin_ashi_streak_min_candles must be a positive integer')
	if isinstance(trend_heikin_ashi_streak_max_ma_gap_atr_multiple,bool) or not math.isfinite(trend_heikin_ashi_streak_max_ma_gap_atr_multiple) or trend_heikin_ashi_streak_max_ma_gap_atr_multiple<=0:raise ValueError('trend_heikin_ashi_streak_max_ma_gap_atr_multiple must be positive and finite')
	out=frame.copy()
	if frame.empty:
		for name in TREND_FIELDS:out[name]=pd.Series(False,index=frame.index,dtype=bool)
		return out
	# The cloud visually at candle i was calculated at completed candle i-26.
	a,b=ichimoku_spans(frame);a,b=a.shift(26),b.shift(26)
	rows=list(frame.itertuples());events=[]
	for point in points:
		confirmed=pd.Timestamp(point['confirmed_at']);evaluation=pd.Timestamp(point.get('time',confirmed-pd.Timedelta(hours=4)))
		if evaluation not in frame.index:continue
		index=frame.index.get_loc(evaluation)
		if pd.Timestamp(rows[index].completed_at)<=confirmed:events.append((confirmed.value,index))
	events.sort();cursor=0;reference=None;result=[]
	cloud_states=[dict(cross=None,armed=False,active=False,continuing=False,previous_gap=None) for _ in range(2)]
	ha=heikin_ashi(frame) if trend_cloud_condition_enabled or trend_heikin_ashi_streak_condition_enabled else None
	long_count=short_count=0;long_streak=short_streak=False
	for i,row in enumerate(rows):
		reset=None
		while cursor<len(events) and events[cursor][0]<=pd.Timestamp(row.completed_at).value:
			reset=events[cursor][1];cursor+=1
		if reset is not None:reference=reset
		upper,lower=row.bollinger_upper,row.bollinger_lower
		valid=finite(upper) and finite(lower) and upper>lower
		fast=row.fast_moving_average
		aligned=valid and finite(a.iloc[i]) and finite(b.iloc[i])
		# References belong to observed crossings of the displaced, visually aligned cloud.
		long_gap=max(a.iloc[i],b.iloc[i])-lower if aligned else None
		short_gap=min(a.iloc[i],b.iloc[i])-upper if aligned else None
		long_cloud=short_cloud=False
		if trend_cloud_condition_enabled:
			long_cloud=_cloud_trend_step(cloud_states[0],i,rows,ha,long_gap,reference,1,trend_cloud_fast_ma_band_gap_fraction)
			short_cloud=_cloud_trend_step(cloud_states[1],i,rows,ha,-short_gap if short_gap is not None else None,reference,-1,trend_cloud_fast_ma_band_gap_fraction)
		if trend_heikin_ashi_streak_condition_enabled:
			candle=ha.iloc[i]
			color=0 if not (finite(candle.open) and finite(candle.close)) else 1 if candle.close>candle.open else -1 if candle.close<candle.open else 0
			long_count=long_count+1 if color==1 else 0
			short_count=short_count+1 if color==-1 else 0
			if color!=1:long_streak=False
			if color!=-1:short_streak=False
			slow,atr=row.slow_moving_average,getattr(row,'atr',None)
			gap_ok=all(finite(v) for v in (fast,slow,atr)) and atr>0 and abs(fast-slow)<=trend_heikin_ashi_streak_max_ma_gap_atr_multiple*atr
			if gap_ok:
				if long_count>=trend_heikin_ashi_streak_min_candles:
					anchor=rows[i-long_count+1].fast_moving_average
					if finite(anchor) and fast>anchor:long_streak=True
				if short_count>=trend_heikin_ashi_streak_min_candles:
					anchor=rows[i-short_count+1].fast_moving_average
					if finite(anchor) and fast<anchor:short_streak=True
		result.append((bool(long_cloud or long_streak),bool(short_cloud or short_streak)))
	for j,name in enumerate(TREND_FIELDS):out[name]=[r[j] for r in result]
	return out
