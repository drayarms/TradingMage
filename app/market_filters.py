"""Causal completed-4h market annotations and entry/exit decisions."""
import math
import pandas as pd

MARKET_FIELDS = ('dema9', 'ranging_market', 'extreme_bullish_price_stretch',
	'extreme_bearish_price_stretch', 'obvious_peak', 'obvious_trough',
	'dema9_lower_touch_slope', 'dema9_upper_touch_slope')

def finite(value):
	return value is not None and math.isfinite(value)

def annotate_market_conditions(frame, stretch_multiple):
	frame=frame.copy()
	if frame.empty: return frame
	ema=frame.close.ewm(span=9,adjust=False,min_periods=9).mean()
	frame['dema9']=2*ema-ema.ewm(span=9,adjust=False,min_periods=9).mean()
	last_upper_gap=last_lower_gap=lower_dema=upper_dema=peak=trough=None
	width_valid=False
	previous=None
	output=[]
	for row in frame.itertuples():
		upper,lower,atr,slow=row.bollinger_upper,row.bollinger_lower,row.atr,row.slow_moving_average
		gap=upper-lower
		ready=all(finite(v) for v in (upper,lower,atr,slow)) and gap>0 and atr>0
		near_upper=ready and row.close>upper-.2*atr
		near_lower=ready and row.close<lower+.2*atr
		if near_upper:
			width_valid=last_lower_gap is not None and .75<=gap/last_lower_gap<=1/.75
			last_upper_gap=gap
			upper_dema=row.dema9 if finite(row.dema9) else None
			trough=None
		if near_lower:
			width_valid=last_upper_gap is not None and .75<=gap/last_upper_gap<=1/.75
			last_lower_gap=gap
			lower_dema=row.dema9 if finite(row.dema9) else None
			peak=None
		cling=False
		if ready and previous is not None:
			cling=(row.close<row.open and previous.close<previous.open and
				(row.low<lower or previous.low<previous.bollinger_lower)) or (
				row.close>row.open and previous.close>previous.open and
				(row.high>upper or previous.high>previous.bollinger_upper))
		ranging=ready and .4<=atr/gap<=.6 and width_valid and not cling
		bullish=ready and slow<upper and row.close>=(upper+lower)/2+stretch_multiple*atr
		bearish=ready and slow>lower and row.close<=(upper+lower)/2-stretch_multiple*atr
		if bullish: peak=row.high
		if bearish: trough=row.low
		output.append(dict(ranging_market=bool(ranging),extreme_bullish_price_stretch=bool(bullish),
			extreme_bearish_price_stretch=bool(bearish),obvious_peak=peak,obvious_trough=trough,
			dema9_lower_touch_slope=row.dema9-lower_dema if lower_dema is not None else None,
			dema9_upper_touch_slope=row.dema9-upper_dema if upper_dema is not None else None))
		previous=row
	for name in MARKET_FIELDS[1:]:
		values=[r[name] for r in output]
		frame[name]=pd.Series(values,index=frame.index,dtype=bool if name in MARKET_FIELDS[1:4] else float)
	return frame

def entry_filter_reason(side, price, snapshot, stretch_multiple):
	upper,lower,atr,slow=(snapshot.get(k) for k in ('bollinger_upper','bollinger_lower','atr','slow_moving_average'))
	if not all(finite(v) for v in (upper,lower,atr,slow)) or atr<=0 or upper<=lower: return None
	if snapshot.get('ranging_market',False):
		if side=='long' and price>upper-.5*atr: return 'long_near_upper_band_in_range'
		if side=='short' and price<lower+.5*atr: return 'short_near_lower_band_in_range'
	mid=(upper+lower)/2
	if side=='long':
		if slow<upper and price>=mid+stretch_multiple*atr: return 'extreme_bullish_price_stretch'
		peak,slope=snapshot.get('obvious_peak'),snapshot.get('dema9_lower_touch_slope')
		if finite(peak) and finite(slope) and price<peak and slope>0: return 'after_obvious_peak_with_positive_dema9_slope'
	else:
		if slow>lower and price<=mid-stretch_multiple*atr: return 'extreme_bearish_price_stretch'
		trough,slope=snapshot.get('obvious_trough'),snapshot.get('dema9_upper_touch_slope')
		if finite(trough) and finite(slope) and price>trough and slope<0: return 'after_obvious_trough_with_negative_dema9_slope'
	return None

def confirmation_candle_exit(candles, events, side, opened_at):
	"""Events must have been received in either candle, at or before pair close.

	Intervals are (start, completion], assigning boundary arrivals to the closing
	candle. No adjacency across a doji or an opposite-color candle is allowed.
	"""
	if len(candles)!=2: return None
	first,second=candles.iloc[0],candles.iloc[1]
	opened_at=pd.Timestamp(opened_at)
	if pd.Timestamp(first.completed_at)<=opened_at or pd.Timestamp(second.completed_at)<=opened_at: return None
	red=side=='long'
	if not all(row.close<row.open if red else row.close>row.open for row in (first,second)): return None
	signals={'sell','sell+','sell_plus'} if red else {'buy','buy+','buy_plus'}
	for event in events:
		if event.get('timeframe') not in {'1h','4h'} or event.get('signal') not in signals: continue
		received=pd.Timestamp(event['received_dt']); signal_time=pd.Timestamp(event['dt'])
		if received<=opened_at or signal_time<=opened_at or signal_time>received: continue
		if any(pd.Timestamp(start)<received<=pd.Timestamp(row.completed_at) for start,row in candles.iterrows()):
			return '4h_two_red_candles_with_sell_signal' if red else '4h_two_green_candles_with_buy_signal'
	return None

def draw_market_shading(ax, bar_times, indicator_rows, shade_ranging_area=True):
	"""Shade each evaluated candle separately, including isolated classifications."""
	from matplotlib.patches import Rectangle, Patch
	rows={pd.Timestamp(row['time']):row for row in indicator_rows}
	colors={'Range':'#EAF7A4','Bullish price stretch':'#DECDF4','Bearish price stretch':'#FFD7AD'}
	used=set()
	for x,when in enumerate(bar_times):
		row=rows.get(pd.Timestamp(when),{})
		upper,lower=row.get('bollinger_upper'),row.get('bollinger_lower')
		if not all(finite(v) for v in (upper,lower)): continue
		mid=(upper+lower)/2
		areas=[]
		if shade_ranging_area and row.get('ranging_market'): areas.append(('Range',lower,upper))
		if row.get('extreme_bullish_price_stretch'): areas.append(('Bullish price stretch',mid,max(upper,row.get('high',upper))))
		if row.get('extreme_bearish_price_stretch'): areas.append(('Bearish price stretch',min(lower,row.get('low',lower)),mid))
		for label,bottom,top in areas:
			ax.add_patch(Rectangle((x-.5,bottom),1,top-bottom,facecolor=colors[label],alpha=.3,edgecolor='none',zorder=.5))
			used.add(label)
	return [Patch(facecolor=color,alpha=.3,label=label) for label,color in colors.items() if label in used]
