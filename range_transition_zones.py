"""Completed-4h band-ratio and acceleration zones."""
import math

def finite(value):
	return value is not None and math.isfinite(value)

def transition_zones(price,row,settings):
	# Price is retained in the shared call signature but no longer classifies zones.
	if not settings.range_transition_zones_enabled:return False,False
	if not row.get('ranging_market',False) or row.get('extreme_bullish_price_stretch',False) or row.get('extreme_bearish_price_stretch',False):return False,False
	upper=row.get('bollinger_upper_exit_gradient_since_gap_minimum');lower=row.get('bollinger_lower_exit_gradient_since_gap_minimum')
	if not all(finite(v) for v in (upper,lower)):return False,False
	u=row.get('bollinger_upper_recent_gradient');previous_u=row.get('bollinger_upper_previous_gradient')
	l=row.get('bollinger_lower_recent_gradient');previous_l=row.get('bollinger_lower_previous_gradient')
	n=settings.range_transition_band_speed_multiple
	bull=lower<0 and upper>=n*(-lower) and all(finite(v) for v in (u,previous_u)) and u>0 and u>previous_u
	bear=upper>0 and (-lower)>=n*upper and all(finite(v) for v in (l,previous_l)) and l<0 and abs(l)>abs(previous_l)
	return bool(bull),bool(bear)

def prohibited_entry(side,price,row,settings):
	bull,_=transition_zones(price,row,settings)
	if side=='short' and bull:return 'short_in_bullish_range_transition'
	return None

def zone_exit_reason(owner,side,price,row,settings):
	# Only bullish-transition short restrictions remain zone-triggered exits.
	return prohibited_entry(side,price,row,settings)


def draw_transition_shading(ax,bar_times,intervals,anchor_timeframe='4h'):
	"""Map causal minute intervals onto candle-width coordinates, between bands."""
	import pandas as pd
	from matplotlib.patches import Rectangle,Patch
	if not intervals or not len(bar_times):return []
	times=pd.DatetimeIndex(bar_times);duration=pd.Timedelta(anchor_timeframe);used=set()
	def coordinate(value):
		value=pd.Timestamp(value);index=int(times.searchsorted(value,side='right'))-1
		if index<0:return 0.
		return index+min(1.,max(0.,(value-times[index])/duration))
	for interval in intervals:
		start=pd.Timestamp(interval['start']);end=pd.Timestamp(interval['end'])
		if end<=times[0] or start>=times[-1]+duration:continue
		x=coordinate(start);right=coordinate(end)
		if right<=x:continue
		ax.add_patch(Rectangle((x,interval['lower']),right-x,interval['upper']-interval['lower'],facecolor='#FFFF00',alpha=.55,edgecolor='none',zorder=.7))
		if interval['bullish']:used.add('Range likely transitioning to a bullish stretch')
		if interval['bearish']:used.add('Range likely transitioning to a bearish stretch')
	return [Patch(facecolor='#FFFF00',alpha=.55,label=label) for label in sorted(used)]
