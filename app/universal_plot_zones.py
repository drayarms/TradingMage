"""Display-only completed 4h Bollinger curvature zones."""
import math
import pandas as pd

UNIVERSAL_FIELDS=('universal_peak','universal_trough')

def annotate_universal_zones(frame,gradient_candles=2,points=()):
	if type(gradient_candles) is not int or gradient_candles<1:raise ValueError('universal_zone_gradient_candles must be a positive integer')
	out=frame.copy();n=gradient_candles
	rows=list(frame.itertuples());events=[]
	for point in points:
		confirmed=pd.Timestamp(point['confirmed_at']);evaluation=pd.Timestamp(point.get('time',confirmed-pd.Timedelta(hours=4)))
		if evaluation not in frame.index:continue
		index=frame.index.get_loc(evaluation)
		if pd.Timestamp(rows[index].completed_at)<=confirmed:events.append((confirmed.value,index))
	events.sort();cursor=0;reference=None;changes=[]
	for row in rows:
		while cursor<len(events) and events[cursor][0]<=pd.Timestamp(row.completed_at).value:
			reference=events[cursor][1];cursor+=1
		fast=row.fast_moving_average;anchor=rows[reference].fast_moving_average if reference is not None else None
		valid_fast=all(v is not None and pd.notna(v) and math.isfinite(v) for v in (fast,anchor))
		changes.append(fast-anchor if valid_fast else float('nan'))
	fast_change=pd.Series(changes,index=frame.index,dtype=float)
	band_conditions=[]
	for band,sign in (('bollinger_lower',1),('bollinger_upper',-1)):
		values=frame[band];latest=values-values.shift(n);previous=values.shift(n)-values.shift(2*n)
		valid=values.map(lambda v:pd.notna(v) and math.isfinite(v))
		valid=valid & valid.shift(n,fill_value=False) & valid.shift(2*n,fill_value=False)
		band_conditions.append(valid & (sign*previous<0) & (sign*(latest-previous)>0))
	shared=band_conditions[0] & band_conditions[1]
	out['universal_peak']=(shared & (fast_change>0)).fillna(False).astype(bool)
	out['universal_trough']=(shared & (fast_change<0)).fillna(False).astype(bool)
	return out
