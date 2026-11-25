"""Completed native 4h HA confirmation for range exits outside the range zone."""
import math
import pandas as pd
from chart_data import heikin_ashi


def annotate_outside_range_candles(frame):
	out=frame.copy();red=green=0;reds=[];greens=[];lows=[];highs=[]
	ha=heikin_ashi(frame) if all(k in frame for k in ('open','high','low','close')) else pd.DataFrame(index=frame.index,columns=['open','high','low','close'],dtype=float)
	for row in ha.itertuples():
		valid=all(math.isfinite(v) for v in (row.open,row.high,row.low,row.close))
		tol=max(abs(row.open),abs(row.close),1.)*1e-10 if valid else 0.
		red=red+1 if valid and row.close<row.open and abs(row.high-row.open)<=tol else 0
		green=green+1 if valid and row.close>row.open and abs(row.low-row.open)<=tol else 0
		reds.append(red);greens.append(green);lows.append(row.low if valid else float('nan'));highs.append(row.high if valid else float('nan'))
	out['outside_range_red_wickless_count']=reds;out['outside_range_green_wickless_count']=greens
	out['outside_range_ha_low']=lows;out['outside_range_ha_high']=highs
	return out


def outside_range_exit(side,row,candles):
	if row is None or 'ranging_market' not in row or pd.isna(row['ranging_market']) or row['ranging_market']:return False
	upper,lower=row.get('bollinger_upper'),row.get('bollinger_lower')
	if upper is None or lower is None or not math.isfinite(upper) or not math.isfinite(lower) or not upper>lower>0:return False
	count=row.get('outside_range_red_wickless_count' if side=='long' else 'outside_range_green_wickless_count',0)
	extreme=row.get('outside_range_ha_low' if side=='long' else 'outside_range_ha_high')
	return count>=candles and extreme is not None and math.isfinite(extreme) and (extreme<lower if side=='long' else extreme>upper)
