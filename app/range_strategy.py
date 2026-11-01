"""Pure range decisions using current prices and completed 4h context."""
import math
import pandas as pd

GAP_REFERENCE='range_widest_bollinger_gap_since_turning_point'

def attach_turning_point_gap_reference(frame,points):
	"""Reset at each confirmed 4h turning point, including its evaluation candle."""
	frame=frame.copy()
	confirmed=sorted({pd.Timestamp(p['confirmed_at']).value for p in points})
	cursor=0;armed=False;widest=float('nan');values=[]
	for row in frame.itertuples():
		reset=False
		while cursor<len(confirmed) and confirmed[cursor]<=pd.Timestamp(row.completed_at).value:
			armed=True;reset=True;cursor+=1
		if reset: widest=float('nan')
		gap=getattr(row,'bollinger_upper',float('nan'))-getattr(row,'bollinger_lower',float('nan'))
		if armed and math.isfinite(gap) and gap>0: widest=max(widest,gap) if math.isfinite(widest) else gap
		values.append(widest)
	frame[GAP_REFERENCE]=pd.Series(values,index=frame.index,dtype=float)
	return frame

def valid(row):
	values=[row.get(k) for k in ('bollinger_upper','bollinger_lower','atr')]
	return all(v is not None and math.isfinite(v) and v>0 for v in values) and values[0]>values[1]

def range_entry_side(price,row,min_gap_ratio=.7):
	if not valid(row) or not row.get('ranging_market',False): return None
	peak=row.get(GAP_REFERENCE)
	if peak is None or not math.isfinite(peak) or peak<=0: return None
	if row['bollinger_upper']-row['bollinger_lower']<min_gap_ratio*peak: return None
	if price<row['bollinger_lower']+.2*row['atr']: return 'long'
	if price>row['bollinger_upper']-.2*row['atr']: return 'short'
	return None

def range_exit_reason(side,price,rows,index):
	row=rows[index]
	if not valid(row): return None
	band='bollinger_upper' if side=='long' else 'bollinger_lower'
	def near(value):
		return price>value-.2*row['atr'] if side=='long' else price<value+.2*row['atr']
	label='upper' if side=='long' else 'lower'
	if near(row[band]): return 'range_'+label+'_band'
	if index>=2:
		previous=rows[index-2].get(band)
		if previous is not None and math.isfinite(previous):
			projected=row[band]+(row[band]-previous)/2
			if projected>0 and near(projected): return 'range_projected_'+label+'_band'
	return None

def range_entry_fraction(price,atr,settings):
	if not all(math.isfinite(v) and v>0 for v in (price,atr)): raise ValueError('Range price and ATR must be positive and finite')
	ratio=atr/price
	weight=max(0.,min(1.,(ratio-settings.range_sizing_low_atr_price_ratio)/(settings.range_sizing_high_atr_price_ratio-settings.range_sizing_low_atr_price_ratio)))
	return settings.range_sizing_max_entry_fraction+weight*(settings.range_sizing_min_entry_fraction-settings.range_sizing_max_entry_fraction)
