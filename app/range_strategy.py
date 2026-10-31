"""Pure range decisions using current prices and completed 4h context."""
import math

def valid(row):
	values=[row.get(k) for k in ('bollinger_upper','bollinger_lower','atr')]
	return all(v is not None and math.isfinite(v) and v>0 for v in values) and values[0]>values[1]

def range_entry_side(price,row):
	if not valid(row) or not row.get('ranging_market',False): return None
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
