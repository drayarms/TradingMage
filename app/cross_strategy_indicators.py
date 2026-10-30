"""Causal crossover exits and alternating fast-average reversal markers."""
import math
from bisect import bisect_left, bisect_right
import pandas as pd

REVERSAL_TIMEFRAMES = {'1m': 1, '5m': 5, '15m': 15, '1h': 60, '4h': 240}


def evaluate_cross_exit(side, price, snapshot, settings):
	"""Price breaches use a completed minute; MA crossings use completed 4H bars."""
	if side not in {'long', 'short'}: return None
	method = settings.cross_exit_method
	if method == 'price':
		slow = snapshot.get('slow_moving_average')
		if any(v is None or not math.isfinite(v) or v <= 0 for v in (price, slow)): return None
		if side == 'short' and price > slow: return '1m_price_above_4h_slow_average'
		if side == 'long' and price < slow: return '1m_price_below_4h_slow_average'
		return None
	crossing = 'golden_cross' if side == 'short' else 'death_cross'
	key = 'dema5_' + crossing if method == 'dema5' else crossing
	return '4h_' + key if snapshot.get(key) else None


def fast_moving_average_reversed(frame, timeframe='4h'):
	"""Detect alternating turning points at their evaluation candles.

	Golden crosses start a maximum search; death crosses start a minimum search.
	Every reversal becomes the inclusive tether for the opposite type. A newer
	cross resets both the tether and first allowed type. confirmed_at records
	the completed candle at which each observation becomes available.
	"""
	if frame.empty: return []
	values = frame.fast_moving_average.to_numpy(dtype=float)
	death = frame.get('death_cross', pd.Series(False, index=frame.index)).to_numpy(dtype=bool)
	gold = frame.get('golden_cross', pd.Series(False, index=frame.index)).to_numpy(dtype=bool)
	# Each adjacent pair represents an open interval of MA levels crossed.
	# A range-update/point-query tree finds the latest qualifying pair in
	# logarithmic time, exactly matching a backward scan even on 1m histories.
	levels=sorted({float(v) for v in values if math.isfinite(v)})
	if not levels: return []
	tree={}
	def update(node,left,right,lo,hi,index):
		if lo>right or hi<left: return
		if lo<=left and right<=hi:
			tree[node]=index;return
		mid=(left+right)//2
		update(node*2,left,mid,lo,hi,index)
		update(node*2+1,mid+1,right,lo,hi,index)
	def query(point):
		node=1;left=0;right=len(levels)-1;found=-1
		while True:
			found=max(found,tree.get(node,-1))
			if left==right: return found
			mid=(left+right)//2
			if point<=mid: node*=2;right=mid
			else: node=node*2+1;left=mid+1
	cross_type = None; cross_index = None; tether_index = None
	tether_type = None; allowed_type = None; markers = []
	for i, current in enumerate(values):
		if death[i] or gold[i]:
			cross_type = 'death_cross' if death[i] else 'golden_cross'
			allowed_type = 'minimum' if death[i] else 'maximum'
			cross_index = tether_index = i
			tether_type = cross_type
			tree.clear()
			continue
		if not math.isfinite(current): tree.clear();continue
		if tether_index is None: continue
		# Include the tether candle as a candidate stopping sample. Intervals
		# from earlier cycles are discarded when either tether event occurs.
		if i-1>=tether_index and math.isfinite(values[i-1]):
			older=values[i-1]
			if (allowed_type == 'minimum' and older>current) or (allowed_type == 'maximum' and older<current):
				lo=bisect_right(levels,min(older,current));hi=bisect_left(levels,max(older,current))-1
				if lo<=hi: update(1,0,len(levels)-1,lo,hi,i-1)
		j=query(bisect_left(levels,current))
		if j<0: continue
		markers.append({
			'type':allowed_type+'_fast_moving_average_reversal',
			'turning_point_type':allowed_type,
			'time':frame.index[i].isoformat(), 'value':float(current),
			'confirmed_at':frame.iloc[i].completed_at.isoformat(),
			'current_candle_time':frame.index[i].isoformat(),
			'search_stop_time':frame.index[j].isoformat(),
			'tether_time':frame.index[tether_index].isoformat(), 'tether_type':tether_type,
			'cross_time':frame.index[cross_index].isoformat(), 'cross_type':cross_type,
			'timeframe':timeframe,
		})
		tether_index=i;tether_type=allowed_type
		allowed_type='maximum' if allowed_type == 'minimum' else 'minimum'
		tree.clear()
	return markers


def latest_completed_minute(frame, as_of, maximum_age_minutes=2):
	if frame.empty: return None
	frame=frame.copy()
	if isinstance(frame.index,pd.MultiIndex): frame.index=frame.index.get_level_values('timestamp')
	frame.index=pd.to_datetime(frame.index,utc=True)
	as_of=pd.Timestamp(as_of)
	eligible=frame.loc[frame.index+pd.Timedelta(minutes=1)<=as_of].sort_index()
	if eligible.empty: return None
	completed_at=eligible.index[-1]+pd.Timedelta(minutes=1)
	price=float(eligible.iloc[-1].close)
	if as_of-completed_at>pd.Timedelta(minutes=maximum_age_minutes) or not math.isfinite(price) or price<=0: return None
	return {'price':price, 'completed_at':completed_at}


def turning_point_exit_reasons(events, markers):
	"""Return additional exit sides in input order, using only confirmed observations.

	Opposite exit signals invalidate the armed point even without an open position.
	Equal confirmation timestamps qualify; invalidating flags take priority.
	"""
	result = [None] * len(events)
	for ticker in {e['ticker'] for e in events}:
		points = sorted(markers.get(ticker, []), key=lambda m: pd.Timestamp(m['confirmed_at']))
		cursor = 0; armed = None; armed_at = None
		ordered = sorted(((i,e) for i,e in enumerate(events) if e['ticker']==ticker), key=lambda pair: pd.Timestamp(pair[1]['received_dt']))
		for i,event in ordered:
			when = pd.Timestamp(event['received_dt'])
			while cursor < len(points) and pd.Timestamp(points[cursor]['confirmed_at']) <= when:
				point = points[cursor]; cursor += 1
				armed = point['turning_point_type']; armed_at = pd.Timestamp(point['confirmed_at'])
			if event.get('timeframe') not in {'1h','4h'}: continue
			raw = event.get('raw_fields') or {}
			def flag(name):
				try: return event.get('signal') == name or float(raw.get(name) or 0) != 0
				except (ValueError,TypeError): return event.get('signal') == name
			bullish,bearish = flag('bullish_exit'),flag('bearish_exit')
			if (armed == 'maximum' and bullish) or (armed == 'minimum' and bearish):
				armed = None
			elif armed_at is not None and min(when,pd.Timestamp(event['dt'])) >= armed_at:
				if armed == 'maximum' and bearish: result[i] = 'long'
				if armed == 'minimum' and bullish: result[i] = 'short'
	return result


def confirmatory_candlestick_exit(state, event, side, opened_at):
	"""Pair post-entry 1H/4H directional signals, clearing on opposite signals."""
	if min(pd.Timestamp(event['received_dt']),pd.Timestamp(event['dt'])) <= pd.Timestamp(opened_at): return None
	tf = event.get('timeframe')
	if tf not in {'1h','4h'}: return None
	signal = str(event.get('signal') or '').lower()
	direction = 'buy' if signal in {'buy','buy+','buy_plus'} else 'sell' if signal in {'sell','sell+','sell_plus'} else None
	if direction is None: return None
	expected = 'sell' if side == 'long' else 'buy'
	if direction != expected:
		state.clear(); return None
	other = '4h' if tf == '1h' else '1h'
	matched = other in state
	state[tf] = pd.Timestamp(event['received_dt']).isoformat()
	return 'confirmatory_candlestick_exit' if matched else None
