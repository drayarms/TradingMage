"""Pure cross-only eligibility/sizing and causal 4h turning-point context."""
import math
import pandas as pd

GRADIENT = 'fast_moving_average_gradient_since_turning_point'


def finite(value):
	return value is not None and math.isfinite(value)


TURNING_INDICATORS = ('fast_moving_average','bollinger_upper','bollinger_lower')
ENTRY_CONTEXT_FIELDS = ('cross_last_stretch','cross_body_upper_progress','cross_body_lower_progress')


def attach_cross_turning_context(frame, points):
	frame = frame.copy()
	if frame.empty: return frame
	events = []
	for point in points:
		when = pd.Timestamp(point['confirmed_at'])
		evaluation = pd.Timestamp(point.get('time', when-pd.Timedelta(hours=4)))
		if evaluation in frame.index:
			events.append((when.value, frame.loc[evaluation]))
	events.sort(key=lambda event:event[0])
	cursor = 0; reference = None
	gradients={name:[] for name in TURNING_INDICATORS}
	references=[]
	for row in frame.itertuples():
		while cursor < len(events) and events[cursor][0] <= pd.Timestamp(row.completed_at).value:
			reference = events[cursor][1]; cursor += 1
		for name in TURNING_INDICATORS:
			value=getattr(row,name,None); previous=reference.get(name) if reference is not None else None
			gradients[name].append(float(value)-float(previous) if finite(value) and finite(previous) else None)
		references.append(reference.get('fast_moving_average') if reference is not None else None)
	for name in TURNING_INDICATORS:
		frame[name+'_gradient_since_turning_point']=pd.Series(gradients[name],index=frame.index,dtype=float)
	frame['fast_moving_average_at_last_turning_point']=pd.Series(references,index=frame.index,dtype=float)
	return attach_exit_gap_gradients(frame,points)


def attach_exit_gap_gradients(frame,points):
	"""Use the most recent running minimum width since the confirmed turn.

	Keep this reference separate from the turning-point fast-MA entry gradient.
	On confirmation, include completed candles from evaluation through now.
	"""
	rows=list(frame.itertuples()); events=[]
	for point in points:
		when=pd.Timestamp(point['confirmed_at'])
		evaluation=pd.Timestamp(point.get('time',when-pd.Timedelta(hours=4)))
		if evaluation not in frame.index: continue
		index=frame.index.get_loc(evaluation)
		if pd.Timestamp(rows[index].completed_at)<=when: events.append((when.value,index))
	events.sort(key=lambda event:event[0])
	cursor=0; minimum=None; reference=None; armed=False
	gradients={name:[] for name in TURNING_INDICATORS}
	for index,row in enumerate(rows):
		reset=None
		while cursor<len(events) and events[cursor][0]<=pd.Timestamp(row.completed_at).value:
			reset=events[cursor][1];cursor+=1
		if reset is not None:
			armed=True;minimum=None;reference=None
		candidates=rows[reset:index+1] if reset is not None else [row] if armed else []
		for candidate in candidates:
			upper=getattr(candidate,'bollinger_upper',None);lower=getattr(candidate,'bollinger_lower',None)
			if finite(upper) and finite(lower) and upper>lower:
				gap=upper-lower
				if minimum is None or gap<=minimum:
					minimum=gap;reference=candidate
		for name in TURNING_INDICATORS:
			value=getattr(row,name,None);previous=getattr(reference,name,None)
			gradients[name].append(value-previous if finite(value) and finite(previous) else None)
	for name in TURNING_INDICATORS:
		frame[name+'_exit_gradient_since_gap_minimum']=pd.Series(gradients[name],index=frame.index,dtype=float)
	return frame


def attach_cross_entry_context(frame):
	"""Remember completed zones; compare completed candle bodies causally."""
	frame=frame.copy()
	if frame.empty: return frame
	last=None; zones=[]
	for row in frame.itertuples():
		if getattr(row,'ranging_market',False): last=None
		elif getattr(row,'extreme_bullish_price_stretch',False): last='bullish'
		elif getattr(row,'extreme_bearish_price_stretch',False): last='bearish'
		zones.append(last)
	frame['cross_last_stretch']=pd.Series(zones,index=frame.index,dtype=object)
	if 'open' in frame and 'close' in frame:
		upper=frame[['open','close']].max(axis=1,skipna=False)
		lower=frame[['open','close']].min(axis=1,skipna=False)
		ready=upper.notna() & upper.shift(1).notna() & upper.shift(2).notna() & lower.notna() & lower.shift(1).notna() & lower.shift(2).notna()
		frame['cross_body_upper_progress']=ready & ((upper>upper.shift(1)) | (upper>upper.shift(2)))
		frame['cross_body_lower_progress']=ready & ((lower<lower.shift(1)) | (lower<lower.shift(2)))
	else:
		frame['cross_body_upper_progress']=False
		frame['cross_body_lower_progress']=False
	return frame


def exit_gradient_blocked(side, context):
	values=[context.get(name+'_exit_gradient_since_gap_minimum') for name in TURNING_INDICATORS]
	return all(finite(value) and (value>0 if side=='long' else value<0) for value in values)


def cross_zone_reason(price, context, settings):
	upper, lower, atr, slow = (context.get(k) for k in ('bollinger_upper','bollinger_lower','atr','slow_moving_average'))
	if not all(finite(v) for v in (upper,lower,atr,slow)) or atr<=0 or upper<=lower:
		return 'missing_completed_market_context'
	if context.get('ranging_market', False):
		return 'cross_entry_in_ranging_market'
	if upper-lower >= settings.extreme_price_stretch_min_bollinger_gap_atr_multiple*atr:
		mid = (upper+lower)/2
		if context.get('lower_bollinger_cusp_confirmed',False) and slow<upper and price>=mid+settings.extreme_price_stretch_atr_multiple*atr:
			return 'extreme_bullish_price_stretch'
		if context.get('upper_bollinger_cusp_confirmed',False) and slow>lower and price<=mid-settings.extreme_price_stretch_atr_multiple*atr:
			return 'extreme_bearish_price_stretch'
	return None


def evaluate_cross_entry(side, price, base_amount, snapshot, current_exposure, pending_exposure, settings):
	result = dict(eligible=False, reason='', quantity=0., clipped_amount=0., desired_amount=0., snapshot=snapshot)
	def reject(reason):
		result['reason'] = reason
		return result
	for name, value in [('price',price),('base_amount',base_amount),('current_exposure',current_exposure),('pending_exposure',pending_exposure)]:
		if not finite(value) or value<0 or (name in {'price','base_amount'} and value<=0):
			raise ValueError('Invalid '+name)
	if snapshot.get('history_ready') is False:
		return reject('insufficient_indicator_prehistory')
	close, fast, slow, atr = (snapshot.get(k) for k in ('close','fast_moving_average','slow_moving_average','atr'))
	if not all(finite(v) and v>0 for v in (close,fast,slow,atr)):
		return reject('missing_cross_or_valid_completed_indicators')
	zone = cross_zone_reason(price, snapshot.get('_market_filter_context',snapshot), settings)
	if zone: return reject(zone)
	context=snapshot.get('_market_filter_context',snapshot)
	upper,lower=context['bollinger_upper'],context['bollinger_lower']
	if context['atr'] >= (upper-lower)/settings.cross_entry_bollinger_gap_atr_divisor:
		return reject('cross_atr_not_below_bollinger_gap_threshold')
	zone=context.get('cross_last_stretch')
	if side=='long' and zone=='bullish' and not (finite(context.get('high')) and context['high']>upper):
		return reject('cross_after_bullish_stretch_without_band_hugging')
	if side=='short' and zone=='bearish' and not (finite(context.get('low')) and context['low']<lower):
		return reject('cross_after_bearish_stretch_without_band_hugging')
	if not context.get('cross_body_upper_progress' if side=='long' else 'cross_body_lower_progress',False):
		return reject('cross_candle_body_not_progressing')
	gradient = snapshot.get(GRADIENT)
	result.update(fast_moving_average_gradient_since_turning_point=gradient)
	if fast==slow: return reject('fast_equals_slow')
	normal = fast>slow if side=='long' else fast<slow
	if not normal and not (finite(gradient) and (gradient>0 if side=='long' else gradient<0)):
		return reject('countertrend_gradient_not_positive' if side=='long' else 'countertrend_gradient_not_negative')
	result['sizing_branch'] = 'normal' if normal else 'countertrend'
	if settings.require_entry_confirmation and (price<=fast if side=='long' else price>=fast):
		return reject('price_not_above_fast_average' if side=='long' else 'price_not_below_fast_average')
	if settings.enforce_atr_invalidation and (close<slow-settings.invalidation_atr_multiple*atr if side=='long' else close>slow+settings.invalidation_atr_multiple*atr):
		return reject('close_below_slow_by_more_than_atr_threshold' if side=='long' else 'close_above_slow_by_more_than_atr_threshold')
	direction = 'long' if side=='long' else 'short'
	legacy = 'golden_cross' if side=='long' else 'death_cross'
	cross = snapshot.get(direction+'_entry_cross_price', snapshot.get(legacy+'_price'))
	if not finite(cross) or cross<=0: return reject('missing_entry_cross_reference')
	age = snapshot.get('candles_since_'+direction+'_entry_cross',snapshot.get('candles_since_'+legacy))
	if settings.cross_point_to_current_moment_weight>0 and (not finite(age) or age<0):
		return reject('missing_'+legacy+'_age')
	age = age if finite(age) and age>=0 else 0.
	distance = abs(price-cross)
	f_slow = atr/(atr+abs(price-slow))
	f_cross = atr/(atr+distance) if normal else distance/(atr+distance)
	if normal and distance>atr: f_cross *= .4
	time_denominator = settings.cross_point_time_decay_candles+age
	f_time = settings.cross_point_time_decay_candles/time_denominator if normal else age/time_denominator
	desired = base_amount*(settings.slow_average_to_price_weight*f_slow+settings.cross_point_to_price_weight*f_cross+settings.cross_point_to_current_moment_weight*f_time)
	capacity = max(0.,base_amount*settings.max_exposure_multiple-current_exposure-pending_exposure)
	amount = min(desired,capacity)
	result.update(cross_reference_price=cross, cross_age_candles=age, time_fraction=f_time,
		cross_point_to_current_moment_weight=settings.cross_point_to_current_moment_weight,
		slow_fraction=f_slow,cross_fraction=f_cross,slow_average_to_price_weight=settings.slow_average_to_price_weight,
		cross_point_to_price_weight=settings.cross_point_to_price_weight,capacity=capacity,
		desired_amount=desired,clipped_amount=amount,quantity=amount/price,eligible=amount>0,
		reason='eligible' if amount>0 else 'exposure_cap_reached')
	return result
