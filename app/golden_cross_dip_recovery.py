"""Pure, causal decisions shared by live trading, simulation and indicator plots."""
from dataclasses import dataclass, asdict
import math
import pandas as pd
from market_filters import annotate_market_conditions, entry_filter_reason, MARKET_FIELDS

STRATEGY_NAME = 'golden_cross_dip_recovery_4h'

@dataclass(frozen=True)
class StrategySettings:
	slow_average_to_price_weight: float = 0.1
	cross_point_to_price_weight: float = 0.7
	max_exposure_multiple: float = 3.0
	invalidation_atr_multiple: float = 1.0
	fast_type: str = 'ema'
	fast_period: int = 9
	slow_type: str = 'sma'
	slow_period: int = 200
	atr_period: int = 14
	require_entry_confirmation: bool = False
	fast_average_exit_atr_multiple: float | None = None
	cross_point_to_current_moment_weight: float = 0.2
	cross_point_time_decay_candles: int = 20
	confirmatory_fast_average_exit_atr_multiple: float = 2.0

	bearish_exit_confirmation_window_candles: int = 0
	profit_protection_enabled: bool = False
	profit_activation_atr_multiple: float = 3.0
	profit_trailing_atr_multiple: float = 3.0

	bullish_exit_above_fast_average_atr_multiple: float = 1.0

	strategy_mode: str = "golden_cross"
	bearish_exit_below_fast_average_atr_multiple: float = 1.0
	bollinger_period: int = 20
	bollinger_std_dev: float = 2.0

	cross_strategy_enabled: bool = True
	range_strategy_enabled: bool = True
	disabled_strategy_virtual_positions_enabled: bool = True
	range_sizing_low_atr_price_ratio: float = 0.01
	range_sizing_high_atr_price_ratio: float = 0.05
	range_sizing_min_entry_fraction: float = 0.25
	range_sizing_max_entry_fraction: float = 1.0
	range_max_atr_to_bollinger_gap_ratio: float = 0.5
	range_bollinger_gap_similarity_ratio: float = 0.6
	extreme_price_stretch_min_bollinger_gap_atr_multiple: float = 3.0
	long_range_entry_filter_enabled: bool = True
	short_range_entry_filter_enabled: bool = True
	long_extreme_stretch_entry_filter_enabled: bool = True
	short_extreme_stretch_entry_filter_enabled: bool = True
	long_post_peak_entry_filter_enabled: bool = True
	short_post_trough_entry_filter_enabled: bool = True
	shade_ranging_area: bool = True
	extreme_price_stretch_atr_multiple: float = 2.0
	entry_trigger: str = 'buy_sell_signal'
	exit_on_confirmatory_candlestick: bool = False
	exit_after_turning_point: bool = False
	cross_exit_method: str = 'price'
	fast_moving_average_reversed: bool = True
	fast_moving_average_reversal_timeframe: str = '4h'

	def __post_init__(self):
		if not all(math.isfinite(v) and v>0 for v in (self.range_sizing_low_atr_price_ratio,self.range_sizing_high_atr_price_ratio)) or self.range_sizing_low_atr_price_ratio>=self.range_sizing_high_atr_price_ratio:
			raise ValueError('Range sizing ATR/price ratios must be positive, finite and low < high')
		if not all(math.isfinite(v) for v in (self.range_sizing_min_entry_fraction,self.range_sizing_max_entry_fraction)) or not 0<self.range_sizing_min_entry_fraction<=self.range_sizing_max_entry_fraction<=1:
			raise ValueError('Range sizing fractions must satisfy 0 < minimum <= maximum <= 1')
		if not math.isfinite(self.range_max_atr_to_bollinger_gap_ratio) or self.range_max_atr_to_bollinger_gap_ratio <= 0:
			raise ValueError("range_max_atr_to_bollinger_gap_ratio must be positive and finite")
		if not math.isfinite(self.range_bollinger_gap_similarity_ratio) or not 0 < self.range_bollinger_gap_similarity_ratio <= 1:
			raise ValueError('range_bollinger_gap_similarity_ratio must be finite and in (0, 1]')
		if not math.isfinite(self.extreme_price_stretch_min_bollinger_gap_atr_multiple) or self.extreme_price_stretch_min_bollinger_gap_atr_multiple <= 0:
			raise ValueError('extreme_price_stretch_min_bollinger_gap_atr_multiple must be positive and finite')
		if not math.isfinite(self.extreme_price_stretch_atr_multiple) or self.extreme_price_stretch_atr_multiple <= 0:
			raise ValueError('extreme_price_stretch_atr_multiple must be positive and finite')
		if self.entry_trigger not in {'buy_sell_signal','turning_point'}:
			raise ValueError('entry_trigger must be buy_sell_signal or turning_point')
		from cross_strategy_indicators import REVERSAL_TIMEFRAMES
		if self.cross_exit_method not in {'price', 'dema5', 'fast_moving_average'}:
			raise ValueError('cross_exit_method must be price, dema5 or fast_moving_average')
		if self.fast_moving_average_reversal_timeframe not in REVERSAL_TIMEFRAMES:
			raise ValueError('fast_moving_average_reversal_timeframe must be 1m, 5m, 15m, 1h or 4h')

		if self.strategy_mode not in {"golden_cross", "death_cross", "both"}:
			raise ValueError("strategy_mode must be golden_cross, death_cross or both")
		if not math.isfinite(self.bollinger_std_dev) or self.bollinger_std_dev <= 0:
			raise ValueError("bollinger_std_dev must be positive and finite")
		if not math.isfinite(self.bearish_exit_below_fast_average_atr_multiple) or self.bearish_exit_below_fast_average_atr_multiple < 0:
			raise ValueError("bearish_exit_below_fast_average_atr_multiple must be finite and nonnegative")
		if not math.isfinite(self.bullish_exit_above_fast_average_atr_multiple) or self.bullish_exit_above_fast_average_atr_multiple < 0:
			raise ValueError("bullish_exit_above_fast_average_atr_multiple must be finite and nonnegative")
		if isinstance(self.bearish_exit_confirmation_window_candles, bool) or not isinstance(self.bearish_exit_confirmation_window_candles, int) or self.bearish_exit_confirmation_window_candles < 0:
			raise ValueError('bearish_exit_confirmation_window_candles must be a nonnegative integer')
		for value in (self.profit_activation_atr_multiple, self.profit_trailing_atr_multiple):
			if not math.isfinite(value) or value <= 0:
				raise ValueError('Profit protection multiples must be positive and finite')
		if self.fast_average_exit_atr_multiple is not None and (not math.isfinite(self.fast_average_exit_atr_multiple) or self.fast_average_exit_atr_multiple < 0):
			raise ValueError("fast_average_exit_atr_multiple must be finite and nonnegative")
		for name in ('slow_average_to_price_weight', 'cross_point_to_price_weight', 'max_exposure_multiple', 'invalidation_atr_multiple', 'cross_point_to_current_moment_weight', 'confirmatory_fast_average_exit_atr_multiple'):
			value = getattr(self, name)
			if not math.isfinite(value) or value < 0:
				raise ValueError(f'{name} must be finite and nonnegative')
		if self.max_exposure_multiple <= 0 or not math.isclose(self.slow_average_to_price_weight + self.cross_point_to_price_weight + self.cross_point_to_current_moment_weight, 1.0, abs_tol=1e-9):
			raise ValueError('Weights must sum to 1; max_exposure_multiple must be positive')
		for name in ('fast_period', 'slow_period', 'atr_period', 'cross_point_time_decay_candles', 'bollinger_period'):
			value = getattr(self, name)
			if isinstance(value, bool) or not isinstance(value, int) or value < 1:
				raise ValueError(f'{name} must be a positive integer')
		if self.fast_type not in {'ema', 'sma'} or self.slow_type not in {'ema', 'sma'}:
			raise ValueError('Moving average types must be ema or sma')

	def to_dict(self):
		return asdict(self)


def build_indicator_frame(bars, settings, bar_duration=None):
	"""Native Alpaca bar timestamps are interval starts; values become usable at +4H."""
	frame = bars.copy()
	if frame.empty:
		return pd.DataFrame()
	if isinstance(frame.index, pd.MultiIndex):
		if 'timestamp' not in frame.index.names:
			raise ValueError('Expected a timestamp index for one symbol')
		frame.index = frame.index.get_level_values('timestamp')
	frame.index = pd.to_datetime(frame.index, utc=True)
	frame = frame[~frame.index.duplicated(keep='last')].sort_index()
	for col in ('open', 'high', 'low', 'close'):
		frame[col] = pd.to_numeric(frame[col], errors='raise')
		if not frame[col].map(lambda v: math.isfinite(v) and v > 0).all():
			raise ValueError('OHLC must be positive and finite')
	if ((frame.high < frame[['open','close','low']].max(axis=1)) | (frame.low > frame[['open','close','high']].min(axis=1))).any():
		raise ValueError('Inconsistent OHLC')
	def moving_average(kind, period):
		if kind == 'sma':
			return frame.close.rolling(period, min_periods=period).mean()
		return frame.close.ewm(span=period, adjust=False, min_periods=period).mean()
	frame['fast_moving_average'] = moving_average(settings.fast_type, settings.fast_period)
	frame['slow_moving_average'] = moving_average(settings.slow_type, settings.slow_period)
	previous_close = frame.close.shift(1)
	tr = pd.concat([frame.high-frame.low, (frame.high-previous_close).abs(), (frame.low-previous_close).abs()], axis=1).max(axis=1)
	# Seed Wilder ATR with the arithmetic mean, then apply Wilder's recursive smoothing.
	atr = pd.Series(float('nan'), index=frame.index)
	if len(frame) >= settings.atr_period:
		atr.iloc[settings.atr_period-1] = tr.iloc[:settings.atr_period].mean()
		for i in range(settings.atr_period, len(frame)):
			atr.iloc[i] = (atr.iloc[i-1]*(settings.atr_period-1)+tr.iloc[i])/settings.atr_period
	frame['atr'] = atr
	ema5 = frame.close.ewm(span=5, adjust=False, min_periods=5).mean()
	ema5_twice = ema5.ewm(span=5, adjust=False, min_periods=5).mean()
	frame['dema5'] = 2*ema5 - ema5_twice
	dema_diff = frame.dema5 - frame.slow_moving_average
	frame['dema5_golden_cross'] = (dema_diff.shift(1) <= 0) & (dema_diff > 0)
	frame['dema5_death_cross'] = (dema_diff.shift(1) >= 0) & (dema_diff < 0)
	diff = frame.fast_moving_average-frame.slow_moving_average
	minimum_prehistory = max(settings.slow_period, settings.fast_period)*5
	frame['history_ready'] = pd.Series(range(len(frame)), index=frame.index) >= minimum_prehistory
	cross = (diff.shift(1) <= 0) & (diff > 0) & frame.history_ready
	t = -diff.shift(1)/(diff-diff.shift(1))
	intersection = frame.slow_moving_average.shift(1)+t*(frame.slow_moving_average-frame.slow_moving_average.shift(1))
	frame['golden_cross_price'] = intersection.where(cross).ffill()
	bar_number = pd.Series(range(len(frame)), index=frame.index, dtype=float)
	frame['candles_since_golden_cross'] = bar_number - bar_number.where(cross).ffill()
	frame['death_cross'] = (diff.shift(1) >= 0) & (diff < 0)
	frame['golden_cross'] = (diff.shift(1) <= 0) & (diff > 0)
	death = frame.death_cross & frame.history_ready
	frame['death_cross_price'] = intersection.where(death).ffill()
	frame['candles_since_death_cross'] = bar_number - bar_number.where(death).ffill()
	band_mid = frame.close.rolling(settings.bollinger_period, min_periods=settings.bollinger_period).mean()
	band_std = frame.close.rolling(settings.bollinger_period, min_periods=settings.bollinger_period).std(ddof=0)
	frame['bollinger_upper'] = band_mid + settings.bollinger_std_dev * band_std
	frame['bollinger_lower'] = band_mid - settings.bollinger_std_dev * band_std
	frame['completed_at'] = frame.index+(bar_duration if bar_duration is not None else pd.Timedelta(hours=4))
	return annotate_market_conditions(attach_entry_cross_references(frame), settings.extreme_price_stretch_atr_multiple, settings.range_bollinger_gap_similarity_ratio, settings.extreme_price_stretch_min_bollinger_gap_atr_multiple, settings.range_max_atr_to_bollinger_gap_ratio)


def attach_entry_cross_references(frame):
	"""Latest completed band/fast crossing supplies entry distance and age only."""
	frame = frame.copy()
	number = pd.Series(range(len(frame)), index=frame.index, dtype=float)
	for direction,band,sign in [('long','bollinger_upper',1),('short','bollinger_lower',-1)]:
		fast = sign*(frame.fast_moving_average-frame.slow_moving_average)
		band_gap = sign*(frame[band]-frame.slow_moving_average)
		crossed = (((fast.shift(1)<=0)&(fast>0)) | ((band_gap.shift(1)<=0)&(band_gap>0))) & frame.history_ready
		frame[direction+'_entry_cross_price'] = frame.slow_moving_average.where(crossed).ffill()
		frame['candles_since_'+direction+'_entry_cross'] = number-number.where(crossed).ffill()
	return frame


def snapshot_at(frame, as_of):
	if frame.empty:
		return {}
	as_of = pd.Timestamp(as_of)
	if as_of.tzinfo is None:
		raise ValueError('as_of must have a timezone')
	eligible = frame.loc[frame.completed_at <= as_of]
	if eligible.empty:
		return {}
	row = eligible.iloc[-1].copy()
	row['previous_fast_moving_average'] = eligible.iloc[-2].fast_moving_average if len(eligible) > 1 else float('nan')
	extra = {name: (None if pd.isna(row.get(name)) else float(row[name])) for name in ('death_cross_price', 'candles_since_death_cross', 'bollinger_upper', 'bollinger_lower', 'dema5')}
	extra.update({name:(None if pd.isna(row[name]) else float(row[name])) for name in ('long_entry_cross_price','short_entry_cross_price','candles_since_long_entry_cross','candles_since_short_entry_cross') if name in row})
	extra['dema5_golden_cross'] = bool(row.get('dema5_golden_cross', False))
	extra['dema5_death_cross'] = bool(row.get('dema5_death_cross', False))
	for name in MARKET_FIELDS:
		if name in row: extra[name] = None if pd.isna(row[name]) else bool(row[name]) if name in MARKET_FIELDS[1:4] else float(row[name])
	extra['golden_cross'] = bool(row.get('golden_cross', False))
	return extra | {name: (None if pd.isna(row[name]) else float(row[name])) for name in ('close', 'fast_moving_average', 'previous_fast_moving_average', 'slow_moving_average', 'atr', 'golden_cross_price')} | {'completed_at': row.completed_at.isoformat(), 'death_cross': bool(row.get('death_cross',False)), 'completed_bar_number': len(eligible)-1, 'history_ready': bool(row.history_ready), 'candles_since_golden_cross': (None if pd.isna(row.get('candles_since_golden_cross')) else float(row['candles_since_golden_cross']))}


def evaluate_entry(price, base_amount, snapshot, current_exposure, pending_buy_exposure, settings):
	result = {'eligible': False, 'reason': '', 'quantity': 0., 'clipped_amount': 0., 'desired_amount': 0., 'snapshot': snapshot}
	for name, value in (('price',price), ('base_amount',base_amount), ('current_exposure',current_exposure), ('pending_buy_exposure',pending_buy_exposure)):
		if not math.isfinite(value) or value < 0 or (name in {'price','base_amount'} and value <= 0):
			raise ValueError(f'Invalid {name}')
	if snapshot.get('history_ready') is False:
		result['reason'] = 'insufficient_indicator_prehistory'; return result
	values = [snapshot.get(k) for k in ('close','fast_moving_average','slow_moving_average','atr','golden_cross_price')]
	if any(v is None or not math.isfinite(v) or v <= 0 for v in values):
		result['reason'] = 'missing_cross_or_valid_completed_indicators'; return result
	close, fast, slow, atr, cross_price = values
	cross_price = snapshot.get('long_entry_cross_price',cross_price)
	if cross_price is None or not math.isfinite(cross_price) or cross_price <= 0:
		result['reason'] = 'missing_entry_cross_reference'; return result
	if fast <= slow:
		result['reason'] = 'fast_not_above_slow'; return result
	if close < slow-settings.invalidation_atr_multiple*atr:
		result['reason'] = 'close_below_slow_by_more_than_atr_threshold'; return result
	if settings.require_entry_confirmation:
		previous_fast = snapshot.get('previous_fast_moving_average')
		if previous_fast is None or not math.isfinite(previous_fast) or previous_fast <= 0:
			result['reason'] = 'missing_previous_completed_fast_average'; return result
		if close <= fast:
			result['reason'] = 'close_not_above_fast_average'; return result
		if fast <= previous_fast:
			result['reason'] = 'fast_average_not_rising'; return result
	filter_reason = entry_filter_reason('long',price,snapshot.get('_market_filter_context',snapshot),settings.extreme_price_stretch_atr_multiple,settings.extreme_price_stretch_min_bollinger_gap_atr_multiple,settings)
	if filter_reason:
		result['reason'] = filter_reason; return result
	f_slow = atr/(atr+abs(price-slow))
	f_cross = atr/(atr+abs(price-cross_price))
	if abs(price-cross_price) > atr:
		f_cross *= 0.4
	age = snapshot.get('candles_since_long_entry_cross',snapshot.get('candles_since_golden_cross'))
	if settings.cross_point_to_current_moment_weight > 0 and (age is None or not math.isfinite(age) or age < 0):
		result['reason'] = 'missing_golden_cross_age'; return result
	f_time = 1.0/(1.0+(age or 0)/settings.cross_point_time_decay_candles)
	desired = base_amount*(settings.slow_average_to_price_weight*f_slow+settings.cross_point_to_price_weight*f_cross + settings.cross_point_to_current_moment_weight*f_time)
	capacity = max(0., base_amount*settings.max_exposure_multiple-current_exposure-pending_buy_exposure)
	amount = min(desired, capacity)
	result.update(time_fraction=f_time, cross_point_to_current_moment_weight=settings.cross_point_to_current_moment_weight, slow_fraction=f_slow, cross_fraction=f_cross, slow_average_to_price_weight=settings.slow_average_to_price_weight, cross_point_to_price_weight=settings.cross_point_to_price_weight, capacity=capacity, desired_amount=desired, clipped_amount=amount, quantity=amount/price, eligible=amount>0, reason='eligible' if amount>0 else 'exposure_cap_reached')
	return result


def evaluate_fast_average_exit(price, snapshot, settings):
	"""Strict price breach against the latest completed 4H fast average and ATR."""
	n = settings.confirmatory_fast_average_exit_atr_multiple
	if n is None:
		return False
	fast, atr = snapshot.get('fast_moving_average'), snapshot.get('atr')
	if any(v is None or not math.isfinite(v) or v <= 0 for v in (price, fast, atr)):
		return False
	return price < fast - n * atr


def evaluate_signal_exit(event, snapshot, settings):
	if settings.cross_exit_method == 'fast_moving_average' and event.get('signal') == 'death_cross' and event.get('timeframe') == '4h' and snapshot.get('death_cross'):
		return '4h_death_cross'
	raw = event.get('raw_fields') or {}
	try:
		bullish = str(event.get('signal') or '').lower() == 'bullish_exit' or float(raw.get('bullish_exit') or 0) != 0
	except (TypeError, ValueError):
		bullish = str(event.get('signal') or '').lower() == 'bullish_exit'
	if not bullish: return None
	if event.get('timeframe') == '4h': return '4h_bullish_exit'
	if event.get('timeframe') != '1h': return None
	try:
		price = float(event.get('price') if event.get('price') is not None else raw.get('price', raw.get('close')))
		fast, atr = float(snapshot.get('fast_moving_average')), float(snapshot.get('atr'))
	except (TypeError, ValueError): return None
	if not all(math.isfinite(v) and v > 0 for v in (price,fast,atr)): return None
	return '1h_bullish_exit_above_fast_average_atr' if price > fast + settings.bullish_exit_above_fast_average_atr_multiple*atr else None


def evaluate_short_entry(price, base_amount, snapshot, current_exposure, pending_sell_exposure, settings):
	result = {'eligible': False, 'reason': '', 'quantity': 0., 'clipped_amount': 0., 'desired_amount': 0., 'snapshot': snapshot}
	for name, value in (('price',price), ('base_amount',base_amount), ('current_exposure',current_exposure), ('pending_sell_exposure',pending_sell_exposure)):
		if not math.isfinite(value) or value < 0 or (name in {'price','base_amount'} and value <= 0):
			raise ValueError(f'Invalid {name}')
	if snapshot.get('history_ready') is False:
		result['reason'] = 'insufficient_indicator_prehistory'; return result
	values = [snapshot.get(k) for k in ('close','fast_moving_average','slow_moving_average','atr','death_cross_price')]
	if any(v is None or not math.isfinite(v) or v <= 0 for v in values):
		result['reason'] = 'missing_cross_or_valid_completed_indicators'; return result
	close, fast, slow, atr, cross_price = values
	cross_price = snapshot.get('short_entry_cross_price',cross_price)
	if cross_price is None or not math.isfinite(cross_price) or cross_price <= 0:
		result['reason'] = 'missing_entry_cross_reference'; return result
	if fast >= slow:
		result['reason'] = 'fast_not_below_slow'; return result
	if close > slow+settings.invalidation_atr_multiple*atr:
		result['reason'] = 'close_above_slow_by_more_than_atr_threshold'; return result
	if settings.require_entry_confirmation:
		previous_fast = snapshot.get('previous_fast_moving_average')
		if previous_fast is None or not math.isfinite(previous_fast) or previous_fast <= 0:
			result['reason'] = 'missing_previous_completed_fast_average'; return result
		if close >= fast:
			result['reason'] = 'close_not_below_fast_average'; return result
		if fast >= previous_fast:
			result['reason'] = 'fast_average_not_falling'; return result
	filter_reason = entry_filter_reason('short',price,snapshot.get('_market_filter_context',snapshot),settings.extreme_price_stretch_atr_multiple,settings.extreme_price_stretch_min_bollinger_gap_atr_multiple,settings)
	if filter_reason:
		result['reason'] = filter_reason; return result
	f_slow = atr/(atr+abs(price-slow))
	f_cross = atr/(atr+abs(price-cross_price))
	if abs(price-cross_price) > atr:
		f_cross *= 0.4
	age = snapshot.get('candles_since_short_entry_cross',snapshot.get('candles_since_death_cross'))
	if settings.cross_point_to_current_moment_weight > 0 and (age is None or not math.isfinite(age) or age < 0):
		result['reason'] = 'missing_death_cross_age'; return result
	f_time = 1.0/(1.0+(age or 0)/settings.cross_point_time_decay_candles)
	desired = base_amount*(settings.slow_average_to_price_weight*f_slow+settings.cross_point_to_price_weight*f_cross + settings.cross_point_to_current_moment_weight*f_time)
	capacity = max(0., base_amount*settings.max_exposure_multiple-current_exposure-pending_sell_exposure)
	amount = min(desired, capacity)
	result.update(time_fraction=f_time, cross_point_to_current_moment_weight=settings.cross_point_to_current_moment_weight, slow_fraction=f_slow, cross_fraction=f_cross, slow_average_to_price_weight=settings.slow_average_to_price_weight, cross_point_to_price_weight=settings.cross_point_to_price_weight, capacity=capacity, desired_amount=desired, clipped_amount=amount, quantity=amount/price, eligible=amount>0, reason='eligible' if amount>0 else 'exposure_cap_reached')
	return result


def evaluate_short_signal_exit(event, snapshot, settings):
	if settings.cross_exit_method == 'fast_moving_average' and event.get('signal') == 'golden_cross' and event.get('timeframe') == '4h' and snapshot.get('golden_cross'):
		return '4h_golden_cross'
	raw = event.get('raw_fields') or {}
	try:
		bearish = str(event.get('signal') or '').lower() == 'bearish_exit' or float(raw.get('bearish_exit') or 0) != 0
	except (TypeError, ValueError):
		bearish = str(event.get('signal') or '').lower() == 'bearish_exit'
	if not bearish: return None
	if event.get('timeframe') == '4h': return '4h_bearish_exit'
	if event.get('timeframe') != '1h': return None
	try:
		price = float(event.get('price') if event.get('price') is not None else raw.get('price', raw.get('close')))
		fast, atr = float(snapshot.get('fast_moving_average')), float(snapshot.get('atr'))
	except (TypeError, ValueError): return None
	if not all(math.isfinite(v) and v > 0 for v in (price,fast,atr)): return None
	return '1h_bearish_exit_below_fast_average_atr' if price < fast - settings.bearish_exit_below_fast_average_atr_multiple*atr else None
