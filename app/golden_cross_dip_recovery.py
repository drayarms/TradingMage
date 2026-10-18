"""Pure, causal decisions shared by live trading, simulation and indicator plots."""
from dataclasses import dataclass, asdict
import math
import pandas as pd

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

	def __post_init__(self):
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
		for name in ('fast_period', 'slow_period', 'atr_period', 'cross_point_time_decay_candles'):
			value = getattr(self, name)
			if isinstance(value, bool) or not isinstance(value, int) or value < 1:
				raise ValueError(f'{name} must be a positive integer')
		if self.fast_type not in {'ema', 'sma'} or self.slow_type not in {'ema', 'sma'}:
			raise ValueError('Moving average types must be ema or sma')

	def to_dict(self):
		return asdict(self)


def build_indicator_frame(bars, settings):
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
	frame['completed_at'] = frame.index+pd.Timedelta(hours=4)
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
	return {name: (None if pd.isna(row[name]) else float(row[name])) for name in ('close', 'fast_moving_average', 'previous_fast_moving_average', 'slow_moving_average', 'atr', 'golden_cross_price')} | {'completed_at': row.completed_at.isoformat(), 'death_cross': bool(row.get('death_cross',False)), 'completed_bar_number': len(eligible)-1, 'history_ready': bool(row.history_ready), 'candles_since_golden_cross': (None if pd.isna(row.get('candles_since_golden_cross')) else float(row['candles_since_golden_cross']))}


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
	f_slow = atr/(atr+abs(price-slow))
	f_cross = atr/(atr+abs(price-cross_price))
	if abs(price-cross_price) > atr:
		f_cross *= 0.4
	age = snapshot.get('candles_since_golden_cross')
	if settings.cross_point_to_current_moment_weight > 0 and (age is None or not math.isfinite(age) or age < 0):
		result['reason'] = 'missing_golden_cross_age'; return result
	f_time = 1.0/(1.0+(age or 0)/settings.cross_point_time_decay_candles)
	desired = base_amount*(settings.slow_average_to_price_weight*f_slow+settings.cross_point_to_price_weight*f_cross + settings.cross_point_to_current_moment_weight*f_time)
	capacity = max(0., base_amount*settings.max_exposure_multiple-current_exposure-pending_buy_exposure)
	amount = min(desired, capacity)
	result.update(time_fraction=f_time, cross_point_to_current_moment_weight=settings.cross_point_to_current_moment_weight, slow_fraction=f_slow, cross_fraction=f_cross, slow_average_to_price_weight=settings.slow_average_to_price_weight, cross_point_to_price_weight=settings.cross_point_to_price_weight, capacity=capacity, desired_amount=desired, clipped_amount=amount, quantity=amount/price, eligible=amount>0, reason='eligible' if amount>0 else 'exposure_cap_reached')
	return result


def bearish_entry_placeholder(*args, **kwargs):
	return {'eligible': False, 'reason': 'bearish_entry_not_implemented'}


def bearish_exit_placeholder(*args, **kwargs):
	return {'eligible': False, 'reason': 'short_position_exit_not_implemented'}


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
	if event.get('signal') == 'death_cross' and event.get('timeframe') == '4h' and snapshot.get('death_cross'):
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
