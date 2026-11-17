"""Public range/trend settings; legacy indicator/range helpers remain internal."""
import math
from dataclasses import fields
from golden_cross_dip_recovery import StrategySettings

STRATEGY_NAME='range_trend_4h'
CROSS_ONLY=set('slow_average_to_price_weight cross_point_to_price_weight cross_point_to_current_moment_weight cross_point_time_decay_candles invalidation_atr_multiple require_entry_confirmation fast_average_exit_atr_multiple confirmatory_fast_average_exit_atr_multiple bearish_exit_confirmation_window_candles profit_protection_enabled profit_activation_atr_multiple profit_trailing_atr_multiple bullish_exit_above_fast_average_atr_multiple bearish_exit_below_fast_average_atr_multiple cross_strategy_enabled exit_cross_on_opposing_heikin_ashi_run do_not_enter_cross_on_opposing_heikin_ashi_run do_not_enter_cross_after_only_opposing_heikin_ashi_since_last_entry entry_trigger cross_entry_bollinger_gap_atr_divisor enforce_atr_invalidation exit_on_confirmatory_candlestick exit_after_turning_point cross_exit_method'.split())
RANGE_FIELDS={f.name for f in fields(StrategySettings)}-CROSS_ONLY

class RangeTrendSettings:
	def __init__(self,trend_strategy_enabled=True,trend_exit_bollinger_atr_multiple=1.,trend_entry_opposing_universal_zone_cooldown_candles=2,trend_exit_bollinger_gap_contraction_divisor=2.,**kwargs):
		if type(trend_entry_opposing_universal_zone_cooldown_candles) is not int or trend_entry_opposing_universal_zone_cooldown_candles<0:raise ValueError('trend_entry_opposing_universal_zone_cooldown_candles must be a nonnegative integer')
		self.trend_entry_opposing_universal_zone_cooldown_candles=trend_entry_opposing_universal_zone_cooldown_candles
		if isinstance(trend_exit_bollinger_gap_contraction_divisor,bool) or not math.isfinite(trend_exit_bollinger_gap_contraction_divisor) or trend_exit_bollinger_gap_contraction_divisor<=0:raise ValueError('trend_exit_bollinger_gap_contraction_divisor must be positive and finite')
		self.trend_exit_bollinger_gap_contraction_divisor=trend_exit_bollinger_gap_contraction_divisor
		unknown=set(kwargs)-RANGE_FIELDS
		if unknown:raise ValueError('Unsupported range/trend arguments: '+', '.join(sorted(unknown)))
		if not isinstance(trend_strategy_enabled,bool):raise ValueError('trend_strategy_enabled must be boolean')
		if isinstance(trend_exit_bollinger_atr_multiple,bool) or not math.isfinite(trend_exit_bollinger_atr_multiple) or trend_exit_bollinger_atr_multiple<0:raise ValueError('trend_exit_bollinger_atr_multiple must be finite and nonnegative')
		for name in ('range_strategy_enabled','disabled_strategy_virtual_positions_enabled'):
			if name in kwargs and not isinstance(kwargs[name],bool):raise ValueError(name+' must be boolean')
		self.entry_strategy_precedence=kwargs.pop('entry_strategy_precedence','range')
		if self.entry_strategy_precedence not in {'range','trend','none'}:raise ValueError('entry_strategy_precedence must be range, trend or none')
		self.trend_strategy_enabled=trend_strategy_enabled;self.trend_exit_bollinger_atr_multiple=trend_exit_bollinger_atr_multiple
		kwargs.setdefault('strategy_mode','both')
		self._legacy=StrategySettings(cross_strategy_enabled=False,entry_strategy_precedence='range',**kwargs)
	def __getattr__(self,name):return getattr(self._legacy,name)
	def to_dict(self):return {**{k:getattr(self,k) for k in RANGE_FIELDS},'trend_exit_bollinger_gap_contraction_divisor':self.trend_exit_bollinger_gap_contraction_divisor,'trend_strategy_enabled':self.trend_strategy_enabled,'trend_exit_bollinger_atr_multiple':self.trend_exit_bollinger_atr_multiple,'trend_entry_opposing_universal_zone_cooldown_candles':self.trend_entry_opposing_universal_zone_cooldown_candles}
