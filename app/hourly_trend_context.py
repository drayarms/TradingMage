"""Native completed-hour trend context, independent of display selections."""
import pandas as pd
from chart_data import PlotOptions
from golden_cross_dip_recovery import build_indicator_frame
from cross_strategy_indicators import fast_moving_average_reversed
from trend_plot_zones import annotate_trend_zones


def build_hourly_trends(raw_frames,settings,options=None):
	options=options or PlotOptions()
	result={}
	for ticker,raw in raw_frames.items():
		frame=build_indicator_frame(raw.sort_index(),settings,pd.Timedelta(hours=1))
		points=fast_moving_average_reversed(frame,'1h')
		result[ticker]=annotate_trend_zones(frame,points,
			trend_cloud_condition_enabled=options.trend_cloud_condition_enabled,
			trend_heikin_ashi_streak_condition_enabled=options.trend_heikin_ashi_streak_condition_enabled,
			trend_cloud_fast_ma_band_gap_fraction=options.trend_cloud_fast_ma_band_gap_fraction,
			trend_heikin_ashi_streak_min_candles=options.trend_heikin_ashi_streak_min_candles,
			trend_heikin_ashi_streak_max_ma_gap_atr_multiple=options.trend_heikin_ashi_streak_max_ma_gap_atr_multiple)
	return result
