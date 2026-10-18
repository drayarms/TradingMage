#!/bin/sh
set -eu
./run_local_endpoint.sh plot \
	strategy_name=golden_cross_dip_recovery_4h \
	start=2026-06-01T04:00:00-04:00 \
	end=2026-07-25T20:00:00-04:00 \
	position_size=6600 \
	lux_algo_symbols=false \
	lux_algo_symbols1m=false \
	lux_algo_symbols5m=false \
	lux_algo_symbols15m=false \
	lux_algo_symbols1h=true \
	lux_algo_symbols4h=true \
	alpha_vantage_symbols=false \
	indicators=true \
	slow_average_to_price_weight=0.1 \
	cross_point_to_price_weight=0.7 \
	cross_point_to_current_moment_weight=0.2 \
	cross_point_time_decay_candles=20 \
	require_entry_confirmation=true \
	confirmatory_fast_average_exit_atr_multiple=2 \
	bearish_exit_confirmation_window_candles=0 \
	profit_protection_enabled=true \
	profit_activation_atr_multiple=3 \
	profit_trailing_atr_multiple=3 \
	"$@"
