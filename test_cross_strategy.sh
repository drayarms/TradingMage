#!/usr/bin/env bash
set -euo pipefail
mode="${1:-death_cross}"
start="${2:?Usage: bash test_cross_strategy.sh MODE START END [BASE_URL]}"
end="${3:?Usage: bash test_cross_strategy.sh MODE START END [BASE_URL]}"
base_url="${4:-http://localhost:8000}"
case "$mode" in golden_cross|death_cross|both) ;; *) echo 'MODE must be golden_cross, death_cross or both' >&2; exit 2 ;; esac
params=(
  --data-urlencode 'strategy_name=golden_cross_dip_recovery_4h'
  --data-urlencode "strategy_mode=$mode"
  --data-urlencode "start=$start"
  --data-urlencode "end=$end"
  --data-urlencode 'position_size=6600'
  --data-urlencode 'slow_average_to_price_weight=0.1'
  --data-urlencode 'cross_point_to_price_weight=0.7'
  --data-urlencode 'cross_point_to_current_moment_weight=0.2'
  --data-urlencode 'require_entry_confirmation=true'
  --data-urlencode 'bullish_exit_above_fast_average_atr_multiple=1'
  --data-urlencode 'bearish_exit_below_fast_average_atr_multiple=1'
  --data-urlencode 'bollinger_period=20'
  --data-urlencode 'bollinger_std_dev=2'
)
curl --fail-with-body --silent --show-error --get "$base_url/backtest/plot" "${params[@]}" \
  --data-urlencode 'lux_algo_symbols=false' \
  --data-urlencode 'lux_algo_symbols1m=false' \
  --data-urlencode 'lux_algo_symbols5m=false' \
  --data-urlencode 'lux_algo_symbols15m=false' \
  --data-urlencode 'lux_algo_symbols1h=true' \
  --data-urlencode 'lux_algo_symbols4h=true' \
  --data-urlencode 'alpha_vantage_symbols=false' \
  --data-urlencode 'indicators=true' \
  --output "${mode}_charts.zip"
curl --fail-with-body --silent --show-error --get "$base_url/backtest/run" "${params[@]}" \
  --output "${mode}_results.json"
printf 'Created %s_charts.zip and %s_results.json\n' "$mode" "$mode"
