# Removed zone-specific trading rules

User requested removal of bearish-yellow long-entry prohibition and long forced exits, plus range long exits in bearish stretches and range short exits in bullish stretches. Both yellow classifications/shading and the bullish-yellow short-entry prohibition/short exits remain. Other strategy rules continue to apply. No endpoint or command changes.

Tests: new focused regressions observed RED then GREEN; existing opposite-zone expectations updated to reflect removal. Final validation: 313 tests passed in 45.435s; Python compilation passed. No live orders or authenticated historical backtest.
