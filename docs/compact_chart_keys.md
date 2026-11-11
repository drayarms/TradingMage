# Compact chart key and connected lines

User requested reduced key footprint and placement in available whitespace, and queried whether steps were caused by HA. Steps came from ax.step; raw indicator calculations were already regular OHLC. Replaced step rendering with connected line segments, including DEMA diagnostic. No prices, indicators, trading or timestamps recomputed.

Added small grouped keys, shortened display labels and rendered-ink occupancy placement with strong mutual-overlap penalty. Explicit fast-MA context retained for turning points after review. Two focused tests observed RED then GREEN for non-step draw style and small in-bounds non-overlapping sections. Reviewed full-size default4h and mixed1h HA PNGs. Reviewer checked additional figure sizes/tight_layout; no bounds/collision failures. Final verification: 328 tests passed in 41.664s; both focused key/line tests passed again after the final key-title adjustment. Python compilation passed.
